require('dotenv').config();
const { logActivity, newCorrelationId } = require('./lib/activityLog');
const { App } = require('@slack/bolt');
const http = require('http');
const Anthropic = require('@anthropic-ai/sdk');
const { createClient } = require('@supabase/supabase-js');
const fs = require('fs');
const cron = require('node-cron');
const { google } = require('googleapis');
const sharp = require('sharp');
const OpenAI = require('openai');
const { Readable } = require('stream');

const slack = new App({
  token: process.env.SLACK_BOT_TOKEN,
  appToken: process.env.SLACK_APP_TOKEN,
  socketMode: true,
});

const anthropic = new Anthropic({ apiKey: process.env.ANTHROPIC_API_KEY });
const openai = new OpenAI({ apiKey: process.env.OPENAI_API_KEY });
const supabase = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_ANON_KEY);
const portalSupabase = createClient(process.env.PORTAL_SUPABASE_URL, process.env.PORTAL_SUPABASE_ANON_KEY);
// REVI (the sales-coaching agent) lives in the `revi` schema of this SAME
// Supabase project. Read-only by convention — every revi.* query in this file
// must go through the REVI DATA ACCESS section so schema changes break loudly
// in exactly one place. Grep ng-revi before renaming anything it reads.
const reviSupabase = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_ANON_KEY, {
  auth: { persistSession: false },
  db: { schema: 'revi' },
});

const { Pool } = require('pg');
const portalPg = process.env.PORTAL_READONLY_DATABASE_URL
  ? new Pool({
      connectionString: process.env.PORTAL_READONLY_DATABASE_URL,
      ssl: { rejectUnauthorized: false },
      max: 3,
      idleTimeoutMillis: 30_000,
    })
  : null;

// Write-scoped portal connection (role max_outcome_writer: INSERT on
// revops_sales_outcomes + UPDATE(status) on revops_prospects, nothing else).
// Separate from portalPg so a bug in any read path can never gain write access.
const portalWriterPg = process.env.PORTAL_WRITER_DATABASE_URL
  ? new Pool({
      connectionString: process.env.PORTAL_WRITER_DATABASE_URL,
      ssl: { rejectUnauthorized: false },
      max: 2,
      idleTimeoutMillis: 30_000,
    })
  : null;

function logLlmFromAnthropicResponse(response, durationMs, correlation_id) {
  if (!response) return;
  logActivity({
    event_type: 'llm_call',
    event_source: 'internal',
    action: 'anthropic.messages.create',
    model: response.model,
    tokens_in: response.usage?.input_tokens,
    tokens_out: response.usage?.output_tokens,
    duration_ms: durationMs,
    correlation_id,
  });
}

// A cron that hangs writes a `started` row and never a terminal one, so it looks
// like nothing at all — not a failure, not a success, just absence. On 2026-08-18
// the Fulfillment EOD Pulse started at 00:00:01 and never returned. No deploy
// restarted the process for the next 22 hours, so it was not killed: it hung. The
// report never posted and nothing said so, and it took the liveness audit to find
// it five days later.
//
// Neither the Anthropic client nor supabase-js has a timeout configured anywhere
// in this file, and supabase-js uses plain fetch, which waits forever on a stalled
// connection. Rather than plumb a timeout through every call site, the watchdog
// goes here — one place, covering all 23 crons.
//
// LIMIT, stated because it matters: Promise.race does not CANCEL the hung job. It
// keeps running and may still hold a connection. What this buys is that the run is
// recorded as an error instead of vanishing, so the audit and the operator find out
// the same day. Real cancellation needs AbortController through every call site.
const CRON_DEFAULT_TIMEOUT_MS = Number(process.env.CRON_DEFAULT_TIMEOUT_MS || 10 * 60 * 1000);

function wrapCronJob(actionName, jobFn, { timeoutMs = CRON_DEFAULT_TIMEOUT_MS } = {}) {
  return async () => {
    const correlation_id = newCorrelationId();
    const started = Date.now();
    let errored = null;
    logActivity({ event_type: 'cron_run', event_source: 'cron', action: actionName, status: 'started', correlation_id });
    let timer;
    try {
      await Promise.race([
        jobFn(correlation_id),
        new Promise((_, reject) => {
          timer = setTimeout(
            () => reject(new Error(`cron '${actionName}' exceeded ${Math.round(timeoutMs / 1000)}s and was abandoned — it may still be running`)),
            timeoutMs,
          );
          // Deliberately NOT unref'd. An unref'd watchdog does not keep the event
          // loop alive, so if the hung job is the only thing pending the process
          // exits before the timer fires — and the run vanishes exactly as before.
          // The timer is always cleared in `finally`, so it holds the loop open
          // only while a job is genuinely in flight, which is when we want it.
        }),
      ]);
    } catch (err) {
      errored = err;
      // Deliberately NOT rethrown. node-cron invokes this at the top of the stack
      // with nothing above it to catch a rejection, and Node 20 exits the process
      // on an unhandled one — so a single failing cron took the whole agent down
      // and left Railway to restart it. There is no unhandledRejection handler in
      // this file to soften that. Adding the watchdog above would have made it
      // worse by throwing more often, so containment lands in the same change.
      // The failure is not swallowed: it is logged here and recorded as an `error`
      // row, which is what the liveness audit reads.
      console.error(`Cron '${actionName}' failed:`, err && err.message ? err.message : err);
    } finally {
      clearTimeout(timer);
      logActivity({
        event_type: 'cron_run',
        event_source: 'cron',
        action: actionName,
        status: errored ? 'error' : 'ok',
        duration_ms: Date.now() - started,
        error_message: errored ? String(errored.message || errored).slice(0, 2000) : null,
        correlation_id,
      });
    }
  };
}

// ─── SYSTEM PROMPT ────────────────────────────────────────────────────────────
const SYSTEM_PROMPT_BASE = `CRITICAL OPERATING RULES — NEVER VIOLATE THESE:

1. When asked about any Slack channel's activity, content, or discussions, you MUST call read_slack_channel immediately. No exceptions. Do not summarize from memory, do not say you lack access, do not explain — just call the tool and report what it returns. If the channel name is provided as a Slack link like <#C09TS6DUTU2|ng-fullfillment-ops>, extract the name after the pipe symbol and use that.

2. When asked about a client, team member, or ongoing situation, call search_knowledge first before answering. Do not rely on conversation history alone for operational context.

3. When Ron shares important context about a client, decision, or process, call save_knowledge to store it permanently using the correct category: client, team, process, decision, or alert.

4. Never use markdown formatting in Slack messages. No ##, no **, no ---, no tables, no bullet points unless the information genuinely requires a list. Write like a colleague sending a message, not a report.

5. When asked to reply or post in a Slack channel, always use draft_channel_post to prepare the message. The draft goes to the person who asked (or escalates to Ron — see escalation criteria in the team-tier section). Never post directly to a channel unless triggered from within that channel.

6. Portal data lookups — applies to EVERY user, not just Ron. When asked for a portal data field the pre-built tools don't cover (client emails, LinkedIn handles, activation call dates, any ad-hoc lookup), do NOT guess table names and do NOT say you lack access. Check the PORTAL DATA MAP below first — if the field has a recipe there, go straight to query_portal_db with it. Otherwise call search_portal_schema, pick the best-matching table from the grouped result, then call query_portal_db with a SELECT statement. HARD RULE: NEVER claim a field or piece of data "doesn't exist", "isn't stored", or "isn't available" until you have (a) consulted the data map, (b) run search_portal_schema, AND (c) attempted at least one query_portal_db SELECT against the most plausible table. An empty schema search proves nothing — business concepts stored as row values (template titles, step names, statuses) only surface by querying the table. Only fall back to list_portal_tables if all of the above return nothing.

---

IDENTITY & ROLE

You are Max, the Project Manager and Personal Assistant for NeuroGrowth Consulting LLC, a boutique LinkedIn growth consultancy run by CEO and Founder Ron Duarte. You operate as an embedded team member, not a generic assistant. You understand the business deeply, know every person on the team by name and function, and are responsible for keeping operations moving, Ron's attention focused on revenue-generating activities, and the team accountable to its commitments.

You communicate naturally. You are not a chatbot. You think, you have context, you have opinions. You respond the way a highly capable chief of staff would — someone who knows everything about the business and speaks plainly.

Your job is to remove operational friction across the whole NeuroGrowth team — status checks, follow-ups, report-pulling, task tracking, drafting. Every person on the team gets more time to do the work only they can do. Ron remains the final decision-maker, but team members can use you directly for operational work in their scope.

---

THE BUSINESS

NeuroGrowth delivers Build & Release: a complete LinkedIn prospecting system (the "LinkedIn Flywheel") built in 14 days and handed off to the client as a fully owned asset. This is not a retainer model. Post-delivery retention tiers:
- OMEGA: 3-month community and support tier
- ROLEX: 6-month Done-With-You coaching tier
- PATEK: 6-month Done-For-You VIP tier

Core promise: 10-30 qualified LinkedIn calls per month with decision-makers. ICP: B2B and B2C coaches, consultants, and premium service providers. Markets: US, Costa Rica, Mexico. Full-service SDR management is no longer offered — legacy accounts are winding down.

---

THE TEAM

Ron Duarte (U05HXGX18H3) — CEO and Founder. Final decision-maker on clients, pricing, offers, hiring.
Josue Duran (U08ABBFNGUW) — Technical Operations Manager (full-time fulfillment). Activation calls, campaign ops, client launch sequencing.
David McKinney (U08ACUHUUP6) — Lead Technology & Automation. Portal, Make.com, Supabase infrastructure.
Valeria (U09Q3BXJ18B) — Fulfillment Operations. Delivery documents, Claude Projects.
Gerald Arias (U0BAAC0KS82) — Fulfillment Operations.
Felipe (U09TNMVML3F) — Technical Campaign Specialist (part-time). Campaign launches, Prosp management.
Oscar M (U0B1S1UMH9P) — Appointment Setter. Books discovery calls.
William B (U0B16P6DQ2F) — Appointment Setter. Books discovery calls.
Sebastian Serrano (U0BFA4SRVQC) — Appointment Setter. Books discovery calls.
Jose Carranza (U0AMTEKDCPN) and Jonathan Madriz (U0APYAE0999) — High-Ticket Closers. They close deals after setting.

---

HOW YOU OPERATE

Task Execution: When Ron assigns a task, confirm you understood it, execute with available tools, and report completion. If blocked or needs Ron's decision, surface that clearly without over-explaining.

Communication Drafting: Draft all routine outgoing messages. Ron reviews client-facing or sales-critical content before it goes out. Write in professional, confident, direct tone for external; efficient and direct for internal.

Standup Accountability (Scheduled Jobs):
- Daily standup (weekdays 9:00 AM Costa Rica): Post in team Slack channel. Ask what each person is working on and if there are blockers. Tag Josue, Valeria, Felipe.
- EOD check (weekdays 5:00 PM Costa Rica): Cross-reference open items against reported completions. Flag anything unresolved.
- Weekly summary (Fridays 4:00 PM Costa Rica): Digest for Ron covering sales closures, delivery status, blockers, Monday priorities.
- Sales call prep (evening before any sales call on calendar): Alert Ron to review prospect brief. If no brief exists, alert Ron directly.

---

LANGUAGE

English and Spanish are active working languages. US market: English. Costa Rica and Mexico clients/team: Spanish. Draft in Spanish when context indicates Spanish-speaking recipient unless Ron specifies otherwise.

---

TONE AND VOICE

Communicate like a sharp, trusted colleague — not a bot reading from a script. Be direct but warm. Match the energy of the conversation. Use natural sentence flow, not bullets and headers, unless structure genuinely helps.

Never start with "Understood.", "Got it.", "Sure!", or any preamble. No sign-offs. No "Let me know if you need anything else." Treat every exchange like two people who know each other and the business.

When you don't know something, say so plainly. When you have an opinion, give it directly. Don't hedge everything into uselessness.

Write in full sentences. Vary sentence length. Sound like a person.

---

KNOWLEDGE MANAGEMENT

Use search_knowledge, save_knowledge, and get_knowledge_category actively — not just when asked, but whenever you encounter information worth remembering.

Categories (use exactly these):
- client — active or former client accounts
- team — team member details, working styles, responsibilities
- process — SOPs, workflows, bottlenecks
- decision — strategic/operational decisions by Ron
- alert — active risks, blockers, urgent items
- intel — market, competitor, delivery trends

Search knowledge before answering any question about a specific client, team member, or ongoing situation.

---

METRICS TO MONITOR

- Weekly close rate on sales calls (target: 22-26%+; alert if two consecutive weeks below 22%)
- Active client count and campaign status (target: all clients with live campaigns within 14 days of signing)
- Monthly revenue collected vs. contracted (AR aging; any balance over 30 days outstanding triggers outreach)
- Make.com automation error status (any scenario failure surfaces within 24 hours)
- Delivery bottleneck count (clients blocked by activation; target: zero blocked for more than 5 business days)

---

System prompt version: April 2026. Maintained in index.js — no external file dependency.`;

// ─── PORTAL DATA MAP ─────────────────────────────────────────────────────────
// Injected into EVERY prompt (Ron + team). Keep tight — this rides every request.
const SYSTEM_PROMPT_DATA_MAP = `

PORTAL DATA MAP — where portal data actually lives (query via query_portal_db):

KEY TABLES
- client_dashboards — one row per client. id (uuid — joins to customer_activities.customer_id), client_name, email, customer_status (phase_0|phase_1|phase_2|phase_3|live|blocked), customer_type, is_active, created_at, stabilization_started_at, linkedin_handler.
- customer_activities — per-client checklist items. customer_id → client_dashboards.id (a few legacy rows point at customer_onboarding.id — check both), template_id → customer_activity_templates.id, status, assigned_to, notes, completed_at (timestamptz — the REAL calendar date the activity was completed).
- customer_activity_templates — id, title, order_index, category. Titles like "Activation Call Completed" are ROW VALUES here, not column names.
- customer_onboarding — intake form: email, first_name, last_name, company, service_tier, payment_status, onboarding_completed_at.
- v_phase0_fulfillment — pre-portal pipeline view: phase0_step, days_in_phase0, terms_accepted_at, booking_calendar_url.
- flywheel_activation_workflow — customer_id, step_name (activation_call_done, activation_recap_done, campaign_configuration_approved, flywheel_ready), completed_at.
- flywheel_ai_onboarding (+ _workflow) — flywheel intake, linkedin_handle, clerk_user_id. WARNING: go_live_at is a PROVISIONING timestamp set before the activation call — it is NOT the campaign launch date.
- revops_appointments / revops_sales_outcomes / revops_prospects / setter_claims / lead_posts — GHL-native sales truth since the 2026-07-23 cutover.
- iclosed_webhook_deliveries / ghl_webhook_deliveries — raw webhook payload history (iClosed rows frozen at cutover).
- In Max's OWN Supabase, NOT the portal (use knowledge/metric tools, not query_portal_db): agent_knowledge, conversations, scheduled_tasks, metric_observations, setter_attributions.

CANONICAL RECIPES
- Activation call date for a client comes from the dashboard CRM, in this order. (1) flywheel_activation_workflow.completed_at where step_name = 'activation_call_done' AND metadata->>'source' = 'ghl_webhook' AND customer_id = the client's client_dashboards.id — the canonical anchor for the 14-day Build and Release window, written automatically when the activation appointment is marked held. Rows WITHOUT that metadata source were ticked by hand and carry a bulk timestamp (the historical backfill stamped May calls with 2026-06-19), so do not read them as the call date. (2) flywheel_ai_onboarding.activation_call_scheduled_for, joined on email (lower(flywheel_ai_onboarding.email) = lower(client_dashboards.email)) — the booking-side date, real and safe to quote. Clients live in the dashboard CRM: never read an activation date, or any other fulfillment state, off a GHL contact field or tag. When someone asks for the date itself, return the date.
- LAST-RESORT FALLBACK when both CRM sources above are null = customer_activities.completed_at joined to customer_activity_templates where templates.title ILIKE '%activation call%' AND customer_activities.customer_id = the client's client_dashboards.id. This is a fulfillment CHECKBOX, not the call: measured across 16 clients it lagged the real call by a median of 6 days and by up to 40. Always say the number is based on the checkbox when you use this fallback, and treat the resulting Day N as optimistic.
- Launch date (campaign go-live) = latest customer_activities.completed_at where the joined template title contains 'campaign qa check' or 'campaign validation' (verified 2026-08-07: these match the team's tracked launch dates; go_live_at does NOT). Time to launch = activation call date → this launch date.
- Stabilization day counts anchor on client_dashboards.stabilization_started_at, not the activation call.

TOOL LIMITATION — search_portal_schema matches TABLE and COLUMN NAMES ONLY. Business concepts stored as row values ("activation call", template titles, step names, statuses) will never match a schema search. An empty schema search means nothing — check this data map and run query_portal_db before concluding anything.

OPS MASTER TRACKER (Google Sheet the ops team maintains by hand — query via get_ops_tracker, open to everyone):
- tab=infrastructure — per-client setup state: Sales Navigator, Prosp license, webhook, Prosp tag, NG dashboard active, plus free-text Notas. Source of truth for "is client X's Prosp/SN/webhook set up".
- tab=change_requests — client change-request log: priority, date, description, owner, updates history, QA, completion, status (Done / In progress / Blocked). Defaults to OPEN items; pass include_done or a client filter for history.
- tab=launch_history — activation date, QA date, launch date, days-to-launch per client, as recorded BY HAND by the ops team. PRECEDENCE: for the activation call date and any day count derived from it, the dashboard CRM (the recipe above) WINS — this sheet is typed by a person, is not written by any automation, and drifts. Use it for what only the ops team records (QA date, launch date, notes) and as a cross-check. If the sheet and the CRM disagree on a client's activation date, report BOTH, say which is which, and treat the CRM as the number — never silently pick one. MEASURED 2026-08-19: they disagree for 14 of 19 clients, median 11 days, max 40, and the sheet is LATER every single time — its Fecha de Activacion matches the portal's activation checkbox, not the call. So the sheet's own 'Dias para Lanzar' understates time-to-launch (mean 15.5d reported vs 25.6d actual across 13 launched clients, and 6 of those 13 look inside the 14-day promise but are not). Never quote 'Dias para Lanzar' as time-to-launch without saying it is measured from the ops sheet's activation tick, not the call.
Client names in the sheet are messy ("Factory - Will", "Sastrería Triana - Carlos Castillo") — the client filter is fuzzy; use a short fragment.

REVI (client-call intelligence, revi schema in Max's own Supabase):
- get_revi_client_context (open to everyone): topic=calls — quicksync + activation-call report summaries per client (what was reviewed, conclusions, risks, health, PDF + recording links), auto-ingested from Fathom. This answers "what was discussed with client X" and "when did X's stabilization topics come up". topic=roster — REVI's client list + status. REVI only sees calls recorded via Fathom on Ron's account.
- get_revi_intelligence (Ron-only, confidentiality not OAuth): coaching teardowns, closer call scores, won/lost deal detail, leadership initiatives.`;

const SYSTEM_PROMPT_RULES = `

CRITICAL BEHAVIOR RULES:

1. ALWAYS CLOSE THE LOOP — This is non-negotiable. Every single action you take — deleting a task, creating a task, sending a message, reading a file, updating Notion, cleaning duplicates, running a cron job, posting to Slack, saving knowledge, anything — must be followed immediately with a explicit completion message. You are never allowed to go silent after saying you are doing something. The confirmation must include all of the following:
   a) What you did (specific action, not vague)
   b) Whether it succeeded or failed (be direct)
   c) The specific outcome (numbers, IDs, names, row counts, links — whatever is measurable)
   d) Next step or what to watch for (if anything is needed)

   WRONG — "Got it, deleting the three duplicates now." [silence]
   WRONG — "Done." [no detail]
   WRONG — "I ran the cleanup." [no outcome]
   RIGHT — "Done. Hard deleted 3 duplicate tasks: Daily Fulfillment Pulse, Weekly Delivery Health Report, Fulfillment Real-Time Alerts. 5 unique tasks remain. IDs removed: 8acbc8b4, 3bc914c6, fe1c6d48."
   RIGHT — "Failed. The delete returned a Supabase RLS error: 'new row violates row-level security policy'. The anon key does not have DELETE permission on scheduled_tasks. Go to Supabase > Authentication > Policies and add a DELETE policy for the anon role."

2. FAILURE IS NOT SILENCE — If something fails, you report it immediately with the exact error. You do not retry silently. You do not say "let me try again" without first telling the user what failed. You surface the error, explain what it means in plain language, and suggest a fix. Then you wait for instruction before retrying.

3. CONFIRMATION IS NOT OPTIONAL EVEN WHEN PUSHED — If Ron asks "can you confirm?" or "did it work?" or "what happened?" that means the confirmation was missing the first time. Do not just say "yes it worked." Give the full specific outcome as required in Rule 1. Asking for confirmation is a signal you failed to close the loop — correct it immediately with full detail.

4. NO MARKDOWN IN SLACK — Never use **bold**, ### headers, or * bullet points in Slack messages. Plain sentences only. Structure with line breaks when genuinely needed, not decoration.

5. NO MID-CHAIN NARRATION — When answering a question requires multiple tool calls (e.g. check GHL, then check knowledge, then check Slack), do ALL of them silently and return ONE final answer. Never narrate between steps. Never say "let me check X" and then go silent. Never say "let me open it" and then stop responding.

   WRONG — "I can see Andres Ch M in GHL. Let me open that conversation." [silence]
   WRONG — "Nothing in sales intelligence yet. Let me check knowledge." [silence]
   WRONG — "Let me pull more history to find him." [silence after 5 minutes]
   RIGHT — Call every relevant tool, compile the result, return a single complete answer: "Andres Chavez — assigned to Oscar M. Last message April 9, no outbound response sent."

   If ALL sources return nothing, say so immediately in one message: "Andres Chavez not found in GHL, sales intelligence, or knowledge base. He may not have been logged yet."

6. THE THREAD IS THE QUESTION — When the message carries a THREAD CONTEXT block, the person is replying inside a Slack thread and that thread is what they are asking about. It outranks the recent conversation history, which is a separate conversation that happened somewhere else. Answer about the thread. If the thread is a list or report you posted earlier and they ask whether it is "still true" or "still pending", re-check the underlying data for the items in THAT thread — do not answer about whatever you were last discussing elsewhere. If the thread does not contain what they are asking about, say so plainly and ask which thread they mean; never quietly substitute the other conversation.

   The rule is: think with tools, speak with results. Never speak while thinking.

   SILENCE IS NEVER ACCEPTABLE — whether the result is a success, a failure, an error, or empty data, Max must always send a final reply. If every tool returns nothing, say so. If a tool errors, report it. If the answer is incomplete, say what was found and what was not. The only wrong answer is no answer.

---

CHANNEL RELEVANCE RULES:

When reading, summarizing, or posting to #ng-fullfillment-ops, you only surface and act on information that is directly relevant to:
- Client delivery status (where is each client in their 14-day build, what phase are they in)
- Onboarding progress and blockers (what is blocking a client from moving forward)
- Missed SLAs or launch risk (clients past Day 7 without progress, past Day 14 without going live)
- Campaign launch readiness (Prosp config, Sales Navigator, sequences built or not)
- Delivery quality flags (issues with docs, sequences, profile optimization)
- Patterns in delivery bottlenecks (same issue appearing across multiple clients)
- Client satisfaction signals that affect delivery (unresponsive clients, scope creep, complaints)
- Team accountability on delivery tasks (who owns what, what is overdue)

You do NOT surface or comment on in #ng-fullfillment-ops:
- General team banter or off-topic messages
- Sales conversations or prospect updates (belongs in sales-goats)
- System or tech discussions unrelated to active client delivery
- Anything that is not directly tied to a client getting their LinkedIn Flywheel built and launched

When reading, summarizing, or posting to #ng-sales-goats, you only surface and act on information that is directly relevant to:
- Appointment setting activity (conversations opened, prospects qualified, calls booked)
- Closing activity (calls taken, deals closed, pipeline status, follow-up needed)
- Prospect quality and pipeline health (how qualified is the book, what are conversion rates)
- Objection patterns (what objections are showing up repeatedly, how they are being handled)
- No-show and follow-up status (who ghosted, who needs re-engagement, FU sequence stage)
- EOD reports from Oscar, William, Sebastian, and Jose (calls booked, pipeline updates, actions needed)
- Sales performance signals (close rate trends, setter-to-closer handoff quality)
- Lead source quality (where are booked calls coming from, which sources convert)

You do NOT surface or comment on in #ng-sales-goats:
- Delivery or fulfillment topics (belongs in fullfillment-ops)
- Tech or system discussions unrelated to sales workflow
- General banter or off-topic messages
- Anything that does not directly affect appointment setting, pipeline, or closing

---

TEAM CHANNEL POSTING RULE — NON-NEGOTIABLE:

#ng-fullfillment-ops and #ng-sales-goats are team-wide channels with human team members reading every message. These are NOT your workspace. They are NOT a place to narrate your process.

NEVER post any of the following to #ng-fullfillment-ops or #ng-sales-goats:
- Status updates about what you are doing ("Drafting the EOD pulse now", "Let me compile this")
- Confirmations that you received a request ("Good, I have everything I need")
- Working commentary ("Pulling the data now", "Give me a moment")
- Error messages or technical failures
- Anything about your own operation, tools, or thinking process
- Draft previews or partial outputs asking for approval
- Meta-commentary of any kind

The ONLY things that go into #ng-fullfillment-ops or #ng-sales-goats are final, complete, polished outputs — delivery reports, EOD summaries, alerts, standup posts. Nothing else. Ever.

If you need to communicate anything about your own process, a failure, a draft for approval, or anything operational about Max himself — post it to #ng-pm-agent or send a DM directly to Ron Duarte (U05HXGX18H3). Those are the only two places for that type of communication.

When a scheduled task fires and posts to a team channel, that post must be the final output. If the data is not available or something fails, do not post a failure message to the team channel — post it to #ng-pm-agent or DM Ron (U05HXGX18H3) instead.

---

GLOBAL REPORT FORMATTING RULE — APPLIES TO EVERY REPORT MAX WRITES:

This rule applies to every report, summary, digest, wrap-up, or structured output Max produces — whether triggered by a scheduled task, a direct request from Ron, or any conversation. No exceptions.

HEADERS AND HEADLINES — must always use backtick format. Wrap every section header and headline in single backticks, like this: backtick SECTION NAME backtick. Do not write headers as plain text or bold text.

Example correct headers: FULFILLMENT EOD PULSE, WINS TODAY, BLOCKERS & AT-RISK — all wrapped in backticks.
Example wrong headers: plain text WINS TODAY, or **WINS TODAY** in bold — never do this.

NAMES — all client names and team member names must be in ALL CAPS throughout every report.

STRUCTURE — each bullet point or statement gets its own line with a blank line after it for readability.

BULLETS — use the bullet character for all lists, never dashes.

NO MARKDOWN — no asterisks, no bold, no italic, no hash headers. Only backtick-wrapped section headers and bullet characters.

This format must be applied automatically to every report Max writes, including ad-hoc summaries, weekly digests, and any structured output produced in conversation.
`;

// ─── RON-ONLY DIRECTIVES ──────────────────────────────────────────────────────
// Injected when the invoking user is Ron. Keeps the Ron-specific voice and the
// "reduce Ron's operational time" framing out of team members' prompts.
const SYSTEM_PROMPT_RON = `

---

RON-SPECIFIC DIRECTIVES

You are talking to Ron Duarte, CEO and final decision-maker. Ron and you have a long-running working relationship — speak plainly, like a trusted colleague.

Your primary directive with Ron is to reduce his operational involvement from ~40-60% of his time on execution to 20% or less. Take everything off his plate that doesn't need his judgment. When he asks for a status, give the actionable summary, not the raw list.

Decision escalation list — things Ron personally needs to decide, not you or a team member:
- A client is expressing dissatisfaction or threatening to cancel
- A sales prospect requires custom pricing outside the standard structure
- A team member raises compensation, contract, or role concerns
- A technical failure affects active client campaigns unresolved in 24 hours
- Any new vendor, platform, or financial commitment above $25

Do NOT escalate for: follow-up timing, calendar scheduling, first-draft copy, routine status checks.

You have full access to his Gmail, Google Calendar, and all personal credentials. Draft replies, schedule calls, and manage his day proactively.`;

// ─── TEAM-TIER DIRECTIVES ─────────────────────────────────────────────────────
// Injected for every non-Ron pilot user. Frames Max as a team assistant with
// Ron reserved for high-priority escalations only.
const SYSTEM_PROMPT_TEAM_TIER = `

---

TEAM-TIER DIRECTIVES

You are Max for the NeuroGrowth team. Team members use you for operational work in their scope — status checks, reports, drafts, reminders, follow-ups, knowledge lookups, task creation. Decisions remain Ron's, but most operational work does not need Ron involved.

Default behavior: when a team member asks you to draft or post something, the approval goes back to THEM. They approve their own drafts. Do not route everything through Ron.

When to escalate a draft to Ron instead of the person who asked:
1. Any outbound client-facing message that makes a commitment (scope, timeline, deliverable, refund, credit).
2. Any public-facing comms (LinkedIn posts, email blasts, newsletters, external announcements).
3. Any message mentioning pricing, contracts, or renewal terms.
4. Any message with reputational exposure (apology to a client, response to a complaint, recovery of a warm lead).
5. Hiring, firing, or compensation-related team comms.

Otherwise the originator approves their own draft. Internal Slack messages, team status updates, reminders, summaries, task notes, personal drafts — all approved by whoever asked for them.

When you escalate, tell the user clearly: "This one needs Ron's call — I'm routing it to him and I'll let you know when he signs off." Never make commitments on Ron's behalf. Never speak for Ron on pricing, scope, hiring, or client-facing promises.

You have access to GHL (live sales source since the 2026-07-23 cutover; iClosed is frozen history), Meta Ads, the portal, Supabase, Slack, Notion, Google Drive/Docs/Sheets, the Ops Master Tracker (get_ops_tracker), and REVI client context (get_revi_client_context — quicksync and activation-call summaries plus the client roster; these are ingested automatically from Fathom recordings). You do NOT have access to Ron's Gmail or Google Calendar (Ron's personal OAuth) or to REVI coaching teardowns, call scores, deal transcripts, leadership meetings, and initiatives (confidential to Ron — NOT an OAuth limitation; never claim REVI itself is OAuth-gated). If asked for any of those, say they are Ron-only, name the reason, and offer the team-accessible alternative.`;

const SYSTEM_PROMPT = SYSTEM_PROMPT_BASE + SYSTEM_PROMPT_DATA_MAP + SYSTEM_PROMPT_RULES + SYSTEM_PROMPT_RON;

const AGENT_CHANNEL         = process.env.AGENT_CHANNEL         || '#ng-pm-agent';
const OPS_CHANNEL           = process.env.OPS_CHANNEL           || '#ng-fullfillment-ops';
const NEW_CLIENT_CHANNEL    = process.env.NEW_CLIENT_CHANNEL    || '#ng-new-client-alerts';
const SALES_CHANNEL         = process.env.SALES_CHANNEL         || '#ng-sales-goats';
const SYSTEMS_CHANNEL       = process.env.SYSTEMS_CHANNEL       || '#ng-app-and-systems-improvents';
const ANNOUNCEMENTS_CHANNEL = process.env.ANNOUNCEMENTS_CHANNEL || '#ng-internal-announcements';

const pendingApprovals = {};

// ─── EMAIL PROXY: stage-1 setter-review drafts (5-min TTL) ───────────────────
// pendingDrafts[setterSlackId] = {
//   kind: 'email_outbound' | 'email_reply',
//   to, cc, subject, body,                       // for email_outbound
//   thread,                                      // email_threads row, for email_reply
//   createdAt,
// }
const pendingDrafts = {};

// ─── EMAIL PROXY: feature flag ────────────────────────────────────────────────
const EMAIL_PROXY_LIVE = String(process.env.EMAIL_PROXY_LIVE || '').toLowerCase() === 'true';

// Roles that skip Ron's Stage-2 approval — setter/closer drafts go out
// directly after Stage-1 "looks good". Ron still gets an audit-trail DM.
const EMAIL_PROXY_AUTOSEND_ROLES = new Set(['setter', 'closer']);
function isAutosendUser(slackId) {
  const role = TEAM_MEMBERS[slackId]?.role;
  return role ? EMAIL_PROXY_AUTOSEND_ROLES.has(role) : false;
}

// ─── SLACK CHANNEL LIST CACHE ─────────────────────────────────────────────────
// conversations.list is rate-limited — cache for 10 minutes instead of calling per-request
let channelListCache = null;
let channelListCachedAt = 0;
const CHANNEL_CACHE_TTL_MS = 10 * 60 * 1000;

async function getCachedChannelList() {
  const now = Date.now();
  if (channelListCache && (now - channelListCachedAt) < CHANNEL_CACHE_TTL_MS) {
    return channelListCache;
  }
  const result = await slack.client.conversations.list({ limit: 200, types: 'public_channel,private_channel,mpim,im' });
  channelListCache = result.channels;
  channelListCachedAt = now;
  return channelListCache;
}

const userRateLimits = {};
function isRateLimited(userId) {
  const now = Date.now();
  const windowMs = 60 * 1000;
  const maxRequests = 10;
  if (!userRateLimits[userId]) userRateLimits[userId] = [];
  userRateLimits[userId] = userRateLimits[userId].filter(t => now - t < windowMs);
  if (userRateLimits[userId].length >= maxRequests) return true;
  userRateLimits[userId].push(now);
  return false;
}

setInterval(() => {
  const now = Date.now();
  for (const userId of Object.keys(userRateLimits)) {
    userRateLimits[userId] = (userRateLimits[userId] || []).filter(t => now - t < 60000);
    if (!userRateLimits[userId].length) delete userRateLimits[userId];
  }
}, 5 * 60 * 1000);

setInterval(() => {
  const now = Date.now();
  for (const userId of Object.keys(pendingApprovals)) {
    if (now - pendingApprovals[userId].createdAt > 30 * 60 * 1000) {
      console.log(`Cleared stale pending approval for user ${userId}`);
      // Notify the originating setter that their draft expired (if it was an email send waiting on Ron).
      const pending = pendingApprovals[userId];
      if (pending.kind === 'email' && pending.requestedBy && pending.requestedBy !== userId) {
        const to = pending.email?.to || 'recipient';
        slack.client.chat.postMessage({
          channel: pending.requestedBy,
          text: `⚠️ Your draft for ${to} expired before approval. Please ping Ron and resend if still needed. Contact Ron/admin if you need help.`,
        }).catch(err => console.error('Expired draft DM failed:', err.message));
        slack.client.chat.postMessage({
          channel: RON_SLACK_ID,
          text: `⚠️ Approval for ${getMemberContext(pending.requestedBy).name}'s email to ${to} expired.`,
        }).catch(() => {});
      }
      delete pendingApprovals[userId];
    }
  }
  for (const setterId of Object.keys(pendingDrafts)) {
    if (now - pendingDrafts[setterId].createdAt > 5 * 60 * 1000) {
      console.log(`Cleared stale pending email draft for setter ${setterId}`);
      delete pendingDrafts[setterId];
    }
  }
}, 5 * 60 * 1000);

const RON_SLACK_ID = 'U05HXGX18H3';

// ─── ACCESS GATE ──────────────────────────────────────────────────────────────
// Roster-based access: anyone in TEAM_MEMBERS can invoke Max wherever Max is
// invited. Non-roster users get a polite bounce. To onboard someone, add them
// to TEAM_MEMBERS below — no other code needs to change.
function isRosterMember(userId) {
  return Boolean(userId && TEAM_MEMBERS[userId]);
}

// Tools restricted to Ron — Gmail and Calendar rely on Ron's personal OAuth.
// Drive/Docs/Sheets stay open to the pilot (read-only against Ron's token).
const RON_ONLY_TOOLS = new Set([
  'get_recent_emails',
  'send_email',
  'get_calendar_events',
  'create_calendar_event',
  'add_calendar_attendees',
  // REVI coaching teardowns + leadership initiatives are Ron-only material.
  'get_revi_intelligence',
  // The weekly recap is a Ron-only DM surface (spend, CAC, revenue).
  'preview_weekly_recap',
]);

// Per-tool refusal reasons. The blocked-tool message is fed back to the model
// as a tool result and gets repeated to the user verbatim — a wrong reason
// here becomes a wrong claim from Max (the old shared OAuth wording had him
// telling the team REVI was OAuth-gated; it never was).
const RON_ONLY_REASONS = {
  get_revi_intelligence: 'REVI coaching teardowns, call scores, deal transcripts, and leadership initiatives are confidential to Ron. For client call summaries (quicksyncs, activation calls) or the client roster, use get_revi_client_context instead — that one is open to the whole team.',
};
const RON_ONLY_DEFAULT_REASON = 'Gmail and Calendar use Ron\'s personal OAuth.';

// ─── TEAM MEMBER REGISTRY ─────────────────────────────────────────────────────
const TEAM_MEMBERS = {
  'U05HXGX18H3': { name: 'Ron',      role: 'ceo',            displayName: 'Ron Duarte NG' },
  'U08ABBFNGUW': { name: 'Josue',    role: 'tech_ops',       displayName: 'Josue Duran NG' },
  'U08ACUHUUP6': { name: 'David',    role: 'tech_lead',      displayName: 'David McKinney NG' },
  'U09Q3BXJ18B': { name: 'Valeria',  role: 'fulfillment',    displayName: 'Valeria Rosales NG' },
  'U09TNMVML3F': { name: 'Felipe',   role: 'campaigns',      displayName: 'Felipe Herrera NG' },
  'U0B1S1UMH9P': { name: 'Oscar',    role: 'setter',         displayName: 'Oscar Neurogrowth' },
  'U0B16P6DQ2F': { name: 'William',  role: 'setter',         displayName: 'William Neurogrowth' },
  'U0BFA4SRVQC': { name: 'Sebastian', role: 'setter',        displayName: 'Sebastian Neurogrowth' },
  'U0BAAC0KS82': { name: 'Gerald',   role: 'fulfillment',    displayName: 'Gerald Arias NG' },
  'U07SMMDMSLQ': { name: 'Tania',    role: 'fulfillment',    displayName: 'Tania NG' },
  'U0AMTEKDCPN': { name: 'Jose',     role: 'closer',         displayName: 'Jose Carranza NG' },
  'U0APYAE0999': { name: 'Jonathan', role: 'closer',         displayName: 'Jonathan Madriz' },
};

const ROLE_PERMISSIONS = {
  ceo: {
    canReadChannels: ['ng-fullfillment-ops','ng-sales-goats','ng-ops-management','ng-new-client-alerts','ng-app-and-systems-improvents','ng-internal-announcements'],
    canPostChannels: ['ng-fullfillment-ops','ng-sales-goats','ng-ops-management','ng-new-client-alerts','ng-app-and-systems-improvents','ng-internal-announcements'],
    canUseEmail: true, canUseCalendar: true, canUseGHL: true,
    canUseDrive: true, canUseNotion: true, canSaveKnowledge: true, fullAccess: true,
  },
  client_success: {
    canReadChannels: ['ng-fullfillment-ops','ng-sales-goats','ng-new-client-alerts','ng-ops-management','ng-app-and-systems-improvents','ng-pm-agent'],
    canPostChannels: ['ng-fullfillment-ops','ng-new-client-alerts'],
    canUseEmail: false, canUseCalendar: false, canUseGHL: true,
    canUseDrive: true, canUseNotion: true, canSaveKnowledge: true, fullAccess: false,
  },
  tech_ops: {
    canReadChannels: ['ng-fullfillment-ops','ng-sales-goats','ng-new-client-alerts','ng-app-and-systems-improvents','ng-ops-management','ng-pm-agent'],
    canPostChannels: ['ng-fullfillment-ops','ng-app-and-systems-improvents'],
    canUseEmail: false, canUseCalendar: false, canUseGHL: true,
    canUseDrive: true, canUseNotion: true, canSaveKnowledge: true, fullAccess: false,
  },
  tech_lead: {
    canReadChannels: ['ng-fullfillment-ops','ng-sales-goats','ng-new-client-alerts','ng-app-and-systems-improvents','ng-ops-management','ng-pm-agent'],
    canPostChannels: ['ng-fullfillment-ops','ng-app-and-systems-improvents'],
    canUseEmail: false, canUseCalendar: false, canUseGHL: true,
    canUseDrive: true, canUseNotion: true, canSaveKnowledge: true, fullAccess: false,
  },
  fulfillment: {
    canReadChannels: ['ng-fullfillment-ops'], canPostChannels: ['ng-fullfillment-ops'],
    canUseEmail: false, canUseCalendar: false, canUseGHL: false,
    canUseDrive: true, canUseNotion: true, canSaveKnowledge: false, fullAccess: false,
  },
  campaigns: {
    canReadChannels: ['ng-fullfillment-ops'], canPostChannels: ['ng-fullfillment-ops'],
    canUseEmail: false, canUseCalendar: false, canUseGHL: false,
    canUseDrive: true, canUseNotion: true, canSaveKnowledge: false, fullAccess: false,
  },
  setter: {
    canReadChannels: ['ng-sales-goats'], canPostChannels: ['ng-sales-goats'],
    canUseEmail: false, canUseCalendar: false, canUseGHL: true,
    canUseDrive: false, canUseNotion: false, canSaveKnowledge: false, fullAccess: false,
  },
  closer: {
    canReadChannels: ['ng-sales-goats'], canPostChannels: ['ng-sales-goats'],
    canUseEmail: false, canUseCalendar: false, canUseGHL: true,
    canUseDrive: false, canUseNotion: false, canSaveKnowledge: false, fullAccess: false,
  },
  ops_management: {
    canReadChannels: ['ng-ops-management','ng-fullfillment-ops','ng-sales-goats','ng-new-client-alerts','ng-app-and-systems-improvents','ng-internal-announcements','ng-pm-agent'],
    canPostChannels: ['ng-ops-management','ng-fullfillment-ops'],
    canUseEmail: false, canUseCalendar: true, canUseGHL: true,
    canUseDrive: true, canUseNotion: true, canSaveKnowledge: true, fullAccess: false,
  },
};

function getMemberContext(userId) {
  return TEAM_MEMBERS[userId] || { name: 'Team Member', role: 'fulfillment', displayName: 'Team Member' };
}
function getMemberPermissions(userId) {
  const member = getMemberContext(userId);
  return ROLE_PERMISSIONS[member.role] || ROLE_PERMISSIONS.fulfillment;
}

function slackIdsByRole(role) {
  return Object.entries(TEAM_MEMBERS)
    .filter(([, m]) => m.role === role)
    .map(([id]) => id);
}

function buildRoleSystemPrompt(userId) {
  const member = getMemberContext(userId);
  const perms  = getMemberPermissions(userId);
  if (userId === RON_SLACK_ID) return SYSTEM_PROMPT;

  const roleContext = {
    client_success: `You are speaking with the Client Success Operations Manager at NeuroGrowth. They are the operational backbone of the business — hybrid Chief of Staff and Client Success role reporting to Ron.

CROSS-ROLE CONTEXT: Fulfillment and sales are not siloed from client success. When they ask about a client, surface both sides — delivery status AND sales history (what was promised on the close call, renewal timing, setter notes). When they ask about a prospect or renewal, pull delivery reputation (on-time launch, blockers, satisfaction signals) since that shapes retention and case-study potential.

PORTAL FOCUS: Lead with their book of business first, then expand. They can ask about any client, prospect, or deal — answer freely. Prioritize client health, onboarding phase, AR status, renewal signals, and any sale-to-delivery handoff gaps.

Their 3 pillars:
- Executive Ops (30%): Draft and manage all contracts and SLAs, maintain contract repo with renewal dates, prepare pre-meeting research packages for Ron, own OKR tracking and sprint completion monitoring, produce weekly 5-min ops summary for Ron.
- Client Success (50%): Primary contact for all non-strategic client comms — respond within 2 hours. Bi-weekly client check-in calls (Ron handles monthly strategic sessions). Track client health scores (target >80/100 average). Monitor early warning signals (reduced responsiveness, declining campaign metrics). Identify upsell and expansion opportunities. Execute case study and testimonial SOP. Coordinate quarterly business reviews with performance data.
- Project and Team Coordination (20%): Own project tracking, coordinate with David on infrastructure, facilitate comms between SDR team and technical team, track action items across team members.

Key KPIs: 100% client retention, >80 health score average, <2hr response time, 90%+ feedback actioned within 1 week, 1 case study per quarter, CEO operational time <20%.

When they ask about a client, give them full health context: engagement level, last interaction, open action items, any risk signals. Help them draft client comms, check-in messages, expansion proposals, escalation summaries, and case study outreach. They cannot access Ron's Gmail, calendar, or GHL.`,

    tech_ops: `You are speaking with Josue, the Technical Operations Manager at NeuroGrowth. He reports to Ron (CEO) and is the single point of accountability for technical campaign excellence across all clients.

CROSS-ROLE CONTEXT: Sales context matters to Josue. When a new client lands in delivery, the close-call promises, price tier, and setter-to-closer notes shape how he scopes the 14-day build. When a client is blocked or at-risk, sales needs to know before the next renewal or case-study ask. Surface both sides freely.

PORTAL FOCUS: Lead with fulfillment pipeline health — phase transitions, launch risk, clients hitting Day 7 or Day 14, SLA status. He can ask about sales, setters, or any client — answer freely, and flag cross-over risks (e.g. a stalled client whose renewal is near).

His role is split:
- 60% Build & Release: Own the complete 14-day launch cycle from client activation through technical deployment. Phase 1 (Days 1-3): client activation & onboarding. Phase 2 (Days 4-10): fulfillment coordination. Phase 3 (Days 11-13): technical QA. Phase 4 (Day 14): launch execution & handoff.
- 40% Full Service / Done-For-You: Monitor and optimize ongoing campaigns for full-service clients. Monday 9AM: 60-min campaign fix session. Fridays: portfolio performance deep dive (GREEN/YELLOW/RED status). Monthly audits every 30-45 days per client.

Key performance targets: 95%+ on-time launch rate within 14-day guarantee, 90%+ SLA compliance across DFY portfolio, keep CEO time on campaign ops under 5 hours/week.

After Day 14, client success becomes primary client contact for satisfaction/admin — Josue remains owner of technical campaign performance.

When Josue asks about a client, pull from knowledge base and fulfillment channel to give him full context: current status, last action taken, what's blocking them, and what the next step is. Be direct and operational — tell him exactly what to do, not a summary. Help him draft channel updates, client comms, campaign fix plans, and escalation messages. He cannot access Ron's email, calendar, or GHL.`,

    tech_lead: `You are speaking with David, the Lead Technology and Automation specialist at NeuroGrowth. He builds and maintains Make.com scenarios, Supabase infrastructure, and the Neurogrowth Portal. Help him with technical questions, systems channel activity, and process documentation.

CROSS-ROLE CONTEXT: System issues rarely stay in the system channel. A Make.com failure can cascade into fulfillment delays, stuck onboarding phases, or missed sales handoffs. When David asks about infrastructure, also surface which active clients or sales workflows are affected downstream.

PORTAL FOCUS: Lead with system health, Make.com scenario activity, portal data integrity, and automation failure signals. He can ask about any client, team, or sales data — answer freely, and flag when a tech issue is touching live client or deal work.

He cannot access Ron's email or calendar.`,

    fulfillment: `You are speaking with ${member.displayName}, a Fulfillment Operations specialist at NeuroGrowth. Their primary role is creating client delivery documents — they run the LinkedIn Flywheel Delivery System (Project 1 and Project 2 pipeline).

How the delivery system works:
- Project 1 (Profile Optimization and Client Intelligence): Takes onboarding form + activation call + LinkedIn PDF as inputs. Runs language gate and activation gate quality checks, then runs 14-step market analysis. Produces 3 docs: Doc 1 (Voice + Calendar) goes to client via WhatsApp, Doc 2 (LinkedIn guide) goes to fulfillment team, Doc 3 (Intelligence bundle) hands off to Project 2.
- Project 2 (Campaign Factory): Takes the intelligence bundle, runs bundle detection and pre-gen summary confirmation, builds 3 sequences (A, B, C — 5 messages each) + voice notes + Sales Navigator D1-D12 + Prosp.ai config. Produces File 1 (internal campaign bible for fulfillment, 7 sections D1-D12) and File 2 (founder-facing campaign overview, plain language).
- Delivery: Doc 1 and File 2 go to founder. Doc 2 and File 1 go to fulfillment team (Felipe).

Client onboarding checklist phases fulfillment owns or coordinates:
- Phase 1: Voice Profile and Content Calendar setup, Video General Overview, Voice Profile Prompt
- Phase 2: Campaign Validation (with Felipe), content calendar and profile steps

Help ${member.name} with delivery doc status, client setup coordination, and fulfillment channel activity. They cannot access Ron's Gmail, calendar, or GHL.`,

    campaigns: `You are speaking with Felipe, the Technical Campaign Specialist at NeuroGrowth. He executes the LinkedIn growth system deliverables that Valeria's docs produce. He works alongside Valeria on fulfillment.

His 3 core areas:
1. LinkedIn Profile Optimization: Uses Success GPT framework to optimize client profiles (headline, banner, about, featured content) and company pages. Target: profile optimized within 48 hours of onboarding.
2. Campaign Building and Deployment: Builds Prosp campaigns using Campaign Factory GPT. Configures sequences (connection requests, soft CTA, hard CTA, nurture — 5 messages each), voice notes, Sales Navigator D1-D12, objection handling SOPs. Target: campaign built and launched within 3-5 hours per client. Benchmarks: 15%+ connection acceptance rate, 8%+ reply rate, 2%+ meeting booking rate.
3. Content Pipeline Management: Uses voice prompt extractor for client voice, ghostwriter agent for content, creates content calendars. Target: 8-12 posts/month + 2-4 long-form pieces per client.

Client onboarding checklist items he owns:
- Phase 1: LinkedIn Profile Optimization, Activation Post (Live), Loom walkthrough, Sales Navigator Coupon
- Phase 2: Campaign Config in Prosp AI, Loom walkthrough Phase 2
Full client setup target: <12 hours start to fully operational.

When Felipe asks about a client, give him their campaign status, profile setup stage, and any performance data from the knowledge base. Help him draft Prosp sequences, objection handlers, content calendars, and campaign SOPs. He cannot access Ron's Gmail, calendar, or GHL.`,

    setter: `You are speaking with ${member.displayName}, an Appointment Setter at NeuroGrowth. They work the B2C LinkedIn outreach pipeline and book discovery calls with qualified prospects.

Daily workflow:
- Works inbound and outbound LinkedIn conversations using the NeuroGrowth setting script
- Qualifies prospects by gathering: niche/service, what they sell, price point, ideal client profile
- Runs the full setting flow: intro → qualification → handle objections → confirm call → send calendar link (https://calendly.com/ron-duarte/linkedin-flywheel) + pre-call material
- Tags prospects in GHL: "Net a Fit" for disqualified, "Send to the Ninjas" for warm transfers
- Day-of-call: sends follow-up message 9-10am, confirms meeting, sends the system overview doc before the call
- Files an EOD report every day summarizing calls booked, pipeline status, and follow-up actions

Key conversation stages:
1. Opening and qualification (gather niche, service, price, ICP)
2. Objection handling (no business → disqualify, bad fit → refer, LinkedIn skeptic → educate)
3. Booking flow: confirm interest → send calendar → confirm day-of → send pre-call doc → get on call
4. Follow-up sequences (FU1 through FU4 + sticker) for non-responders

When asked about a prospect, pull from GHL conversations and knowledge base. Help them draft follow-up messages, objection responses, and booking confirmations in Spanish (they work LATAM). Help them prep their EOD report. They cannot access Ron's Gmail or calendar.`,

    closer: `You are speaking with a High-Ticket Closer at NeuroGrowth. The closers are Jose Carranza (U0AMTEKDCPN) and Jonathan Madriz (U0APYAE0999). They take booked calls from Oscar, William, and Sebastian and close them into paying clients.

His daily responsibilities:
- Build and manage his own sales pipeline from booked calls
- Take discovery and closing calls with prospects set by the setters
- Nurture pipeline: follow up with no-shows and maybes, handle objections, re-engage cold leads
- Collect payments from new clients
- Add new clients into the NeuroGrowth system (Neurogrowth portal and GHL)
- File an EOD report every day: calls taken, deals closed, pipeline updates, follow-ups needed

Key context:
- Prospects arrive pre-qualified by the setters with niche, service, price point, and ICP already gathered
- He works the post-call nurture sequence for no-shows and undecided prospects
- Payment collection and client data entry into GHL is his responsibility on close
- Works with client success on handoff once a client pays

When Jose asks about a prospect or pipeline, pull from GHL conversations and knowledge base. Help him draft follow-up messages, re-engagement scripts, and closing sequences. Help him prep his EOD report. He cannot access Ron's Gmail or calendar.`,

    ops_management: `You are speaking with ${member.displayName}, an Operations Manager at NeuroGrowth. They oversee fulfillment and campaign delivery health across the client portfolio and report to Ron.

CROSS-ROLE CONTEXT: Operations management sits above day-to-day fulfillment and campaigns — they need full visibility into delivery pipeline health, sales→delivery handoff quality, and any signal that a client is at risk. When they ask about a client, give them the full operational picture: phase status, SLA risk, blockers, sales context (close-call promises, price tier), and any cross-team dependencies.

PORTAL FOCUS: Lead with portfolio health — phase transitions, launch risk, SLA compliance, gap detection alerts, fulfillment throughput. They can ask about sales, fulfillment, campaigns, or any client — answer freely and surface cross-team risks (e.g. a stalled fulfillment client whose renewal is near, a sales promise that delivery can't meet).

Their role:
- Oversight of the 14-day launch cycle and DFY portfolio (works alongside Josue on technical campaign excellence)
- Owns operational gap detection and escalation: when something is slipping, they're the one who notices and routes it
- Coordinates between fulfillment (Valeria), campaigns (Felipe), client success, and tech lead (David)
- Surfaces operational issues to Ron with recommended actions, not raw status

When they ask about ops health, pull fulfillment channel activity, recent gap detection alerts, SLA status across active clients, and any cross-team dependencies that are blocking. Help them draft escalation summaries for Ron, ops channel updates, and coordination messages between teams. They can use GHL, Drive, Notion, and calendar; they cannot send email on Ron's behalf.`,
  };

  const baseContext = roleContext[member.role] || roleContext.fulfillment;
  const channelList = perms.canReadChannels.join(', ');
  return `${SYSTEM_PROMPT_BASE}${SYSTEM_PROMPT_DATA_MAP}${SYSTEM_PROMPT_RULES}${SYSTEM_PROMPT_TEAM_TIER}\n\n---\nCURRENT USER CONTEXT:\n${baseContext}\n\nThis user can access these channels: ${channelList}\nAddress this person by their first name: ${member.name}.\nKeep responses focused on their operational scope. Do not share sensitive business financials or information outside their role.`;
}

// ─── SUPABASE: CONVERSATION MEMORY ───────────────────────────────────────────
async function loadHistory(userId, limit = 10) {
  try {
    const { data, error } = await supabase
      .from('conversations')
      .select('role, content')
      .eq('user_id', userId)
      .order('created_at', { ascending: false })
      .limit(limit);
    if (error) throw error;
    return (data || []).reverse();
  } catch (err) {
    console.error('Supabase load error:', err.message);
    return [];
  }
}

async function saveMessage(userId, role, content) {
  try {
    let safeContent = content;
    if (containsSensitiveData(content)) {
      safeContent = content
        .replace(/sk-[A-Za-z0-9]{20,}/g, '[REDACTED-API-KEY]')
        .replace(/xox[bpoas]-[A-Za-z0-9-]{10,}/g, '[REDACTED-SLACK-TOKEN]')
        .replace(/eyJ[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]*/g, '[REDACTED-JWT]')
        .replace(/-----BEGIN[^-]+-----[\s\S]+?-----END[^-]+-----/g, '[REDACTED-KEY]');
      console.warn(`Sensitive data redacted in conversation message for user ${userId}`);
    }
    const { error } = await supabase
      .from('conversations')
      .insert({ user_id: userId, role, content: safeContent.substring(0, 8000) });
    if (error) throw error;
    await pruneConversationHistory(userId);
  } catch (err) {
    console.error('Supabase save error:', err.message);
  }
}

async function pruneConversationHistory(userId, maxRows = 40) {
  try {
    const { count } = await supabase
      .from('conversations')
      .select('*', { count: 'exact', head: true })
      .eq('user_id', userId);
    if (!count || count <= maxRows) return;
    const { data: oldest } = await supabase
      .from('conversations')
      .select('id')
      .eq('user_id', userId)
      .order('created_at', { ascending: true })
      .limit(count - maxRows);
    if (oldest?.length) {
      const ids = oldest.map(r => r.id);
      await supabase.from('conversations').delete().in('id', ids);
      console.log(`Pruned ${ids.length} old messages for user ${userId}`);
    }
  } catch (err) {
    console.error('Conversation prune error:', err.message);
  }
}

// ─── SUPABASE: KNOWLEDGE STORE ────────────────────────────────────────────────
// Visibility model: entries are either 'shared' (visible to all users) or
// 'private' (visible only to the owner recorded in user_id). Pass userId so
// private entries for that user surface alongside shared ones.
function applyKnowledgeVisibility(query, userId) {
  if (!userId) return query.eq('visibility', 'shared');
  return query.or(`visibility.eq.shared,and(visibility.eq.private,user_id.eq.${userId})`);
}

async function searchKnowledge(query, category = null, userId = null) {
  try {
    const safeQuery = (query || '').replace(/[%_\\]/g, '\\$&').substring(0, 200);
    let q = supabase
      .from('agent_knowledge')
      .select('category, key, value, visibility, user_id, updated_at')
      .ilike('value', `%${safeQuery}%`)
      .order('updated_at', { ascending: false })
      .limit(8);
    if (category) q = q.eq('category', category);
    q = applyKnowledgeVisibility(q, userId);
    const { data, error } = await q;
    if (error) throw error;
    if (!data || !data.length) return `No knowledge found for: ${query}`;
    return data.map(r => {
      const tag = r.visibility === 'private' ? ' (private)' : '';
      return `[${r.category}] ${r.key}${tag}: ${r.value} (updated ${new Date(r.updated_at).toLocaleDateString()})`;
    }).join('\n');
  } catch (err) {
    return `Knowledge search error: ${err.message}`;
  }
}

const SENSITIVE_PATTERNS = [
  /password/i, /passwd/i, /secret/i, /api.?key/i, /access.?token/i,
  /private.?key/i, /credentials/i, /auth.?token/i, /bearer/i,
  /eyJ[A-Za-z0-9_-]{10,}/,
  /sk-[A-Za-z0-9]{20,}/,
  /xox[bpoas]-[A-Za-z0-9-]{10,}/,
  /-----BEGIN/,
  /[0-9]{16}/,
  /\d{3}-\d{2}-\d{4}/,
];

function containsSensitiveData(text) {
  if (!text) return false;
  return SENSITIVE_PATTERNS.some(pattern => pattern.test(text));
}

async function upsertKnowledge(category, key, value, source = 'agent', userId = null, visibility = 'shared') {
  try {
    if (containsSensitiveData(value) || containsSensitiveData(key)) {
      console.warn(`Knowledge save blocked — sensitive data detected in [${category}] ${key}`);
      return `Knowledge save skipped — sensitive data detected. This information was not stored.`;
    }
    const safeVisibility = visibility === 'private' ? 'private' : 'shared';
    const { error } = await supabase
      .from('agent_knowledge')
      .upsert(
        {
          category,
          key,
          value: value.substring(0, 2000),
          source,
          user_id: userId || RON_SLACK_ID,
          visibility: safeVisibility,
          updated_at: new Date().toISOString(),
        },
        { onConflict: 'category,key' }
      );
    if (error) throw error;
    const tag = safeVisibility === 'private' ? ' (private)' : '';
    return `Knowledge saved${tag}: [${category}] ${key}`;
  } catch (err) {
    return `Knowledge save error: ${err.message}`;
  }
}

// ── Report feedback learning loop ────────────────────────────────────────────

// When a team member @mentions Max in a thread on a Max-posted report, extract
// the lesson from their feedback and store it so future reports apply the fix.
async function extractAndSaveReportLesson(originalReport, feedbackText, channelName, userId, correlationId) {
  try {
    const prompt = `A team member gave feedback on a Max report posted in #${channelName}.\n\nOriginal report:\n${originalReport.substring(0, 1500)}\n\nFeedback:\n${feedbackText}\n\nExtract the lesson in 2-3 sentences: (1) what was wrong or inaccurate in the report, (2) what Max should do differently in future reports for this channel. Be specific and actionable. No preamble.`;
    const res = await anthropic.messages.create({
      model: 'claude-sonnet-4-6',
      max_tokens: 200,
      messages: [{ role: 'user', content: prompt }],
    });
    const lesson = res.content.filter(b => b.type === 'text').map(b => b.text).join('').trim();
    if (!lesson) return null;
    const reportId = inferReportId(originalReport, channelName);
    const key = `report_lesson:${reportId}:${new Date().toISOString().slice(0, 10)}`;
    await upsertKnowledge('process', key, lesson, 'report-feedback', userId, 'shared');
    console.log(`Report lesson saved for #${channelName}: ${lesson.substring(0, 100)}`);
    return lesson;
  } catch (err) {
    console.error('extractAndSaveReportLesson error:', err.message);
    return null;
  }
}

// Map a report's root message text to a stable report ID so lessons are scoped
// by report type rather than just channel (handles DM reports too).
function inferReportId(messageText, fallbackChannel) {
  const t = (messageText || '').toLowerCase();
  if (t.includes('setter brief'))                                        return 'sales-standup-setter';
  if (t.includes('closer brief'))                                        return 'sales-standup-closer';
  if (t.includes('weekly sales') && t.includes('marketing recap'))       return 'weekly-sales-marketing-recap';
  if (t.includes('monday delivery gap report') || t.includes('gap report')) return 'gap-detection';
  if (t.includes('fulfillment standup') || t.includes('delivery standup')) return 'fulfillment-standup';
  if (t.includes('eod pulse') || t.includes('end of day pulse'))         return 'fulfillment-eod';
  if (t.includes('week in review') || t.includes('friday delivery'))     return 'friday-delivery-wrap';
  if (t.includes('anomaly') || t.includes('drifted') || t.includes('σ')) return 'anomaly-alert';
  if (t.includes('still missing an iclosed outcome') || t.includes('still missing an outcome in ghl') || t.includes('outcome not logged')) return 'unlogged-outcome-reminder';
  return fallbackChannel || 'general-report';
}

// Retrieve the last N lessons for a report (last 90 days) to prepend to reports.
async function getReportLessons(reportId) {
  try {
    const cutoff = new Date(Date.now() - 90 * 24 * 60 * 60 * 1000).toISOString();
    const { data } = await supabase
      .from('agent_knowledge')
      .select('value, updated_at')
      .eq('category', 'process')
      .eq('source', 'report-feedback')
      .ilike('key', `report_lesson:${reportId}:%`)
      .gte('updated_at', cutoff)
      .order('updated_at', { ascending: false })
      .limit(5);
    return data || [];
  } catch (err) {
    console.error('getReportLessons error:', err.message);
    return [];
  }
}

// ── Interactive correction learning ──────────────────────────────────────────
// Cheap two-stage correction detector for DMs and non-report threads: regex
// gate first, then Haiku confirms it's a real correction and extracts the
// lesson. Fire-and-forget at call sites — never blocks the reply. Saved
// lessons flow back into every interactive chat via getGlobalLessons().
const CORRECTION_HINT_RE = /\b(wrong|incorrect|not (?:true|right|correct)|that'?s (?:not|false)|correction|you (?:said|claimed|told)|does exist|we do (?:have|store|track)|s[ií] (?:existe|hay|tenemos)|est[áa] mal|incorrecto|no es (?:cierto|correcto|as[ií]))\b/i;

async function detectAndSaveCorrection(userText, priorAssistantText, userId) {
  try {
    if (!userText || !priorAssistantText) return null;
    if (!CORRECTION_HINT_RE.test(userText)) return null;
    const prompt = `Max (an ops agent) previously said:\n${priorAssistantText.slice(0, 1200)}\n\nThe user replied:\n${userText.slice(0, 800)}\n\nIs the user CORRECTING a factual claim or behavior of Max's (not just disagreeing, negotiating, or changing topic)? If yes respond JSON: {"topic":"<2-4 word kebab-case topic>","lesson":"<2-3 sentences: what Max got wrong and what to do instead>"}. If no: {"topic":null}. JSON only, no markdown fences.`;
    const res = await anthropic.messages.create({
      model: 'claude-haiku-4-5-20251001',
      max_tokens: 200,
      messages: [{ role: 'user', content: prompt }],
    });
    const raw = res.content.filter(b => b.type === 'text').map(b => b.text).join('').trim();
    const parsed = JSON.parse(raw.replace(/^```(?:json)?\s*|\s*```$/g, ''));
    if (!parsed?.topic) return null;
    const key = `correction:${parsed.topic}:${new Date().toISOString().slice(0, 10)}`;
    await upsertKnowledge('process', key, parsed.lesson, 'correction', userId, 'shared');
    console.log(`Correction lesson saved: ${key}`);
    return parsed.lesson;
  } catch (err) {
    console.error('detectAndSaveCorrection error:', err.message);
    return null;
  }
}

// Global lessons for INTERACTIVE chats (scheduled reports already inject their
// own via getReportLessons). Shared-visibility only — private notes must not
// leak into other users' prompts. Hard-capped: rides every request.
async function getGlobalLessons() {
  try {
    const cutoff = new Date(Date.now() - 90 * 24 * 60 * 60 * 1000).toISOString();
    const { data } = await supabase
      .from('agent_knowledge')
      .select('value, updated_at')
      .eq('category', 'process')
      .eq('visibility', 'shared')
      .in('source', ['report-feedback', 'correction'])
      .gte('updated_at', cutoff)
      .order('updated_at', { ascending: false })
      .limit(10);
    return data || [];
  } catch (err) {
    console.error('getGlobalLessons error:', err.message);
    return [];
  }
}

// Extract client-level context from any thread where Max is tagged.
// Uses Haiku (cheap) — runs on every thread mention.
async function extractClientContext(threadMessages, mentionText, channelName, userId) {
  try {
    const threadText = threadMessages.map(m => m.text || '').join('\n').substring(0, 2000);
    const prompt = `A team member tagged Max in a Slack thread in #${channelName}.\n\nThread:\n${threadText}\n\nTag: ${mentionText}\n\nDoes this thread contain a specific update about a named client? If yes, respond with JSON: {"client": "<client name>", "context": "<1-2 sentence summary of the update, blocker, status change, or action item>"}. If no specific client is mentioned, respond with: {"client": null}. No preamble, JSON only.`;
    const res = await anthropic.messages.create({
      model: 'claude-haiku-4-5-20251001',
      max_tokens: 150,
      messages: [{ role: 'user', content: prompt }],
    });
    const raw = res.content.filter(b => b.type === 'text').map(b => b.text).join('').trim();
    const parsed = JSON.parse(raw);
    if (!parsed.client || !parsed.context) return null;
    return parsed;
  } catch (_) { return null; }
}

// Retrieve recent knowledge for a specific client (last N days) from agent_knowledge.
// Matches entries saved by both nightly learning and thread-context extraction.
async function getClientContext(clientName, days = 30) {
  try {
    const cutoff = new Date(Date.now() - days * 24 * 60 * 60 * 1000).toISOString();
    const slug = clientName.toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/(^-|-$)/g, '');
    const { data } = await supabase
      .from('agent_knowledge')
      .select('value, updated_at')
      .eq('category', 'client')
      .ilike('key', `client:${slug}:%`)
      .gte('updated_at', cutoff)
      .order('updated_at', { ascending: false })
      .limit(5);
    return data || [];
  } catch (err) {
    console.error('getClientContext error:', err.message);
    return [];
  }
}

// ── Standup delta helpers ─────────────────────────────────────────────────────

async function saveStandupSnapshot(role, snapshot) {
  const key = `standup:${role}:${new Date().toISOString().slice(0, 10)}`;
  await upsertKnowledge('process', key, JSON.stringify(snapshot), 'fulfillment-standup');
}

async function getYesterdayStandupSnapshot(role) {
  try {
    const yesterday = new Date(Date.now() - 24 * 60 * 60 * 1000).toISOString().slice(0, 10);
    const { data } = await supabase
      .from('agent_knowledge')
      .select('value')
      .eq('category', 'process')
      .eq('key', `standup:${role}:${yesterday}`)
      .single();
    return data?.value ? JSON.parse(data.value) : null;
  } catch (_) { return null; }
}

// Returns { new, resolved, unchanged } between two name arrays
function diffStandupList(todayNames = [], yesterdayNames = []) {
  const prev = new Set(yesterdayNames || []);
  const curr = new Set(todayNames);
  return {
    new:       todayNames.filter(n => !prev.has(n)),
    resolved:  (yesterdayNames || []).filter(n => !curr.has(n)),
    unchanged: todayNames.filter(n => prev.has(n)),
  };
}

// Render a delta section — full detail for new, count for unchanged, celebrate resolved
function renderDelta(label, newItems, resolvedItems, unchangedItems, renderItem) {
  const lines = [];
  if (newItems.length) {
    lines.push(`🆕 *${label} — new/changed (${newItems.length}):*`);
    newItems.forEach(i => lines.push(`• ${renderItem(i)}`));
    lines.push('');
  }
  if (resolvedItems.length) {
    lines.push(`✅ *Resolved since yesterday:* ${resolvedItems.join(', ')}`);
    lines.push('');
  }
  if (unchangedItems.length && !newItems.length && !resolvedItems.length) {
    lines.push(`📋 *${label}:* ${unchangedItems.length} client${unchangedItems.length > 1 ? 's' : ''} — same as yesterday, no new flags`);
    lines.push('');
  } else if (unchangedItems.length) {
    lines.push(`📋 *${unchangedItems.length} unchanged* — holding steady`);
    lines.push('');
  }
  return lines;
}

async function getAllKnowledgeByCategory(category, userId = null) {
  try {
    let q = supabase
      .from('agent_knowledge')
      .select('key, value, visibility, updated_at')
      .eq('category', category)
      .order('updated_at', { ascending: false })
      .limit(20);
    q = applyKnowledgeVisibility(q, userId);
    const { data, error } = await q;
    if (error) throw error;
    if (!data || !data.length) return `No knowledge in category: ${category}`;
    return data.map(r => {
      const tag = r.visibility === 'private' ? ' (private)' : '';
      return `${r.key}${tag}: ${r.value}`;
    }).join('\n');
  } catch (err) {
    return `Knowledge fetch error: ${err.message}`;
  }
}

// ─── DYNAMIC CRON SCHEDULER ───────────────────────────────────────────────────
const activeDynamicCrons = {};

async function loadAndRegisterDynamicCrons() {
  try {
    const { data: tasks, error } = await supabase
      .from('scheduled_tasks')
      .select('*')
      .eq('active', true);
    if (error) throw error;
    if (!tasks || !tasks.length) { console.log('No dynamic cron tasks found.'); return; }
    const seen = {};
    const dedupedTasks = [];
    for (const task of tasks) {
      const key = task.name.toLowerCase().trim();
      if (!seen[key]) { seen[key] = true; dedupedTasks.push(task); }
      else { console.log(`Skipping duplicate cron task: "${task.name}" (${task.id})`); }
    }
    for (const task of dedupedTasks) { registerDynamicCron(task); }
    console.log(`Loaded ${dedupedTasks.length} dynamic cron task(s).`);
  } catch (err) {
    console.error('Dynamic cron load error:', err.message);
  }
}

// Team channels that require Ron's approval before posting
const APPROVAL_REQUIRED_CHANNELS = [
  '#ng-fullfillment-ops',           'ng-fullfillment-ops',
  '#ng-sales-goats',                'ng-sales-goats',
  '#ng-new-client-alerts',          'ng-new-client-alerts',
  '#ng-internal-announcements',     'ng-internal-announcements',
  '#ng-ops-management',             'ng-ops-management',
  '#ng-app-and-systems-improvents', 'ng-app-and-systems-improvents',
];

function requiresApproval(channel) {
  const ch = (channel || '').toLowerCase().replace('#', '');
  return APPROVAL_REQUIRED_CHANNELS.some(c => c.replace('#', '') === ch);
}

// APPROVAL_NEEDED|<channelName>|<escalate>|<originUserId>|<reason>|<message...>
// draft_channel_post returns this sentinel as its tool result; scheduled reports
// post directly, so any reply still carrying it must be reduced to its message —
// every callClaude reply in the cron handler (first attempt AND re-prompt) goes
// through this before validation, or the sentinel posts verbatim to the channel.
function stripApprovalSentinel(text) {
  if (!text || !text.startsWith('APPROVAL_NEEDED|')) return text;
  return text.split('|').slice(5).join('|').trim();
}

// ─── PREAMBLE GUARD ──────────────────────────────────────────────────────────
// A scheduled task's reply is posted to the channel VERBATIM, so any narration
// the model writes before the deliverable is published to the team. On
// 2026-08-18 the nightly closer nudge posted as:
//   "6 calls today. Alberto García is already marked no-show… The remaining 5
//    still show as scheduled.\n\nHere is the final reminder text:\n\n---\n\n
//    <!subteam^…> Quick reminder before you sign off — …"
// The prompt already said "return ONLY the final reminder text"; the model
// complied and then framed it anyway. Prose instructions are probabilistic, so
// the fix is machine-checked: a task whose deliverable has a known FIRST token
// declares it here, and everything before that token is cut.
//
// Whitelist by design (the TASK_HEADERS lesson at :1421): "starts with X" has
// one shape, "does not contain any of these framing phrases" has infinite gaps.
const TASK_LEAD_ANCHOR = {
  'Daily Closer Outcome Reminder': '<!subteam^',
  'Daily Sales Call Roster':       '<!subteam^',
};

// Cut anything before the anchor. Returns the text unchanged when the task
// declares no anchor, or when the anchor is absent — an absent anchor is a
// different failure (the reply isn't the deliverable at all) and belongs to
// validateFinalReport, which re-prompts for it.
function trimToLeadAnchor(text, taskName) {
  const anchor = TASK_LEAD_ANCHOR[taskName];
  if (!anchor || !text) return text;
  const i = text.indexOf(anchor);
  if (i <= 0) return text; // 0 = already clean, -1 = absent
  return text.slice(i).trim();
}

function registerDynamicCron(task) {
  try {
    if (activeDynamicCrons[task.id]) { activeDynamicCrons[task.id].stop(); }
    const job = cron.schedule(task.cron_expression, async () => {
      const correlation_id = newCorrelationId();
      const started = Date.now();
      const cronAction = `dynamic_cron:${task.name}`;
      let errored = null;
      let lastErr = null;
      let validationFailure = null;
      logActivity({ event_type: 'cron_run', event_source: 'cron', action: cronAction, status: 'started', correlation_id, metadata: { task_id: task.id } });
      try {
      console.log(`Running dynamic cron: ${task.name}`);

      // Inject live email + calendar context into scheduled report prompts.
      // Ron's inbox is confidential (bank/billing notices land there) — email
      // context is ONLY injected when the report is delivered to Ron directly,
      // never into a prompt whose output posts to a team-visible channel.
      const taskChannel = (task.channel || AGENT_CHANNEL).replace(/^#/, '');
      const deliversToRonOnly = taskChannel === RON_SLACK_ID;
      let liveContext = '';
      try {
        const todayEvents = await getCalendarEvents(0, 1);
        if (todayEvents && !todayEvents.includes('error') && !todayEvents.includes('No events')) {
          liveContext += `\n\nTODAY'S CALENDAR (${_crDayLabel(0)} CR):\n${todayEvents}`;
        }
        const tomorrowEvents = await getCalendarEvents(1, 1);
        if (tomorrowEvents && !tomorrowEvents.includes('error') && !tomorrowEvents.includes('No events')) {
          liveContext += `\n\nTOMORROW'S CALENDAR (${_crDayLabel(1)} CR):\n${tomorrowEvents}`;
        }
        if (deliversToRonOnly) {
          const emails = await getRecentEmails();
          if (emails && !emails.includes('error')) {
            liveContext += `\n\nRECENT EMAILS (unread):\n${emails}`;
          }
        }
      } catch (e) { console.error('Live context fetch error for scheduled task:', e.message); }

      // Inject any lessons learned from team feedback on previous reports for this channel
      const taskLessons = await getReportLessons(taskChannel);
      const lessonContext = taskLessons.length
        ? `\n\nPREVIOUS FEEDBACK FROM TEAM (apply these corrections to this report):\n${taskLessons.map(l => `• ${l.value}`).join('\n')}`
        : '';
      const { data: recentClientCtx } = await supabase
        .from('agent_knowledge')
        .select('key, value')
        .eq('category', 'client')
        .gte('updated_at', new Date(Date.now() - 7 * 24 * 60 * 60 * 1000).toISOString())
        .order('updated_at', { ascending: false })
        .limit(15);
      const clientCtxBlock = recentClientCtx?.length
        ? `\n\nRECENT CLIENT UPDATES FROM TEAM (last 7 days — apply where relevant):\n${recentClientCtx.map(r => `• ${(r.key.split(':')[1] || r.key).replace(/-/g, ' ')}: ${r.value}`).join('\n')}`
        : '';
      // Task-specific pre-computed data blocks (GHL-first, etc).
      // For the Weekly Sales Pod Leaderboard (task name: "Weekly Closer Comparison"),
      // inject both closer and setter stats so the report renders TOP CLOSERS + TOP SETTERS
      // without relying on self-reported EOD as primary truth.
      let taskDataBlock = '';
      let weeklyCloserStats = null; // used downstream for dynamic header validation
      let weeklySetterStats = null;
      // Closer leaderboard (Mondays) — closers only since 2026-07-26; setters
      // moved to their own "Setter Leaderboard" task (Tue + Sat).
      if (task.name === 'Weekly Closer Comparison') {
        try {
          const now = new Date();
          // Last 7 days, ending now
          const weekEnd   = now;
          const weekStart = new Date(now.getTime() - 7 * 24 * 60 * 60 * 1000);
          weeklyCloserStats = await getCloserWeeklyStats(weekStart.toISOString(), weekEnd.toISOString());
          const closerBlock = formatCloserWeeklyStatsBlock(weeklyCloserStats, weekStart.toISOString(), weekEnd.toISOString());
          taskDataBlock = `\n\n---\nPRECOMPUTED DATA (use these numbers as the primary truth source; GHL-native appointment + outcome data is truth for calls/show rate/sold/revenue; EOD self-reports were retired at the GHL cutover 2026-07-23):\n\n${closerBlock}`;
        } catch (statsErr) {
          console.error('Weekly closer stats failed:', statsErr.message);
          taskDataBlock = `\n\n---\nNOTE: Failed to pre-compute closer stats (${statsErr.message}). Fall back to your usual data tools.`;
        }
      }

      // Setter leaderboard (Tue + Sat) — MONTH-TO-DATE, setters only. CR-anchored:
      // resets on the 1st of each month (Ron 2026-08-04 — this is the monthly
      // top-performer board, not a rolling week).
      // Meshed intel: GHL bookings/outcomes (truth) + setter_claims (#ng-sales-goats
      // ✋ flow) + REVI call scores (lead quality of what each setter feeds closers)
      // + GHL conversation hygiene. Each enrichment is non-fatal — the leaderboard
      // must still post if REVI or the GHL API is unreachable.
      if (task.name === 'Setter Leaderboard') {
        try {
          const now = new Date();
          const todayCR   = now.toLocaleDateString('en-CA', { timeZone: 'America/Costa_Rica' });
          const weekEnd   = now;
          const weekStart = new Date(`${todayCR.slice(0, 8)}01T06:00:00.000Z`); // CR midnight on the 1st (UTC-6, no DST)
          weeklySetterStats = await getSetterWeeklyStats(weekStart.toISOString(), weekEnd.toISOString());
          const setterBlock = formatSetterWeeklyStatsBlock(weeklySetterStats, weekStart.toISOString(), weekEnd.toISOString());

          // REVI lead quality — scored calls among this week's setter-booked appts.
          let reviBlock = '';
          try {
            const { data: wkAppts } = await portalSupabase
              .from('revops_appointments')
              .select('setter_id, prospect:prospect_id ( email )')
              .not('setter_id', 'is', null)
              .gte('scheduled_start', weekStart.toISOString())
              .lte('scheduled_start', weekEnd.toISOString())
              .limit(150); // sized for a full month-to-date window, not 7 days
            const bySetter = {};
            const seenEmails = new Set(); // a rebooked prospect is one lead, one REVI lookup
            for (const a of (wkAppts || [])) {
              const email = a.prospect?.email;
              if (!email || seenEmails.has(email)) continue;
              seenEmails.add(email);
              const scored = await reviFindCallsByProspect(email, 1);
              const sc = scored[0];
              if (!sc || sc.overall_score == null) continue;
              const name = resolveSalesMember(a.setter_id);
              if (!bySetter[name]) bySetter[name] = { scores: [], signals: [] };
              bySetter[name].scores.push(sc.overall_score);
              const sig = sc.prospect_signals || {};
              if (sig.buying_signal_strength) bySetter[name].signals.push(sig.buying_signal_strength);
            }
            const reviLines = Object.entries(bySetter).map(([name, d]) => {
              const avg = Math.round(d.scores.reduce((a, b) => a + b, 0) / d.scores.length);
              const sigStr = d.signals.length ? ` | buying signals: ${d.signals.join(', ')}` : '';
              return `  ${name}: ${d.scores.length} scored call(s), avg call score ${avg}${sigStr}`;
            });
            if (reviLines.length) reviBlock = `\n\nREVI LEAD QUALITY (call scores for prospects each setter booked — proxy for lead quality fed to closers):\n${reviLines.join('\n')}`;
          } catch (reviErr) {
            console.warn('Setter leaderboard REVI enrichment failed:', reviErr.message);
          }

          // GHL conversation hygiene — unread / stale (3d+) convos per assigned setter.
          let hygieneBlock = '';
          try {
            const locationId = process.env.GHL_LOCATION_ID;
            const apiKey     = process.env.GHL_API_KEY;
            const res  = await fetch(`https://services.leadconnectorhq.com/conversations/search?locationId=${locationId}&limit=100`, { headers: { 'Authorization': `Bearer ${apiKey}`, 'Version': '2021-07-28' } });
            const data = await res.json();
            const dayMs = 24 * 60 * 60 * 1000;
            const agg = {};
            for (const c of (data.conversations || [])) {
              const assigned = resolveSalesMember(c.assignedTo || c.userId || '');
              if (!weeklySetterStats[assigned]) continue; // only setters on this leaderboard
              if (!agg[assigned]) agg[assigned] = { unread: 0, stale: 0 };
              if (c.unreadCount > 0) agg[assigned].unread += 1;
              if ((Date.now() - c.lastMessageDate) / dayMs >= 3) agg[assigned].stale += 1;
            }
            const hygLines = Object.entries(agg).map(([name, d]) => `  ${name}: ${d.unread} unread convo(s), ${d.stale} stale (3d+ no touch)`);
            if (hygLines.length) hygieneBlock = `\n\nGHL FOLLOW-UP HYGIENE (assigned conversations, latest 100):\n${hygLines.join('\n')}`;
          } catch (hygErr) {
            console.warn('Setter leaderboard hygiene enrichment failed:', hygErr.message);
          }

          taskDataBlock = `\n\n---\nPRECOMPUTED DATA (use these numbers as the primary truth source; GHL-native setter attribution + outcomes are truth for calls booked/show rate/qualified-attended-calls; leads claimed + claimed→booked cross-check come from setter_claims, the ✋ flow in #ng-sales-goats; EOD self-reports were retired at the GHL cutover 2026-07-23):\n\n${setterBlock}${reviBlock}${hygieneBlock}`;
        } catch (statsErr) {
          console.error('Setter leaderboard stats failed:', statsErr.message);
          taskDataBlock = `\n\n---\nNOTE: Failed to pre-compute setter stats (${statsErr.message}). Fall back to your usual data tools.`;
        }
      }

      // Fulfillment EOD Pulse — inject filtered portal alerts (no aged blockers)
      // plus a fulfillment-domain anomaly block. Blockers and anomalies render as
      // separate report sections (BLOCKERS vs ANOMALIES) — see the Supabase-stored
      // task prompt for the section split.
      if (task.name === 'Fulfillment EOD Pulse') {
        try {
          const dailyAlerts = await getPortalAlerts({ mode: 'daily' });
          const fulfillmentMetrics = METRIC_REGISTRY.filter(m => m.domain === 'fulfillment');
          const snapshots = await Promise.all(fulfillmentMetrics.map(m => detectAnomaly(m.name).catch(() => null)));
          const triggered = snapshots.filter(s => s && s.triggered);
          let anomalyBlock;
          if (!triggered.length) {
            anomalyBlock = 'No fulfillment anomalies triggered in the last 24h.';
          } else {
            const lines = await Promise.all(triggered.map(async s => {
              const labelObj = METRIC_REGISTRY.find(m => m.name === s.metric);
              const label = labelObj ? labelObj.label : s.metric;
              const direction = s.z > 0 ? 'above' : 'below';
              const narration = await narrateAnomaly(s);
              const base = `• ${label} = ${s.value} (${Math.abs(s.z).toFixed(1)}σ ${direction} ${s.sampleSize}-sample mean ${s.mean.toFixed(2)})`;
              return narration ? `${base} — ${narration}` : base;
            }));
            anomalyBlock = `PRECOMPUTED FULFILLMENT ANOMALIES (last 24h, ≥${ANOMALY_THRESHOLD_SIGMA}σ from baseline):\n${lines.join('\n')}`;
          }
          taskDataBlock = `\n\n---\nFRESH BLOCKERS ONLY (aged + fully blocked clients are deliberately hidden from the daily — they belong in the Friday Delivery Wrap-Up):\n${dailyAlerts}\n\n---\n${anomalyBlock}`;
        } catch (fErr) {
          console.error('Fulfillment EOD precompute failed:', fErr.message);
          taskDataBlock = `\n\n---\nNOTE: Failed to pre-compute fulfillment data block (${fErr.message}). Use your usual data tools.`;
        }
      }

      // Sales tasks get REVI coaching context appended so scheduled reports can
      // explain the numbers (call quality, objections, initiative movement) —
      // not just state them. Non-fatal; report runs REVI-blind on error.
      if (task.name === 'Sales EOD Report' || task.name === 'Weekly Closer Comparison') {
        try {
          const reviCtx = await reviBuildDailyDigest(task.name === 'Weekly Closer Comparison' ? 7 * 24 : 26);
          if (reviCtx) {
            taskDataBlock += `\n\n---\nREVI COACHING CONTEXT (read-only intelligence from the sales-coach agent — weave in ONLY where it explains the sales numbers, e.g. a low show/close rate alongside weak buying signals or a recurring objection. Do not turn this report into a coaching report, and do not rank closers by coaching score):\n${reviCtx}`;
          }
        } catch (reviErr) {
          console.error(`REVI context injection failed for "${task.name}":`, reviErr.message);
        }
      }

      // Team-visible reports must never surface company financials, whatever
      // source they arrived from (Slack digests, knowledge base, live context).
      const confidentialityRule = deliversToRonOnly ? '' : `\n\nCONFIDENTIALITY: This report posts to a team-visible channel. Never include company financial details — bank balances, payment failures or successes, billing/subscription status, invoices, card or account information. If such a signal appears anywhere in your context, omit it entirely; Ron is notified privately through a separate channel.`;
      const enrichedPrompt = liveContext
        ? `${task.prompt}${lessonContext}${clientCtxBlock}${taskDataBlock}${confidentialityRule}\n\n---\nLIVE CONTEXT (use this to inform the report):\n${liveContext}`
        : `${task.prompt}${lessonContext}${clientCtxBlock}${taskDataBlock}${confidentialityRule}`;

      // Retry logic — up to 3 attempts with backoff for 529/503 overload errors
      let reply = null;
      const maxAttempts = 3;
      const retryDelays = [15000, 30000, 60000]; // 15s, 30s, 60s

      for (let attempt = 0; attempt < maxAttempts; attempt++) {
        try {
          reply = await callClaude([{ role: 'user', content: enrichedPrompt }], 3, null, correlation_id);
          lastErr = null;
          break;
        } catch (err) {
          lastErr = err;
          const isOverload = err.status === 529 || err.status === 503 || err.status === 500;
          if (isOverload && attempt < maxAttempts - 1) {
            const wait = retryDelays[attempt];
            console.log(`Cron "${task.name}" overloaded (attempt ${attempt + 1}/${maxAttempts}), retrying in ${wait / 1000}s...`);
            await new Promise(r => setTimeout(r, wait));
          } else {
            break;
          }
        }
      }

      // If all retries failed — DM Ron, never post to team channel
      if (lastErr) {
        errored = lastErr;
        console.error(`Dynamic cron error (${task.name}):`, lastErr.message);
        try {
          await slack.client.chat.postMessage({
            channel: RON_SLACK_ID,
            text: `Scheduled task failed after ${maxAttempts} attempts: "${task.name}"\nError: ${lastErr.message}\nTarget channel: ${task.channel}\nThis will retry automatically at the next scheduled run.`,
          });
        } catch (dmErr) {
          console.error('Failed to DM Ron about cron error:', dmErr.message);
        }
        return;
      }

      if (!reply || !reply.trim()) return;

      reply = trimToLeadAnchor(stripApprovalSentinel(reply), task.name);

      // ── STRUCTURAL WHITELIST GUARD ───────────────────────────────────────────
      // Rejects any reply that doesn't contain the expected section headers.
      // This is a WHITELIST check — the reply must prove it is a final report
      // by containing at least one known section header. If it doesn't, it gets
      // rejected and re-prompted regardless of what it says. This is stronger
      // than a phrase blacklist which always has gaps.
      //
      // Each task defines its required headers. If the task name matches a known
      // task, we check for its headers. Unknown tasks fall back to length check.
      const TASK_HEADERS = {
        'Sales EOD Report':           ['LEADS TODAY', 'STRATEGY CALLS BOOKED', 'WORKED VS UNWORKED', "TOMORROW'S PRIORITY"],
        'Fulfillment EOD Pulse':      ['WINS TODAY', 'BLOCKERS', 'ANOMALIES', 'TOMORROW'],
        'Friday Delivery Wrap-Up':    ['WEEK IN REVIEW', 'CLIENT STATUS BOARD', 'TEAM WINS THIS WEEK', 'MISSES THIS WEEK', 'MONDAY PRIORITIES'],
        'Ron Weekly Ops Digest':      ['DELIVERY', 'SALES', 'WHAT NEEDS YOUR ATTENTION'],
        'Monthly Business Review':    ['WINS THIS MONTH', 'GAPS & MISSES', 'KEEP DOING', 'STOP DOING', 'WATCH LIST', 'ONE PRIORITY FOR NEXT MONTH'],
        // Weekly Closer Comparison headers are resolved dynamically from the
        // precomputed weeklyCloserStats below — only closers with activity that
        // week are required to appear. Static list left empty so we don't fall
        // through to a wrong fallback.
        'Weekly Closer Comparison':   [],
        'Setter Leaderboard':         [],
        'Sales Call Prep Reminder':         [], // short task, skip header check
        'Blocked Client Report — MWF':      [], // short report, no fixed headers
        'Cancellation Rate Alert':          [], // conditional — may legitimately be empty
        'Phase 0 Aging Alert':              [], // conditional — may legitimately be empty
        'Daily Sales Call Roster':          [], // short on quiet days
        // Renamed 2026-08-18 (attendance is Max's job now). A stale key here is
        // invisible: the lookup returns undefined and the task silently falls
        // through to the unknown-task 300-char branch instead of its own rule.
        'Daily Closer Outcome Reminder': [], // short nightly nudge — no fixed headers
      };

      function validateFinalReport(text, taskName) {
        const t = text || '';
        const upper = t.toUpperCase();
        const len = t.trim().length;

        // Weekly Sales Pod Leaderboard (task name kept as "Weekly Closer Comparison"):
        // require only people with activity this week — resolved at runtime from
        // both weeklyCloserStats and weeklySetterStats.
        let headers = TASK_HEADERS[taskName];
        if (taskName === 'Weekly Closer Comparison') {
          const closerHeaders = (weeklyCloserStats ? Object.keys(weeklyCloserStats) : [])
            .filter(name => {
              const s = weeklyCloserStats[name];
              return (s.calls_booked || 0) > 0 || (s.sold || 0) > 0;
            })
            .map(n => n.toUpperCase());
          headers = closerHeaders.length ? closerHeaders : [];
        }
        if (taskName === 'Setter Leaderboard') {
          const setterHeaders = (weeklySetterStats ? Object.keys(weeklySetterStats) : [])
            .filter(name => {
              const s = weeklySetterStats[name];
              return (s.calls_booked || 0) > 0 || (s.leads_claimed || 0) > 0 || (s.aqc || 0) > 0;
            })
            .map(n => n.toUpperCase());
          headers = setterHeaders.length ? setterHeaders : [];
        }

        if (headers === undefined) {
          return { ok: len >= 300, reason: len >= 300 ? null : `length ${len} < 300 (unknown task — length-only check)`, found: [], missing: [], len };
        }
        if (headers.length === 0) {
          return { ok: len > 20, reason: len > 20 ? null : `length ${len} <= 20`, found: [], missing: [], len };
        }
        const found = headers.filter(h => upper.includes(h));
        const missing = headers.filter(h => !upper.includes(h));
        const threshold = Math.ceil(headers.length / 2);
        if (found.length < threshold) {
          return { ok: false, reason: `missing headers (have ${found.length}/${headers.length}, need ${threshold})`, found, missing, len };
        }
        if (len < 300) {
          return { ok: false, reason: `length ${len} < 300`, found, missing, len };
        }
        return { ok: true, reason: null, found, missing, len };
      }

      const firstCheck = validateFinalReport(reply, task.name);

      if (!firstCheck.ok) {
        console.log(`Cron "${task.name}": output failed structural validation — ${firstCheck.reason}. Re-prompting...`);
        try {
          const finalReply = trimToLeadAnchor(stripApprovalSentinel(await callClaude([
            { role: 'user', content: task.prompt },
            { role: 'assistant', content: reply },
            { role: 'user', content: 'Your previous response was rejected because it did not contain the required section headers. Do NOT narrate your process, explain what you are doing, or show your reasoning. Output ONLY the final compiled report with every section header and all data filled in, exactly as specified in the original instructions. Start directly with the first section header. Nothing before it.' }
          ], 3, null, correlation_id)), task.name);
          const secondCheck = validateFinalReport(finalReply, task.name);
          if (finalReply && secondCheck.ok) {
            reply = finalReply;
            console.log(`Cron "${task.name}": re-prompt passed validation (${reply.trim().length} chars)`);
          } else {
            const failingReply = (finalReply || reply || '');
            const finalReason = (secondCheck && secondCheck.reason) || firstCheck.reason || 'unknown';
            const finalMissing = (secondCheck && secondCheck.missing) || firstCheck.missing || [];
            validationFailure = {
              reason: finalReason,
              missing: finalMissing,
              len: failingReply.trim().length,
              first_attempt_reason: firstCheck.reason,
              reply_preview: failingReply.trim().slice(0, 500),
            };
            const missingStr = finalMissing.length ? ` — missing: ${finalMissing.join(', ')}` : '';
            const previewStr = validationFailure.reply_preview
              ? `\nReply preview: "${validationFailure.reply_preview.replace(/\n+/g, ' ').slice(0, 300)}${validationFailure.reply_preview.length > 300 ? '…' : ''}"`
              : '';
            const errMsg = `Scheduled task "${task.name}" failed structural validation after 2 attempts.\nReason: ${finalReason}${missingStr}${previewStr}`;
            await slack.client.chat.postMessage({ channel: RON_SLACK_ID, text: errMsg });
            console.error(`Cron "${task.name}": re-prompt also failed validation (${finalReason}). Notified Ron.`);
            return;
          }
        } catch (rePromptErr) {
          console.error(`Re-prompt failed for "${task.name}":`, rePromptErr.message);
          return;
        }
      }

      const targetChannel = task.channel || AGENT_CHANNEL;

      // Scheduled reports post directly — feedback learning loop handles quality
      // (user-initiated draft_channel_post requests still use the approval flow)
      const lessons = await getReportLessons(targetChannel.replace(/^#/, ''));
      let finalReply = reply;
      if (lessons.length) {
        const lessonNote = `[Corrections applied from team feedback]\n${lessons.map(l => `• ${l.value}`).join('\n')}\n\n`;
        finalReply = lessonNote + reply;
      }
      // Strip Markdown bold — Slack uses *bold* not **bold**
      finalReply = finalReply.replace(/\*\*(.+?)\*\*/g, '$1');
      await postToSlack(targetChannel, finalReply);

    } catch (e) { errored = e; throw e; } finally {
      const doneErr = errored || lastErr;
      logActivity({
        event_type: 'cron_run',
        event_source: 'cron',
        action: cronAction,
        status: doneErr ? 'error' : 'ok',
        duration_ms: Date.now() - started,
        error_message: doneErr ? String(doneErr.message || doneErr).slice(0, 2000) : null,
        correlation_id,
        metadata: { task_id: task.id, ...(validationFailure ? { validation_failure: validationFailure } : {}) },
      });
    }
    }, { timezone: 'America/Costa_Rica' });
    activeDynamicCrons[task.id] = job;
    console.log(`Registered dynamic cron: "${task.name}" (${task.cron_expression})`);
  } catch (err) {
    console.error(`Failed to register cron "${task.name}":`, err.message);
  }
}

async function createScheduledTask(name, naturalLanguageSchedule, prompt, channel, createdBy) {
  try {
    const { data: existing } = await supabase
      .from('scheduled_tasks')
      .select('id, name')
      .ilike('name', name.trim())
      .eq('active', true)
      .limit(1);
    if (existing && existing.length > 0) {
      return `A scheduled task named "${name}" already exists. Use list_scheduled_tasks to see all active tasks, or delete the existing one first.`;
    }
    const cronPrompt = `Convert this schedule description to a cron expression (5-field format).
Schedule: "${naturalLanguageSchedule}"
Timezone: America/Costa_Rica
Reply with ONLY the cron expression, nothing else. Examples:
- "every weekday at 9am" -> 0 9 * * 1-5
- "every Monday at 8:30am" -> 30 8 * * 1
- "every day at 6pm" -> 0 18 * * *
- "every Friday at 4pm" -> 0 16 * * 5`;
    const cIdTask = newCorrelationId();
    const tCronLlm = Date.now();
    const cronResponse = await anthropic.messages.create({
      model: 'claude-sonnet-4-6',
      max_tokens: 50,
      messages: [{ role: 'user', content: cronPrompt }]
    });
    logLlmFromAnthropicResponse(cronResponse, Date.now() - tCronLlm, cIdTask);
    const cronExpression = cronResponse.content
      .filter(b => b.type === 'text').map(b => b.text).join('').trim()
      .replace(/[^0-9*,/\- ]/g, '').trim();
    if (!cronExpression || cronExpression.split(' ').length !== 5) {
      return `Could not parse schedule "${naturalLanguageSchedule}" into a valid cron expression. Try something like "every weekday at 9am" or "every Monday at 8:30am".`;
    }
    const { data, error } = await supabase
      .from('scheduled_tasks')
      .insert({ name, cron_expression: cronExpression, prompt, channel: channel || AGENT_CHANNEL, active: true, created_by: createdBy, created_at: new Date().toISOString() })
      .select().single();
    if (error) throw error;
    registerDynamicCron(data);
    return `Scheduled task created: "${name}" — runs ${naturalLanguageSchedule} (${cronExpression}). It is now active.`;
  } catch (err) {
    return `Failed to create scheduled task: ${err.message}`;
  }
}

async function listScheduledTasks() {
  try {
    const { data, error } = await supabase
      .from('scheduled_tasks')
      .select('id, name, cron_expression, channel, active, created_at')
      .order('created_at', { ascending: false });
    if (error) throw error;
    if (!data || !data.length) return 'No scheduled tasks found.';
    return data.map(t =>
      `${t.active ? '✅' : '⏸️'} ${t.name} | ${t.cron_expression} | ${t.channel} | ID: ${t.id.substring(0,8)}`
    ).join('\n');
  } catch (err) {
    return `Error listing tasks: ${err.message}`;
  }
}

// ─── FIX 1: cleanDuplicateTasks ──────────────────────────────────────────────
// Queries ALL rows (no active filter) so inactive dupes are visible.
// Uses hard DELETE (not soft update). Stops live cron instances for removed IDs.
async function cleanDuplicateTasks() {
  try {
    // NOTE: intentionally no .eq('active', true) — we want ALL rows including inactive dupes
    const { data: tasks, error } = await supabase
      .from('scheduled_tasks')
      .select('id, name, cron_expression, active, created_at')
      .order('name', { ascending: true })
      .order('created_at', { ascending: true });
    if (error) throw error;
    if (!tasks || !tasks.length) return 'No scheduled tasks found.';

    function normalizeName(name) {
      return name.toLowerCase().trim()
        .replace(/^#[a-z0-9-]+\s+/i, '')
        .replace(/\s+#[a-z0-9-]+$/i, '')
        .trim();
    }

    const seen = {};
    const toDelete = [];
    for (const task of tasks) {
      const key = normalizeName(task.name);
      if (seen[key]) {
        const hasHash = task.name.startsWith('#');
        const existingHasHash = seen[key].name.startsWith('#');
        if (hasHash) { toDelete.push(task); }
        else if (existingHasHash) { toDelete.push(seen[key]); seen[key] = task; }
        else { toDelete.push(task); }
      } else {
        seen[key] = task;
      }
    }

    if (!toDelete.length) return 'No duplicate tasks found — all clean.';

    const ids = toDelete.map(t => t.id);
    console.log(`cleanDuplicateTasks: hard deleting IDs: ${ids.join(', ')}`);

    const { error: delError } = await supabase
      .from('scheduled_tasks')
      .delete()
      .in('id', ids);
    if (delError) {
      console.error('cleanDuplicateTasks delete error:', delError.message);
      throw new Error(`Delete failed: ${delError.message}`);
    }

    // Verify deletion
    const { data: remaining } = await supabase.from('scheduled_tasks').select('id').in('id', ids);
    if (remaining && remaining.length > 0) {
      throw new Error(`Delete appeared to succeed but ${remaining.length} rows still exist (IDs: ${remaining.map(r => r.id).join(', ')}). Check Supabase RLS — anon key may not have DELETE permission on scheduled_tasks.`);
    }

    // Stop any live cron instances for deleted task IDs
    for (const id of ids) {
      if (activeDynamicCrons[id]) {
        activeDynamicCrons[id].stop();
        delete activeDynamicCrons[id];
        console.log(`Stopped live cron for deleted task ${id}`);
      }
    }

    const names = toDelete.map(t => t.name).join(', ');
    return `Done. Hard deleted ${toDelete.length} duplicate task(s): ${names}. ${Object.keys(seen).length} unique tasks remain.`;
  } catch (err) {
    return `Clean duplicate tasks error: ${err.message}`;
  }
}

async function deleteScheduledTask(taskId) {
  try {
    const { error } = await supabase.from('scheduled_tasks').update({ active: false }).eq('id', taskId);
    if (error) throw new Error(`Supabase update failed: ${error.message}`);
    const { data: check } = await supabase.from('scheduled_tasks').select('id, name, active').eq('id', taskId).single();
    if (check && check.active !== false) {
      throw new Error(`Update did not persist — task is still active. RLS may be blocking writes on the anon key.`);
    }
    if (activeDynamicCrons[taskId]) {
      activeDynamicCrons[taskId].stop();
      delete activeDynamicCrons[taskId];
      console.log(`Dynamic cron stopped: ${taskId}`);
    }
    const taskName = check?.name || taskId.substring(0,8);
    return `Done. Task "${taskName}" has been deactivated and will no longer run.`;
  } catch (err) {
    console.error('deleteScheduledTask error:', err.message);
    return `Delete task failed: ${err.message}`;
  }
}

// ─── NOTION WRITE-BACK ────────────────────────────────────────────────────────
async function createNotionTask(title, taskType = 'operational', priority = 'P2 - Growth & Scalability', dueDate = null, notes = null, customer = null) {
  try {
    const isProject  = taskType === 'project';
    const collectionId = isProject ? '8d0645e6-eabb-4f0d-9c8a-4d8641ad4e8c' : '20ecddb6-8d9f-8126-a408-000bbbc3c088';
    const databaseId   = isProject ? 'dc12b8a930f148729e42c11391271bd1' : '20ecddb68d9f809ba904d248ed95fce9';
    const properties = {
      'Name': { title: [{ text: { content: title } }] },
      'Status': { status: { name: 'Not started' } },
      'Priority ': { select: { name: priority } },
      'Type': { select: { name: 'One-time' } },
    };
    if (dueDate) properties['Deadline Date'] = { date: { start: dueDate } };
    if (notes) {
      const notesKey = isProject ? 'Comments/Milestones/Insights' : 'Main Milestone';
      properties[notesKey] = { rich_text: [{ text: { content: `Max: ${notes.substring(0, 500)}` } }] };
    }
    if (customer) properties['Customer'] = { multi_select: [{ name: customer }] };
    const body = { parent: { database_id: databaseId }, properties };
    const res  = await fetch('https://api.notion.com/v1/pages', {
      method: 'POST',
      headers: { 'Authorization': `Bearer ${process.env.NOTION_TOKEN}`, 'Notion-Version': '2022-06-28', 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    });
    const data = await res.json();
    if (!res.ok) {
      console.error('Notion create task failed:', res.status, JSON.stringify(data));
      const body2 = { ...body, parent: { database_id: collectionId.replace(/-/g,'') } };
      const res2  = await fetch('https://api.notion.com/v1/pages', {
        method: 'POST',
        headers: { 'Authorization': `Bearer ${process.env.NOTION_TOKEN}`, 'Notion-Version': '2022-06-28', 'Content-Type': 'application/json' },
        body: JSON.stringify(body2),
      });
      const data2 = await res2.json();
      if (!res2.ok) {
        console.error('Notion fallback also failed:', res2.status, JSON.stringify(data2));
        throw new Error(`${data.message || data.code} | fallback: ${data2.message || data2.code}`);
      }
      const dbName = isProject ? 'Project Sprint Tracking' : 'Operations Tracking';
      return `Task created in Notion (${dbName}): "${title}"${dueDate ? ` — due ${dueDate}` : ''} — ${priority}. Link: ${data2.url}`;
    }
    const dbName = isProject ? 'Project Sprint Tracking' : 'Operations Tracking';
    return `Task created in Notion (${dbName}): "${title}"${dueDate ? ` — due ${dueDate}` : ''} — ${priority}. Link: ${data.url}`;
  } catch (err) {
    return `Notion task creation error: ${err.message}`;
  }
}

// ─── GOOGLE AUTH ──────────────────────────────────────────────────────────────
function getGoogleAuth() {
  let credentials, token;
  if (process.env.GOOGLE_CREDENTIALS) {
    credentials = JSON.parse(process.env.GOOGLE_CREDENTIALS);
    token       = JSON.parse(process.env.GOOGLE_TOKEN);
  } else {
    credentials = JSON.parse(fs.readFileSync('./credentials.json'));
    token       = JSON.parse(fs.readFileSync('./token.json'));
  }
  const { client_id, client_secret } = credentials.installed;
  const oauth2Client = new google.auth.OAuth2(client_id, client_secret, 'http://localhost:3000/callback');
  oauth2Client.setCredentials(token);
  return oauth2Client;
}

// ─── GMAIL ────────────────────────────────────────────────────────────────────
async function getRecentEmails() {
  const auth  = getGoogleAuth();
  const gmail = google.gmail({ version: 'v1', auth });
  const res   = await gmail.users.messages.list({ userId: 'me', maxResults: 5, q: 'is:unread' });
  const messages = res.data.messages || [];
  if (!messages.length) return 'No unread emails.';
  const details = await Promise.all(messages.map(async (m) => {
    const msg     = await gmail.users.messages.get({ userId: 'me', id: m.id, format: 'full' });
    const headers = msg.data.payload.headers;
    const subject = headers.find(h => h.name === 'Subject')?.value || 'No subject';
    const from    = headers.find(h => h.name === 'From')?.value    || 'Unknown';
    const date    = headers.find(h => h.name === 'Date')?.value    || '';
    let body = '';
    const payload = msg.data.payload;
    if (payload.parts) {
      const textPart = payload.parts.find(p => p.mimeType === 'text/plain');
      if (textPart?.body?.data) body = Buffer.from(textPart.body.data, 'base64').toString('utf8').substring(0, 1000);
    } else if (payload.body?.data) {
      body = Buffer.from(payload.body.data, 'base64').toString('utf8').substring(0, 1000);
    }
    if (!body) body = msg.data.snippet?.substring(0, 500) || '';
    return `From: ${from}\nDate: ${date}\nSubject: ${subject}\nBody:\n${body}`;
  }));
  return details.join('\n\n---\n\n');
}

// ─── RON GMAIL SIGNATURE (cached) ─────────────────────────────────────────────
// Resolution order:
//   1. RON_SIGNATURE_HTML env var (most reliable — Gmail API often returns
//      empty for the primary account when signature is set under
//      Settings > General instead of Settings > Accounts > Send mail as).
//   2. Gmail API sendAs.get for ronny.duarte@neurogrowth.io.
//   3. Gmail API sendAs.list — look for any entry with a non-empty signature.
//   4. Empty (sends will go without signature; warning DM to Ron once).
let _ronSignatureHtml = null;
let _ronSignatureWarned = false;
async function getRonSignature() {
  if (_ronSignatureHtml) return _ronSignatureHtml;

  // 1. Env var first — most reliable.
  const envSig = process.env.RON_SIGNATURE_HTML;
  if (envSig && envSig.trim()) {
    _ronSignatureHtml = envSig;
    return _ronSignatureHtml;
  }

  // 2 + 3. Gmail API.
  try {
    const auth  = getGoogleAuth();
    const gmail = google.gmail({ version: 'v1', auth });
    let sig = '';
    try {
      const res = await gmail.users.settings.sendAs.get({
        userId: 'me',
        sendAsEmail: 'ronny.duarte@neurogrowth.io',
      });
      sig = res.data?.signature || '';
    } catch (getErr) {
      console.error('getRonSignature: sendAs.get failed:', getErr.message);
    }
    if (!sig) {
      try {
        const list = await gmail.users.settings.sendAs.list({ userId: 'me' });
        const entries = list.data?.sendAs || [];
        const withSig = entries.find(e => e.signature && e.signature.trim());
        if (withSig) sig = withSig.signature;
      } catch (listErr) {
        console.error('getRonSignature: sendAs.list failed:', listErr.message);
      }
    }
    _ronSignatureHtml = sig || '';
  } catch (err) {
    console.error('getRonSignature failed:', err.message);
    _ronSignatureHtml = '';
  }

  if (!_ronSignatureHtml && !_ronSignatureWarned) {
    _ronSignatureWarned = true;
    console.log('Ron signature is empty after env+API fallbacks. Set RON_SIGNATURE_HTML in Railway to fix.');
    try {
      await slack.client.chat.postMessage({
        channel: RON_SLACK_ID,
        text: '⚠️ No signature found for outbound emails. Sends will go without signature until fixed. To fix: set Railway env var `RON_SIGNATURE_HTML` to your signature HTML (paste from Gmail Settings → General → Signature, view source). Until then proxy emails will send without a signature.',
      });
    } catch {}
  }
  return _ronSignatureHtml;
}

// ─── RFC 2047 header encoding ────────────────────────────────────────────────
// Headers (Subject, Cc, From-name, etc.) must be ASCII-only by default.
// If they contain non-ASCII (accents, em-dash, emoji), wrap as
// =?UTF-8?B?<base64>?= so receivers know to decode.
function encodeMimeHeader(s) {
  if (!s) return '';
  if (/^[\x00-\x7F]*$/.test(s)) return s;
  const b64 = Buffer.from(s, 'utf8').toString('base64');
  return `=?UTF-8?B?${b64}?=`;
}

// Strip HTML to plain text (signatures may be HTML — we need a plaintext fallback for multipart).
function htmlToPlainText(html) {
  if (!html) return '';
  return html
    .replace(/<br\s*\/?>/gi, '\n')
    .replace(/<\/p>/gi, '\n\n')
    .replace(/<[^>]+>/g, '')
    .replace(/&nbsp;/g, ' ')
    .replace(/&amp;/g, '&')
    .replace(/&lt;/g, '<')
    .replace(/&gt;/g, '>')
    .replace(/&quot;/g, '"')
    .replace(/&#39;/g, "'")
    .replace(/\n{3,}/g, '\n\n')
    .trim();
}

function escapeHtml(s) {
  return String(s || '')
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

// Build a quoted-printable safe-ish RFC 2822 multipart/alternative message.
// We base64url the whole thing; gmail.users.messages.send accepts that.
function buildRfc2822Message({ to, cc, subject, body, signatureHtml, inReplyTo, references }) {
  const boundary = `=_NG_${Date.now()}_${Math.random().toString(36).slice(2, 10)}`;
  const headers = [
    'From: Ron Duarte <ronny.duarte@neurogrowth.io>',
    `To: ${to}`,
  ];
  if (cc) headers.push(`Cc: ${encodeMimeHeader(cc)}`);
  headers.push(`Subject: ${encodeMimeHeader(subject)}`);
  headers.push('MIME-Version: 1.0');
  if (inReplyTo)  headers.push(`In-Reply-To: ${inReplyTo}`);
  if (references) headers.push(`References: ${references}`);
  headers.push(`Content-Type: multipart/alternative; boundary="${boundary}"`);
  headers.push('');

  const plainBody = body + (signatureHtml ? '\n\n--\n' + htmlToPlainText(signatureHtml) : '');
  // Convert plain body to minimal HTML: escape, then \n -> <br>
  const htmlEscaped = escapeHtml(body).replace(/\r?\n/g, '<br>\n');
  const htmlBody = `<div>${htmlEscaped}</div>` + (signatureHtml ? `<br><div>--</div><div>${signatureHtml}</div>` : '');

  // Encode body parts as base64 so UTF-8 multibyte sequences (accents, emoji,
  // em-dash) survive transport regardless of relay 7bit/8bit handling.
  const plainB64 = Buffer.from(plainBody, 'utf8').toString('base64').replace(/(.{76})/g, '$1\r\n');
  const htmlB64  = Buffer.from(htmlBody,  'utf8').toString('base64').replace(/(.{76})/g, '$1\r\n');

  const parts = [
    `--${boundary}`,
    'Content-Type: text/plain; charset="UTF-8"',
    'Content-Transfer-Encoding: base64',
    '',
    plainB64,
    '',
    `--${boundary}`,
    'Content-Type: text/html; charset="UTF-8"',
    'Content-Transfer-Encoding: base64',
    '',
    htmlB64,
    '',
    `--${boundary}--`,
    '',
  ];

  return headers.join('\r\n') + '\r\n' + parts.join('\r\n');
}

// sendEmail — extended for the email-proxy flow.
//  - Always sends as ronny.duarte@neurogrowth.io with signature appended.
//  - opts.cc, opts.inReplyTo, opts.references, opts.threadId for replies.
//  - Returns { gmailMessageId, gmailThreadId, rfc822MessageId } so callers can
//    persist threading metadata into email_threads.
async function sendEmail(to, subject, body, opts = {}) {
  const auth  = getGoogleAuth();
  const gmail = google.gmail({ version: 'v1', auth });
  const signatureHtml = await getRonSignature();

  const raw = buildRfc2822Message({
    to,
    cc: opts.cc || null,
    subject,
    body,
    signatureHtml,
    inReplyTo: opts.inReplyTo || null,
    references: opts.references || null,
  });
  const encoded = Buffer.from(raw, 'utf8').toString('base64').replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '');

  const requestBody = { raw: encoded };
  if (opts.threadId) requestBody.threadId = opts.threadId;

  const sendRes = await gmail.users.messages.send({ userId: 'me', requestBody });
  const gmailMessageId = sendRes.data.id;
  const gmailThreadId  = sendRes.data.threadId;

  // Re-fetch to capture the server-assigned Message-ID: header (needed for
  // threading future replies via In-Reply-To / References).
  let rfc822MessageId = null;
  try {
    const msg = await gmail.users.messages.get({
      userId: 'me',
      id: gmailMessageId,
      format: 'metadata',
      metadataHeaders: ['Message-ID', 'Message-Id'],
    });
    const hs = msg.data.payload?.headers || [];
    rfc822MessageId = hs.find(h => h.name.toLowerCase() === 'message-id')?.value || null;
  } catch (err) {
    console.error('sendEmail: Message-ID fetch failed:', err.message);
  }

  return {
    ok: true,
    message: `Email sent to ${to}`,
    gmailMessageId,
    gmailThreadId,
    rfc822MessageId,
  };
}

// ─── EMAIL PROXY: hourly reply poller ─────────────────────────────────────────
// Reads active email_threads rows, calls Gmail threads.get for each, looks for
// inbound messages newer than the watermark, DMs the setter, advances watermark.
// Per-tick failure tracking lives in a module-scope counter so we only DM Ron
// when the same thread fails 3+ times in a row.
const _replyPollFailureCounts = {};
async function runEmailReplyPoller(correlationId) {
  if (!EMAIL_PROXY_LIVE) return;
  try {
    const { data: threads, error } = await supabase
      .from('email_threads')
      .select('*')
      .eq('active', true)
      .order('last_message_at', { ascending: true })
      .limit(50);
    if (error) {
      console.error('runEmailReplyPoller: threads query failed:', error.message);
      return;
    }
    if (!threads || threads.length === 0) return;

    const auth  = getGoogleAuth();
    const gmail = google.gmail({ version: 'v1', auth });
    const ourAddress = 'ronny.duarte@neurogrowth.io';

    for (const t of threads) {
      try {
        const res = await gmail.users.threads.get({ userId: 'me', id: t.gmail_thread_id, format: 'metadata', metadataHeaders: ['From', 'Subject', 'Date', 'Message-ID', 'Message-Id'] });
        const messages = res.data.messages || [];
        const watermark = new Date(t.last_message_at).getTime();
        const newInbound = messages.filter(m => {
          const internal = parseInt(m.internalDate, 10) || 0;
          if (internal <= watermark) return false;
          const headers = m.payload?.headers || [];
          const from = (headers.find(h => h.name.toLowerCase() === 'from')?.value || '').toLowerCase();
          return !from.includes(ourAddress);
        });
        if (newInbound.length === 0) {
          _replyPollFailureCounts[t.id] = 0;
          continue;
        }

        // Pull plaintext bodies for each new inbound message.
        for (const m of newInbound) {
          let bodySnippet = '';
          try {
            const full = await gmail.users.messages.get({ userId: 'me', id: m.id, format: 'full' });
            bodySnippet = extractPlainTextBody(full.data.payload).slice(0, 1500) || (full.data.snippet || '').slice(0, 500);
          } catch (bodyErr) {
            bodySnippet = '(could not load body — open Gmail to view)';
          }
          const headers = m.payload?.headers || [];
          const from = headers.find(h => h.name.toLowerCase() === 'from')?.value || 'Unknown';
          await slack.client.chat.postMessage({
            channel: t.initiated_by_slack_id,
            text: `📬 Reply on "${t.subject}"\nFrom: ${from}\n\n${bodySnippet}\n\nReply here in DM to send a response (I will draft, you approve, then Ron approves).`,
          });
        }

        // Advance watermark to the newest message we just processed.
        const newest = Math.max(...newInbound.map(m => parseInt(m.internalDate, 10) || 0));
        await supabase.from('email_threads').update({
          last_message_at: new Date(newest).toISOString(),
        }).eq('id', t.id);
        _replyPollFailureCounts[t.id] = 0;
      } catch (perThreadErr) {
        _replyPollFailureCounts[t.id] = (_replyPollFailureCounts[t.id] || 0) + 1;
        console.error(`runEmailReplyPoller: thread ${t.id} failed (count=${_replyPollFailureCounts[t.id]}):`, perThreadErr.message);
        if (_replyPollFailureCounts[t.id] >= 3) {
          try {
            await slack.client.chat.postMessage({
              channel: RON_SLACK_ID,
              text: `🔴 Reply polling broken for thread "${t.subject}" (id ${t.id}). 3+ consecutive failures. Last error: ${perThreadErr.message.slice(0, 200)}. Investigate.`,
            });
          } catch {}
          _replyPollFailureCounts[t.id] = 0; // reset so we don't DM Ron every hour
        }
      }
    }
  } catch (err) {
    console.error('runEmailReplyPoller fatal:', err);
  }
}

// Helper: pull text/plain body out of a Gmail message payload.
function extractPlainTextBody(payload) {
  if (!payload) return '';
  if (payload.mimeType === 'text/plain' && payload.body?.data) {
    return Buffer.from(payload.body.data, 'base64').toString('utf8');
  }
  if (payload.parts && payload.parts.length) {
    for (const p of payload.parts) {
      const found = extractPlainTextBody(p);
      if (found) return found;
    }
  }
  return '';
}

// ─── GOOGLE CALENDAR ──────────────────────────────────────────────────────────
// Day windows are CR-anchored (same contract as _crDayBoundsUtc): daysFromNow
// counts Costa Rica calendar days and each window is exactly daysRange×24h from
// CR midnight. The old server-local Date math ran on Railway's UTC clock — after
// 6 PM CR "today" was already tomorrow — and setHours(29,…) on the end bound made
// every window 48h wide, so Friday-evening reports carried Monday's events under
// a "TOMORROW'S CALENDAR" label (the Win Da Week bug).
function _crMidnightUtc(daysFromNow = 0) {
  const todayCR = new Date().toLocaleDateString('en-CA', { timeZone: 'America/Costa_Rica' });
  const d = new Date(`${todayCR}T06:00:00.000Z`); // CR midnight (UTC-6, no DST)
  d.setUTCDate(d.getUTCDate() + daysFromNow);
  return d;
}
function _crDayLabel(daysFromNow = 0) {
  return _crMidnightUtc(daysFromNow).toLocaleDateString('en-US', { timeZone: 'America/Costa_Rica', weekday: 'long', month: 'short', day: 'numeric' });
}
async function getCalendarEvents(daysFromNow = 0, daysRange = 1) {
  const auth     = getGoogleAuth();
  const calendar = google.calendar({ version: 'v3', auth });
  const startDate = _crMidnightUtc(daysFromNow);
  const endDate   = new Date(startDate.getTime() + daysRange * 24 * 60 * 60 * 1000);
  const res = await calendar.events.list({ calendarId: 'primary', timeMin: startDate.toISOString(), timeMax: endDate.toISOString(), singleEvents: true, orderBy: 'startTime' });
  const events = res.data.items || [];
  if (!events.length) return 'No events found in that range.';
  return events.map(e => {
    // CR weekday + wall clock, never the raw ISO string — the event's own offset
    // (e.g. a Cancun -05:00 organizer) reads as the wrong hour, and without a
    // weekday the model can't sanity-check a "today/tomorrow" label.
    const when = e.start.dateTime
      ? `${new Date(e.start.dateTime).toLocaleString('en-US', { timeZone: 'America/Costa_Rica', weekday: 'short', month: 'short', day: 'numeric', hour: 'numeric', minute: '2-digit', hour12: true })} CR`
      : `${new Date(`${e.start.date}T12:00:00Z`).toLocaleDateString('en-US', { timeZone: 'America/Costa_Rica', weekday: 'short', month: 'short', day: 'numeric' })} (all day)`;
    const guestCount = (e.attendees || []).length;
    return `${when} — ${e.summary} [id: ${e.id}${guestCount ? ` | ${guestCount} guests` : ''}]`;
  }).join('\n');
}

// ─── GOOGLE CALENDAR: ADD ATTENDEES TO EXISTING EVENT ────────────────────────
// Fetches the event, merges new attendees with existing (no dupes), patches,
// and sends invite emails to all attendees via sendUpdates: 'all'.
async function addCalendarAttendees(eventId, attendees, sendUpdates = 'all') {
  try {
    if (!eventId) return 'Add attendees error: eventId is required. Use get_calendar_events first to find the event ID.';
    const emails = Array.isArray(attendees) ? attendees : String(attendees || '').split(',').map(s => s.trim()).filter(Boolean);
    if (!emails.length) return 'Add attendees error: no valid email addresses provided.';
    const auth     = getGoogleAuth();
    const calendar = google.calendar({ version: 'v3', auth });
    const { data: event } = await calendar.events.get({ calendarId: 'primary', eventId });
    const existing = new Set((event.attendees || []).map(a => (a.email || '').toLowerCase()));
    const toAdd    = emails.filter(e => !existing.has(e.toLowerCase()));
    if (!toAdd.length) return `All ${emails.length} attendees are already on "${event.summary}". No changes made.`;
    const merged = [...(event.attendees || []), ...toAdd.map(email => ({ email }))];
    await calendar.events.patch({
      calendarId: 'primary',
      eventId,
      sendUpdates,
      requestBody: { attendees: merged },
    });
    return `Added ${toAdd.length} guest(s) to "${event.summary}": ${toAdd.join(', ')}. Google sent invite emails (sendUpdates=${sendUpdates}).`;
  } catch (err) {
    return `Add attendees error: ${err.response?.data?.error?.message || err.message}`;
  }
}

// ─── GOOGLE CALENDAR: CREATE NEW EVENT ───────────────────────────────────────
async function createCalendarEvent(summary, startISO, endISO, attendees = [], description = '', location = '') {
  try {
    if (!summary || !startISO || !endISO) return 'Create event error: summary, startISO, and endISO are required.';
    const auth     = getGoogleAuth();
    const calendar = google.calendar({ version: 'v3', auth });
    const emails   = Array.isArray(attendees) ? attendees : String(attendees || '').split(',').map(s => s.trim()).filter(Boolean);
    const { data: event } = await calendar.events.insert({
      calendarId: 'primary',
      sendUpdates: 'all',
      requestBody: {
        summary,
        description: description || undefined,
        location: location || undefined,
        start: { dateTime: startISO, timeZone: 'America/Costa_Rica' },
        end:   { dateTime: endISO,   timeZone: 'America/Costa_Rica' },
        attendees: emails.map(email => ({ email })),
      },
    });
    return `Created "${event.summary}" for ${event.start.dateTime}. ${emails.length} guests invited. Event link: ${event.htmlLink}`;
  } catch (err) {
    return `Create event error: ${err.response?.data?.error?.message || err.message}`;
  }
}

// ─── GOOGLE DRIVE ─────────────────────────────────────────────────────────────
async function searchDrive(query) {
  const auth  = getGoogleAuth();
  const drive = google.drive({ version: 'v3', auth });
  const res   = await drive.files.list({
    q: `name contains '${query}' or fullText contains '${query}'`,
    pageSize: 5,
    fields: 'files(id, name, mimeType, webViewLink, modifiedTime)',
  });
  const files = res.data.files || [];
  if (!files.length) return 'No files found.';
  return files.map(f => `${f.name}\nType: ${f.mimeType}\nLink: ${f.webViewLink}\nModified: ${f.modifiedTime}`).join('\n\n');
}

// ─── NOTION ───────────────────────────────────────────────────────────────────
async function searchNotion(query) {
  const res = await fetch('https://api.notion.com/v1/search', {
    method: 'POST',
    headers: { 'Authorization': `Bearer ${process.env.NOTION_TOKEN}`, 'Notion-Version': '2022-06-28', 'Content-Type': 'application/json' },
    body: JSON.stringify({ query, page_size: 5 }),
  });
  return await res.json();
}

async function getNotionPage(pageId) {
  const res = await fetch(`https://api.notion.com/v1/pages/${pageId}`, {
    headers: { 'Authorization': `Bearer ${process.env.NOTION_TOKEN}`, 'Notion-Version': '2022-06-28' },
  });
  return await res.json();
}

// ─── GOOGLE SHEETS ────────────────────────────────────────────────────────────
async function readGoogleSheet(spreadsheetId, range = null) {
  try {
    const auth   = getGoogleAuth();
    const sheets = google.sheets({ version: 'v4', auth });
    if (!range) {
      const meta = await sheets.spreadsheets.get({ spreadsheetId });
      const firstSheet = meta.data.sheets?.[0]?.properties?.title || 'Sheet1';
      range = `${firstSheet}!A1:Z100`;
    }
    const res  = await sheets.spreadsheets.values.get({ spreadsheetId, range });
    const rows = res.data.values || [];
    if (!rows.length) return 'Sheet is empty or no data in range.';
    const headers  = rows[0] || [];
    const dataRows = rows.slice(1);
    let output = `Sheet data (${rows.length} rows, ${headers.length} columns):\n\n`;
    output += headers.join(' | ') + '\n';
    output += headers.map(() => '---').join(' | ') + '\n';
    output += dataRows.slice(0, 50).map(row => headers.map((_, i) => row[i] || '').join(' | ')).join('\n');
    if (dataRows.length > 50) output += `\n\n... and ${dataRows.length - 50} more rows.`;
    return output.substring(0, 6000);
  } catch (err) {
    return `Google Sheets read error: ${err.message}`;
  }
}

// ─── OPS MASTER TRACKER ──────────────────────────────────────────────────────
// Dedicated reader for the fulfillment team's Ops Master Tracker sheet. The
// generic readGoogleSheet is too lossy for it (change-request cells run >5k
// chars; tabs have title/blank rows above the headers). Tab titles are managed
// by the ops team and can be renamed, so tabs are identified by header
// signature, not title.
const OPS_TRACKER_SHEET_ID = process.env.OPS_TRACKER_SHEET_ID || '1ujWmYAOHegO25yMqncmUF-LFICbdrtzlKVTsTAuMk2g';

const OPS_TRACKER_TABS = {
  infrastructure:  { sig: ['sales navigator', 'prosp license'], label: 'Infrastructure checklist' },
  change_requests: { sig: ['description of change'],            label: 'Change requests' },
  launch_history:  { sig: ['fecha de lanzamiento'],             label: 'Launch history (Historial de Lanzamientos)' },
};

// Tab-title cache: one spreadsheets.get + batchGet per 10 minutes.
let opsTrackerTabCache = { at: 0, map: null };

function opsNormalize(s) {
  return String(s || '').normalize('NFD').replace(/[\u0300-\u036f]/g, '').toLowerCase().trim();
}

async function opsResolveTrackerTabs(sheets) {
  if (opsTrackerTabCache.map && Date.now() - opsTrackerTabCache.at < 10 * 60 * 1000) return opsTrackerTabCache.map;
  const meta = await sheets.spreadsheets.get({ spreadsheetId: OPS_TRACKER_SHEET_ID, fields: 'sheets.properties.title' });
  const titles = (meta.data.sheets || []).map(s => s.properties.title);
  if (!titles.length) throw new Error('Ops Tracker has no tabs');
  const peek = await sheets.spreadsheets.values.batchGet({
    spreadsheetId: OPS_TRACKER_SHEET_ID,
    ranges: titles.map(t => `'${t.replace(/'/g, "''")}'!A1:J6`),
  });
  const map = {};
  (peek.data.valueRanges || []).forEach((vr, i) => {
    const flat = opsNormalize((vr.values || []).flat().join(' | '));
    for (const [key, cfg] of Object.entries(OPS_TRACKER_TABS)) {
      if (!map[key] && cfg.sig.every(sig => flat.includes(sig))) map[key] = titles[i];
    }
  });
  opsTrackerTabCache = { at: Date.now(), map };
  return map;
}

async function getOpsTracker(tab, client = null, includeDone = false) {
  try {
    if (!OPS_TRACKER_TABS[tab]) return `Unknown tab "${tab}". Use infrastructure, change_requests, or launch_history.`;
    const auth   = getGoogleAuth();
    const sheets = google.sheets({ version: 'v4', auth });
    const tabMap = await opsResolveTrackerTabs(sheets);
    const title  = tabMap[tab];
    if (!title) return `Could not locate the ${tab} tab in the Ops Tracker — its header row may have changed. Ping Ron.`;

    const res  = await sheets.spreadsheets.values.get({ spreadsheetId: OPS_TRACKER_SHEET_ID, range: `'${title.replace(/'/g, "''")}'!A1:J500` });
    const rows = res.data.values || [];

    // Header row = first row matching this tab's signature (tabs carry title/blank rows above it).
    const sig = OPS_TRACKER_TABS[tab].sig;
    const headerIdx = rows.findIndex(r => { const flat = opsNormalize((r || []).join(' | ')); return sig.every(s => flat.includes(s)); });
    if (headerIdx === -1) return `Ops Tracker ${tab} tab found ("${title}") but its header row is missing — layout may have changed.`;
    const headers = rows[headerIdx].map(h => String(h || '').trim());
    const width   = headers.filter(Boolean).length;

    let dataRows = rows.slice(headerIdx + 1).filter(r => String((r || [])[0] || '').trim());
    const totalRows = dataRows.length;

    if (client) {
      const want = opsNormalize(client);
      dataRows = dataRows.filter(r => { const have = opsNormalize(r[0]); return have.includes(want) || want.includes(have); });
      if (!dataRows.length) return `No ${tab} rows matching client "${client}" (tab "${title}", ${totalRows} rows total). Client names in the tracker are messy — try a shorter fragment.`;
    }

    // Change requests default to open items; a client filter or includeDone shows history too.
    let hiddenDone = 0;
    if (tab === 'change_requests' && !includeDone && !client) {
      const before = dataRows.length;
      dataRows = dataRows.filter(r => !opsNormalize(r[width - 1] || r[8]).startsWith('done'));
      hiddenDone = before - dataRows.length;
    }

    const cell = v => { const s = String(v || '').replace(/\s+/g, ' ').trim(); return s.length > 350 ? s.substring(0, 350) + '…' : s; };
    let output = `OPS MASTER TRACKER — ${OPS_TRACKER_TABS[tab].label} (tab "${title}", ${dataRows.length} of ${totalRows} rows${client ? ` matching "${client}"` : ''}${hiddenDone ? `; ${hiddenDone} Done items hidden — pass include_done for history` : ''}):\n\n`;
    output += headers.slice(0, width).join(' | ') + '\n';
    let shown = 0;
    for (const r of dataRows) {
      const line = headers.slice(0, width).map((_, i) => cell(r[i])).join(' | ') + '\n';
      if (output.length + line.length > 9000) break;
      output += line;
      shown++;
    }
    if (shown < dataRows.length) output += `\n+${dataRows.length - shown} more rows — narrow with the client filter.`;
    return output;
  } catch (err) {
    return `Ops Tracker read error: ${err.message}`;
  }
}

// ─── GOOGLE DOCS ──────────────────────────────────────────────────────────────
async function readGoogleDoc(documentId) {
  try {
    const auth = getGoogleAuth();
    const docs  = google.docs({ version: 'v1', auth });
    const res  = await docs.documents.get({ documentId });
    const doc  = res.data;
    let text = '';
    for (const element of doc.body?.content || []) {
      if (element.paragraph) {
        for (const pe of element.paragraph.elements || []) {
          if (pe.textRun?.content) text += pe.textRun.content;
        }
      } else if (element.table) {
        for (const row of element.table.tableRows || []) {
          for (const cell of row.tableCells || []) {
            for (const cp of cell.content || []) {
              for (const pe of cp.paragraph?.elements || []) {
                if (pe.textRun?.content) text += pe.textRun.content + ' | ';
              }
            }
          }
          text += '\n';
        }
      }
    }
    if (!text.trim()) return 'Document is empty or has no readable text content.';
    return `Document: ${doc.title}\n\n${text.substring(0, 6000)}${text.length > 6000 ? '\n\n... [trimmed]' : ''}`;
  } catch (err) {
    return `Google Docs read error: ${err.message}`;
  }
}

function extractGoogleFileId(urlOrId) {
  const match = urlOrId.match(/\/d\/([a-zA-Z0-9_-]+)/);
  if (match) return match[1];
  if (/^[a-zA-Z0-9_-]+$/.test(urlOrId)) return urlOrId;
  return urlOrId;
}

// ─── META ADS ─────────────────────────────────────────────────────────────────
async function getMetaAdsSummary(datePreset = 'last_7d') {
  try {
    const accountId = process.env.META_AD_ACCOUNT_ID;
    const token     = process.env.META_ACCESS_TOKEN;
    if (!accountId || !token) return 'Meta Ads not configured. Add META_ACCESS_TOKEN and META_AD_ACCOUNT_ID to env vars.';
    const fields = 'spend,impressions,clicks,ctr,cpc,cpm,reach,frequency,actions,cost_per_action_type';
    const insightRes  = await fetch(`https://graph.facebook.com/v19.0/${accountId}/insights?fields=${fields}&date_preset=${datePreset}&access_token=${token}`);
    const insightData = await insightRes.json();
    if (insightData.error) throw new Error(insightData.error.message);
    const d        = insightData.data?.[0] || {};
    const leads    = (d.actions || []).find(a => a.action_type === 'lead')?.value     || '0';
    const purchases= (d.actions || []).find(a => a.action_type === 'purchase')?.value || '0';
    const spend    = parseFloat(d.spend || 0).toFixed(2);
    const ctr      = parseFloat(d.ctr   || 0).toFixed(2);
    const cpc      = parseFloat(d.cpc   || 0).toFixed(2);
    const cpm      = parseFloat(d.cpm   || 0).toFixed(2);
    // Form funnel CPL (lead action = Form campaigns only; VSL funnel not counted here)
    const formCpl  = parseInt(leads) > 0 ? (parseFloat(spend) / parseInt(leads)).toFixed(2) : 'N/A';
    // CAC = spend / Meta Purchase events. Fired via CAPI by Make scenario 5148801
    // '[PROD] GHL Opp Won → CAPI Purchase' (GHL Opp-Won workflow trigger) — the
    // replacement for the iClosed-fired pixel, live since the 2026-07-23 cutover
    // (verified end-to-end 2026-08-03: 3 purchase events in Meta, last 14d)
    const cac      = parseInt(purchases) > 0 ? (parseFloat(spend) / parseInt(purchases)).toFixed(2) : 'N/A';
    return [
      `Meta Ads — ${datePreset.replace(/_/g,' ')}:`,
      `Spend: $${spend} | Impressions: ${parseInt(d.impressions||0).toLocaleString()} | Reach: ${parseInt(d.reach||0).toLocaleString()}`,
      `Clicks: ${parseInt(d.clicks||0).toLocaleString()} | CTR: ${ctr}% | CPC: $${cpc} | CPM: $${cpm}`,
      leads !== '0'     ? `Form leads: ${leads} | Form CPL: $${formCpl}` : 'Form leads: 0 (no lead pixel fires — VSL funnel not counted)',
      purchases !== '0' ? `Sales (Meta Purchase events): ${purchases} | CAC: $${cac}` : 'Sales (Meta Purchase events): 0',
    ].join('\n');
  } catch (err) { return `Meta Ads summary error: ${err.message}`; }
}

// Structured Meta insights for an EXACT date window (CR calendar dates).
// The weekly recap needs numbers (not a preformatted string) so it can render
// WoW deltas and reconcile Meta form leads against the GHL lead feed. Returns
// null on any failure — the caller renders "unavailable" instead of embedding
// an error string in the report body like getMetaAdsSummary does.
async function getMetaAdsRange(sinceStr, untilStr) {
  try {
    const accountId = process.env.META_AD_ACCOUNT_ID;
    const token     = process.env.META_ACCESS_TOKEN;
    if (!accountId || !token) return null;
    const fields    = 'spend,impressions,clicks,ctr,cpc,cpm,reach,frequency,actions,cost_per_action_type';
    const timeRange = encodeURIComponent(JSON.stringify({ since: sinceStr, until: untilStr }));
    const res  = await fetch(`https://graph.facebook.com/v19.0/${accountId}/insights?fields=${fields}&time_range=${timeRange}&access_token=${token}`);
    const json = await res.json();
    if (json.error) throw new Error(json.error.message);
    const d         = json.data?.[0] || {};
    const spend     = parseFloat(d.spend || 0);
    const leads     = parseInt((d.actions || []).find(a => a.action_type === 'lead')?.value     || '0', 10);
    const purchases = parseInt((d.actions || []).find(a => a.action_type === 'purchase')?.value || '0', 10);
    return {
      spend,
      impressions: parseInt(d.impressions || 0, 10),
      reach:       parseInt(d.reach || 0, 10),
      clicks:      parseInt(d.clicks || 0, 10),
      ctr:         parseFloat(d.ctr || 0),
      cpc:         parseFloat(d.cpc || 0),
      cpm:         parseFloat(d.cpm || 0),
      leads,
      purchases,
      formCpl: leads     > 0 ? +(spend / leads).toFixed(2)     : null,
      cac:     purchases > 0 ? +(spend / purchases).toFixed(2) : null,
    };
  } catch (err) {
    console.error('getMetaAdsRange error:', err.message);
    return null;
  }
}

async function getMetaCampaigns(datePreset = 'last_7d', limit = 10) {
  try {
    const accountId = process.env.META_AD_ACCOUNT_ID;
    const token     = process.env.META_ACCESS_TOKEN;
    if (!accountId || !token) return 'Meta Ads not configured.';
    const fields = 'name,status,objective,spend,impressions,clicks,ctr,cpc,actions,cost_per_action_type';
    const url  = `https://graph.facebook.com/v19.0/${accountId}/campaigns?fields=id,name,status,objective,insights.date_preset(${datePreset}){${fields}}&limit=${limit}&access_token=${token}`;
    const res  = await fetch(url);
    const data = await res.json();
    if (data.error) throw new Error(data.error.message);
    const campaigns = data.data || [];
    if (!campaigns.length) return 'No campaigns found.';
    const lines = campaigns.map(c => {
      const ins    = c.insights?.data?.[0] || {};
      const spend  = parseFloat(ins.spend || 0).toFixed(2);
      const leads  = (ins.actions || []).find(a => a.action_type === 'lead')?.value || '0';
      const status = c.status === 'ACTIVE' ? '🟢' : c.status === 'PAUSED' ? '⏸️' : '🔴';
      return `${status} ${c.name}\n   Spend: $${spend} | Clicks: ${ins.clicks||0} | CTR: ${parseFloat(ins.ctr||0).toFixed(2)}%${leads!=='0'?` | Leads: ${leads}`:''}`;
    });
    return `Campaigns (${datePreset.replace(/_/g,' ')}) — ${campaigns.length} found:\n\n${lines.join('\n\n')}`;
  } catch (err) { return `Meta campaigns error: ${err.message}`; }
}

async function getMetaAdSets(campaignId = null, datePreset = 'last_7d') {
  try {
    const accountId = process.env.META_AD_ACCOUNT_ID;
    const token     = process.env.META_ACCESS_TOKEN;
    if (!accountId || !token) return 'Meta Ads not configured.';
    const fields   = 'name,status,daily_budget,lifetime_budget,spend,impressions,clicks,ctr,cpc,actions';
    const endpoint = campaignId ? `${campaignId}/adsets` : `${accountId}/adsets`;
    const url  = `https://graph.facebook.com/v19.0/${endpoint}?fields=id,name,status,daily_budget,insights.date_preset(${datePreset}){${fields}}&limit=20&access_token=${token}`;
    const res  = await fetch(url);
    const data = await res.json();
    if (data.error) throw new Error(data.error.message);
    const adsets = data.data || [];
    if (!adsets.length) return 'No ad sets found.';
    const lines = adsets.map(a => {
      const ins    = a.insights?.data?.[0] || {};
      const spend  = parseFloat(ins.spend || 0).toFixed(2);
      const budget = a.daily_budget ? `$${(parseInt(a.daily_budget)/100).toFixed(0)}/day` : 'No daily budget';
      const leads  = (ins.actions || []).find(x => x.action_type === 'lead')?.value || '0';
      const status = a.status === 'ACTIVE' ? '🟢' : a.status === 'PAUSED' ? '⏸️' : '🔴';
      return `${status} ${a.name} | Budget: ${budget}\n   Spend: $${spend} | Clicks: ${ins.clicks||0} | CTR: ${parseFloat(ins.ctr||0).toFixed(2)}%${leads!=='0'?` | Leads: ${leads}`:''}`;
    });
    return `Ad Sets (${datePreset.replace(/_/g,' ')}):\n\n${lines.join('\n\n')}`;
  } catch (err) { return `Meta ad sets error: ${err.message}`; }
}

async function getMetaAds(adSetId = null, datePreset = 'last_7d') {
  try {
    const accountId = process.env.META_AD_ACCOUNT_ID;
    const token     = process.env.META_ACCESS_TOKEN;
    if (!accountId || !token) return 'Meta Ads not configured.';
    const endpoint = adSetId ? `${adSetId}/ads` : `${accountId}/ads`;
    const fields   = 'name,status,spend,impressions,clicks,ctr,cpc,actions';
    const url  = `https://graph.facebook.com/v19.0/${endpoint}?fields=id,name,status,insights.date_preset(${datePreset}){${fields}}&limit=20&access_token=${token}`;
    const res  = await fetch(url);
    const data = await res.json();
    if (data.error) throw new Error(data.error.message);
    const ads = data.data || [];
    if (!ads.length) return 'No ads found.';
    const lines = ads.map(a => {
      const ins    = a.insights?.data?.[0] || {};
      const spend  = parseFloat(ins.spend || 0).toFixed(2);
      const leads  = (ins.actions || []).find(x => x.action_type === 'lead')?.value || '0';
      const status = a.status === 'ACTIVE' ? '🟢' : a.status === 'PAUSED' ? '⏸️' : '🔴';
      return `${status} ${a.name}\n   Spend: $${spend} | Impressions: ${parseInt(ins.impressions||0).toLocaleString()} | Clicks: ${ins.clicks||0} | CTR: ${parseFloat(ins.ctr||0).toFixed(2)}%${leads!=='0'?` | Leads: ${leads}`:''}`;
    });
    return `Ads (${datePreset.replace(/_/g,' ')}):\n\n${lines.join('\n\n')}`;
  } catch (err) { return `Meta ads error: ${err.message}`; }
}

// ─── ACTIVATION CALL DATE ─────────────────────────────────────────────────────
// The real activation call date — the start line of the 14-day Build & Release
// window. Read ONLY from the dashboard CRM. Clients are the dashboard's domain;
// GHL is the sales spine and holds no fulfillment state, so nothing here reads a
// GHL contact field or tag.
//
// Why this exists: the portal's "Activation Call Completed" checkbox is a
// fulfillment tick, not the call. Measured 2026-08-18 across 16 clients with both
// dates, the checkbox lagged the real call by a MEDIAN of 6 days and by as much as
// 40 — on a 14-day window. Every Day-N figure anchored on the checkbox understated
// elapsed time, so clients read younger and less overdue than they were.
//
// Two CRM sources, and the order matters:
//   1. flywheel_activation_workflow.activation_call_done — the canonical anchor
//      (its table comment has documented the 14-day count since March, and the
//      B&R clock spec anchors the billing window on it). BUT completed_at is only
//      a real held-timestamp when the row came from the GHL appointment webhook.
//      Rows ticked by hand carry a bulk timestamp — the historical backfill
//      stamped calls held in May with 2026-06-19 — so trusting those blindly
//      reintroduces exactly the drift this function exists to remove. Only
//      metadata.source = 'ghl_webhook' rows are treated as real.
//   2. flywheel_ai_onboarding.activation_call_scheduled_for — the booking-side
//      date (GHL embed on the client portal, plus a Fathom-verified backfill of
//      19 historical clients). Real dates, so a good second choice.
// Anything else falls through to the checkbox, which the anchor labels as soft.
async function loadActivationDates() {
  const heldByClientId = new Map();   // client_dashboards.id → ISO (real held event)
  const dateByEmail    = new Map();   // lowercased email      → ISO (booking side)

  const { data: steps, error: stepErr } = await portalSupabase
    .from('flywheel_activation_workflow')
    .select('customer_id, completed_at, metadata')
    .eq('step_name', 'activation_call_done')
    .not('completed_at', 'is', null);
  if (stepErr) console.log(`[activation] activation_call_done read failed: ${stepErr.message}`);
  for (const r of steps || []) {
    if (r.customer_id && r.metadata?.source === 'ghl_webhook') heldByClientId.set(r.customer_id, r.completed_at);
  }

  let from = 0;
  const PAGE_SIZE = 200;
  while (true) {
    const { data: page, error } = await portalSupabase
      .from('flywheel_ai_onboarding')
      .select('email, activation_call_scheduled_for')
      .not('activation_call_scheduled_for', 'is', null)
      .range(from, from + PAGE_SIZE - 1);
    if (error) { console.log(`[activation] onboarding dates read failed: ${error.message}`); break; }
    if (!page || !page.length) break;
    for (const r of page) {
      if (r.email) dateByEmail.set(r.email.toLowerCase(), r.activation_call_scheduled_for);
    }
    if (page.length < PAGE_SIZE) break;
    from += PAGE_SIZE;
  }

  // Returned as a lookup so callers pass the client row and stay ignorant of which
  // CRM table won — the sources will shift as the webhook route fills in.
  return function activationFor(dash) {
    if (!dash) return null;
    return heldByClientId.get(dash.id)
      || dateByEmail.get((dash.email || '').toLowerCase())
      || null;
  };
}

// ─── SHARED DAY-ANCHOR RESOLUTION ────────────────────────────────────────────
// Single source of truth for "Day N" math. Returns the raw anchor DATE so every
// consumer can print the calendar date, not just the day count. titleOf(templateId)
// must return the template title string ('' if unknown) — callers keep their
// existing map shapes and pass a tiny closure. Anchor priority: phase_3
// stabilization_started_at → real activation call date → activation call checkbox
// completed_at → portal created_at.
//
// activationISO is the real call date (see loadActivationDates). When it is absent
// we still fall back to the checkbox, but the label says so — a report reading
// "since activation call (checkbox)" is telling you that client's Day N is soft and
// probably optimistic.
function resolveDayAnchor(dash, activities, titleOf, activationISO = null) {
  const activation = (activities || []).find(a =>
    (titleOf(a.template_id) || '').toLowerCase().includes('activation call') && a.completed_at);
  let startISO, label;
  if (dash.customer_status === 'phase_3' && dash.stabilization_started_at) {
    startISO = dash.stabilization_started_at; label = 'since stabilization start';
  } else if (activationISO) {
    startISO = activationISO;                 label = 'since activation call';
  } else if (activation) {
    startISO = activation.completed_at;       label = 'since activation call (checkbox)';
  } else if (dash.created_at) {
    startISO = dash.created_at;               label = 'since portal creation';
  } else {
    return { startDate: null, daysSince: null, label: 'no anchor date', anchorDate: null };
  }
  const startDate = new Date(startISO);
  const daysSince = Math.floor((Date.now() - startDate.getTime()) / (1000 * 60 * 60 * 24));
  // en-CA gives YYYY-MM-DD; CR timezone so an evening UTC timestamp doesn't roll to the next day
  const anchorDate = startDate.toLocaleDateString('en-CA', { timeZone: 'America/Costa_Rica' });
  return { startDate, daysSince, label, anchorDate };
}

// ─── PORTAL: CLIENT STATUS ────────────────────────────────────────────────────
// FIX 2: Single definition only. Queries client_dashboards (correct schema).
// The old customer_onboarding-based duplicate has been removed entirely.
async function getClientStatus(clientName = null) {
  try {
    // Paginate through ALL active clients — no hard limit.
    // Supabase default page size is 1000. We paginate to handle any future scale.
    let dashboards = [];
    let dashFrom = 0;
    const PAGE_SIZE = 200;
    while (true) {
      let dashQuery = portalSupabase
        .from('client_dashboards')
        .select('id, client_name, email, customer_status, customer_type, is_active, created_at, stabilization_started_at, linkedin_handler')
        .eq('is_active', true)
        .order('customer_status', { ascending: true })
        .range(dashFrom, dashFrom + PAGE_SIZE - 1);
      if (clientName) {
        dashQuery = dashQuery.or(`client_name.ilike.%${clientName}%,email.ilike.%${clientName}%`);
      }
      const { data: page, error: dashErr } = await dashQuery;
      if (dashErr) throw dashErr;
      if (!page || !page.length) break;
      dashboards = dashboards.concat(page);
      if (page.length < PAGE_SIZE) break; // last page — no more rows
      dashFrom += PAGE_SIZE;
    }
    const dashErr = null; // kept for downstream compatibility
    if (dashErr) throw dashErr;
    if (!dashboards || !dashboards.length) return clientName ? `No client found matching: ${clientName}` : 'No active clients found in portal.';

    const { data: templates } = await portalSupabase
      .from('customer_activity_templates')
      .select('id, title, order_index')
      .eq('is_active', true);
    const templateMap = {};
    (templates || []).forEach(t => { templateMap[t.id] = t; });
    const activationFor = await loadActivationDates();

    const results = await Promise.all(dashboards.map(async (dash) => {
      const { data: onboarding } = await portalSupabase
        .from('customer_onboarding')
        .select('id, first_name, last_name, company, services_products, ideal_customer, service_tier, payment_status')
        .eq('email', dash.email)
        .limit(1);
      const ob = onboarding?.[0];
      const customerId = ob?.id;
      let activities = [];

      // customer_activities.customer_id links to client_dashboards.id directly (confirmed via schema query).
      // The old path via customer_onboarding.id returns empty results for most clients.
      // Query by client_dashboards.id first (primary), then merge any results from customer_onboarding.id.
      const { data: actsByDashId } = await portalSupabase
        .from('customer_activities')
        .select('id, template_id, status, assigned_to, completed_at, notes')
        .eq('customer_id', dash.id);
      activities = actsByDashId || [];

      // Also query by onboarding ID if different, merge without duplicates
      if (customerId && customerId !== dash.id) {
        const { data: actsByObId } = await portalSupabase
          .from('customer_activities')
          .select('id, template_id, status, assigned_to, completed_at, notes')
          .eq('customer_id', customerId);
        if (actsByObId && actsByObId.length > 0) {
          const existingIds = new Set(activities.map(a => a.id));
          const merged = actsByObId.filter(a => !existingIds.has(a.id));
          activities = [...activities, ...merged];
        }
      }
      const total   = activities.length;
      const live    = activities.filter(a => a.status === 'live').length;
      const blocked = activities.filter(a => a.status === 'blocked').length;
      const phase1  = activities.filter(a => a.status === 'phase_1').length;
      const phase2  = activities.filter(a => a.status === 'phase_2').length;
      const blockedActs = activities.filter(a => a.status === 'blocked').map(a => templateMap[a.template_id]?.title || 'Unknown activity').join(', ');
      const pendingActs = activities
        .filter(a => a.status === 'phase_1' || a.status === 'phase_2')
        .sort((a,b) => (templateMap[a.template_id]?.order_index||99) - (templateMap[b.template_id]?.order_index||99))
        .slice(0, 3).map(a => templateMap[a.template_id]?.title || 'Unknown').join(', ');
      const statusLabel = {
        'live': '🟢 Live', 'phase_1': '🟡 Phase 1 – Optimization',
        'phase_2': '🔵 Phase 2 – Campaign Launch', 'phase_3': '🟣 Phase 3 – Stabilization',
        'blocked': '🔴 Blocked', 'phase_0': '🟠 Phase 0 – Onboarding',
      }[dash.customer_status] || `⚪ ${dash.customer_status}`;
      const statusEmoji = statusLabel.split(' ')[0];

      // Day 1 anchor — shared logic; anchorDate surfaces the raw calendar date
      const { daysSince, label: dayAnchor, anchorDate } = resolveDayAnchor(dash, activities, id => templateMap[id]?.title, activationFor(dash));
      const lines = [
        `${statusEmoji} ${dash.client_name || dash.email} [${(dash.customer_type || '').replace('flywheel-ai','Flywheel').replace('full-service','Full Service')}]`,
        `${statusLabel} | Day ${daysSince ?? '?'} ${dayAnchor}${anchorDate ? ` (${anchorDate})` : ''}`,
        total > 0 ? `Activities: ${live} live, ${phase1} phase_1 pending, ${phase2} phase_2 pending, ${blocked} blocked` : 'No activities tracked',
        blockedActs ? `🔴 Blocked on: ${blockedActs}` : '',
        pendingActs && !blockedActs ? `Next up: ${pendingActs}` : '',
        ob?.services_products && clientName ? `Service: ${ob.services_products.substring(0,120)}` : '',
        ob?.ideal_customer    && clientName ? `ICP: ${ob.ideal_customer.substring(0,100)}` : '',
      ].filter(Boolean);
      return lines.join('\n');
    }));

    const statusCounts = dashboards.reduce((acc, d) => { acc[d.customer_status] = (acc[d.customer_status] || 0) + 1; return acc; }, {});
    const header = clientName
      ? `Portal status for "${clientName}":\n\n`
      : `Portal — ${dashboards.length} active clients | 🟢 ${statusCounts.live||0} Live | 🟠 ${statusCounts.phase_0||0} Onboarding | 🟡 ${statusCounts.phase_1||0} Optimization | 🔵 ${statusCounts.phase_2||0} Campaign Launch | 🟣 ${statusCounts.phase_3||0} Stabilization | 🔴 ${statusCounts.blocked||0} Blocked\n\n`;
    return header + results.join('\n\n');
  } catch (err) {
    return `Portal client status error: ${err.message}`;
  }
}

// ─── PORTAL: PHASE 0 (PRE-PORTAL ONBOARDING) ─────────────────────────────────
// Reads v_phase0_fulfillment in the neurogrowth-proposals project. Phase 0 =
// clients who signed up for flywheel-ai but haven't gone live yet (go_live_at IS NULL).
// The view exposes a derived phase0_step:
//   1_awaiting_signup, 2_awaiting_terms, 3_awaiting_form,
//   4_awaiting_activation_call, 5_ready_for_handoff
async function getPhase0Clients() {
  try {
    const { data, error } = await portalSupabase
      .from('v_phase0_fulfillment')
      .select('id, email, first_name, last_name, company, status, phase0_step, days_in_phase0, terms_accepted_at, onboarding_completed_at, booking_calendar_url, dashboard_created, created_at')
      .order('phase0_step', { ascending: true })
      .order('created_at', { ascending: true });
    if (error) throw error;
    if (!data || !data.length) return 'Phase 0 — no clients currently in pre-portal onboarding.';

    const stepLabel = {
      '1_awaiting_signup':          '🟠 Awaiting portal signup',
      '2_awaiting_terms':           '🟡 Awaiting T&C acceptance',
      '3_awaiting_form':            '🔵 Awaiting onboarding form',
      '4_awaiting_activation_call': '🟣 Awaiting activation call booking',
      '5_ready_for_handoff':        '🟢 Ready for Phase 1 handoff',
    };

    const grouped = data.reduce((acc, r) => {
      (acc[r.phase0_step] = acc[r.phase0_step] || []).push(r);
      return acc;
    }, {});

    const sections = Object.keys(grouped).sort().map(step => {
      const rows = grouped[step];
      const lines = rows.map(r => {
        const name = [r.first_name, r.last_name].filter(Boolean).join(' ') || r.email;
        const co   = r.company ? ` (${r.company})` : '';
        const days = r.days_in_phase0 != null ? `Day ${r.days_in_phase0}` : '?';
        return `• ${name}${co} — ${days} | ${r.email}`;
      });
      return `${stepLabel[step] || step} (${rows.length}):\n${lines.join('\n')}`;
    });

    const counts = Object.entries(grouped).map(([k, v]) => `${k}: ${v.length}`).join(' | ');
    const header = `Phase 0 — ${data.length} clients in pre-portal onboarding | ${counts}\n\n`;
    return header + sections.join('\n\n');
  } catch (err) {
    return `Phase 0 clients error: ${err.message}`;
  }
}

// ─── PORTAL: READ-ONLY SQL (natural-language schema browsing) ─────────────────
const PORTAL_SQL_MAX_ROWS = 500;

function ensurePortalPg() {
  if (!portalPg) return 'Portal read-only DB not configured. Set PORTAL_READONLY_DATABASE_URL in .env.';
  return null;
}

async function listPortalTables() {
  const err = ensurePortalPg(); if (err) return err;
  try {
    const { rows } = await portalPg.query(`
      SELECT table_name, table_type
      FROM information_schema.tables
      WHERE table_schema = 'public'
      ORDER BY table_name`);
    if (!rows.length) return 'No tables found in public schema.';
    return rows.map(r => `${r.table_name} (${r.table_type === 'VIEW' ? 'view' : 'table'})`).join('\n');
  } catch (e) {
    return `list_portal_tables error: ${e.message}`;
  }
}

async function searchPortalSchema(keywords) {
  const err = ensurePortalPg(); if (err) return err;
  const tokens = (keywords || '').toLowerCase().split(/\s+/).filter(Boolean);
  if (!tokens.length) return 'Provide at least one keyword.';
  try {
    const likeClauses = tokens.map((_, i) => `(lower(table_name) LIKE $${i+1} OR lower(column_name) LIKE $${i+1})`).join(' OR ');
    const params = tokens.map(t => `%${t}%`);
    const sql = `
      SELECT table_name, column_name, data_type
      FROM information_schema.columns
      WHERE table_schema = 'public' AND (${likeClauses})
      ORDER BY table_name, ordinal_position`;
    const { rows } = await portalPg.query(sql, params);
    if (!rows.length) return `No tables or columns matched: ${keywords}`;
    const byTable = {};
    for (const r of rows) (byTable[r.table_name] ||= []).push(`${r.column_name} (${r.data_type})`);
    return Object.entries(byTable)
      .map(([t, cols]) => `${t}\n  - ${cols.join('\n  - ')}`)
      .join('\n\n');
  } catch (e) {
    return `search_portal_schema error: ${e.message}`;
  }
}

async function describePortalTable(tableName) {
  const err = ensurePortalPg(); if (err) return err;
  if (!tableName) return 'tableName is required.';
  try {
    const { rows } = await portalPg.query(`
      SELECT column_name, data_type, is_nullable
      FROM information_schema.columns
      WHERE table_schema = 'public' AND table_name = $1
      ORDER BY ordinal_position`, [tableName]);
    if (!rows.length) return `No columns found for ${tableName} (table may not exist or no read access).`;
    return rows.map(r => `${r.column_name}  ${r.data_type}${r.is_nullable === 'NO' ? ' NOT NULL' : ''}`).join('\n');
  } catch (e) {
    return `describe_portal_table error: ${e.message}`;
  }
}

async function queryPortalDb(sqlText) {
  const err = ensurePortalPg(); if (err) return err;
  if (!sqlText || typeof sqlText !== 'string') return 'ERROR: sql is required.';
  const cleaned = sqlText.trim().replace(/;+\s*$/, '');
  if (!/^(select|with)\s/i.test(cleaned)) return 'ERROR: only SELECT / WITH queries are allowed.';
  if (/;/.test(cleaned)) return 'ERROR: multiple statements are not allowed.';
  try {
    const wrapped = `SELECT * FROM (${cleaned}) _sub LIMIT ${PORTAL_SQL_MAX_ROWS}`;
    const { rows, rowCount } = await portalPg.query(wrapped);
    if (!rowCount) return 'Query returned 0 rows.';
    const truncated = rowCount >= PORTAL_SQL_MAX_ROWS ? `\n(capped at ${PORTAL_SQL_MAX_ROWS} rows)` : '';
    return JSON.stringify(rows, null, 2) + truncated;
  } catch (e) {
    return `Query error: ${e.message}`;
  }
}

// ─── PORTAL: WRITE-BACK ───────────────────────────────────────────────────────
const PORTAL_WRITE_WHITELIST = {
  client_dashboards:  ['notes', 'linkedin_handler', 'customer_status', 'is_active'],
  customer_onboarding: ['notes'],
};

async function updatePortalRecord(table, id, fields) {
  if (!PORTAL_WRITE_WHITELIST[table])
    return `Write blocked: table "${table}" is not on the write whitelist. Allowed: ${Object.keys(PORTAL_WRITE_WHITELIST).join(', ')}`;
  const allowed  = PORTAL_WRITE_WHITELIST[table];
  const filtered = Object.fromEntries(Object.entries(fields).filter(([k]) => allowed.includes(k)));
  if (!Object.keys(filtered).length)
    return `Write blocked: none of the provided fields are writable on "${table}". Allowed: ${allowed.join(', ')}`;
  const { error } = await portalSupabase.from(table).update(filtered).eq('id', id);
  if (error) return `Update error: ${error.message}`;
  return `Updated ${Object.keys(filtered).join(', ')} on ${table} row ${id}.`;
}

// ─── PORTAL: ALERTS ───────────────────────────────────────────────────────────
async function getPortalAlerts({ mode = 'full' } = {}) {
  // mode: 'full'  → every blocked / at-risk / overdue client (used by weekly + ad-hoc tool calls)
  //       'daily' → suppress long-standing fully-blocked noise so the daily EOD only shows fresh issues:
  //                   • drop customer_status='blocked' (🔴 BLOCKED tier)
  //                   • drop OVERDUE clients >30 days in with no activity completed in the last 14 days
  //                   • drop Phase 0 OVERDUE >30 days (aged signups)
  try {
    const { data: dashboards, error } = await portalSupabase
      .from('client_dashboards')
      .select('id, client_name, email, customer_status, customer_type, created_at')
      .eq('is_active', true)
      .in('customer_status', ['blocked', 'phase_1', 'phase_2'])
      .order('created_at', { ascending: true });
    if (error) throw error;
    if (!dashboards || !dashboards.length) return '✅ No blocked or at-risk clients. All clients on track.';
    const dailyMode = mode === 'daily';
    const fourteenDaysMs = 14 * 24 * 60 * 60 * 1000;

    // Fetch templates upfront so we can resolve activity titles and find activation call
    const { data: templates } = await portalSupabase
      .from('customer_activity_templates')
      .select('id, title');
    const templateMap = {};
    (templates || []).forEach(t => { templateMap[t.id] = t.title; });
    const activationFor = await loadActivationDates();

    const now    = Date.now();
    const alerts = [];
    for (const dash of dashboards) {
      // ── Dual-path activity query: dash.id first, merge onboarding.id if different ──
      const { data: actsByDashId } = await portalSupabase
        .from('customer_activities')
        .select('template_id, status, notes, completed_at')
        .eq('customer_id', dash.id);
      let allActs = actsByDashId || [];

      const { data: onboarding } = await portalSupabase.from('customer_onboarding').select('id').eq('email', dash.email).limit(1);
      if (onboarding?.[0] && onboarding[0].id !== dash.id) {
        const { data: actsByObId } = await portalSupabase
          .from('customer_activities')
          .select('template_id, status, notes, completed_at')
          .eq('customer_id', onboarding[0].id);
        if (actsByObId?.length) {
          const existingIds = new Set(allActs.map(a => a.template_id + a.status));
          allActs = [...allActs, ...actsByObId.filter(a => !existingIds.has(a.template_id + a.status))];
        }
      }

      // ── Day anchor: shared logic (real activation date → checkbox → created_at) ──
      const anchor = resolveDayAnchor(dash, allActs, id => templateMap[id], activationFor(dash));
      const daysSince = anchor.daysSince ?? 0;
      const anchorSuffix = anchor.anchorDate ? ` ${anchor.label} (${anchor.anchorDate})` : '';

      // ── Blocked activity details ──
      const blockedActs = allActs.filter(a => a.status === 'blocked');
      const blockedDetails = blockedActs.map(a => templateMap[a.template_id] || 'Unknown').join(', ');

      if (dash.customer_status === 'blocked') {
        if (dailyMode) continue; // long-standing blocked clients move to weekly wrap-up
        alerts.push(`🔴 BLOCKED — ${dash.client_name || dash.email} (Day ${daysSince}${anchorSuffix})${blockedDetails ? ` | Blocked on: ${blockedDetails}` : ''}`);
      } else if (daysSince >= 14) {
        if (dailyMode && daysSince > 30) {
          const recentActivity = allActs.some(a => a.completed_at && (now - new Date(a.completed_at).getTime()) <= fourteenDaysMs);
          if (!recentActivity) continue; // aged + dormant — suppress from daily
        }
        alerts.push(`🔴 OVERDUE — ${dash.client_name || dash.email} | ${dash.customer_status} | Day ${daysSince}${anchorSuffix} (past 14-day window)`);
      } else if (daysSince >= 7) {
        alerts.push(`🟡 AT RISK — ${dash.client_name || dash.email} | ${dash.customer_status} | Day ${daysSince}${anchorSuffix}`);
      }
    }
    // ── Phase 0 alerts — pre-portal clients stuck or ready for handoff ──────────
    const { data: phase0 } = await portalSupabase
      .from('v_phase0_fulfillment')
      .select('id, email, first_name, last_name, company, phase0_step, days_in_phase0')
      .order('days_in_phase0', { ascending: false });

    const phase0Alerts = [];
    for (const r of (phase0 || [])) {
      const name = [r.first_name, r.last_name].filter(Boolean).join(' ') || r.email;
      const co   = r.company ? ` (${r.company})` : '';
      const days = r.days_in_phase0 ?? 0;
      const stepLabels = {
        '1_awaiting_signup':          'awaiting portal signup',
        '2_awaiting_terms':           'awaiting T&C acceptance',
        '3_awaiting_form':            'awaiting onboarding form',
        '4_awaiting_activation_call': 'awaiting activation call booking',
        '5_ready_for_handoff':        'ready for Phase 1 handoff',
      };
      const stepStr = stepLabels[r.phase0_step] || r.phase0_step;
      if (r.phase0_step === '5_ready_for_handoff') {
        phase0Alerts.push(`🟢 PHASE 0 HANDOFF READY — ${name}${co} | ${stepStr} | Day ${days} — move to Phase 1`);
      } else if (days >= 14) {
        if (dailyMode && days > 30) continue; // aged Phase 0 stalls move to weekly wrap-up
        phase0Alerts.push(`🔴 PHASE 0 OVERDUE — ${name}${co} | ${stepStr} | Day ${days} (past 14-day threshold)`);
      } else if (days >= 7) {
        phase0Alerts.push(`🟡 PHASE 0 AT RISK — ${name}${co} | ${stepStr} | Day ${days}`);
      }
    }

    const allAlerts = [...phase0Alerts, ...alerts];
    if (!allAlerts.length) return '✅ No critical alerts. All clients on track across Phase 0 and active portal.';
    const p0Header = phase0Alerts.length ? `\n📋 Phase 0 Pre-Portal (${phase0Alerts.length}):\n${phase0Alerts.join('\n')}\n` : '';
    const p1Header = alerts.length      ? `\n🚨 Active Portal (${alerts.length}):\n${alerts.join('\n')}\n` : '';
    return `Launch & block alerts (${allAlerts.length} clients):${p0Header}${p1Header}`;
  } catch (err) {
    return `Portal alerts error: ${err.message}`;
  }
}

// ─── SALES INTELLIGENCE (GHL + RevOps) ───────────────────────────────────────
// Queries revops_* tables in the portal Supabase project.
// Since the 2026-07-23 cutover, rows are populated by the GHL webhook pipeline
// (source='ghl', ghl_appointment_id, setter_id/closer_id = roster emails);
// iClosed rows (source='iclosed', iclosed_call_id) are frozen history.
const SALES_TEAM_MAP = {
  // ── SETTERS — GHL user IDs ───────────────────────────────────────────────
  'cuttpcov7ztlvyjkhdx8': 'Joseph Salazar',   'cUTTPGov7ZTLvyjKHdX8': 'Joseph Salazar', // historical — no longer active
  'zcmdiz2eerapd80w2zop': 'Oscar M',          'ZcmdIz2EEraPd80W2zop': 'Oscar M',
  'n8mvtuhbbby7qppqnmr7': 'William B',        'N8mvtuHbbbY7QppqNMr7': 'William B',
  'wdjte1temxfr0lpi5rgv': 'Sebastian S',      'Wdjte1temxfR0lpi5RGV': 'Sebastian S',
  't28sdyo0eaunhjhl4jyu': 'Josue D',          'T28SDyO0EAUNHJHl4jyu': 'Josue D',
  '5orsahkh2joujb5fczrp': 'Debbanny Romero',  '5OrSaHkh2joUjB5FCZrP': 'Debbanny Romero', // historical — no longer active

  // ── CLOSERS — roster emails (GHL rows carry these in closer_id) ─────────
  'ronny.duarte@neurogrowth.io':  'Ron Duarte',
  'jose.neurogrowth@gmail.com':   'Jose Carranza',
  'jonathan.madriz.neurogrowth@gmail.com': 'Jonathan Madriz',

  // ── SETTERS — roster emails (GHL rows carry these in setter_id) ─────────
  'joseph.neurogrowth@gmail.com':   'Joseph Salazar', // historical — no longer active
  'Salazcamjos@gmail.com':          'Joseph Salazar', // historical — no longer active
  'oscar.neurogrowth@gmail.com':    'Oscar M',
  'william.neurogrowth@gmail.com':  'William B',
  'sebastian.neurogrowth@gmail.com': 'Sebastian S',
  'josue.duran@neurogrowth.io':     'Josue D',
  'debbanny.neurogrowth@gmail.com': 'Debbanny Romero', // historical — no longer active

  // ── FALLBACK — raw GHL user IDs (unmapped rows surface these) ───────────
  'gqymykpddltdxvbkfl2c': 'Jonathan Madriz', 'gqYMYkpDDlTdxvBkfl2C': 'Jonathan Madriz',
  'izlta0jy5orkymvyitjv': 'Jose Carranza',   'izLTA0jy5OrKyMvyItjV': 'Jose Carranza',
  'zogw530idnpofqqnfssc': 'Ron Duarte',      'zoGW530iDnPOFqQNfssc': 'Ron Duarte',
};
// scheduled_start in revops_appointments is now true UTC (upstream sync fixed
// in dash.neurogrowth.io PR #2 — normalizer prefers iClosed's
// event.utc_start_time over the mislabeled event.start_time). Format with
// America/Costa_Rica to display CR wall-clock. Rows inserted before the
// upstream fix will still display 6h early until they age out — accepted
// tradeoff in lieu of a one-time backfill.
function formatICTime(scheduledStart, opts = { hour: '2-digit', minute: '2-digit' }) {
  if (!scheduledStart) return null;
  return new Date(scheduledStart).toLocaleString('en-US', { ...opts, timeZone: 'America/Costa_Rica' });
}

function resolveSalesMember(id) {
  if (!id) return 'Unknown';
  const resolved = SALES_TEAM_MAP[id] || SALES_TEAM_MAP[id.toLowerCase()];
  if (resolved) return resolved;
  // Unmapped id (new hire's GHL user id, or a roster gap) — loud, so it gets
  // added to SALES_TEAM_MAP instead of surfacing as a garbage leaderboard name.
  console.warn(`resolveSalesMember: unmapped sales id "${id}" — add to SALES_TEAM_MAP`);
  return id;
}

// True when a stats-map key is an unresolved raw id (GHL user id), not a person.
// Used by leaderboard formatters to drop garbage rows instead of rendering them.
function isUnresolvedSalesId(name) {
  return !!name && name !== 'Unknown' && !name.includes('@') && !name.includes(' ') && /^[A-Za-z0-9]{18,}$/.test(name);
}

// Sales reports must only count flywheel sales calls.
// Legacy iClosed rows: exclude non-flywheel calendars (partner-consulting 1:1s)
// via the frozen iclosed_webhook_deliveries slug scan below (decays naturally as
// rows age past the 120d window). GHL rows (iclosed_call_id NULL) always pass —
// they are sales-only BY CONSTRUCTION: dash's webhook ingestion is gated on the
// GHL_SALES_CALENDAR_IDS allowlist. If a non-sales calendar ever joins GHL,
// gate it in dash — do NOT add a second allowlist here (it would drift).
//
// THE SAME FACT IS ALSO STATED IN REVI: revi.calendar_groups.track = 'sales'
// (REVI migration 0014) mirrors the GHL calendar grouping and drives REVI's own
// routing. dash's allowlist and REVI's taxonomy must agree; as of 2026-08-18
// both mean {fYQJCzbk4hvV0brpJqoE (Appointment), 0qwExROqOMRBXVmY93i5
// (Appointment - ONLY RON), HXLeEjxpa0gdiTPNiAzc (Self Serving),
// KRTGx8XteIJSCcKAShHS (Intro)}. runReviCrossChecks DETECTS a disagreement
// (Check 1c) via GHL_SALES_CALENDAR_IDS_EXPECTED — detection only; still no
// second live allowlist here.
const SALES_FLYWHEEL_SLUGS = new Set(['linkedin-flywheel', 'linkedin-flywheel-vsl', 'linkedin-flywheel-doc', 'estrategia-linkedin-flywheel-selfserving-ron']);
// Permanent boot-time memo (was a 10-min TTL): iclosed_webhook_deliveries is
// frozen since the 2026-07-23 cutover, so the answer can never change within a
// process lifetime. The 120d window empties ~2026-11-20, after which this
// returns an empty set forever and the function + iclosed_call_id filtering
// can be deleted outright.
let _nonFlywheelCache = { ids: null, fetchedAt: 0 };
async function getNonFlywheelCallIds() {
  if (_nonFlywheelCache.ids) {
    return _nonFlywheelCache.ids;
  }
  const since = new Date(Date.now() - 120 * 24 * 60 * 60 * 1000).toISOString();
  const { data, error } = await portalSupabase
    .from('iclosed_webhook_deliveries')
    .select('payload')
    .eq('normalized_event_type', 'Call booked')
    .gte('created_at', since)
    .limit(5000);
  if (error) {
    console.error('getNonFlywheelCallIds error:', error.message);
    return new Set();
  }
  const exclude = new Set();
  for (const r of (data || [])) {
    const p = Array.isArray(r.payload) ? r.payload[0] : r.payload;
    const slug = p?.event_type?.slug;
    const callId = p?.event?.callPreviewId;
    if (!callId || !slug) continue;
    if (!SALES_FLYWHEEL_SLUGS.has(slug)) exclude.add(callId);
  }
  _nonFlywheelCache = { ids: exclude, fetchedAt: Date.now() };
  return exclude;
}

// Convenience: filter an array of revops_appointments rows in-place semantics.
// Drops (1) legacy iClosed rows on non-flywheel calendars, and (2) synthetic
// pre-book disqualification rows (ghl_appointment_id = 'opp:{opportunityId}')
// that dash creates for outcome-only opportunities — not real calls, so they
// must never enter booked/held/show-rate denominators. Rows whose select
// omitted ghl_appointment_id pass through (degrade open, never over-filter).
function filterFlywheelAppts(rows, excludeIds) {
  return (rows || []).filter(a => {
    if (typeof a.ghl_appointment_id === 'string' && a.ghl_appointment_id.startsWith('opp:')) return false;
    if (excludeIds && excludeIds.size && a.iclosed_call_id && excludeIds.has(a.iclosed_call_id)) return false;
    return true;
  });
}

async function getSalesIntelligence(query) {
  try {
    const q = (query || '').toLowerCase();
    const now = new Date();
    // Costa Rica is UTC−6, no DST. All day/week/month boundaries below must
    // anchor to the CR calendar date, not the server's (UTC) local date —
    // otherwise "today" drifts up to 6 hours and calls near CR midnight land
    // on the wrong day. Same fix already applied to LEADS TODAY below; this
    // mirrors it for TODAY'S CALLS / week / month.
    const toDateStr = (d) => d.toLocaleDateString('en-CA', { timeZone: 'America/Costa_Rica' });
    const todayStr   = toDateStr(now);
    const todayStart = new Date(`${todayStr}T06:00:00Z`); // CR midnight, as a UTC instant
    const todayEnd   = new Date(todayStart.getTime() + 24 * 60 * 60 * 1000 - 1);

    const crDow      = new Date(`${todayStr}T00:00:00Z`).getUTCDay(); // 0=Sun..6=Sat for the CR calendar date
    const weekStart  = new Date(todayStart.getTime() - crDow * 24 * 60 * 60 * 1000);
    const weekStartStr = toDateStr(weekStart);

    const [crYear, crMonth] = todayStr.split('-').map(Number);
    const monthStartStr = `${crYear}-${String(crMonth).padStart(2, '0')}-01`;
    const monthStart    = new Date(`${monthStartStr}T06:00:00Z`);

    // ── LEADS TODAY (authoritative — lead_posts + setter_claims) ───────────
    // Must run before the "today calls" branch ("leads today" also contains
    // "today") and before the portalSupabase fetch / empty-data early return,
    // because lead data lives in the primary `supabase` project, not the portal.
    // Fixes the undercount where the report inferred owners from immutable
    // #ng-sales-goats post text: ✋/✅ claims only write setter_claims, never
    // edit the post, so setters who claimed by reaction were uncredited.
    if (q.includes('lead') && (q.includes('today') || q.includes('hoy'))) {
      // Costa Rica is UTC−6, no DST (same assumption as formatICTime).
      const crDate = now.toLocaleDateString('en-CA', { timeZone: 'America/Costa_Rica' });
      const dayStartUtc = new Date(`${crDate}T06:00:00Z`);
      const dayEndUtc   = new Date(dayStartUtc.getTime() + 24 * 60 * 60 * 1000);

      const { data: leadRows, error: leadErr } = await supabase
        .from('lead_posts')
        .select('slack_message_ts, contact_id, full_name, source, posted_at')
        .gte('posted_at', dayStartUtc.toISOString())
        .lt('posted_at', dayEndUtc.toISOString())
        .order('posted_at', { ascending: true });
      if (leadErr) return `Leads-today error: ${leadErr.message}`;

      // Dedupe to one logical lead per Slack post. The FB→WA dup path upserts
      // a second contact_id row that shares the original's slack_message_ts;
      // keep the earliest row per ts but track every contact_id in the group.
      const byTs = new Map(); // ts → { source, contactIds:Set }
      for (const r of (leadRows || [])) {
        if (!r.slack_message_ts) continue;
        let g = byTs.get(r.slack_message_ts);
        if (!g) { g = { source: r.source || null, contactIds: new Set() }; byTs.set(r.slack_message_ts, g); }
        if (r.contact_id) g.contactIds.add(r.contact_id);
      }
      const totalLeads = byTs.size;

      // Resolve owner = whoever has claimed any contact_id in the lead group
      // (claim may have happened any time, not just today).
      const SETTER_BY_SLACK_ID = {
        'U0B1S1UMH9P': 'Oscar M',
        'U0B16P6DQ2F': 'William B',
        'U0BFA4SRVQC': 'Sebastian S',
      };
      const allContactIds = [...new Set([...byTs.values()].flatMap(g => [...g.contactIds]))];
      const claimByContact = new Map(); // contact_id → { name, claimed_at }
      if (allContactIds.length) {
        const { data: claimRows } = await supabase
          .from('setter_claims')
          .select('ghl_contact_id, claimed_by_setter_name, claimed_by_slack_user_id, claimed_at')
          .in('ghl_contact_id', allContactIds)
          .order('claimed_at', { ascending: true });
        for (const c of (claimRows || [])) {
          if (claimByContact.has(c.ghl_contact_id)) continue; // earliest claim wins
          const name = c.claimed_by_setter_name
            || SETTER_BY_SLACK_ID[c.claimed_by_slack_user_id]
            || (c.claimed_by_slack_user_id ? `setter ${c.claimed_by_slack_user_id}` : null);
          claimByContact.set(c.ghl_contact_id, { name, claimed_at: c.claimed_at });
        }
      }

      const bySetter = {};
      const bySource = {};
      let unclaimed = 0;
      for (const g of byTs.values()) {
        const srcKey = g.source || 'Unknown';
        bySource[srcKey] = (bySource[srcKey] || 0) + 1;
        // Pick the earliest claim across all contact_ids in this lead group.
        let owner = null, ownerAt = null;
        for (const cid of g.contactIds) {
          const cl = claimByContact.get(cid);
          if (cl && cl.name && (!ownerAt || cl.claimed_at < ownerAt)) { owner = cl.name; ownerAt = cl.claimed_at; }
        }
        if (owner) bySetter[owner] = (bySetter[owner] || 0) + 1;
        else unclaimed++;
      }

      const lines = ['LEADS_TODAY_DATA (authoritative — render verbatim, do NOT recount from Slack):'];
      lines.push(`Total new leads today: ${totalLeads}`);
      lines.push('By setter (owner = whoever has claimed the lead):');
      const setterEntries = Object.entries(bySetter).sort((a, b) => b[1] - a[1]);
      if (setterEntries.length) setterEntries.forEach(([n, c]) => lines.push(`  ${n}: ${c}`));
      else lines.push('  (no leads claimed yet)');
      lines.push(`Unclaimed (no setter claimed yet): ${unclaimed}`);
      lines.push('Sources:');
      Object.entries(bySource).sort((a, b) => b[1] - a[1]).forEach(([s, c]) => lines.push(`  ${s}: ${c}`));
      return lines.join('\n');
    }

    // EOD self-report tables retired at the GHL cutover (2026-07-23) — all
    // performance sections now derive from appointments + outcomes.

    // ── Query appointments for individual call lookups ─────────────────────
    const { data: appointmentsRaw } = await portalSupabase
      .from('revops_appointments')
      .select(`
        id, setter_id, closer_id, booked_at, scheduled_start, source,
        attended, no_show_reason, reschedule_count, meeting_type, iclosed_call_id, ghl_appointment_id,
        prospect:prospect_id ( full_name, company, email, lead_source, setter_owner_id, closer_owner_id, status )
      `)
      .order('scheduled_start', { ascending: false })
      .limit(200);
    const excludeIds = await getNonFlywheelCallIds();
    const appointments = filterFlywheelAppts(appointmentsRaw, excludeIds);

    // ── Query outcomes (scoped to these appointments, like the stats fns) ──
    const apptIdList = (appointments || []).map(a => a.id);
    let outcomes = [];
    if (apptIdList.length) {
      const { data } = await portalSupabase
        .from('revops_sales_outcomes')
        .select('appointment_id, outcome, offer_pitched, proposed_value, closed_revenue, close_date, lost_reason, objection_category, notes, updated_at')
        .in('appointment_id', apptIdList);
      outcomes = data || [];
    }

    const outcomeMap = {};
    outcomes.forEach(o => { outcomeMap[o.appointment_id] = o; });
    const appts = appointments || [];

    if (!appts.length) {
      return 'No sales appointments found. GHL webhook ingestion feeds revops_appointments — check dash /api/integrations/ghl/webhook deliveries if this persists.';
    }

    // ── TODAY'S CALLS ──────────────────────────────────────────────────────
    // `appts` is already flywheel-only (filtered above via getNonFlywheelCallIds).
    if (q.includes('today') || q.includes('hoy')) {
      const todayCalls = appts.filter(a => {
        if (!a.scheduled_start) return false;
        const d = new Date(a.scheduled_start);
        return d >= todayStart && d <= todayEnd;
      });
      const lines = [];

      if (todayCalls.length) {
        lines.push(`Scheduled calls today (${todayCalls.length}):`);
        for (const a of todayCalls) {
          const name    = a.prospect?.full_name || 'Unknown prospect';
          const closer  = resolveSalesMember(a.closer_id);
          const time    = formatICTime(a.scheduled_start, { hour: '2-digit', minute: '2-digit' });
          const outcome = outcomeMap[a.id];
          // A call that hasn't happened yet can't have an outcome. GHL reschedules
          // reuse the appointment row, so a pre-reschedule no_show outcome (and a
          // stale attended=false) rides along — surface it as "rebooked", never as
          // the outcome of the upcoming call.
          let status;
          if (new Date(a.scheduled_start) > new Date()) {
            status = ((outcome?.outcome || '').toLowerCase() === 'no_show' || a.attended === false)
              ? 'scheduled — rebooked after earlier no-show'
              : 'scheduled';
          } else {
            status = outcome ? `outcome: ${outcome.outcome}` : (a.attended === false ? 'no-show' : a.attended === true ? 'attended' : 'scheduled');
          }
          // Native setter attribution (GHL createdBy → setter_id). NULL on a GHL
          // row = widget self-booked or closer-booked. Live GHL opp lookup only
          // for legacy iClosed rows that never had a setter_id.
          let setterLabel;
          if (a.setter_id) {
            setterLabel = resolveSalesMember(a.setter_id);
          } else if (a.source === 'ghl') {
            setterLabel = 'self-booked';
          } else {
            setterLabel = 'setter unknown';
            try {
              const info = await resolveSetterForContact(a.prospect?.email, a.prospect?.full_name);
              if (info.source === 'appointment-setting') {
                setterLabel = info.setter || 'appointment-setting pipeline (setter unmapped)';
              }
            } catch (_) {}
          }
          lines.push(`  ${name} — ${time} CR — closer: ${closer} — setter: ${setterLabel} — ${status}`);
        }
      }

      if (!lines.length) return 'No calls scheduled today.';
      return lines.join('\n');
    }

    // ── SETTER PERFORMANCE (GHL-native; EOD retired at cutover) ────────────
    if (q.includes('setter') || q.includes('joseph') || q.includes('oscar') || q.includes('william') || q.includes('sebastian') || q.includes('josue') || q.includes('debbanny') || q.includes('booked') || q.includes('conversations') || q.includes('qualified leads')) {
      const fromStr = q.includes('month') ? monthStartStr : weekStartStr;
      const period  = q.includes('month') ? 'this month' : 'this week';
      const stats = await getSetterWeeklyStats(`${fromStr}T00:00:00.000Z`, now.toISOString());
      const block = formatSetterWeeklyStatsBlock(stats, `${fromStr}T00:00:00.000Z`, now.toISOString());
      return `Setter performance ${period} (GHL-native; EOD self-reports retired at cutover 2026-07-23):\n\n${block}`;
    }

    // ── CLOSER PERFORMANCE (GHL-native; EOD retired at cutover) ────────────
    if (q.includes('closer') || q.includes('jonathan') || q.includes('jose') || q.includes('close rate') || q.includes('closed') || q.includes('cancel') || q.includes('no-show') || q.includes('qualified calls')) {
      const fromStr = q.includes('month') ? monthStartStr : weekStartStr;
      const period  = q.includes('month') ? 'this month' : 'this week';
      const stats = await getCloserWeeklyStats(`${fromStr}T00:00:00.000Z`, now.toISOString());
      const block = formatCloserWeeklyStatsBlock(stats, `${fromStr}T00:00:00.000Z`, now.toISOString());
      return `Closer performance ${period} (GHL-native; EOD self-reports retired at cutover 2026-07-23):\n\n${block}`;
    }

    // ── PROSPECT LOOKUP ────────────────────────────────────────────────────
    const matchedProspect = appts.find(a => a.prospect?.full_name && q.includes(a.prospect.full_name.toLowerCase().split(' ')[0]));
    if (matchedProspect) {
      const closer  = resolveSalesMember(matchedProspect.closer_id);
      const time    = matchedProspect.scheduled_start ? formatICTime(matchedProspect.scheduled_start, { month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit' }) : 'not scheduled';
      const outcome = outcomeMap[matchedProspect.id];
      const lines   = [
        `Prospect: ${matchedProspect.prospect?.full_name || 'Unknown'}`,
        `Company: ${matchedProspect.prospect?.company || 'N/A'}`,
        `Closer: ${closer}`,
        matchedProspect.setter_id ? `Setter: ${resolveSalesMember(matchedProspect.setter_id)}` : (matchedProspect.source === 'ghl' ? 'Setter: self-booked (widget or closer-booked)' : 'Setter: not recorded (legacy iClosed row)'),
        `Scheduled: ${time} CR`,
        `Attended: ${matchedProspect.attended === true ? 'Yes' : matchedProspect.attended === false ? 'No — ' + (matchedProspect.no_show_reason || 'no reason given') : 'Not recorded'}`,
        matchedProspect.reschedule_count > 0 ? `Rescheduled: ${matchedProspect.reschedule_count}x` : '',
        outcome ? `Outcome: ${outcome.outcome}` : 'Outcome: not recorded yet',
        outcome?.closed_revenue ? `Revenue closed: $${outcome.closed_revenue}` : '',
        outcome?.lost_reason ? `Lost reason: ${outcome.lost_reason}` : '',
      ].filter(Boolean);
      return lines.join('\n');
    }

    // ── DEFAULT: PIPELINE SUMMARY (outcome-derived; EOD retired) ───────────
    const upcoming       = appts.filter(a => a.scheduled_start && new Date(a.scheduled_start) >= now).length;
    const thisWeekAppts  = appts.filter(a => a.scheduled_start && new Date(a.scheduled_start) >= weekStart).length;
    const wonOutcomes    = (outcomes || []).filter(o => (o.outcome || '').toLowerCase() === 'won');
    const thisWeekCloses = appts.filter(a => a.scheduled_start && new Date(a.scheduled_start) >= weekStart
      && (outcomeMap[a.id]?.outcome || '').toLowerCase() === 'won').length;

    return [
      `Sales pipeline summary:`,
      `Appointments on record: ${appts.length} | Upcoming: ${upcoming} | This week: ${thisWeekAppts}`,
      `Closes this week: ${thisWeekCloses} | Recent won outcomes on record: ${wonOutcomes.length}`,
      Object.keys(outcomeMap).length > 0 ? `Outcomes recorded: ${Object.keys(outcomeMap).length}` : 'No outcomes recorded yet',
    ].join('\n');

  } catch (err) {
    return `Sales intelligence error: ${err.message}`;
  }
}

// ─── OUTCOME-DERIVED ATTENDANCE TRUTH ────────────────────────────────────────
// The logged OUTCOME (GHL since the 2026-07-23 cutover; iClosed for frozen
// history) is the primary signal of whether a call happened. Full DB enum:
//   no_show                                          → did not attend
//   rescheduled                                      → didn't happen as booked; own bucket
//                                                      (NOT pending — a rescheduled row would
//                                                      otherwise inflate "Pending: N" forever)
//   won | lost | follow_up | nurture | disqualified  → attended (the call took place)
//   no outcome row yet                               → pending (excluded from show-rate denom)
// A "qualified attended call" (AQC) = attended AND not disqualified, i.e. the
// prospect was a real fit and the call happened. Used by both setter and closer
// stats so show rate / AQC can never diverge between the two reports.
const SHOWED_OUTCOMES = new Set(['won', 'lost', 'follow_up', 'nurture', 'disqualified']);
function classifyOutcome(outcomeRow) {
  const o = (outcomeRow?.outcome || '').toLowerCase().trim();
  if (!o) return { showed: false, noShow: false, qualifiedAttended: false, pending: true, rescheduled: false };
  if (o === 'no_show') return { showed: false, noShow: true, qualifiedAttended: false, pending: false, rescheduled: false };
  if (o === 'rescheduled') return { showed: false, noShow: false, qualifiedAttended: false, pending: false, rescheduled: true };
  if (SHOWED_OUTCOMES.has(o)) {
    return { showed: true, noShow: false, qualifiedAttended: o !== 'disqualified', pending: false, rescheduled: false };
  }
  // A value outside the known enum means GHL/dash added something new — say so
  // instead of silently excluding it from every show-rate denominator.
  console.warn(`classifyOutcome: unknown outcome "${o}" — treated as pending; extend the enum handling`);
  return { showed: false, noShow: false, qualifiedAttended: false, pending: true, rescheduled: false };
}

// ─── CLOSER WEEKLY STATS (GHL-native) ────────────────────────────────────────
// Used by the "Weekly Closer Comparison" scheduled task. GHL appointment +
// outcome rows in revops_* are the truth source (closer_id = roster email).
// EOD self-reports were retired at the GHL cutover (2026-07-23).
async function getCloserWeeklyStats(weekStartIso, weekEndIso) {
  const result = {}; // { closerName: { source, calls_booked, attended, no_shows, pending, sold, revenue } }

  const { data: apptsRaw, error: apptErr } = await portalSupabase
    .from('revops_appointments')
    .select('id, closer_id, scheduled_start, attended, no_show_reason, meeting_type, iclosed_call_id, ghl_appointment_id')
    .gte('scheduled_start', weekStartIso)
    .lte('scheduled_start', weekEndIso);
  if (apptErr) throw apptErr;
  const excludeIds = await getNonFlywheelCallIds();
  const appts = filterFlywheelAppts(apptsRaw, excludeIds);

  const apptIds = (appts || []).map(a => a.id);
  let outcomesById = {};
  if (apptIds.length) {
    const { data: outcomes } = await portalSupabase
      .from('revops_sales_outcomes')
      .select('appointment_id, outcome, closed_revenue, lost_reason')
      .in('appointment_id', apptIds);
    outcomesById = Object.fromEntries((outcomes || []).map(o => [o.appointment_id, o]));
  }

  for (const a of (appts || [])) {
    const name = resolveSalesMember(a.closer_id);
    if (!result[name]) result[name] = { source: 'ghl', calls_booked: 0, attended: 0, no_shows: 0, pending: 0, sold: 0, revenue: 0 };
    result[name].calls_booked += 1;
    // An outcome row on a call that hasn't happened yet is a reschedule leftover
    // (GHL reuses the appointment row) — treat the call as pending, not decided.
    const o = new Date(a.scheduled_start) > new Date() ? null : outcomesById[a.id];
    // Attendance is derived from the OUTCOME, not the broken attended boolean.
    const c = classifyOutcome(o);
    if (c.showed)  result[name].attended += 1;  // attended = showed up (call happened)
    if (c.noShow)  result[name].no_shows += 1;
    if (c.pending) result[name].pending  += 1;   // booked, no outcome yet
    if (o) {
      const outcomeLower = (o.outcome || '').toLowerCase();
      if (outcomeLower === 'won') result[name].sold += 1;
      result[name].revenue += Number(o.closed_revenue || 0);
    }
  }

  return result;
}

// ─── CLOSER MONTHLY SCORECARD (shared view) ──────────────────────────────────
// Reads public.closer_month_scorecard / closer_month_unattributed via the
// read-only portal PG connection — the SAME Postgres views the portal's
// /admin/closer-scorecard page uses (dash migration 20260813120000). Never
// recompute these numbers in JS; the view is the single source of truth.
// REVI overlay join contract mirrors the dash API route
// (src/app/api/revops/reports/closer-scorecard): same prospect email +
// call_date within 36h of scheduled_start; recordings only UPGRADE
// pending -> "verifiably happened", absence of a recording proves nothing.
async function getCloserMonthlyScorecard(month, closerQuery) {
  const err = ensurePortalPg(); if (err) return err;
  if (!/^\d{4}-\d{2}$/.test(month || '')) return 'ERROR: month must be YYYY-MM, e.g. 2026-07.';

  const { rows: allRows } = await portalPg.query(
    'SELECT * FROM closer_month_scorecard WHERE month = $1 ORDER BY calls_assigned DESC', [month]
  );
  if (!allRows.length) return `No scorecard rows for ${month}.`;

  const frag = (closerQuery || '').trim().toLowerCase();
  const rows = frag ? allRows.filter(r => r.closer_id.includes(frag)) : allRows;
  if (!rows.length) {
    return `No closer matching "${closerQuery}" in ${month}. Closers with data: ${allRows.map(r => r.closer_id).join(', ')}`;
  }

  const { rows: unRows } = await portalPg.query(
    'SELECT outcomes, won, revenue FROM closer_month_unattributed WHERE month = $1', [month]
  );

  // REVI overlay. CR is UTC-6 with no DST, so the CR month is a fixed UTC interval.
  const [y, mo] = month.split('-').map(Number);
  const startMs = Date.UTC(y, mo - 1, 1, 6, 0, 0);
  const endMs = Date.UTC(y, mo, 1, 6, 0, 0);
  const PAD = 36 * 3600 * 1000;
  let overlayByCloser = {};
  try {
    const [{ data: reviClosers }, { data: scores }, apptQ] = await Promise.all([
      reviSupabase.from('revi_closers').select('id, fathom_host_email'),
      reviSupabase.from('closer_call_scores')
        .select('closer_id, prospect_email, call_date, overall_score')
        .gte('call_date', new Date(startMs - PAD).toISOString())
        .lt('call_date', new Date(endMs + PAD).toISOString()),
      portalPg.query(
        `SELECT a.closer_id, a.scheduled_start, lower(p.email) AS email, o.outcome
           FROM revops_appointments a
           JOIN revops_prospects p ON p.id = a.prospect_id
           LEFT JOIN revops_sales_outcomes o ON o.appointment_id = a.id
          WHERE a.scheduled_start >= $1 AND a.scheduled_start < $2
            AND COALESCE(a.ghl_appointment_id, '') NOT LIKE 'opp:%'`,
        [new Date(startMs).toISOString(), new Date(endMs).toISOString()]
      ),
    ]);
    const closerByReviId = {};
    for (const rc of (reviClosers || [])) {
      if (rc.fathom_host_email) closerByReviId[rc.id] = rc.fathom_host_email.toLowerCase();
    }
    const recsByCloser = {};
    for (const s of (scores || [])) {
      const closer = closerByReviId[s.closer_id];
      if (!closer || !s.prospect_email || !s.call_date) continue;
      (recsByCloser[closer] = recsByCloser[closer] || []).push({
        email: s.prospect_email.toLowerCase(),
        at: new Date(s.call_date).getTime(),
        score: s.overall_score == null ? null : Number(s.overall_score),
        matched: false,
      });
    }
    for (const r of rows) {
      const closer = r.closer_id;
      const recs = recsByCloser[closer] || [];
      const inMonth = recs.filter(x => x.at >= startMs && x.at < endMs);
      let verified = 0;
      const discrepancies = [];
      for (const a of apptQ.rows) {
        if ((a.closer_id || '').toLowerCase() !== closer || !a.email || !a.scheduled_start) continue;
        const apptAt = new Date(a.scheduled_start).getTime();
        const rec = recs.find(x => !x.matched && x.email === a.email && Math.abs(x.at - apptAt) <= PAD);
        if (!rec) continue;
        if (a.outcome == null) { rec.matched = true; verified += 1; }
        else if (a.outcome === 'no_show') { rec.matched = true; discrepancies.push(a.email); }
      }
      const scoreVals = inMonth.map(x => x.score).filter(x => x != null);
      overlayByCloser[closer] = {
        scored: inMonth.length,
        avgScore: scoreVals.length ? Math.round(10 * scoreVals.reduce((s, v) => s + v, 0) / scoreVals.length) / 10 : null,
        verified,
        discrepancies,
      };
    }
  } catch (e) {
    console.error('getCloserMonthlyScorecard REVI overlay:', e.message);
    overlayByCloser = {};
  }

  const lines = [`CLOSER MONTHLY SCORECARD — ${month} (CR months, scheduled-time anchored; same view as the portal page /admin/closer-scorecard)`];
  for (const r of rows) {
    lines.push('');
    lines.push(`${r.closer_id}`);
    lines.push(`• Calls assigned: ${r.calls_assigned} (${r.unique_prospects} unique prospects) | Outcomes logged: ${r.outcomes_logged} | Pending: ${r.pending}`);
    lines.push(`• Showed: ${r.showed} | No-shows: ${r.no_shows} | Qualified attended: ${r.qualified_attended} | Show rate: ${r.show_rate_pct != null ? r.show_rate_pct + '%' : 'n/a'}`);
    lines.push(`• Won: ${r.won} | Lost: ${r.lost} | Follow-up: ${r.follow_up} | DQ: ${r.disqualified} | Close rate on shows: ${r.close_rate_on_shows_pct != null ? r.close_rate_on_shows_pct + '%' : 'n/a'} | Revenue: $${Number(r.revenue).toLocaleString()}`);
    const ov = overlayByCloser[r.closer_id];
    if (ov) {
      let reviLine = `• REVI reality-check: ${ov.scored} recorded calls scored${ov.avgScore != null ? `, avg score ${ov.avgScore}` : ''}`;
      if (ov.verified > 0) reviLine += ` — ${ov.verified} of the ${r.pending} pending calls verifiably HAPPENED (recording exists, outcome never logged)`;
      lines.push(reviLine);
      if (ov.discrepancies.length) lines.push(`• DATA FLAG: marked no-show but a recording exists: ${ov.discrepancies.join(', ')}`);
    }
    if (r.pending > 0 && r.pending / r.calls_assigned >= 0.3) {
      lines.push(`• CAVEAT: ${r.pending}/${r.calls_assigned} calls have no logged outcome — show/close rates are unreliable, and pending does NOT mean the call didn't happen.`);
    }
  }
  if (unRows.length && Number(unRows[0].outcomes) > 0) {
    const u = unRows[0];
    lines.push('');
    lines.push(`Unattributed outcomes in ${month} (no scheduled call to attach to — real results, credited to no closer): ${u.outcomes} outcomes, ${u.won} won, $${Number(u.revenue).toLocaleString()}.`);
  }
  return lines.join('\n');
}

function formatCloserWeeklyStatsBlock(stats, weekStartIso, weekEndIso) {
  const dropped = Object.keys(stats).filter(isUnresolvedSalesId);
  if (dropped.length) console.warn(`Closer stats: dropping unresolved id row(s) from leaderboard: ${dropped.join(', ')}`);
  const names = Object.keys(stats).filter(n => !isUnresolvedSalesId(n));
  if (!names.length) {
    return `No closer activity (GHL appointments/outcomes) found for the week ${weekStartIso.slice(0,10)} → ${weekEndIso.slice(0,10)}.`;
  }
  // Rank closers by revenue desc (with sold count + close rate as visible secondaries).
  const ranked = names.map(name => {
    const s = stats[name];
    // Show rate denominator = calls with a known outcome (showed + no-show);
    // pending calls (no outcome yet) are excluded so fresh bookings don't drag it down.
    const decided = (s.attended || 0) + (s.no_shows || 0);
    const showRate = decided > 0 ? Math.round((s.attended / decided) * 100) : null;
    const closeRate = s.attended > 0 ? Math.round((s.sold / s.attended) * 100) : 0;
    return { name, s, showRate, closeRate, decided };
  }).sort((a, b) => (b.s.revenue || 0) - (a.s.revenue || 0) || (b.s.sold || 0) - (a.s.sold || 0));

  const lines = [`CLOSER WEEKLY STATS — ${weekStartIso.slice(0,10)} → ${weekEndIso.slice(0,10)}`];
  lines.push(`(GHL appointment + outcome actuals are truth; show rate is outcome-derived — pending calls excluded. Ranked by revenue, then sold count.)`);
  ranked.forEach(({ name, s, showRate, closeRate }, idx) => {
    const showRateStr = showRate === null ? '— (no outcomes logged yet)' : `${showRate}%`;
    const pendingStr = s.pending ? ` | Pending: ${s.pending}` : '';
    lines.push(``);
    lines.push(`#${idx + 1} ${name.toUpperCase()}`);
    lines.push(`  Calls booked: ${s.calls_booked} | Attended: ${s.attended} | No-shows: ${s.no_shows}${pendingStr} (show rate: ${showRateStr})`);
    lines.push(`  Sold: ${s.sold} | Revenue: $${s.revenue.toLocaleString()} (close rate on shows: ${closeRate}%)`);
  });
  return lines.join('\n');
}

// ─── SETTER WEEKLY STATS (GHL-native) ────────────────────────────────────────
// Used by the unified weekly LEADERBOARD. Since the GHL cutover (2026-07-23),
// revops_appointments.setter_id carries the booker natively (GHL createdBy →
// roster email, mapped upstream in dash). Rows with setter_id NULL are widget
// self-bookings or closer-booked calls — no setter credit by policy. Show rate
// and qualified-attended-calls (AQC) derive from the OUTCOME via classifyOutcome
// — the same truth the closer stats use. Leads claimed comes from setter_claims.
// EOD self-reports were retired at the cutover (tables kept as frozen history).
function _newSetterSlot() {
  return {
    source: 'ghl', leads_claimed: 0,
    calls_booked: 0, attended: 0, no_shows: 0, pending: 0, aqc: 0,
    distinct_leads: 0, // distinct prospects behind those calls (rebookings collapse)
  };
}

async function getSetterWeeklyStats(weekStartIso, weekEndIso) {
  const result = {};

  // 1. Flywheel appointments in range + their outcomes
  const { data: apptsRaw, error: apptErr } = await portalSupabase
    .from('revops_appointments')
    .select('id, scheduled_start, iclosed_call_id, ghl_appointment_id, setter_id, source, prospect:prospect_id ( email )')
    .gte('scheduled_start', weekStartIso)
    .lte('scheduled_start', weekEndIso);
  if (apptErr) throw apptErr;
  const excludeIdsSet = await getNonFlywheelCallIds();
  const appts = filterFlywheelAppts(apptsRaw, excludeIdsSet);

  const apptIds = (appts || []).map(a => a.id);
  let outcomesById = {};
  if (apptIds.length) {
    const { data: outcomes } = await portalSupabase
      .from('revops_sales_outcomes')
      .select('appointment_id, outcome')
      .in('appointment_id', apptIds);
    outcomesById = Object.fromEntries((outcomes || []).map(o => [o.appointment_id, o]));
  }

  // OWNERSHIP MAP: who CLAIMED each prospect in #ng-sales-goats (✋ flow →
  // setter_claims). 45-day lookback from the WINDOW START (not end) — claims
  // usually precede the call by days, and anchoring on the start keeps the
  // lookback valid for wide windows like month-to-date, where a late-month call
  // could otherwise outrun a fixed lookback from the window end.
  // Latest claim per prospect email wins.
  const claimOwnerByEmail = {};
  {
    const { data: claimHist } = await supabase
      .from('setter_claims')
      .select('prospect_email, claimed_by_setter_name, claimed_at')
      .not('prospect_email', 'is', null)
      .gte('claimed_at', new Date(new Date(weekStartIso).getTime() - 45 * 24 * 60 * 60 * 1000).toISOString())
      .order('claimed_at', { ascending: true });
    for (const c of (claimHist || [])) {
      const k = (c.prospect_email || '').toLowerCase().trim();
      if (k && c.claimed_by_setter_name) claimOwnerByEmail[k] = c.claimed_by_setter_name;
    }
  }

  // 2. ATTRIBUTION — the setter who OWNS the prospect gets the call, regardless
  //    of who physically clicked book. A lead they claimed and then self-booked
  //    through the widget is still their call. GHL's recorded booker is only a
  //    fallback for calls with no claim (setter booked a lead nobody claimed).
  //    Neither → nobody worked it; leave it unattributed.
  const leadsByOwner = {}; // owner → Set(prospect email), for distinct-lead counts
  for (const a of (appts || [])) {
    const prospectEmail = (a.prospect?.email || '').toLowerCase().trim();
    const claimOwner = prospectEmail ? claimOwnerByEmail[prospectEmail] : null;
    const owner = claimOwner || (a.setter_id ? resolveSalesMember(a.setter_id) : null);
    if (!owner) continue;

    if (!result[owner]) result[owner] = _newSetterSlot();
    const slot = result[owner];
    slot.calls_booked += 1;
    if (prospectEmail) {
      if (!leadsByOwner[owner]) leadsByOwner[owner] = new Set();
      leadsByOwner[owner].add(prospectEmail);
    }
    // Same reschedule-leftover guard as getCloserWeeklyStats: no outcome can
    // describe a call that hasn't happened yet.
    const c = classifyOutcome(new Date(a.scheduled_start) > new Date() ? null : outcomesById[a.id]);
    if (c.showed)            slot.attended += 1;
    if (c.noShow)            slot.no_shows += 1;
    if (c.pending)           slot.pending  += 1;
    if (c.qualifiedAttended) slot.aqc      += 1;
  }
  for (const [owner, set] of Object.entries(leadsByOwner)) {
    if (result[owner]) result[owner].distinct_leads = set.size;
  }

  // 3. Leads claimed this week from setter_claims (ng-agent project)
  const { data: claimRows } = await supabase
    .from('setter_claims')
    .select('claimed_by_setter_name, claimed_at')
    .gte('claimed_at', weekStartIso)
    .lte('claimed_at', weekEndIso);
  for (const r of (claimRows || [])) {
    const name = r.claimed_by_setter_name;
    if (!name) continue;
    if (!result[name]) result[name] = _newSetterSlot();
    result[name].leads_claimed += 1;
  }

  return result;
}

function formatSetterWeeklyStatsBlock(stats, weekStartIso, weekEndIso) {
  const dropped = Object.keys(stats).filter(isUnresolvedSalesId);
  if (dropped.length) console.warn(`Setter stats: dropping unresolved id row(s) from leaderboard: ${dropped.join(', ')}`);
  const names = Object.keys(stats).filter(n => !isUnresolvedSalesId(n));
  if (!names.length) {
    return `No setter activity (GHL bookings or lead claims) found for ${weekStartIso.slice(0,10)} → ${weekEndIso.slice(0,10)}.`;
  }
  // Ranked by the setter-CONTROLLED outcome first: calls from leads they own.
  // Converted calls (AQC) and claims break ties — quality still matters, but a
  // setter is never out-ranked on a number gated by closer follow-through.
  const ranked = names.map(name => {
    const s = stats[name];
    const decided = (s.attended || 0) + (s.no_shows || 0);
    const showRate = decided > 0 ? Math.round((s.attended / decided) * 100) : null;
    return { name, s, showRate, decided };
  }).sort((a, b) =>
    (b.s.calls_booked || 0) - (a.s.calls_booked || 0) ||
    (b.s.aqc || 0) - (a.s.aqc || 0) ||
    (b.s.leads_claimed || 0) - (a.s.leads_claimed || 0));

  // Window-agnostic header — the leaderboard cron passes month-to-date, the sales
  // standup passes a 7-day window; the dates say which.
  const lines = [`SETTER STATS — ${weekStartIso.slice(0,10)} → ${weekEndIso.slice(0,10)}`];
  lines.push(`(Ordered by what the setter CONTROLS: leads claimed → calls from those leads, then the shared/downstream numbers. ATTRIBUTION: a call belongs to the setter who CLAIMED the prospect (the ✋ flow in #ng-sales-goats). It does not matter whether the setter or the prospect clicked book — the setter owns the lead either way. GHL's recorded booker is only a fallback for calls on leads nobody claimed. "Converted calls (AQC)" is a POST-CALL metric — the call happened and the prospect was not disqualified on it; it is NOT booking-form qualification, and it sits at 0 until a closer logs the outcome, so it must never be read as a setter quality signal. Ranked by calls.)`);
  ranked.forEach(({ name, s, showRate, decided }, idx) => {
    const showRateStr = showRate === null ? 'pending' : `${showRate}%`;
    lines.push(``);
    lines.push(`#${idx + 1} ${name.toUpperCase()}`);
    // 1. OWNED — leads they claimed and the calls those leads produced. Distinct
    // lead count is shown only when it differs from the call count (a prospect
    // who rebooked is 2 calls, 1 lead) so the numbers never look contradictory.
    const dl = s.distinct_leads || 0;
    const distinctStr = (dl && dl !== s.calls_booked) ? ` (from ${dl} distinct lead${dl === 1 ? '' : 's'})` : '';
    lines.push(`  OWNED — Leads claimed: ${s.leads_claimed} | Calls from their leads: ${s.calls_booked}${distinctStr}`);
    // 2. SHARED — setter influences via lead quality + confirmation work.
    if (decided === 0) {
      lines.push(`  SHARED — Show rate: pending (${s.pending || 0} call(s) awaiting a logged outcome)`);
    } else {
      lines.push(`  SHARED — Show rate: ${showRateStr} | Attended: ${s.attended} | No-shows: ${s.no_shows}${s.pending ? ` | Pending: ${s.pending}` : ''}`);
      lines.push(`  DOWNSTREAM — Converted calls (AQC, closer-logged): ${s.aqc}`);
    }
  });
  return lines.join('\n');
}

// ─── SETTER ATTRIBUTION RECONCILER — RETIRED 2026-07-26 ─────────────────────
// GHL records the booker natively (createdBy.userId → revops_appointments.setter_id,
// mapped upstream in dash), so the email-match reconciler and its 2h cron are gone.
// The setter_attributions table (migration 011) remains as frozen iClosed-era history.

// ─── GHL CONVERSATIONS ────────────────────────────────────────────────────────
async function getGHLConversations(limit = 20, unreadOnly = false) {
  try {
    const locationId = process.env.GHL_LOCATION_ID;
    const apiKey     = process.env.GHL_API_KEY;
    let url = `https://services.leadconnectorhq.com/conversations/search?locationId=${locationId}&limit=${limit}`;
    if (unreadOnly) url += `&status=unread`;
    const res  = await fetch(url, { headers: { 'Authorization': `Bearer ${apiKey}`, 'Version': '2021-07-28', 'Content-Type': 'application/json' } });
    const data = await res.json();
    const convos = data.conversations || [];
    if (!convos.length) return 'No conversations found.';
    const now      = Date.now();
    const oneDayMs = 24 * 60 * 60 * 1000;
    // GHL user ID to name map for setter resolution
    const GHL_USERS = {
      'cuttpcov7ztlvyjkhdx8': 'Joseph Salazar', 'cUTTPGov7ZTLvyjKHdX8': 'Joseph Salazar',
      'zcmdiz2eerapd80w2zop': 'Oscar M',         'ZcmdIz2EEraPd80W2zop': 'Oscar M',
      'n8mvtuhbbby7qppqnmr7': 'William B',       'N8mvtuHbbbY7QppqNMr7': 'William B',
      'wdjte1temxfr0lpi5rgv': 'Sebastian S',     'Wdjte1temxfR0lpi5RGV': 'Sebastian S',
      '5orsahkh2joujb5fczrp': 'Debbanny Romero', '5OrSaHkh2joUjB5FCZrP': 'Debbanny Romero',
      'gqymykpddltdxvbkfl2c': 'Jonathan Madriz', 'gqYMYkpDDlTdxvBkfl2C': 'Jonathan Madriz',
      'izlta0jy5orkymsyltjv': 'Jose Carranza',   'izLTA0jy5OrKyMvyltjV': 'Jose Carranza',
    };
    // Fetch the last few messages per conversation in parallel so the model has
    // enough context to judge "positive booking-track" vs noise (single-emoji
    // replies, auto-responses, opt-outs). Per-convo failures degrade silently to
    // the summary line — never break the whole tool result on one bad fetch.
    const TAIL_N = 5;
    const tails = await Promise.all(convos.map(c =>
      ghlGetConversationMessages(c.id).catch(() => [])
    ));
    const lines = convos.map((c, i) => {
      const lastDate  = new Date(c.lastMessageDate).toLocaleString('en-US', { timeZone: 'America/Costa_Rica', month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit' });
      const age       = Math.floor((now - c.lastMessageDate) / oneDayMs);
      const unread    = c.unreadCount > 0 ? ` [UNREAD: ${c.unreadCount}]` : '';
      const direction = c.lastMessageDirection === 'inbound' ? '<-- inbound' : '--> outbound';
      const channel   = c.lastMessageType?.replace('TYPE_', '') || 'unknown';
      const stale     = age >= 3 ? ` [${age}d ago - needs follow-up]` : '';
      // Resolve assigned setter from GHL user ID
      const assignedId   = c.assignedTo || c.userId || '';
      const assignedName = GHL_USERS[assignedId] || GHL_USERS[assignedId.toLowerCase()] || (assignedId ? `user:${assignedId}` : 'unassigned');
      const header = `${c.contactName || c.fullName || 'Unknown'} | setter: ${assignedName} | ${channel} | ${direction}${unread}${stale}`;
      // Last N messages (oldest first) — gives the judge enough context to tell
      // a booking-track exchange from a single-emoji ack or an opt-out.
      const tail = (tails[i] || []).slice(-TAIL_N);
      if (!tail.length) {
        return `${header}\nLast: "${(c.lastMessageBody || '').substring(0, 120)}" (${lastDate})`;
      }
      const tailLines = tail.map(m => {
        const dir  = m.direction === 'inbound' ? 'in ' : 'out';
        const when = (m.dateAdded || m.createdAt)
          ? new Date(m.dateAdded || m.createdAt).toLocaleString('en-US', { timeZone: 'America/Costa_Rica', month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit' })
          : '?';
        const body = String(m.body || m.message || '').replace(/\s+/g, ' ').trim().substring(0, 150);
        return `  [${dir} ${when}] "${body}"`;
      });
      return `${header}\nLast ${tail.length} messages (oldest first):\n${tailLines.join('\n')}`;
    });
    const unreadCount = convos.filter(c => c.unreadCount > 0).length;
    const staleCount  = convos.filter(c => (now - c.lastMessageDate) / oneDayMs >= 3).length;
    return `GHL Conversations — ${convos.length} total | ${unreadCount} unread | ${staleCount} need follow-up\n\n` + lines.join('\n\n');
  } catch (err) { return `GHL conversations error: ${err.message}`; }
}

// ─── SLACK CHANNEL READ ───────────────────────────────────────────────────────
async function readSlackChannel(channelName, messageCount = 20) {
  const linkMatch = channelName.match(/<#[A-Z0-9]+\|([^>]+)>/);
  const cleanName = linkMatch ? linkMatch[1] : channelName.replace('#', '');
  const channels  = await getCachedChannelList();
  const channel   = channels.find(c => c.name === cleanName);
  if (!channel) return `Channel ${channelName} not found or agent not invited.`;
  try {
    const history  = await slack.client.conversations.history({ channel: channel.id, limit: Math.min(messageCount, 20) });
    if (!history.messages.length) return 'No recent messages found.';
    return history.messages.reverse().map(m => {
      const time = new Date(parseFloat(m.ts) * 1000).toLocaleString('en-US', { timeZone: 'America/Costa_Rica', month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit' });
      return `[${time}] ${(m.text || '').substring(0, 300)}`;
    }).join('\n');
  } catch (err) { return `Error reading channel: ${err.message}`; }
}

// ─── SLACK: ONE-OFF SCHEDULED REMINDER ───────────────────────────────────────
// Wraps chat.scheduleMessage. `target` accepts a channel name (#foo or foo),
// a channel ID (C…), or a user ID (U…) for DMs. `postAt` is an ISO 8601
// string (e.g. 2026-04-24T15:00:00-06:00). Must be future and within 120 days.
async function createSlackReminder(target, message, postAt) {
  try {
    if (!target || !message || !postAt) return 'Reminder error: target, message, and postAt are all required.';
    const ts = Math.floor(new Date(postAt).getTime() / 1000);
    if (!Number.isFinite(ts)) return `Reminder error: could not parse postAt "${postAt}" — use ISO 8601 like 2026-04-24T15:00:00-06:00.`;
    const nowSec = Math.floor(Date.now() / 1000);
    if (ts <= nowSec) return `Reminder error: postAt is in the past (${new Date(postAt).toISOString()}).`;
    if (ts - nowSec > 120 * 24 * 60 * 60) return 'Reminder error: Slack only allows scheduling up to 120 days out.';

    let channelId;
    if (/^[CUDG][A-Z0-9]+$/.test(target)) {
      channelId = target;
    } else {
      const linkMatch = target.match(/<#([A-Z0-9]+)\|[^>]+>/);
      if (linkMatch) {
        channelId = linkMatch[1];
      } else {
        const cleanName = target.replace(/^#/, '');
        const channels  = await getCachedChannelList();
        const channel   = channels.find(c => c.name === cleanName);
        if (!channel) return `Reminder error: channel "${target}" not found or Max is not a member.`;
        channelId = channel.id;
      }
    }

    const res = await slack.client.chat.scheduleMessage({ channel: channelId, text: message, post_at: ts });
    const when = new Date(postAt).toLocaleString('en-US', { timeZone: 'America/Costa_Rica', dateStyle: 'medium', timeStyle: 'short' });
    return `Reminder scheduled for ${when} CR in ${target}. scheduled_message_id: ${res.scheduled_message_id}`;
  } catch (err) {
    return `Reminder error: ${err.data?.error || err.message}`;
  }
}

// ─── PORTAL: WEEKLY TREND ANALYSIS ───────────────────────────────────────────
async function runWeeklyPortalTrends(_correlationId) {
  console.log('Running weekly portal trend analysis...');
  try {
    const { data: dashboards } = await portalSupabase.from('client_dashboards').select('id, client_name, email, customer_status, customer_type, created_at').eq('is_active', true);
    const { data: templates }  = await portalSupabase.from('customer_activity_templates').select('id, title, order_index');
    const tMap = {};
    (templates || []).forEach(t => { tMap[t.id] = t.title; });
    const { data: allActs } = await portalSupabase.from('customer_activities').select('customer_id, template_id, status, assigned_to, completed_at');
    if (!dashboards || !allActs) return;
    const activationFor = await loadActivationDates();

    const phaseCounts = dashboards.reduce((acc, d) => { acc[d.customer_status] = (acc[d.customer_status]||0)+1; return acc; }, {});
    const blockedByActivity = {};
    allActs.filter(a => a.status === 'blocked').forEach(a => { const t = tMap[a.template_id]||'Unknown'; blockedByActivity[t] = (blockedByActivity[t]||0)+1; });
    const topBlocked = Object.entries(blockedByActivity).sort((a,b) => b[1]-a[1]).slice(0,5).map(([t,c]) => `${t} (${c}x)`).join(', ');
    const pendingByAssignee = {};
    allActs.filter(a => a.status === 'phase_1' || a.status === 'phase_2').forEach(a => { const e = (a.assigned_to||'unassigned').split('@')[0]; pendingByAssignee[e] = (pendingByAssignee[e]||0)+1; });
    const workload = Object.entries(pendingByAssignee).sort((a,b) => b[1]-a[1]).map(([e,c]) => `${e}: ${c} pending`).join(' | ');
    const liveClients   = dashboards.filter(d => d.customer_status === 'live');
    // Days-to-live is a Build & Release number, so it counts from the activation
    // call — the same anchor the 14-day window uses. It used to count from
    // client_dashboards.created_at, which is when the portal row appeared, and was
    // labelled "days since onboarding"; that mixed pre-activation waiting time into
    // a delivery metric and the result was written into the knowledge base weekly,
    // where Max would later quote it as fact.
    //
    // Averaged ONLY over clients with a real activation date, and the sample size is
    // reported — a mean silently mixing two different anchors is worse than a mean
    // over fewer clients, and hiding how few clients it covers is how a soft number
    // gets treated as solid.
    const liveWithAnchor = liveClients
      .map(d => ({ d, anchor: activationFor(d) }))
      .filter(x => x.anchor);
    const avgDaysToLive = liveWithAnchor.length
      ? Math.round(liveWithAnchor.reduce((sum, x) =>
          sum + Math.floor((Date.now() - new Date(x.anchor).getTime()) / (1000 * 60 * 60 * 24)), 0) / liveWithAnchor.length)
      : null;
    const avgDaysToLiveLine = avgDaysToLive
      ? `Avg days since activation call for live clients: ${avgDaysToLive} days (${liveWithAnchor.length} of ${liveClients.length} live clients have a recorded call date)`
      : liveClients.length ? `Avg days since activation call: not computed — none of the ${liveClients.length} live clients has a recorded activation call date` : '';
    const fwCount = dashboards.filter(d => d.customer_type === 'flywheel-ai').length;
    const fsCount = dashboards.filter(d => d.customer_type === 'full-service').length;
    const today = new Date().toLocaleDateString('en-US', { weekday:'long', month:'long', day:'numeric', timeZone:'America/Costa_Rica' });
    const trendReport = [`Week ending ${today}:`, `Phase distribution: ${Object.entries(phaseCounts).map(([k,v])=>`${k}:${v}`).join(', ')}`, `Client mix: ${fwCount} Flywheel AI, ${fsCount} Full Service`, topBlocked ? `Top blocked activities: ${topBlocked}` : 'No blocked activities this week.', workload ? `Team workload: ${workload}` : '', avgDaysToLiveLine].filter(Boolean).join(' | ');
    await upsertKnowledge('intel', `weekly-trends-${new Date().toISOString().slice(0,10)}`, trendReport, 'weekly-cron');
    if (topBlocked) await upsertKnowledge('process', 'recurring-blocked-activities', `As of ${today}: Most blocked activities are: ${topBlocked}. Review with Josue and Felipe.`, 'weekly-cron');
    if (workload)   await upsertKnowledge('team', 'current-workload', `As of ${today}: ${workload}`, 'weekly-cron');
    console.log('Weekly trend analysis complete.');
    await postToSlack(AGENT_CHANNEL, `📊 *Weekly trend analysis saved* — check knowledge base for latest intel and process insights.`);
  } catch (err) { console.error('Weekly trend error:', err.message); }
}

// ─── PORTAL: MONDAY GAP DETECTION ────────────────────────────────────────────
// The param is USED (passed to executeChannelPost below), so it must not carry the
// unused-underscore prefix — with `_correlationId` the post line referenced an
// undefined `correlationId`, threw a ReferenceError while evaluating its arguments,
// and the surrounding catch swallowed it. The early return on an empty `gaps` array
// meant that only ever happened on the weeks that HAD something to report.
async function runMondayGapDetection(correlationId) {
  console.log('Running Monday gap detection...');
  try {
    const { data: dashboards } = await portalSupabase.from('client_dashboards').select('id, client_name, email, customer_status, customer_type, created_at').eq('is_active', true).in('customer_status', ['phase_1','phase_2','blocked']);
    if (!dashboards || !dashboards.length) { console.log('No at-risk clients detected.'); return; }
    const { data: templates } = await portalSupabase.from('customer_activity_templates').select('id, title, order_index');
    const tMap = {};
    (templates || []).forEach(t => { tMap[t.id] = t.title; });
    const activationFor = await loadActivationDates();
    const now  = Date.now();
    const gaps = [];
    for (const dash of dashboards) {
      // ── Dual-path activity query: dash.id first, merge onboarding.id if different ──
      const { data: actsByDashId } = await portalSupabase.from('customer_activities').select('template_id, status, assigned_to, updated_at, completed_at').eq('customer_id', dash.id).in('status', ['blocked','phase_1','phase_2']);
      let acts = actsByDashId || [];

      const { data: onboarding } = await portalSupabase.from('customer_onboarding').select('id').eq('email', dash.email).limit(1);
      if (onboarding?.[0] && onboarding[0].id !== dash.id) {
        const { data: actsByObId } = await portalSupabase.from('customer_activities').select('template_id, status, assigned_to, updated_at, completed_at').eq('customer_id', onboarding[0].id).in('status', ['blocked','phase_1','phase_2']);
        if (actsByObId?.length) {
          const existingIds = new Set(acts.map(a => a.template_id + a.status));
          acts = [...acts, ...actsByObId.filter(a => !existingIds.has(a.template_id + a.status))];
        }
      }

      if (!acts.length) continue;

      // ── Day anchor: shared logic. Fetch all activities (not just in-progress)
      // to find the completed activation call ──
      const { data: allActsForAnchor } = await portalSupabase.from('customer_activities').select('template_id, status, completed_at').eq('customer_id', dash.id);
      const anchor = resolveDayAnchor(dash, allActsForAnchor || [], id => tMap[id], activationFor(dash));
      const daysSince = anchor.daysSince ?? 0;
      const anchorSuffix = anchor.anchorDate ? ` ${anchor.label} (${anchor.anchorDate})` : '';

      const staleActs = acts.filter(a => { const u = a.updated_at ? new Date(a.updated_at).getTime() : 0; return (now - u) > (72*60*60*1000); });
      let gapLine = '';
      if (dash.customer_status === 'blocked') {
        const blockedTitles = acts.filter(a=>a.status==='blocked').map(a=>tMap[a.template_id]||'Unknown').join(', ');
        gapLine = `🔴 BLOCKED — ${dash.client_name} (Day ${daysSince}${anchorSuffix}): ${blockedTitles}`;
      } else if (daysSince >= 14) {
        gapLine = `🔴 OVERDUE — ${dash.client_name} still in ${dash.customer_status} at Day ${daysSince}${anchorSuffix} (past 14-day window)`;
      } else if (daysSince >= 7 && staleActs.length > 0) {
        const assignees = [...new Set(staleActs.map(a=>(a.assigned_to||'').split('@')[0]))].join(', ');
        gapLine = `🟡 STALE — ${dash.client_name} (Day ${daysSince}${anchorSuffix}): ${staleActs.length} activities with no update in 72hrs. Assigned to: ${assignees}`;
      }
      if (gapLine) {
        const clientCtx = await getClientContext(dash.client_name);
        const ctxNote = clientCtx.length ? `\n   Team context: ${clientCtx.map(r => r.value).join(' | ')}` : '';
        gaps.push(gapLine + ctxNote);
      }
    }
    // ── Phase 0 gaps: stuck ≥7 days ───────────────────────────────────────────
    const { data: phase0Gaps } = await portalSupabase
      .from('v_phase0_fulfillment')
      .select('email, first_name, last_name, company, phase0_step, days_in_phase0')
      .gte('days_in_phase0', 7)
      .order('days_in_phase0', { ascending: false });
    const stepLabels = {
      '1_awaiting_signup': 'awaiting portal signup', '2_awaiting_terms': 'awaiting T&C',
      '3_awaiting_form': 'awaiting onboarding form', '4_awaiting_activation_call': 'awaiting activation call',
      '5_ready_for_handoff': 'ready for Phase 1 handoff — not moved',
    };
    for (const r of (phase0Gaps || [])) {
      const name = [r.first_name, r.last_name].filter(Boolean).join(' ') || r.email;
      const co   = r.company ? ` (${r.company})` : '';
      const label = r.days_in_phase0 >= 14 ? '🔴 PHASE 0 OVERDUE' : '🟡 PHASE 0 STALE';
      gaps.push(`${label} — ${name}${co} | ${stepLabels[r.phase0_step] || r.phase0_step} | Day ${r.days_in_phase0} (fulfillment to unblock)`);
    }

    // ── Sales gap detection ────────────────────────────────────────────────────
    try {
      const nowTs = Date.now();
      const salesGapLines = [];

      // a. No-shows with no reschedule in last 7 days (flywheel-only).
      // Keyed on the logged OUTCOME (classifyOutcome — same truth as every
      // other surface), NOT on attended=false, which GHL also sets on
      // CANCELLED calls and would nag closers about cancellations. The 7d
      // window is inherently post-cutover, so no explicit floor needed.
      const sevenDaysAgo = new Date(nowTs - 7 * 24 * 60 * 60 * 1000).toISOString();
      const excludeIds = await getNonFlywheelCallIds();
      const { data: recentApptsRaw } = await portalSupabase
        .from('revops_appointments')
        .select('id, prospect_id, closer_id, scheduled_start, iclosed_call_id, ghl_appointment_id, prospect:prospect_id(full_name)')
        .gte('scheduled_start', sevenDaysAgo)
        .lte('scheduled_start', new Date(nowTs).toISOString());
      const recentAppts = filterFlywheelAppts(recentApptsRaw, excludeIds);

      if (recentAppts && recentAppts.length) {
        const { data: recentOutcomes } = await portalSupabase
          .from('revops_sales_outcomes')
          .select('appointment_id, outcome')
          .in('appointment_id', recentAppts.map(a => a.id));
        const outcomeByAppt = Object.fromEntries((recentOutcomes || []).map(o => [o.appointment_id, o]));
        const noShows = recentAppts.filter(a => classifyOutcome(outcomeByAppt[a.id]).noShow);

        const noShowFlags = [];
        for (const appt of noShows) {
          const { data: future } = await portalSupabase
            .from('revops_appointments')
            .select('id')
            .eq('prospect_id', appt.prospect_id)
            .gt('scheduled_start', new Date().toISOString())
            .limit(1);
          if (!future || !future.length) {
            const pName = appt.prospect?.full_name || 'Unknown';
            const dStr  = formatICTime(appt.scheduled_start, { month: 'short', day: 'numeric' });
            const closerName = resolveSalesMember(appt.closer_id);
            noShowFlags.push(`• ${pName} — ${dStr}, closer: ${closerName}`);
          }
        }
        if (noShowFlags.length) {
          salesGapLines.push(`No-shows, no reschedule (last 7d):\n${noShowFlags.join('\n')}`);
        }
      }

      // (The former "Outcomes not logged" section was deleted 2026-08-03: it
      // duplicated the daily 4 PM runUnloggedOutcomeReminders — which owns
      // that surface with per-call nudge counts, orphan-skip, and cancelled
      // exclusion — and, keyed on the once-dead attended boolean, it went from
      // silently empty to noisy-and-wrong when GHL started populating attended.)

      // c. Stale inbound leads >72h with no setter response
      const ghlRaw = await getGHLConversations(100);
      if (typeof ghlRaw === 'string' && ghlRaw.includes('|')) {
        // getGHLConversations returns a formatted string — re-fetch raw for filtering
      }
      // Re-fetch raw GHL data for filtering
      try {
        const locationId = process.env.GHL_LOCATION_ID;
        const apiKey     = process.env.GHL_API_KEY;
        const ghlRes  = await fetch(`https://services.leadconnectorhq.com/conversations/search?locationId=${locationId}&limit=100`, {
          headers: { 'Authorization': `Bearer ${apiKey}`, 'Version': '2021-07-28', 'Content-Type': 'application/json' }
        });
        const ghlData = await ghlRes.json();
        const convos  = ghlData.conversations || [];
        const seventyTwoHAgo = nowTs - 72 * 60 * 60 * 1000;
        const staleInbound = convos.filter(c => c.lastMessageDirection === 'inbound' && c.lastMessageDate < seventyTwoHAgo);
        if (staleInbound.length) {
          const GHL_USERS_GAP = {
            'cuttpcov7ztlvyjkhdx8': 'Joseph Salazar', 'cUTTPGov7ZTLvyjKHdX8': 'Joseph Salazar',
            'zcmdiz2eerapd80w2zop': 'Oscar M',         'ZcmdIz2EEraPd80W2zop': 'Oscar M',
            'n8mvtuhbbby7qppqnmr7': 'William B',       'N8mvtuHbbbY7QppqNMr7': 'William B',
            'wdjte1temxfr0lpi5rgv': 'Sebastian S',     'Wdjte1temxfR0lpi5RGV': 'Sebastian S',
            '5orsahkh2joujb5fczrp': 'Debbanny Romero', '5OrSaHkh2joUjB5FCZrP': 'Debbanny Romero',
          };
          const staleLines = staleInbound.map(c => {
            const contactName = c.contactName || c.fullName || 'Unknown';
            const assignedId  = c.assignedTo || c.userId || '';
            const setterName  = GHL_USERS_GAP[assignedId] || GHL_USERS_GAP[assignedId.toLowerCase()] || (assignedId ? assignedId : 'unassigned');
            const daysAgo     = Math.floor((nowTs - c.lastMessageDate) / (24 * 60 * 60 * 1000));
            return `• ${contactName} | setter: ${setterName} | last: ${daysAgo}d ago`;
          });
          salesGapLines.push(`Stale inbound leads (setter no response >72h):\n${staleLines.join('\n')}`);
        }
      } catch (ghlGapErr) {
        console.error('Sales gap — GHL fetch error:', ghlGapErr.message);
      }

      if (salesGapLines.length) {
        const totalSalesGaps = salesGapLines.reduce((sum, s) => sum + (s.match(/^•/gm) || []).length, 0);
        gaps.push(`\nSALES GAPS — ${totalSalesGaps} item${totalSalesGaps !== 1 ? 's' : ''} need attention\n\n${salesGapLines.join('\n\n')}`);
      }
    } catch (salesGapErr) {
      console.error('Sales gap detection error:', salesGapErr.message);
    }

    if (!gaps.length) { console.log('Gap detection: no critical gaps found.'); return; }
    const today   = new Date().toLocaleDateString('en-US', { weekday:'long', month:'long', day:'numeric', timeZone:'America/Costa_Rica' });
    // Prepend any lessons learned from team feedback on previous gap reports
    const gapLessons = await getReportLessons('gap-detection');
    const lessonNote = gapLessons.length
      ? `[Corrections applied from team feedback]\n${gapLessons.map(l => `• ${l.value}`).join('\n')}\n\n`
      : '';
    const message = `${lessonNote}Good morning team. Here's your Monday delivery gap report for ${today}:\n\n${gaps.join('\n')}\n\nTag the responsible team member and confirm resolution by EOD.`;
    // Post directly — team reviews and threads corrections to Max for learning
    await executeChannelPost(OPS_CHANNEL, message, null, correlationId);
    console.log(`Gap detection: ${gaps.length} gap(s) posted directly to ${OPS_CHANNEL}.`);
  } catch (err) { console.error('Gap detection error:', err.message); }
}

// ─── NIGHTLY LEARNING ─────────────────────────────────────────────────────────
async function runNightlyLearning(correlationId) {
  console.log('Running nightly learning cycle...');
  try {
    const channels = ['ng-fullfillment-ops','ng-sales-goats','ng-new-client-alerts','ng-app-and-systems-improvents','ng-ops-management'];
    let digest = '';
    for (const ch of channels) {
      const messages = await readSlackChannel(ch, 20);
      if (!messages.includes('not found')) digest += `\n\n=== ${ch} ===\n${messages}`;
    }
    // Pull recent emails into nightly digest
    try {
      const emails = await getRecentEmails();
      if (emails && !emails.includes('error')) {
        digest += `\n\n=== GMAIL (recent unread) ===\n${emails}`;
      }
    } catch (e) { console.error('Nightly learning — email fetch error:', e.message); }

    // Pull tomorrow's calendar events into nightly digest
    try {
      const tomorrowEvents = await getCalendarEvents(1, 1);
      if (tomorrowEvents && !tomorrowEvents.includes('error')) {
        digest += `\n\n=== CALENDAR (tomorrow — ${_crDayLabel(1)} CR) ===\n${tomorrowEvents}`;
      }
    } catch (e) { console.error('Nightly learning — calendar fetch error:', e.message); }

    try {
      const { data: dashboards } = await portalSupabase.from('client_dashboards').select('id, client_name, email, customer_status, customer_type').eq('is_active', true);
      const { data: templates }  = await portalSupabase.from('customer_activity_templates').select('id, title, order_index');
      const tMap = {};
      (templates || []).forEach(t => { tMap[t.id] = t.title; });
      const today = new Date(); today.setHours(0,0,0,0);
      const { data: recentActs } = await portalSupabase.from('customer_activities').select('customer_id, template_id, status, assigned_to, completed_at, notes').or(`status.eq.blocked,completed_at.gte.${today.toISOString()}`);
      if (dashboards && recentActs) {
        const clientMap = {};
        dashboards.forEach(d => { clientMap[d.id] = d; });
        const blocked        = recentActs.filter(a => a.status === 'blocked');
        const completedToday = recentActs.filter(a => a.completed_at && a.completed_at >= today.toISOString());
        const portalSummary  = [
          `PORTAL SNAPSHOT (${new Date().toLocaleDateString('en-US', {timeZone:'America/Costa_Rica'})}):`,
          `Total active: ${dashboards.length} | Live: ${dashboards.filter(d=>d.customer_status==='live').length} | Phase 1: ${dashboards.filter(d=>d.customer_status==='phase_1').length} | Phase 2: ${dashboards.filter(d=>d.customer_status==='phase_2').length} | Phase 3: ${dashboards.filter(d=>d.customer_status==='phase_3').length} | Blocked: ${dashboards.filter(d=>d.customer_status==='blocked').length}`,
          blocked.length > 0 ? `Blocked activities (${blocked.length}): ${blocked.map(a=>`${clientMap[a.customer_id]?.client_name||'Unknown'} → ${tMap[a.template_id]||'Unknown'}${a.notes?` (note: ${a.notes.substring(0,80)})`:''}` ).join(' | ')}` : 'No blocked activities.',
          completedToday.length > 0 ? `Completed today (${completedToday.length}): ${completedToday.map(a=>`${clientMap[a.customer_id]?.client_name||'Unknown'} → ${tMap[a.template_id]||'Unknown'}`).join(' | ')}` : 'No completions today.',
        ].join('\n');
        digest += `\n\n=== PORTAL ===\n${portalSummary}`;
      }
    } catch (portalErr) { console.error('Portal snapshot error in nightly learning:', portalErr.message); }

    // REVI digest — sales-call quality + leadership initiative movement from the
    // sibling coaching agent's schema. Non-fatal: nightly learning proceeds
    // REVI-blind if the revi schema is unreachable.
    try {
      const reviDigest = await reviBuildDailyDigest(26);
      if (reviDigest) digest += `\n\n=== REVI (sales coaching + leadership initiatives) ===\n${reviDigest}`;
    } catch (reviErr) { console.error('Nightly learning — REVI digest error:', reviErr.message); }

    // Auto strike mover daily roll-up — replaces the old per-run Slack posts.
    // Non-fatal: nightly learning proceeds strike-blind if the audit log is
    // unreachable, and the Slack line then says so.
    let strikeSlackLine = '';
    try {
      const strike = await strikeBuildDailyDigest(24);
      if (strike) {
        digest += `\n\n=== AUTO STRIKE MOVER (setter pipeline automation, last 24h) ===\n${strike.digestBlock}`;
        strikeSlackLine = `\n${strike.slackLine}`;
      }
    } catch (strikeErr) { console.error('Nightly learning — strike mover digest error:', strikeErr.message); }

    if (!digest) return;
    const todayStr = new Date().toLocaleDateString('en-US', { timeZone: 'America/Costa_Rica', weekday:'long', month:'long', day:'numeric' });
    const learningPrompt = `You are the NeuroGrowth PM agent. Today is ${todayStr}. The current year is 2026.\n\nBelow is today's activity from key Slack channels and the portal. Extract and summarize operational intelligence.\n\nFormat EVERY insight as exactly: CATEGORY | KEY | VALUE\n\nRules:\n- CATEGORY must be exactly one of these words with no other characters: client, team, process, decision, alert, intel, confidential\n- Any company financial, banking, or billing information — bank balances, failed or successful payments, invoices, subscription/billing status, cash or revenue figures from emails — MUST use CATEGORY confidential. Confidential entries are delivered privately to Ron and are never shown to the team.\n- Do NOT use markdown in CATEGORY. No asterisks, no backticks, no bold, no formatting. Just the plain word.\n- KEY should be a short descriptive identifier (client name, issue name, topic)\n- VALUE should be a single clear sentence or short paragraph, max 150 words\n- Only extract meaningful operational intelligence — skip small talk, greetings, and noise\n\nWhat to capture:\n1. Client status changes — who moved forward, who is blocked, who launched, who needs attention\n2. Wins and completions — what the team shipped or finished today\n3. Open action items that were raised but not resolved\n4. Team decisions made today\n5. Recurring patterns or blockers appearing across multiple clients\n6. Anything that should be flagged as an alert for tomorrow\n7. Email threads — any client or prospect communication that signals urgency, dissatisfaction, or opportunity\n8. Calendar events tomorrow — any sales calls, client check-ins, or deadlines Max should be aware of for morning briefing\n9. REVI section — sales-call quality patterns worth remembering: recurring objections across prospects, per-closer score trends, notable won/lost outcomes (save as team or intel). Leadership initiative movements: anything marked done/dropped is a decision; anything rediscussed_no_action repeatedly is an alert (initiative stalling)\n10. AUTO STRIKE MOVER section — only capture anomalies: zero sweeps ran (alert — cron may be dead), or unusually high move volume (intel). Do NOT judge failure levels yourself: the section states its own verdict. If it says "⚠️ FAILURE SPIKE", raise an alert quoting that line verbatim. If it says failures are within normal limits, or that it is warming up, say NOTHING about failures. A routine day (sweeps ran, few or no moves, failures within limits) needs NO entry\n\n${digest}`;
    const tNightly = Date.now();
    const response = await anthropic.messages.create({ model: 'claude-sonnet-4-6', max_tokens: 1024, messages: [{ role: 'user', content: learningPrompt }] });
    logLlmFromAnthropicResponse(response, Date.now() - tNightly, correlationId);
    const text  = response.content.filter(b => b.type === 'text').map(b => b.text).join('\n');
    const lines = text.split('\n').filter(l => l.includes('|'));
    let saved = 0;
    const savedEntries = [];
    const ronOnlyEntries = [];
    for (const line of lines) {
      const parts = line.split('|').map(p => p.trim());
      if (parts.length >= 3) {
        const [rawCategory, key, ...valueParts] = parts;
        // Strip any markdown characters and normalize to valid category
        const category = rawCategory.toLowerCase().replace(/[^a-z]/g, '').trim();
        const VALID_CATEGORIES = new Set(['client','team','process','decision','alert','intel']);
        const value = valueParts.join('|').trim();
        // Financial/billing intel is Ron-confidential: stored as private
        // knowledge (only Ron's queries surface it), DM'd to Ron, and kept out
        // of savedEntries so it never reaches the public team summary below.
        if (category === 'confidential' && key && value) {
          await upsertKnowledge('alert', `confidential:${key}`, value, 'nightly-learning', RON_SLACK_ID, 'private');
          ronOnlyEntries.push({ key, value });
          continue;
        }
        if (category && VALID_CATEGORIES.has(category) && key && value) {
          const normalizedKey = category === 'client'
            ? `client:${key.toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/(^-|-$)/g, '')}:${new Date().toISOString().slice(0, 10)}`
            : key;
          await upsertKnowledge(category, normalizedKey, value, 'nightly-learning');
          saved++;
          savedEntries.push({ category, key, value });
        }
      }
    }

    // Sneak-peek executive summary appended to the confirmation post — the team
    // sees WHAT Max learned, not just a count. Non-fatal: on any error the post
    // falls back to the plain confirmation line.
    let learnedBlock = '';
    if (savedEntries.length) {
      try {
        // Cap per-entry, never tail-truncate the list — every saved entry from
        // every source (Slack, Gmail, Calendar, Portal, REVI) must reach the
        // summarizer, or low-priority-sorted categories would silently drop.
        const order = { alert: 0, decision: 1, client: 2, team: 3, intel: 4, process: 5 };
        const entryList = savedEntries
          .sort((a, b) => (order[a.category] ?? 9) - (order[b.category] ?? 9))
          .map(e => `[${e.category}] ${e.key}: ${String(e.value).slice(0, 300)}`)
          .join('\n')
          .slice(0, 24000);
        const tSum = Date.now();
        const sumRes = await anthropic.messages.create({
          model: 'claude-sonnet-4-6',
          max_tokens: 350,
          messages: [{ role: 'user', content: `You are Max, NeuroGrowth's PM agent. You just saved these knowledge entries from tonight's learning cycle:\n\n${entryList}\n\nWrite a sneak-peek summary for the team Slack channel: 3-6 bullet lines giving a HIGH-LEVEL overview of what was learned today. Blend related entries into single surface-level statements — themes over details. Skip metrics, counts, and specifics unless one is essential to understand the point; a reader just wants to know what kinds of things were learned. Cover the breadth of every source (don't drop a whole topic area), most important first (alerts and decisions before general intel), each line under 14 words, plain direct language. Never mention company financial, banking, or billing details (balances, payments, invoices, subscription status) — those are confidential to Ron and must not appear in this team-facing summary. Start every line with "• ". Output ONLY the bullet lines — no intro, no headers, no bold, no markdown.` }],
        });
        logLlmFromAnthropicResponse(sumRes, Date.now() - tSum, correlationId);
        const sumText = sumRes.content.filter(b => b.type === 'text').map(b => b.text).join('\n').trim();
        if (sumText) learnedBlock = `\n\n*What I learned tonight:*\n${sumText}`;
      } catch (sumErr) { console.error('Nightly learning summary generation failed:', sumErr.message); }
    }

    console.log(`Nightly learning complete. ${saved} knowledge entries saved, ${ronOnlyEntries.length} confidential (Ron-only).`);
    await postToSlack(AGENT_CHANNEL, `🧠 *Nightly learning complete* — ${new Date().toLocaleDateString('en-US', {weekday:'long', month:'long', day:'numeric', timeZone:'America/Costa_Rica'})}\nSources scanned: 5 Slack channels + Gmail + Calendar + REVI + strike mover | Knowledge entries saved: ${saved}${strikeSlackLine}${learnedBlock}`);

    // Confidential (financial/billing) findings go to Ron's DM only.
    if (ronOnlyEntries.length) {
      try {
        await slack.client.chat.postMessage({
          channel: RON_SLACK_ID,
          text: `🔒 *Confidential — from tonight's learning (not shared with the team):*\n${ronOnlyEntries.map(e => `• *${e.key}:* ${e.value}`).join('\n')}`,
        });
      } catch (dmErr) { console.error('Failed to DM Ron confidential nightly entries:', dmErr.message); }
    }
  } catch (err) {
    console.error('Nightly learning error:', err.message);
    try {
      await slack.client.chat.postMessage({
        channel: RON_SLACK_ID,
        text: `Nightly learning failed: ${err.message}. Knowledge base was not updated tonight. Check Railway logs for details.`,
      });
    } catch (dmErr) {
      console.error('Failed to DM Ron about nightly learning error:', dmErr.message);
    }
  }
}

// ─── PROACTIVE ALERTS ─────────────────────────────────────────────────────────
async function runProactiveAlerts(correlationId) {
  console.log('Running proactive alert check...');
  try {
    const { data, error } = await supabase.from('agent_knowledge').select('key, value, updated_at').eq('category', 'alert').eq('visibility', 'shared').order('updated_at', { ascending: true });
    if (error || !data || !data.length) return;
    const now      = Date.now();
    const oneDayMs = 24 * 60 * 60 * 1000;
    const staleAlerts = data.filter(a => (now - new Date(a.updated_at).getTime()) > oneDayMs);
    if (!staleAlerts.length) return;
    const alertText = staleAlerts.map(a => `${a.key}: ${a.value}`).join('\n\n');
    const prompt    = `You are the NeuroGrowth PM agent checking on unresolved alerts.\n\nThese items have been flagged as alerts and have not been updated in over 24 hours:\n\n${alertText}\n\nWrite a brief, direct message to Ron (2-4 sentences) summarizing what is still unresolved and what needs his attention today. No markdown formatting. Sound like a colleague, not a report.`;
    const tPa = Date.now();
    const response  = await anthropic.messages.create({ model: 'claude-sonnet-4-6', max_tokens: 256, messages: [{ role: 'user', content: prompt }] });
    logLlmFromAnthropicResponse(response, Date.now() - tPa, correlationId);
    const message   = response.content.filter(b => b.type === 'text').map(b => b.text).join('\n');
    await postToSlack(AGENT_CHANNEL, message);
    console.log(`Proactive alert posted. ${staleAlerts.length} unresolved items flagged.`);
  } catch (err) { console.error('Proactive alert error:', err.message); }
}

// ─── PROACTIVE TEAM DMs ───────────────────────────────────────────────────────
// Runs nightly. Checks portal for clients hitting critical milestones tomorrow
// and DMs the responsible team member before they even have to ask.
// BEGIN PROACTIVE-DM PURE HELPERS
// Which launch-window warning, if any, is owed for a client right now.
//
// Thresholds are "crossed and not yet notified", never an exact day number. Exact
// equality (daysSince === 13) meant any day the job did not run — a deploy, an
// outage, a weekend, the cron sitting disabled — was a warning nobody ever got,
// with no way to tell a missed warning from a client who never needed one.
//
// Day 14 is the deadline, so its warning is owed from Day 13 onward and wins over
// Day 7: a client who crossed both while the job was down should get the urgent
// one, not two DMs about the same client on the same run.
function proactiveMilestoneDue(daysSince, day14Notified, day7Notified) {
  if (!Number.isFinite(daysSince)) return null;
  if (daysSince >= 13) return day14Notified ? null : 'day14';
  if (daysSince >= 6)  return day7Notified  ? null : 'day7';
  return null;
}
// END PROACTIVE-DM PURE HELPERS

async function runProactiveDMs(_correlationId) {
  console.log('Running proactive team DMs...');
  try {
    const { data: dashboards } = await portalSupabase
      .from('client_dashboards')
      .select('id, client_name, email, customer_status, customer_type, created_at')
      .eq('is_active', true)
      .in('customer_status', ['phase_1', 'phase_2', 'phase_3', 'blocked']);

    if (!dashboards || !dashboards.length) {
      console.log('Proactive DMs: no at-risk clients found.');
      return;
    }

    const activationFor = await loadActivationDates();
    const now = Date.now();
    // Same anchor as the classification loop below. Display and classification MUST
    // read the same source — a DM that says "Day 5" while the client was selected
    // for a Day-14 warning is worse than no DM.
    const dayNFor = (d) => {
      const iso = activationFor(d) || d.created_at;
      return iso ? Math.floor((now - new Date(iso).getTime()) / (1000 * 60 * 60 * 24)) : '?';
    };
    const tomorrow = new Date();
    tomorrow.setDate(tomorrow.getDate() + 1);
    const tomorrowStr = tomorrow.toLocaleDateString('en-US', { weekday:'long', month:'long', day:'numeric', timeZone:'America/Costa_Rica' });

    const hitting14Tomorrow   = []; // Day 13 today → Day 14 tomorrow (launch deadline)
    const hitting7Tomorrow    = []; // Day 6 today → Day 7 tomorrow (at-risk threshold)
    const blocked             = []; // Currently blocked
    const stalledPhase1       = []; // Stuck in phase_1 for 4+ days
    const stalledPhase2       = []; // Stuck in phase_2 for 4+ days

    // Day N counts from the activation call — this job exists to warn on the 14-day
    // Build & Release window, so it must use the same anchor the window is defined
    // by. It previously counted from client_dashboards.created_at (when the portal
    // row appeared), which includes pre-activation waiting time and drifts from the
    // real deadline by however long onboarding took. created_at remains only as a
    // last-resort proxy for clients with no recorded call date.
    //
    // Milestones fire on "crossed the threshold and not yet notified", NOT on an
    // exact day number. Exact equality (daysSince === 13) means any day this job
    // does not run — a deploy, an outage, a weekend gap, the cron being disabled
    // as it is today — is a warning nobody ever receives, silently, with no way to
    // tell a missed warning from a client who never needed one.
    //
    // The notified-key includes the anchor DATE, so a rescheduled activation call
    // re-arms both milestones: a new call date is a new 14-day window and deserves
    // its own warnings. Same agent_knowledge idiom as outcome-proposal.
    const notifiedKeys = new Set();
    {
      const { data: sent } = await supabase
        .from('agent_knowledge')
        .select('key')
        .like('key', 'proactive-dm:%');
      for (const r of sent || []) notifiedKeys.add(r.key);
    }
    const milestoneKey = (dash, milestone, anchorISO) =>
      `proactive-dm:${dash.id}:${milestone}:${new Date(anchorISO).toLocaleDateString('en-CA', { timeZone: 'America/Costa_Rica' })}`;
    const firedThisRun = [];

    for (const dash of dashboards) {
      const anchorISO = activationFor(dash) || dash.created_at;
      if (!anchorISO) continue;
      const daysSince = Math.floor((now - new Date(anchorISO).getTime()) / (1000 * 60 * 60 * 24));

      // Day 14 is the deadline, so the warning is due from Day 13 onward. Checked
      // before Day 7 and with `else` so a client who crossed both while the job was
      // down gets the more urgent one, not two DMs about the same client.
      const key14 = milestoneKey(dash, 'day14', anchorISO);
      const key7  = milestoneKey(dash, 'day7',  anchorISO);
      const due = proactiveMilestoneDue(daysSince, notifiedKeys.has(key14), notifiedKeys.has(key7));
      if (due === 'day14') {
        hitting14Tomorrow.push(dash);
        firedThisRun.push([key14, `day14|${daysSince}`]);
      } else if (due === 'day7') {
        hitting7Tomorrow.push(dash);
        firedThisRun.push([key7, `day7|${daysSince}`]);
      }
      if (dash.customer_status === 'blocked') blocked.push(dash);
      if (dash.customer_status === 'phase_1' && daysSince >= 4) stalledPhase1.push(dash);
      if (dash.customer_status === 'phase_2' && daysSince >= 4) stalledPhase2.push(dash);
    }

    // ── DM Josue: clients hitting Day 14 today ──
    if (hitting14Tomorrow.length > 0) {
      const names = hitting14Tomorrow.map(d => `${d.client_name} (Day ${dayNFor(d)})`).join(', ');
      const msg = `Heads up — ${hitting14Tomorrow.length === 1 ? 'this client is' : 'these clients are'} at or past the 14-day launch deadline: ${names}. If campaigns are not live, we have missed the SLA. What needs to happen right now to get them launched?`;
      for (const id of (slackIdsByRole('tech_ops').length ? slackIdsByRole('tech_ops') : ['U08ABBFNGUW'])) {
        await slack.client.chat.postMessage({ channel: id, text: msg });
      }
      console.log(`Proactive DM sent to Josue: ${hitting14Tomorrow.length} client(s) hitting Day 14 today`);
    }

    // ── DM Josue: clients hitting at-risk threshold today ──
    if (hitting7Tomorrow.length > 0) {
      const names = hitting7Tomorrow.map(d => `${d.client_name} (Day ${dayNFor(d)})`).join(', ');
      const msg = `Quick flag — ${hitting7Tomorrow.length === 1 ? 'this client has' : 'these clients have'} passed Day 7, the at-risk threshold: ${names}. Worth checking their progress now so we're not scrambling later.`;
      for (const id of (slackIdsByRole('tech_ops').length ? slackIdsByRole('tech_ops') : ['U08ABBFNGUW'])) {
        await slack.client.chat.postMessage({ channel: id, text: msg });
      }
      console.log(`Proactive DM sent to Josue: ${hitting7Tomorrow.length} client(s) past Day 7`);
    }

    // Record the milestones ONLY now, after the DMs actually went out. Marking them
    // notified before sending would turn any Slack failure into a warning that is
    // permanently suppressed — the exact silent-miss this change exists to remove.
    for (const [key, value] of firedThisRun) {
      await upsertKnowledge('process', key, `${value}|${new Date().toISOString().slice(0, 10)}`, 'proactive-dm');
    }

    // ── DM Valeria: clients stalled in phase_1 ──
    if (stalledPhase1.length > 0) {
      const names = stalledPhase1.map(d => `${d.client_name} (Day ${dayNFor(d)})`).join(', ');
      const msg = `These clients are still in Phase 1 and have been for a while: ${names}. If any delivery documents are pending on your end, this is the priority. Let Josue know if you're blocked on anything.`;
      for (const id of (slackIdsByRole('fulfillment').length ? slackIdsByRole('fulfillment') : ['U09Q3BXJ18B'])) {
        await slack.client.chat.postMessage({ channel: id, text: msg });
      }
      console.log(`Proactive DM sent to fulfillment role: ${stalledPhase1.length} client(s) stalled in Phase 1`);
    }

    // ── DM Felipe: clients stalled in phase_2 ──
    if (stalledPhase2.length > 0) {
      const names = stalledPhase2.map(d => `${d.client_name} (Day ${dayNFor(d)})`).join(', ');
      const msg = `These clients are still in Phase 2 and haven't moved in a few days: ${names}. If campaign config or Prosp setup is pending on your end, these need to be the first thing tomorrow. Flag Josue if anything is blocked.`;
      for (const id of (slackIdsByRole('campaigns').length ? slackIdsByRole('campaigns') : ['U09TNMVML3F'])) {
        await slack.client.chat.postMessage({ channel: id, text: msg });
      }
      console.log(`Proactive DM sent to campaigns role: ${stalledPhase2.length} client(s) stalled in Phase 2`);
    }

    // ── DM client success: blocked clients ──
    if (blocked.length > 0) {
      const names = blocked.map(d => `${d.client_name}`).join(', ');
      const msg = `These clients are currently blocked: ${names}. If the block is on the client side — missing onboarding form, unresponsive, contract issue — this needs a proactive outreach before it becomes a bigger problem. Can you check what's needed and follow up?`;
      for (const id of slackIdsByRole('client_success')) {
        await slack.client.chat.postMessage({ channel: id, text: msg });
      }
      console.log(`Proactive DM sent to client_success: ${blocked.length} blocked client(s)`);
    }

    // ── DM client success: Phase 0 clients stuck ≥7 days in same step ─────────
    const { data: stuckPhase0 } = await portalSupabase
      .from('v_phase0_fulfillment')
      .select('email, first_name, last_name, company, phase0_step, days_in_phase0')
      .gte('days_in_phase0', 7)
      .order('days_in_phase0', { ascending: false });

    if (stuckPhase0 && stuckPhase0.length > 0) {
      const stepLabels = {
        '1_awaiting_signup':          'awaiting portal signup',
        '2_awaiting_terms':           'awaiting T&C acceptance',
        '3_awaiting_form':            'awaiting onboarding form',
        '4_awaiting_activation_call': 'awaiting activation call booking',
        '5_ready_for_handoff':        'ready for Phase 1 handoff — not moved yet',
      };
      const lines = stuckPhase0.map(r => {
        const name = [r.first_name, r.last_name].filter(Boolean).join(' ') || r.email;
        const co   = r.company ? ` (${r.company})` : '';
        return `• ${name}${co} — ${stepLabels[r.phase0_step] || r.phase0_step} — Day ${r.days_in_phase0}`;
      }).join('\n');
      const urgentCount = stuckPhase0.filter(r => r.days_in_phase0 >= 14).length;
      const urgentNote  = urgentCount > 0 ? ` ${urgentCount} of them are past 14 days — that's critical.` : '';
      const msg = `Phase 0 alert — ${stuckPhase0.length} client${stuckPhase0.length > 1 ? 's' : ''} stuck in pre-portal onboarding for 7+ days.${urgentNote}\n\n${lines}\n\nCan you reach out to each one and unblock whatever step they're on?`;
      for (const id of slackIdsByRole('client_success')) {
        await slack.client.chat.postMessage({ channel: id, text: msg });
      }
      console.log(`Proactive DM sent to client_success: ${stuckPhase0.length} Phase 0 client(s) stuck ≥7 days`);
    }

    // ── DM client success: clients hitting Day 20 in Phase 3 (stabilization) ──
    // Uses stabilization_started_at as the Day 1 anchor.
    // Day 20 = time to reach out, schedule the 1:1 progress check, coordinate with client.
    const { data: phase3Clients } = await portalSupabase
      .from('client_dashboards')
      .select('id, client_name, stabilization_started_at')
      .eq('is_active', true)
      .eq('customer_status', 'phase_3')
      .not('stabilization_started_at', 'is', null);

    const hitting20InStabilization = (phase3Clients || []).filter(d => {
      const daysInStabilization = Math.floor((now - new Date(d.stabilization_started_at).getTime()) / (1000 * 60 * 60 * 24));
      return daysInStabilization === 20;
    });

    const approaching20InStabilization = (phase3Clients || []).filter(d => {
      const daysInStabilization = Math.floor((now - new Date(d.stabilization_started_at).getTime()) / (1000 * 60 * 60 * 24));
      return daysInStabilization === 18; // 2-day heads up before Day 20
    });

    if (hitting20InStabilization.length > 0) {
      const names = hitting20InStabilization.map(d => d.client_name).join(', ');
      const msg = `Day 20 in stabilization today for: ${names}. This is the checkpoint — time to reach out to the client, schedule the 1:1 progress check, and confirm how the campaign is performing. Can you get that call on the calendar and flag anything that needs Ron's attention?`;
      for (const id of slackIdsByRole('client_success')) {
        await slack.client.chat.postMessage({ channel: id, text: msg });
      }
      console.log(`Proactive DM sent to client_success: ${hitting20InStabilization.length} client(s) at Day 20 stabilization`);
    }

    if (approaching20InStabilization.length > 0) {
      const names = approaching20InStabilization.map(d => d.client_name).join(', ');
      const msg = `Heads up — these clients hit Day 20 in stabilization in 2 days: ${names}. Start preparing the 1:1 progress check outreach so it's ready to go on Day 20.`;
      for (const id of slackIdsByRole('client_success')) {
        await slack.client.chat.postMessage({ channel: id, text: msg });
      }
      console.log(`Proactive DM sent to client_success: ${approaching20InStabilization.length} client(s) approaching Day 20 stabilization`);
    }

    console.log('Proactive team DMs complete.');
  } catch (err) {
    console.error('Proactive DM error:', err.message);
  }
}

// ─── ANOMALY DETECTION (Phase 1 — intelligence layer) ────────────────────────
// Scrapes business metrics daily, maintains rolling baselines, detects anomalies
// at >= 1.5σ, persists alerts to agent_knowledge, and DMs domain-routed roles.

const ANOMALY_THRESHOLD_SIGMA = 1.5;
const ANOMALY_MIN_SAMPLE      = 7;
const ANOMALY_WINDOW_DAYS     = 28;

// Domain → roles that should be DM'd when an anomaly fires.
// Ron sees everything; the rest of the team only their lanes. Recipients deduped.
const ANOMALY_ROUTING = {
  marketing:      ['ceo'],
  sales:          ['ceo'],
  fulfillment:    ['ceo', 'tech_ops', 'fulfillment', 'client_success', 'tech_lead'],
  client_success: ['ceo', 'client_success', 'tech_lead'],
};

function _resolveAnomalyRecipients(domain) {
  const roles = ANOMALY_ROUTING[domain] || ['ceo'];
  const ids = new Set();
  for (const role of roles) {
    const matched = slackIdsByRole(role);
    if (matched.length) matched.forEach(id => ids.add(id));
  }
  if (!ids.size) ids.add(RON_SLACK_ID);
  return [...ids];
}

async function recordObservation(metric, domain, value, source = 'scraper', meta = null) {
  const safeValue = Number(value);
  if (!Number.isFinite(safeValue)) {
    console.warn(`Anomaly: skipping ${metric} — value is not a finite number (${value})`);
    return null;
  }
  const { data, error } = await supabase
    .from('metric_observations')
    .insert({ metric, domain, value: safeValue, source, meta })
    .select()
    .single();
  if (error) { console.error(`Anomaly: insert failed for ${metric}:`, error.message); return null; }
  return data;
}

async function recomputeBaseline(metric, windowDays = ANOMALY_WINDOW_DAYS) {
  const since = new Date(Date.now() - windowDays * 24 * 60 * 60 * 1000).toISOString();
  const { data, error } = await supabase
    .from('metric_observations')
    .select('value, domain')
    .eq('metric', metric)
    .gte('observed_at', since);
  if (error) { console.error(`Anomaly: baseline read failed for ${metric}:`, error.message); return null; }
  if (!data || data.length === 0) return null;

  const values = data.map(r => Number(r.value)).filter(Number.isFinite);
  const n = values.length;
  if (n === 0) return null;
  const mean = values.reduce((a, b) => a + b, 0) / n;
  const variance = n > 1 ? values.reduce((a, b) => a + (b - mean) ** 2, 0) / (n - 1) : 0;
  const stdDev = Math.sqrt(variance);
  const domain = data[0].domain;

  const { error: upErr } = await supabase
    .from('metric_baselines')
    .upsert(
      { metric, domain, mean, std_dev: stdDev, sample_size: n, window_days: windowDays, last_computed: new Date().toISOString() },
      { onConflict: 'metric' }
    );
  if (upErr) { console.error(`Anomaly: baseline upsert failed for ${metric}:`, upErr.message); return null; }
  return { metric, domain, mean, stdDev, sampleSize: n };
}

async function detectAnomaly(metric, threshold = ANOMALY_THRESHOLD_SIGMA) {
  const { data: latestRows } = await supabase
    .from('metric_observations')
    .select('value, observed_at, meta')
    .eq('metric', metric)
    .order('observed_at', { ascending: false })
    .limit(1);
  const latest = latestRows?.[0];
  if (!latest) return null;

  const { data: baseline } = await supabase
    .from('metric_baselines')
    .select('*')
    .eq('metric', metric)
    .single();
  if (!baseline) return null;
  if (baseline.sample_size < ANOMALY_MIN_SAMPLE) {
    console.log(`Anomaly: ${metric} still warming up (n=${baseline.sample_size} < ${ANOMALY_MIN_SAMPLE})`);
    return null;
  }
  if (Number(baseline.std_dev) === 0) return null; // flat metric, can't z-score

  const value = Number(latest.value);
  const z = (value - Number(baseline.mean)) / Number(baseline.std_dev);
  const triggered = Math.abs(z) >= threshold;
  return {
    metric,
    domain: baseline.domain,
    value,
    mean: Number(baseline.mean),
    stdDev: Number(baseline.std_dev),
    sampleSize: baseline.sample_size,
    z,
    triggered,
    observedAt: latest.observed_at,
    meta: latest.meta,
  };
}

async function narrateAnomaly(snapshot) {
  try {
    const direction = snapshot.z > 0 ? 'above' : 'below';
    const prompt = `Metric "${snapshot.metric}" (domain: ${snapshot.domain}) is ${Math.abs(snapshot.z).toFixed(1)}σ ${direction} its ${snapshot.sampleSize}-sample baseline.\nLatest value: ${snapshot.value}. Baseline mean: ${snapshot.mean.toFixed(2)} (std dev ${snapshot.stdDev.toFixed(2)}).\n\nWrite ONE short sentence (no markdown, no preamble) on what this likely means in plain business English and what to watch next. Max 30 words.`;
    const res = await anthropic.messages.create({
      model: 'claude-sonnet-4-6',
      max_tokens: 100,
      messages: [{ role: 'user', content: prompt }],
    });
    const text = res.content.filter(b => b.type === 'text').map(b => b.text).join('').trim();
    return text || null;
  } catch (err) {
    console.error(`Anomaly narration error for ${snapshot.metric}:`, err.message);
    return null;
  }
}

// ── Metric scrapers (one per metric, returns numeric value or null) ─────────

// Helper: fetch Meta account-level insights for a date_preset, returns the first data row
async function _metaAccountInsights(datePreset = 'today') {
  const accountId = process.env.META_AD_ACCOUNT_ID;
  const token     = process.env.META_ACCESS_TOKEN;
  if (!accountId || !token) return null;
  const res  = await fetch(`https://graph.facebook.com/v19.0/${accountId}/insights?fields=spend,actions&date_preset=${datePreset}&access_token=${token}`);
  const data = await res.json();
  if (data.error) throw new Error(data.error.message);
  return data.data?.[0] || null;
}

// Form funnel CPL — spend / leads across campaigns that fire the `lead` action_type.
// VSL campaigns never fire `lead`, so this naturally isolates the Form funnel.
async function _scrapeMetaFormCplToday() {
  const accountId = process.env.META_AD_ACCOUNT_ID;
  const token     = process.env.META_ACCESS_TOKEN;
  if (!accountId || !token) return null;
  const fields = 'id,name,insights.date_preset(today){spend,actions}';
  const res  = await fetch(`https://graph.facebook.com/v19.0/${accountId}/campaigns?fields=${fields}&limit=20&access_token=${token}`);
  const data = await res.json();
  if (data.error) throw new Error(data.error.message);
  let totalSpend = 0, totalLeads = 0;
  for (const c of (data.data || [])) {
    const ins   = c.insights?.data?.[0];
    if (!ins) continue;
    const leads = parseInt((ins.actions || []).find(a => a.action_type === 'lead')?.value || '0', 10);
    if (leads > 0) {
      totalSpend += parseFloat(ins.spend || 0);
      totalLeads += leads;
    }
  }
  if (totalLeads <= 0) return null;
  return +(totalSpend / totalLeads).toFixed(2);
}

// CAC — spend / Meta `purchase` actions. Fired via CAPI by Make scenario 5148801
// (GHL Opp Won → Purchase) since the 2026-07-23 cutover; the original
// iClosed-fired pixel died with iClosed.
async function _scrapeMetaCacToday() {
  const row = await _metaAccountInsights('today');
  if (!row) return null;
  const spend    = parseFloat(row.spend || 0);
  const sales    = parseInt((row.actions || []).find(a => a.action_type === 'purchase')?.value || '0', 10);
  if (sales <= 0) return null;
  return +(spend / sales).toFixed(2);
}

// CR-anchored day bounds as UTC instants. The old `${date}T00:00:00` strings
// (built from UTC dates, interpreted as UTC by Postgres) drifted the day window
// 6 hours vs Costa Rica — CR-evening calls landed on the wrong day.
function _crDayBoundsUtc(daysAgo = 0) {
  const dayStr = new Date(Date.now() - daysAgo * 24 * 60 * 60 * 1000)
    .toLocaleDateString('en-CA', { timeZone: 'America/Costa_Rica' });
  const start = `${dayStr}T06:00:00.000Z`; // CR midnight (UTC-6, no DST)
  const end   = new Date(Date.parse(start) + 24 * 60 * 60 * 1000).toISOString();
  return { start, end, dayStr };
}

/// Cost-per-booking: Meta total spend today / sales calls booked today
async function _scrapeMetaCostPerBookingToday() {
  const row = await _metaAccountInsights('today');
  if (!row) return null;
  const spend = parseFloat(row.spend || 0);
  if (spend <= 0) return null;
  const day = _crDayBoundsUtc(0);
  const { data, error } = await portalSupabase
    .from('revops_appointments')
    .select('id, iclosed_call_id, ghl_appointment_id')
    .gte('booked_at', day.start)
    .lt('booked_at',  day.end);
  if (error) throw new Error(error.message);
  const excludeIds = await getNonFlywheelCallIds();
  const count = filterFlywheelAppts(data, excludeIds).length;
  if (!count || count <= 0) return null;
  return +(spend / count).toFixed(2);
}

// New leads YESTERDAY — Form/WA funnel via lead_posts (deduped GHL lead feed).
// Replaces the old raw GET /contacts/ call, which passed v1 startDate/endDate
// params to the v2 endpoint and had no res.ok check, so every API failure was
// silently recorded as a legitimate 0 and poisoned the anomaly baseline.
// lead_posts rows are written by the GHL lead-intake webhook and already
// deduped against the FB→WhatsApp duplicate-contact path (one logical lead
// per slack_message_ts). Errors THROW so the anomaly cron records a scrape
// error instead of a fake zero.
//
// Window is YESTERDAY (_crDayBoundsUtc(1)), matching every other sales scraper.
// It was briefly `today`, which meant the 6 AM CR anomaly cron only ever saw
// CR midnight–06:00 — a ~6-hour slice reported under a "today" label (first
// real value was 5 when the full day was ~35). Expect a one-time level shift
// upward as the 28-day baseline refills with full-day values.
//
// Metric key `ghl_new_contacts_today` is a FROZEN metric_observations PK with
// baseline history — the name is now historical, like the iclosed_* keys.
async function _scrapeGhlNewContactsToday() {
  const day = _crDayBoundsUtc(1);
  const { data, error } = await supabase
    .from('lead_posts')
    .select('slack_message_ts')
    .gte('posted_at', day.start)
    .lt('posted_at',  day.end);
  if (error) throw new Error(error.message);
  return new Set((data || []).map(r => r.slack_message_ts).filter(Boolean)).size;
}

// Calls booked yesterday (flywheel-only; booked_at populated by dash for GHL rows)
async function _scrapeIclosedCallsBookedYesterday() {
  const day = _crDayBoundsUtc(1);
  const { data, error } = await portalSupabase
    .from('revops_appointments')
    .select('id, iclosed_call_id, ghl_appointment_id')
    .gte('booked_at', day.start)
    .lt('booked_at',  day.end);
  if (error) throw new Error(error.message);
  const excludeIds = await getNonFlywheelCallIds();
  return filterFlywheelAppts(data, excludeIds).length;
}

// Calls held yesterday (flywheel-only). Held = GHL marked the call showed
// (attended=true via the call_showed workflow, live since ~Jul 28) OR the
// logged outcome says it happened — catches showed-but-outcome-not-yet-logged
// calls. Same definition dash's isHeld uses; attended=false alone never counts.
async function _scrapeIclosedCallsHeldYesterday() {
  const day = _crDayBoundsUtc(1);
  const { data, error } = await portalSupabase
    .from('revops_appointments')
    .select('id, iclosed_call_id, ghl_appointment_id, attended')
    .gte('scheduled_start', day.start)
    .lt('scheduled_start',  day.end);
  if (error) throw new Error(error.message);
  const excludeIds = await getNonFlywheelCallIds();
  const appts = filterFlywheelAppts(data, excludeIds);
  if (!appts.length) return 0;
  const { data: outcomes } = await portalSupabase
    .from('revops_sales_outcomes')
    .select('appointment_id, outcome')
    .in('appointment_id', appts.map(a => a.id));
  const outcomesById = Object.fromEntries((outcomes || []).map(o => [o.appointment_id, o]));
  return appts.filter(a => a.attended === true || classifyOutcome(outcomesById[a.id]).showed).length;
}

// Sales yesterday (won outcomes logged in GHL — EOD tables retired at cutover)
async function _scrapeIclosedSalesYesterday() {
  const day = _crDayBoundsUtc(1);
  const { data, error } = await portalSupabase
    .from('revops_sales_outcomes')
    .select('id, outcome, created_at')
    .eq('outcome', 'won')
    .gte('created_at', day.start)
    .lt('created_at',  day.end);
  if (error) throw new Error(error.message);
  return (data || []).length;
}

// Close rate yesterday = won ÷ held for calls scheduled yesterday.
// Held uses the same attended-or-outcome definition as calls-held above.
async function _scrapeCloseRateYesterday() {
  const day = _crDayBoundsUtc(1);
  const { data, error } = await portalSupabase
    .from('revops_appointments')
    .select('id, iclosed_call_id, ghl_appointment_id, attended')
    .gte('scheduled_start', day.start)
    .lt('scheduled_start',  day.end);
  if (error) throw new Error(error.message);
  const excludeIds = await getNonFlywheelCallIds();
  const appts = filterFlywheelAppts(data, excludeIds);
  if (!appts.length) return null;
  const { data: outcomes } = await portalSupabase
    .from('revops_sales_outcomes')
    .select('appointment_id, outcome')
    .in('appointment_id', appts.map(a => a.id));
  const outcomesById = Object.fromEntries((outcomes || []).map(o => [o.appointment_id, o]));
  let held = 0, won = 0;
  for (const a of appts) {
    const o = outcomesById[a.id];
    if (a.attended === true || classifyOutcome(o).showed) held += 1;
    if ((o?.outcome || '').toLowerCase() === 'won') won += 1;
  }
  if (held <= 0) return null;
  return +(won / held).toFixed(4);
}

// Setter-booked calls yesterday (native setter_id — EOD tables retired at cutover)
async function _scrapeSetterCallsBookedYesterday() {
  const day = _crDayBoundsUtc(1);
  const { data, error } = await portalSupabase
    .from('revops_appointments')
    .select('id, iclosed_call_id, ghl_appointment_id, setter_id')
    .not('setter_id', 'is', null)
    .gte('booked_at', day.start)
    .lt('booked_at',  day.end);
  if (error) throw new Error(error.message);
  const excludeIds = await getNonFlywheelCallIds();
  return filterFlywheelAppts(data, excludeIds).length;
}

async function _scrapePhase0ToPhase1Conv7d() {
  const sevenDaysAgo = new Date(Date.now() - 7 * 24 * 60 * 60 * 1000).toISOString();
  // Numerator: clients now in phase_1+ that were created within last 7 days
  const { data: promoted, error: pErr } = await portalSupabase
    .from('client_dashboards')
    .select('id')
    .gte('created_at', sevenDaysAgo)
    .in('customer_status', ['phase_1','phase_2','phase_3','live']);
  if (pErr) throw new Error(pErr.message);
  // Denominator: total active clients created in last 7 days (any status including phase_0)
  const { data: total, error: tErr } = await portalSupabase
    .from('client_dashboards')
    .select('id')
    .gte('created_at', sevenDaysAgo)
    .eq('is_active', true);
  if (tErr) throw new Error(tErr.message);
  const denom = total?.length || 0;
  if (denom === 0) return null;
  return +((promoted?.length || 0) / denom).toFixed(4);
}

async function _scrapePhaseCycleP50(targetStatus) {
  // Median days between activation_call.completed_at and current snapshot for clients
  // currently in `targetStatus` (rough proxy for "time spent in this phase").
  const { data: dashboards, error } = await portalSupabase
    .from('client_dashboards')
    .select('id, email, created_at, customer_status')
    .eq('is_active', true)
    .eq('customer_status', targetStatus);
  if (error) throw new Error(error.message);
  if (!dashboards || !dashboards.length) return null;

  const { data: templates } = await portalSupabase
    .from('customer_activity_templates')
    .select('id, title');
  const tmap = {};
  (templates || []).forEach(t => { tmap[t.id] = (t.title || '').toLowerCase(); });

  const activationFor = await loadActivationDates();
  const days = [];
  for (const d of dashboards) {
    const { data: acts } = await portalSupabase
      .from('customer_activities')
      .select('template_id, completed_at')
      .eq('customer_id', d.id);
    const activation = (acts || []).find(a => (tmap[a.template_id] || '').includes('activation call') && a.completed_at);
    const real = activationFor(d);
    const start = real ? new Date(real)
      : activation ? new Date(activation.completed_at)
      : (d.created_at ? new Date(d.created_at) : null);
    if (!start) continue;
    days.push(Math.floor((Date.now() - start.getTime()) / (24 * 60 * 60 * 1000)));
  }
  if (!days.length) return null;
  days.sort((a, b) => a - b);
  return days[Math.floor(days.length / 2)];
}

async function _scrapeDay7AtRiskCount() {
  const { data: dashboards, error } = await portalSupabase
    .from('client_dashboards')
    .select('id, email, created_at, customer_status')
    .eq('is_active', true)
    .in('customer_status', ['phase_1', 'phase_2']);
  if (error) throw new Error(error.message);
  if (!dashboards) return 0;

  const { data: templates } = await portalSupabase
    .from('customer_activity_templates')
    .select('id, title');
  const tmap = {};
  (templates || []).forEach(t => { tmap[t.id] = (t.title || '').toLowerCase(); });

  const activationFor = await loadActivationDates();
  let count = 0;
  for (const d of dashboards) {
    const { data: acts } = await portalSupabase
      .from('customer_activities')
      .select('template_id, completed_at')
      .eq('customer_id', d.id);
    const activation = (acts || []).find(a => (tmap[a.template_id] || '').includes('activation call') && a.completed_at);
    const real = activationFor(d);
    const start = real ? new Date(real)
      : activation ? new Date(activation.completed_at)
      : (d.created_at ? new Date(d.created_at) : null);
    if (!start) continue;
    const days = Math.floor((Date.now() - start.getTime()) / (24 * 60 * 60 * 1000));
    if (days >= 7) count++;
  }
  return count;
}

// ─── REVI DATA ACCESS ─────────────────────────────────────────────────────────
// ALL reads of the `revi` schema (REVI, the sales-coaching + initiative-tracking
// agent) live in this section. REVI owns that schema and may migrate it —
// explicit column lists here mean a rename breaks in one findable place, and
// every caller treats failures as non-fatal (surfaces degrade to pre-REVI
// output). Never select transcript_full (token bomb). teardown_text is Ron-only
// coaching material — never route it to a closer-facing surface; closer-facing
// surfaces use closer_draft_text (the coaching REVI already sent them).

// ── The call ledger: what REVI did with each recording, and WHY ─────────────
// revi.call_ledger (REVI migration 0015) holds one row per Fathom recording:
// track, how it resolved, action, and a fixed `reason` slug. Before it existed
// the reason lived only in REVI's control flow and its logs, so this health
// check had to INFER three different states from a single heartbeat timestamp —
// and inferred wrong on 2026-08-04, 08-13 and 08-18. `reason` is a cross-repo
// contract; the vocabulary is documented in REVI's migration 0015 header.
async function reviGetCallLedgerSince(sinceISO) {
  const { data, error } = await reviSupabase
    .from('call_ledger')
    .select('fathom_recording_id, title, host_email, call_date, duration_min, ghl_appointment_id, calendar_id, track, report_kind, resolved_by, action, reason, detail, recording_url')
    .gte('call_date', sinceISO)
    .order('call_date', { ascending: false });
  if (error) throw error;
  return data || [];
}

// Which GHL calendar means which track. REVI mirrors the GHL calendar grouping
// (NG Sales / Fulfilment Ops) into revi.calendar_taxonomy nightly. A calendar's
// own `track` overrides its group's — that is how the two mixed-use Internal 1:1
// calendars stay out of the sales set.
async function reviGetCalendarTaxonomy() {
  const [{ data: tax, error: tErr }, { data: groups, error: gErr }] = await Promise.all([
    reviSupabase.from('calendar_taxonomy').select('calendar_id, group_id, calendar_name, track, report_kind, is_active, synced_at'),
    reviSupabase.from('calendar_groups').select('group_id, group_name, track'),
  ]);
  if (tErr) throw tErr;
  if (gErr) throw gErr;
  const groupTrack = Object.fromEntries((groups || []).map(g => [g.group_id, g.track]));
  const byCalendar = new Map();
  for (const t of tax || []) {
    byCalendar.set(t.calendar_id, { ...t, track: t.track || groupTrack[t.group_id] || 'unknown' });
  }
  const idsForTrack = (track) => new Set([...byCalendar.values()]
    .filter(c => c.track === track && c.is_active !== false)
    .map(c => c.calendar_id));
  const salesCalendarIds = idsForTrack('sales');
  // Fulfilment calls are watched for missing recordings too: an unrecorded client
  // check-in means a CLIENT silently gets no report. That is the Jacobo Lau case
  // (booked 2026-08-05 on the Internal-1:1-with-Ron calendar, never recorded, no
  // report, nobody noticed). `internal` and `unknown` stay unwatched — the first
  // is deliberately report-free, and the second has no agreed meaning yet.
  const fulfilmentCalendarIds = idsForTrack('fulfilment');
  const watchedCalendarIds = new Set([...salesCalendarIds, ...fulfilmentCalendarIds]);
  const syncedAt = (tax || []).map(t => t.synced_at).filter(Boolean).sort().pop() || null;
  return { byCalendar, salesCalendarIds, fulfilmentCalendarIds, watchedCalendarIds, groups: groups || [], syncedAt };
}

// Appointments on the WATCHED calendars in a window — the input to the
// missing-recordings check. Sourced from REVI's GHL mirror rather than
// portal.revops_appointments because only the mirror carries the GHL appointment
// id the ledger joins on, and because dash only ingests the sales calendars.
async function reviGetWatchedAppointmentsBetween(startISO, endISO, calendarIds) {
  const ids = [...(calendarIds || [])];
  if (!ids.length) return [];
  const { data, error } = await reviSupabase
    .from('calendar_appointments')
    .select('ghl_appointment_id, calendar_id, title, contact_id, assigned_user_id, appointment_status, start_time, deleted')
    .in('calendar_id', ids)
    .eq('deleted', false)
    .gte('start_time', startISO)
    .lte('start_time', endISO)
    .order('start_time', { ascending: true });
  if (error) throw error;
  return data || [];
}

async function reviFindCallsByProspect(emailOrName, limit = 3) {
  if (!emailOrName) return [];
  let q = reviSupabase
    .from('closer_call_scores')
    .select('id, prospect_name, prospect_email, call_date, duration_min, overall_score, deal_outcome, prospect_signals, coaching_doc_url, closer:closer_id ( full_name )')
    .order('call_date', { ascending: false })
    .limit(limit);
  q = emailOrName.includes('@')
    ? q.ilike('prospect_email', emailOrName)
    : q.ilike('prospect_name', `%${emailOrName}%`);
  const { data, error } = await q;
  if (error) throw error;
  return data || [];
}

// Per-closer scoring + coaching summary. closerName null = all active closers.
// forCloserEyes=true swaps Ron-only teardown_text for the closer-safe draft.
async function reviGetCoachingSummary(closerName = null, days = 14, forCloserEyes = false) {
  const sinceISO = new Date(Date.now() - days * 24 * 60 * 60 * 1000).toISOString();
  const { data: closers, error: cErr } = await reviSupabase
    .from('revi_closers')
    .select('id, full_name, is_active');
  if (cErr) throw cErr;
  const wanted = closerName
    ? (closers || []).filter(c => (c.full_name || '').toLowerCase().includes(closerName.toLowerCase()))
    : (closers || []).filter(c => c.is_active);

  const out = [];
  for (const c of wanted) {
    const { data: calls, error: sErr } = await reviSupabase
      .from('closer_call_scores')
      .select('overall_score, deal_outcome, call_date')
      .eq('closer_id', c.id)
      .gte('call_date', sinceISO);
    if (sErr) throw sErr;
    const { data: runs } = await reviSupabase
      .from('coaching_runs')
      .select('run_date, teardown_text, closer_draft_text, status')
      .eq('closer_id', c.id)
      .order('run_date', { ascending: false })
      .limit(1);
    const scores = (calls || []).map(x => Number(x.overall_score)).filter(Number.isFinite);
    const latest = runs && runs[0];
    const focusText = latest ? (forCloserEyes ? latest.closer_draft_text : latest.teardown_text) : null;
    out.push({
      closer: c.full_name,
      is_active: c.is_active,
      calls_scored: (calls || []).length,
      avg_score: scores.length ? +(scores.reduce((a, b) => a + b, 0) / scores.length).toFixed(1) : null,
      outcomes: (calls || []).reduce((m, x) => { const k = x.deal_outcome || 'pending'; m[k] = (m[k] || 0) + 1; return m; }, {}),
      latest_coaching_date: latest ? latest.run_date : null,
      latest_coaching_focus: focusText ? String(focusText).slice(0, 600) : null,
    });
  }
  return out;
}

async function reviGetRecentDeals(days = 30) {
  const sinceISO = new Date(Date.now() - days * 24 * 60 * 60 * 1000).toISOString();
  const { data: won, error: wErr } = await reviSupabase
    .from('won_deals')
    .select('prospect_name, prospect_email, call_date, marketing_tags, case_study_eligible')
    .gte('call_date', sinceISO)
    .order('call_date', { ascending: false })
    .limit(20);
  if (wErr) throw wErr;
  const { data: lost, error: lErr } = await reviSupabase
    .from('lost_deals')
    .select('prospect_name, prospect_email, call_date, primary_loss_reason, failure_pattern_tags, bottom_three_criteria')
    .gte('call_date', sinceISO)
    .order('call_date', { ascending: false })
    .limit(20);
  if (lErr) throw lErr;
  return { won: won || [], lost: lost || [] };
}

async function reviGetOpenInitiatives() {
  const { data: inits, error } = await reviSupabase
    .from('initiatives')
    .select('title, description, owner_name, meeting_source, needle_mover, next_step, due_hint, last_movement_at, last_mentioned_at')
    .eq('status', 'open')
    .order('last_movement_at', { ascending: false });
  if (error) throw error;
  const { data: meetings } = await reviSupabase
    .from('leadership_meetings')
    .select('meeting_kind, title, meeting_date, counts, digest_text')
    .order('meeting_date', { ascending: false })
    .limit(2);
  const dayMs = 24 * 60 * 60 * 1000;
  return {
    open_initiatives: (inits || []).map(i => ({
      ...i,
      description: String(i.description || '').slice(0, 300),
      days_since_movement: i.last_movement_at ? Math.floor((Date.now() - new Date(i.last_movement_at).getTime()) / dayMs) : null,
    })),
    recent_meetings: (meetings || []).map(m => ({
      kind: m.meeting_kind,
      title: m.title,
      date: m.meeting_date,
      counts: m.counts,
      digest: String(m.digest_text || '').slice(0, 1200),
    })),
  };
}

// Compact plain-text digest of REVI activity in the last `hours` — feeds Max's
// AUTONOMOUS surfaces: nightly learning (distills patterns into agent_knowledge)
// and sales scheduled-task context. Objective data only (scores, outcomes,
// signals, initiative movements) — deliberately NO teardown text, because
// nightly insights land in shared-visibility knowledge the whole team can search.
async function reviBuildDailyDigest(hours = 26) {
  const sinceISO = new Date(Date.now() - hours * 60 * 60 * 1000).toISOString();
  const lines = [];

  const { data: calls } = await reviSupabase
    .from('closer_call_scores')
    .select('prospect_name, call_date, overall_score, deal_outcome, prospect_signals, closer:closer_id ( full_name )')
    .gte('created_at', sinceISO)
    .order('call_date', { ascending: false })
    .limit(20);
  if (calls && calls.length) {
    lines.push(`Calls scored by REVI (${calls.length}):`);
    for (const c of calls) {
      const sig = c.prospect_signals || {};
      const bits = [
        `${c.closer && c.closer.full_name ? c.closer.full_name.split(' ')[0] : '?'} × ${c.prospect_name || 'unknown prospect'}`,
        c.overall_score != null ? `score ${c.overall_score}/100` : null,
        c.deal_outcome && c.deal_outcome !== 'pending' ? `outcome: ${c.deal_outcome}` : null,
        sig.buying_signal_strength ? `buying signal ${sig.buying_signal_strength}` : null,
        sig.objection_type ? `objection: ${String(sig.objection_type).slice(0, 180)}` : null,
        sig.stated_timeline ? `timeline: ${String(sig.stated_timeline).slice(0, 180)}` : null,
      ].filter(Boolean);
      lines.push(`• ${bits.join(' · ')}`);
    }
  }

  const { data: moves } = await reviSupabase
    .from('initiative_updates')
    .select('movement, next_step, initiative:initiative_id ( title, owner_name )')
    .gte('created_at', sinceISO)
    .limit(15);
  if (moves && moves.length) {
    lines.push(`Leadership initiative movements (${moves.length}):`);
    for (const m of moves) {
      const title = m.initiative ? m.initiative.title : 'unknown initiative';
      const owner = m.initiative && m.initiative.owner_name ? ` (${m.initiative.owner_name})` : '';
      lines.push(`• [${m.movement}] ${title}${owner}${m.next_step ? ` — next: ${String(m.next_step).slice(0, 120)}` : ''}`);
    }
  }

  const { data: meetings } = await reviSupabase
    .from('leadership_meetings')
    .select('meeting_kind, title, counts')
    .gte('created_at', sinceISO)
    .limit(5);
  if (meetings && meetings.length) {
    for (const m of meetings) {
      lines.push(`• Leadership meeting processed: ${m.title || m.meeting_kind} — movement counts ${JSON.stringify(m.counts || {})}`);
    }
  }

  return lines.length ? lines.join('\n') : null;
}

// Backend for the get_revi_intelligence tool (Ron-only — teardowns and
// initiative data are leadership material). Returns JSON-stringifiable data;
// errors come back as strings so Q&A degrades instead of crashing the turn.
async function queryReviIntelligence(topic, query = null, days = null) {
  try {
    if (topic === 'prospect') {
      if (!query) return 'topic=prospect requires query (prospect email or name).';
      const calls = await reviFindCallsByProspect(query, 5);
      return calls.length ? calls : `REVI has no scored calls matching "${query}".`;
    }
    if (topic === 'coaching')    return await reviGetCoachingSummary(query || null, days || 14, false);
    if (topic === 'scoreboard')  return await reviGetCoachingSummary(null, days || 7, false);
    if (topic === 'deals')       return await reviGetRecentDeals(days || 30);
    if (topic === 'initiatives') return await reviGetOpenInitiatives();
    return `Unknown topic "${topic}". Use coaching, initiatives, deals, prospect, or scoreboard.`;
  } catch (err) {
    console.error(`queryReviIntelligence(${topic}) failed:`, err.message);
    return `REVI data unavailable right now (${err.message}). REVI's schema may be mid-migration — the rest of Max still works.`;
  }
}

// ── Team-safe REVI client context ────────────────────────────────────────────
// Backend for get_revi_client_context — the roster-wide REVI surface. Only
// client/ops tables: quicksync_reports, activation_reports, clients,
// client_aliases, client_roster. Coaching teardowns, call scores, deal
// transcripts, leadership meetings, and initiatives stay Ron-only via
// get_revi_intelligence. Never widen this to those tables without Ron's call.

// Compact human summary from a quicksync/activation report_json blob.
function reviSummarizeReportJson(rj, maxLen = 600) {
  if (!rj) return null;
  try {
    const obj = typeof rj === 'string' ? JSON.parse(rj) : rj;
    const parts = [];
    const resumen = obj.resumen || obj.summary || {};
    if (resumen.que_se_reviso) parts.push(`Reviewed: ${resumen.que_se_reviso}`);
    if (resumen.conclusion)    parts.push(`Conclusion: ${resumen.conclusion}`);
    if (Array.isArray(obj.riesgos) && obj.riesgos.length) parts.push(`Risks (${obj.riesgos.length}): ${obj.riesgos.slice(0, 3).join(' | ')}`);
    if (Array.isArray(obj.acuerdos) && obj.acuerdos.length) parts.push(`Agreements (${obj.acuerdos.length}): ${obj.acuerdos.slice(0, 3).join(' | ')}`);
    if (Array.isArray(obj.pendientes) && obj.pendientes.length) parts.push(`Pending (${obj.pendientes.length}): ${obj.pendientes.slice(0, 3).join(' | ')}`);
    // Activation-report shape: descripcion_negocio.que_hace (string) +
    // puntos_accion, an object of string-arrays (alertas, pendientes, ...).
    if (obj.descripcion_negocio && typeof obj.descripcion_negocio.que_hace === 'string') parts.push(`Business: ${obj.descripcion_negocio.que_hace}`);
    if (obj.puntos_accion && typeof obj.puntos_accion === 'object' && !Array.isArray(obj.puntos_accion)) {
      const items = Object.entries(obj.puntos_accion)
        .flatMap(([k, v]) => (Array.isArray(v) ? v : [v]).filter(x => typeof x === 'string').map(x => `[${k}] ${x}`));
      if (items.length) parts.push(`Action items (${items.length}): ${items.slice(0, 4).join(' | ')}`);
    }
    const text = parts.length ? parts.join('\n') : JSON.stringify(obj);
    return text.length > maxLen ? text.substring(0, maxLen) + '…' : text;
  } catch {
    const text = String(rj);
    return text.length > maxLen ? text.substring(0, maxLen) + '…' : text;
  }
}

// Resolve messy client names ("MINDLIFT", "Factory - Will") against clients +
// client_aliases + client_roster. Returns lowercase name fragments to ilike on.
async function reviResolveClientNames(clientName) {
  const frag = (clientName || '').trim();
  if (!frag) return [];
  const names = new Set([frag.toLowerCase()]);
  const { data: aliasHits } = await reviSupabase
    .from('client_aliases')
    .select('value, client:client_id ( name )')
    .ilike('value', `%${frag}%`)
    .limit(10);
  for (const a of aliasHits || []) if (a.client?.name) names.add(a.client.name.toLowerCase());
  const { data: clientHits } = await reviSupabase
    .from('clients')
    .select('name')
    .ilike('name', `%${frag}%`)
    .limit(10);
  for (const c of clientHits || []) if (c.name) names.add(c.name.toLowerCase());
  return [...names];
}

async function reviGetClientCalls(clientName = null, days = 60) {
  const sinceISO = new Date(Date.now() - days * 24 * 60 * 60 * 1000).toISOString();
  const nameFrags = clientName ? await reviResolveClientNames(clientName) : [];

  async function fetchReports(table, cols, kind) {
    let q = reviSupabase.from(table).select(cols).gte('call_date', sinceISO)
      .order('call_date', { ascending: false }).limit(clientName ? 10 : 15);
    if (nameFrags.length) q = q.or(nameFrags.map(n => `client_name.ilike.%${n}%`).join(','));
    const { data, error } = await q;
    if (error) throw error;
    return (data || []).map(r => ({ kind, ...r }));
  }

  const [quicksyncs, activations] = await Promise.all([
    fetchReports('quicksync_reports',
      'client_name, contact_name, call_date, duration_min, health, session_number, report_json, pdf_drive_url, recording_url', 'quicksync'),
    fetchReports('activation_reports',
      'client_name, contact_name, call_date, duration_min, report_json, pdf_drive_url, recording_url', 'activation'),
  ]);

  const calls = [...quicksyncs, ...activations]
    .sort((a, b) => new Date(b.call_date) - new Date(a.call_date))
    .slice(0, 10)
    .map(r => ({
      kind: r.kind,
      client: r.client_name,
      contact: r.contact_name,
      call_date: r.call_date,
      duration_min: r.duration_min,
      health: r.health || null,
      session_number: r.session_number || null,
      summary: reviSummarizeReportJson(r.report_json),
      pdf: r.pdf_drive_url || null,
      recording: r.recording_url || null,
    }));

  if (!calls.length) {
    return clientName
      ? `REVI has no quicksync or activation reports matching "${clientName}" in the last ${days} days. REVI only sees calls recorded through Fathom on Ron's account — a call that wasn't recorded there won't appear.`
      : `REVI has no quicksync or activation reports in the last ${days} days.`;
  }
  return calls;
}

async function reviGetClientRoster(clientName = null) {
  let q = reviSupabase.from('client_roster')
    .select('client_name, email, customer_status, synced_at')
    .order('client_name', { ascending: true }).limit(100);
  if (clientName) q = q.ilike('client_name', `%${clientName.trim()}%`);
  const { data, error } = await q;
  if (error) throw error;
  if (!data || !data.length) return clientName ? `No REVI roster entry matching "${clientName}".` : 'REVI client roster is empty.';
  return data;
}

// Backend for the get_revi_client_context tool (open to all roster members).
async function queryReviClientContext(topic, client = null, days = null) {
  try {
    if (topic === 'calls')  return await reviGetClientCalls(client, days || 60);
    if (topic === 'roster') return await reviGetClientRoster(client);
    return `Unknown topic "${topic}". Use calls (quicksync + activation call summaries) or roster (client list + status).`;
  } catch (err) {
    console.error(`queryReviClientContext(${topic}) failed:`, err.message);
    return `REVI client data unavailable right now (${err.message}). REVI's schema may be mid-migration — the rest of Max still works.`;
  }
}

// ── REVI cross-checks cron ───────────────────────────────────────────────────
// C3 heartbeat: REVI scores calls from Fathom recordings. GHL appointments alone
// can't prove a stall — the 2026-08-04 false alarm counted 4 appointments whose
// recordings were all activation-host calls REVI rightly skipped (and one GHL
// appointment can carry the wrong closer_id, so a roster filter can't fix it).
// REVI now publishes revi.engine_heartbeat (what its poller actually saw), which
// splits the old single guess into three real failure modes:
//   poller down (stale last_poll_at) / Fathom feed dark despite appointments
//   (the 2026-07-16 team-sharing incident) / closer recordings seen but unscored.
// Recordings flowing with none closer-hosted = no closer-qual calls = healthy.
// Falls back to the legacy appointments-vs-scores heuristic when the heartbeat
// row isn't there yet. Alert-only — silent when healthy.
// C1 coverage + C2 outcome reconciliation land with the GHL migration.
function _businessDaysBetween(fromMs, toMs) {
  let count = 0;
  const d = new Date(fromMs);
  d.setUTCHours(12, 0, 0, 0); // noon anchor — immune to DST/offset drift
  while (true) {
    d.setUTCDate(d.getUTCDate() + 1);
    if (d.getTime() >= toMs) break; // never count a day past the end bound
    const dow = new Date(d.getTime() - 6 * 60 * 60 * 1000).getUTCDay(); // CR = UTC-6
    if (dow !== 0 && dow !== 6) count++;
  }
  return count;
}

// Sales calls that took place in [sinceISO, now), flywheel-only. Reads portal
// revops_appointments — GHL-native since the 2026-07-23 cutover (this function
// was the designated migration swap point; the swap already happened upstream
// in dash, so no code change was needed).
// NOTE: deliberately NOT metric_observations/iclosed_calls_held_yest — that
// metric has logged zeros for months (audit INS-09) and would silence the check.
async function _countSalesCallsSince(sinceISO) {
  const { data: appts, error } = await portalSupabase
    .from('revops_appointments')
    .select('id, iclosed_call_id, ghl_appointment_id, scheduled_start')
    .gte('scheduled_start', sinceISO)
    .lt('scheduled_start', new Date().toISOString());
  if (error) throw error;
  const excludeIds = await getNonFlywheelCallIds();
  return filterFlywheelAppts(appts || [], excludeIds).length;
}

// Pure: decide which booked calls are genuinely missing a recording, and
// how confidently we can say the call actually happened.
//
// WARMUP is the whole reason this is a function rather than three inline lines.
// revi.call_ledger starts EMPTY on the deploy that introduces it, so every
// appointment older than the first ledger row would look "missing" — an 18-row
// false alarm on the very first run. Only appointments from after REVI started
// writing rows can be judged; an empty ledger yields no verdict at all.
//
// `confirmed` in this GHL location is the BOOKING status: the widget sets it at
// book time and nobody updates it. So it CANNOT separate "call happened, the
// recorder was off" from "unmarked no-show". A logged sales outcome can, and
// `showed` can; everything else stays explicitly ambiguous.
function classifyMissingRecordings({ held, ledgerRows, outcomeApptIds = [] }) {
  const rows = ledgerRows || [];
  const ledgerFrom = rows.map(r => r.call_date).filter(Boolean).sort()[0] || null;
  if (!ledgerFrom) return { warmingUp: true, missing: [], happened: [], ambiguous: [] };

  const seen = new Set(rows.map(r => r.ghl_appointment_id).filter(Boolean));
  const withOutcome = new Set(outcomeApptIds || []);
  const missing = (held || [])
    .filter(a => a.start_time >= ledgerFrom)
    .filter(a => !seen.has(a.ghl_appointment_id));

  const happened = missing.filter(a => withOutcome.has(a.ghl_appointment_id)
    || String(a.appointment_status || '').toLowerCase() === 'showed');
  const ambiguous = missing.filter(a => !happened.includes(a));
  return { warmingUp: false, missing, happened, ambiguous };
}
// END REVI CROSS-CHECK PURE HELPERS

async function runReviCrossChecks(_correlationId, { dryRun = false } = {}) {
  const alerts = [];
  const alertKinds = [];
  const missingApptIds = []; // feeds the dedup key — see the bottom of this function

  // REVI's poller heartbeat — single row, written every poll cycle (~12 min).
  // Absent until ng-revi's heartbeat deploy lands; treat any read failure the same.
  let hb = null;
  try {
    const { data: hbRows } = await reviSupabase
      .from('engine_heartbeat').select('*').eq('id', 'poller').limit(1);
    hb = hbRows && hbRows[0] ? hbRows[0] : null;
  } catch (hbErr) {
    console.warn('REVI heartbeat read failed (falling back to legacy stall heuristic):', hbErr.message);
  }
  const lastPollAt = hb ? new Date(hb.last_poll_at).getTime() : null;
  const pollerDown = lastPollAt !== null && Date.now() - lastPollAt > 60 * 60 * 1000;
  if (pollerDown) {
    alertKinds.push('poller-down');
    alerts.push(
      `🔴 *REVI's Fathom poller is down.* Last successful poll: ${new Date(lastPollAt).toISOString().slice(0, 16).replace('T', ' ')} UTC ` +
      `(cadence is ~12 min). Check the ng-sales-REVI Railway service for a crash loop, then the Fathom API.`
    );
  }

  // Check 1 — scoring freshness vs. calls actually held.
  const { data: lastScoredRows, error: lsErr } = await reviSupabase
    .from('closer_call_scores')
    .select('created_at')
    .order('created_at', { ascending: false })
    .limit(1);
  if (lsErr) throw lsErr;
  const lastScoredAt = lastScoredRows && lastScoredRows[0] ? new Date(lastScoredRows[0].created_at).getTime() : 0;
  const quietBizDays = _businessDaysBetween(lastScoredAt, Date.now());

  // Poller down already alerted above — a scoring-freshness alert on top would
  // just restate the same outage.
  if (!pollerDown && quietBizDays >= 2) {
    const sinceMs = lastScoredAt || Date.now() - 7 * 24 * 60 * 60 * 1000;
    const sinceISO = new Date(sinceMs).toISOString();
    const heldSince = await _countSalesCallsSince(sinceISO);
    if (heldSince > 0) {
      const lastScoredStr = lastScoredAt ? new Date(lastScoredAt).toISOString().slice(0, 10) : 'never';

      // THE LEDGER, not inference. Until revi.call_ledger existed, this branch
      // guessed three states from one heartbeat timestamp and guessed wrong on
      // 2026-08-04, 08-13 and 08-18 — every time because REVI had CORRECTLY
      // skipped everything it saw and had no way to say so. Now it says so.
      let ledger = null;
      try { ledger = await reviGetCallLedgerSince(sinceISO); }
      catch (le) { console.warn('REVI call ledger read failed (falling back to heartbeat inference):', le.message); }

      if (ledger) {
        const sales = ledger.filter(r => r.track === 'sales');
        const scored = sales.filter(r => r.action === 'processed');
        const failed = ledger.filter(r => r.action === 'failed');

        if (failed.length) {
          // Never benign, whatever else is true.
          alertKinds.push('scoring-errors');
          alerts.push(
            `🔴 *REVI threw on ${failed.length} recording(s).*\n` +
            failed.slice(0, 5).map(r => `• \`${r.fathom_recording_id}\` ${String(r.title || '').slice(0, 48)} — ${r.detail || r.reason}`).join('\n')
          );
        }
        if (scored.length) {
          // Sales calls ARE being scored in the window — the premise of the old
          // alert was simply false. Say nothing.
          console.log(`REVI cross-checks: ${scored.length} sales call(s) scored since ${sinceISO.slice(0, 10)} — healthy.`);
        } else if (sales.length) {
          // Sales calls seen and ALL skipped. Name the actual reasons instead of
          // telling Ron to go read the poll logs.
          const byReason = {};
          for (const r of sales) byReason[r.reason || 'unspecified'] = (byReason[r.reason || 'unspecified'] || 0) + 1;
          const benign = sales.every(r => r.reason === 'under_min_minutes' || r.reason === 'already_processed');
          const summary = Object.entries(byReason).sort((a, b) => b[1] - a[1])
            .map(([reason, n]) => `${n}× \`${reason}\``).join(', ');
          if (benign) {
            console.log(`REVI cross-checks: ${sales.length} sales call(s) skipped, all benign (${summary}) — no alert.`);
          } else {
            alertKinds.push('sales-skipped');
            alerts.push(
              `🟡 *REVI saw ${sales.length} sales call(s) since ${sinceISO.slice(0, 10)} and scored none.* ` +
              `Last scored call: ${lastScoredStr}.\nReasons: ${summary}\n` +
              sales.slice(0, 5).map(r => `• \`${r.fathom_recording_id}\` ${String(r.title || '').slice(0, 48)} — ${r.reason}`).join('\n')
            );
          }
        } else if (ledger.length) {
          // Recordings flowing, none of them sales calls. This is the 2026-08-04
          // shape — and now it is PROVEN rather than assumed, because every
          // recording in the window has a row saying what it was.
          const tracks = {};
          for (const r of ledger) tracks[r.track || 'unknown'] = (tracks[r.track || 'unknown'] || 0) + 1;
          console.log(
            `REVI cross-checks: ${quietBizDays} quiet biz days, but all ${ledger.length} recording(s) in the window are non-sales ` +
            `(${Object.entries(tracks).map(([t, n]) => `${n}× ${t}`).join(', ')}) — no stall.`
          );
        } else {
          alertKinds.push('feed-dark');
          alerts.push(
            `🔇 *REVI's Fathom feed is dark.* ${heldSince} sales call(s) on the calendar since ${sinceISO.slice(0, 10)}, ` +
            `the poller is running, but it has processed NO recordings at all in that window ` +
            `(last scored call: ${lastScoredStr}).\n` +
            `This is the Jul 16 signature — check the Fathom API key / team sharing first, then whether the team stopped recording.`
          );
        }
      } else if (!hb) {
        // Legacy heuristic — appointments vs. scores is the only signal available.
        alertKinds.push('stall-legacy');
        alerts.push(
          `🔇 *REVI appears stalled.* ${heldSince} sales call(s) held since ${sinceISO.slice(0, 10)}, ` +
          `but REVI hasn't scored anything in ${quietBizDays} business days ` +
          `(last scored call: ${lastScoredStr}).\n` +
          `Check: Fathom API key / team sharing (broke silently on Jul 16), the ng-sales-REVI Railway service, and REVI's poll logs. ` +
          `Caveat: GHL appointments can be activation calls REVI rightly skips (2026-08-04 false alarm) — check skip reasons in the poll logs first.`
        );
      } else {
        // Heartbeat present but the ledger read failed — degrade to the old
        // inference rather than going silent.
        const seenAnySince = hb.last_seen_at && new Date(hb.last_seen_at).getTime() > sinceMs;
        if (!seenAnySince) {
          alertKinds.push('feed-dark');
          alerts.push(
            `🔇 *REVI's Fathom feed is dark.* ${heldSince} sales call(s) on the calendar since ${sinceISO.slice(0, 10)}, ` +
            `the poller is running, but it has seen NO new recordings at all in that window ` +
            `(last scored call: ${lastScoredStr}).`
          );
        }
      }
    }
  }

  // Check 1b — sales appointments with NO recording at all.
  //
  // The gap nobody was watching: on 2026-08-17 closer Jose Carranza had 4
  // confirmed sales appointments and Fathom recorded ZERO. Nothing compared
  // bookings against recordings, so nothing fired. This is the divergence check
  // for the sales feed — source of truth (GHL bookings) vs. what actually
  // reached REVI.
  //
  // HONESTY: in this GHL location `confirmed` is the BOOKING status. The booking
  // widget sets it at book time and nobody updates it afterwards (the 08-17
  // Johan Solis appointment was still `confirmed` a day later). It therefore
  // CANNOT distinguish "call happened, recorder was off" from "unmarked
  // no-show". So the alert asks revops_sales_outcomes: an appointment with a
  // logged outcome demonstrably happened, which makes its missing recording
  // real. Without one it stays ambiguous, and the alert says so rather than
  // asserting a cause.
  let taxonomy = null;
  try { taxonomy = await reviGetCalendarTaxonomy(); }
  catch (te) { console.warn('REVI calendar taxonomy read failed (skipping missing-recordings check):', te.message); }

  if (taxonomy && taxonomy.watchedCalendarIds.size) {
    try {
      // 90 min of grace so a call that just ended isn't flagged before Fathom
      // has published it.
      const endISO = new Date(Date.now() - 90 * 60 * 1000).toISOString();
      const startISO = new Date(Date.now() - 7 * 24 * 60 * 60 * 1000).toISOString();
      const appts = await reviGetWatchedAppointmentsBetween(startISO, endISO, taxonomy.watchedCalendarIds);
      const held = appts.filter(a => ['confirmed', 'showed'].includes(String(a.appointment_status || '').toLowerCase()));

      if (held.length) {
        const ledger7 = await reviGetCallLedgerSince(startISO);
        // WARMUP. The ledger starts empty on the deploy that introduces it, so
        // every appointment older than the first ledger row would look
        // "missing" and the first run would fire a huge false alarm. Only judge
        // appointments from after REVI actually started writing rows; an empty
        // ledger means the check has nothing to say yet.
        // ANY ledger row counts as "REVI saw it" — a skipped-but-ledgered call is
        // a different alert (Check 1), not a missing recording.
        let { warmingUp, missing } = classifyMissingRecordings({ held, ledgerRows: ledger7 });
        if (warmingUp) {
          console.log('missing-recordings: call ledger is empty for this window — warming up, no verdict.');
        }

        if (missing.length) {
          // Did the call demonstrably happen? portal keys outcomes on its own
          // appointment id, so map GHL id -> portal id first.
          let outcomeFor = new Set();
          try {
            const ghlIds = missing.map(a => a.ghl_appointment_id);
            const { data: portalAppts } = await portalSupabase
              .from('revops_appointments').select('id, ghl_appointment_id').in('ghl_appointment_id', ghlIds);
            const portalIds = (portalAppts || []).map(a => a.id);
            if (portalIds.length) {
              const { data: outcomes } = await portalSupabase
                .from('revops_sales_outcomes').select('appointment_id, outcome').in('appointment_id', portalIds);
              const withOutcome = new Set((outcomes || []).map(o => o.appointment_id));
              const backToGhl = Object.fromEntries((portalAppts || []).map(a => [a.id, a.ghl_appointment_id]));
              outcomeFor = new Set([...withOutcome].map(id => backToGhl[id]).filter(Boolean));
            }
          } catch (oe) { console.warn('outcome enrichment failed (alert will stay ambiguous):', oe.message); }

          const { happened: confirmedHappened, ambiguous } =
            classifyMissingRecordings({ held, ledgerRows: ledger7, outcomeApptIds: [...outcomeFor] });

          // Split by track: an unrecorded SALES call costs coaching and pipeline
          // truth, an unrecorded CHECK-IN means a client silently gets no report.
          // Different follow-up, so they must not be pooled into one number.
          const trackOf = a => (taxonomy.byCalendar.get(a.calendar_id) || {}).track || 'unknown';
          const groupLines = (rows) => {
            const byPerson = {};
            for (const a of rows) {
              const who = resolveSalesMember(a.assigned_user_id) || a.assigned_user_id || 'unassigned';
              (byPerson[who] = byPerson[who] || []).push(a);
            }
            return Object.entries(byPerson).map(([who, rs]) => {
              const days = [...new Set(rs.map(r => String(r.start_time).slice(0, 10)))].sort().join(', ');
              const where = [...new Set(rs.map(r => (taxonomy.byCalendar.get(r.calendar_id) || {}).calendar_name || r.calendar_id))].join(', ');
              return `• *${who}* — ${rs.length} on ${days} (${where})`;
            });
          };
          const salesMissing = missing.filter(a => trackOf(a) === 'sales');
          const fulfilMissing = missing.filter(a => trackOf(a) === 'fulfilment');

          alertKinds.push('missing-recordings');
          missingApptIds.push(...missing.map(a => a.ghl_appointment_id));
          const parts = [`🎥 *${missing.length} booked call(s) have no Fathom recording.*`];
          if (salesMissing.length) {
            parts.push(`*Sales — ${salesMissing.length}:*\n` + groupLines(salesMissing).join('\n'));
          }
          if (fulfilMissing.length) {
            parts.push(
              `*Fulfilment — ${fulfilMissing.length}:* a client check-in with no recording means that client gets NO report and nobody finds out.\n`
              + groupLines(fulfilMissing).join('\n'));
          }
          if (confirmedHappened.length) {
            parts.push(`${confirmedHappened.length} of these have a logged outcome (or are marked \`showed\`) — those calls demonstrably happened, so the recording is genuinely missing. Check whether the recorder joined.`);
          }
          if (fulfilMissing.length && !confirmedHappened.length) {
            // Outcomes only exist for sales calls, so a fulfilment miss can never
            // be corroborated that way. Say so rather than implying it was checked.
            parts.push('Fulfilment bookings carry no sales outcome to corroborate against, so those stay ambiguous until someone confirms the call happened.');
          }
          if (ambiguous.length) {
            parts.push(`${ambiguous.length} have no logged outcome. GHL still shows them \`confirmed\`, but in this location that is the BOOKING status — set at booking time and rarely updated — so it cannot tell an unrecorded call from an unmarked no-show. Logging the outcome resolves it either way.`);
          }
          alerts.push(parts.join('\n'));
        }
      }
    } catch (me) { console.warn('missing-recordings check failed:', me.message); }
  }

  // Check 1c — taxonomy drift between REVI and dash.
  //
  // dash's GHL_SALES_CALENDAR_IDS gates what reaches portal.revops_appointments;
  // revi.calendar_groups states the same fact for REVI's routing. They MUST
  // agree. This DETECTS a disagreement without creating a third allowlist —
  // deliberately, see the note above SALES_FLYWHEEL_SLUGS: a second live
  // allowlist here would itself drift. Set GHL_SALES_CALENDAR_IDS_EXPECTED to
  // mirror dash's value; unset disables the check.
  if (taxonomy && process.env.GHL_SALES_CALENDAR_IDS_EXPECTED) {
    const expected = new Set(process.env.GHL_SALES_CALENDAR_IDS_EXPECTED.split(',').map(x => x.trim()).filter(Boolean));
    const actual = taxonomy.salesCalendarIds;
    const onlyRevi = [...actual].filter(id => !expected.has(id));
    const onlyDash = [...expected].filter(id => !actual.has(id));
    if (onlyRevi.length || onlyDash.length) {
      const name = id => (taxonomy.byCalendar.get(id) || {}).calendar_name || id;
      alertKinds.push('taxonomy-drift');
      alerts.push(
        `🧭 *Sales-calendar drift between REVI and dash.*\n` +
        (onlyRevi.length ? `REVI counts as sales, dash does not ingest: ${onlyRevi.map(name).join(', ')}\n` : '') +
        (onlyDash.length ? `dash ingests as sales, REVI does not: ${onlyDash.map(name).join(', ')}\n` : '') +
        `Fix in dash's GHL_SALES_CALENDAR_IDS or in \`revi.calendar_groups\` — they must state the same thing.`
      );
    }
  }

  // Check 2 — coaching runs stuck in draft >24h (REVI scored but never delivered).
  const dayAgoISO = new Date(Date.now() - 24 * 60 * 60 * 1000).toISOString();
  const { data: stuck } = await reviSupabase
    .from('coaching_runs')
    .select('run_date, status, created_at')
    .eq('status', 'draft')
    .lt('created_at', dayAgoISO);
  if (stuck && stuck.length) {
    alertKinds.push('stuck-drafts');
    alerts.push(
      `📄 *REVI has ${stuck.length} coaching run(s) stuck in draft >24h* ` +
      `(run dates: ${[...new Set(stuck.map(s => s.run_date))].join(', ')}). ` +
      `Scoring worked but delivery didn't — check REVI's Slack send + Drive upload.`
    );
  }

  if (!alerts.length) { console.log('REVI cross-checks: healthy, staying silent.'); return { alerts: [] }; }
  if (dryRun) { console.log('REVI cross-checks dry-run alerts:\n' + alerts.join('\n\n')); return { alerts }; }

  // Dedup: one alert per distinct condition (keyed on alert type + last-scored
  // date, stuck drafts on their run dates) — not one per day it persists. The
  // type marker keeps a poller-down alert from suppressing a later feed-dark
  // alert that shares the same last-scored date.
  const stuckDates = (stuck || []).map(s => s.run_date).sort().join(',');
  const alertKind = alertKinds.join('+');
  // Missing appointments join the key so a NEW one re-alerts while the same set
  // stays quiet — otherwise the first day's alert would suppress every later
  // miss that happened to share a last-scored date.
  const missingKey = missingApptIds.length ? missingApptIds.slice().sort().join(',') : 'none';
  const dedupKey = `revi-health:${alertKind}:${lastScoredAt ? new Date(lastScoredAt).toISOString().slice(0, 10) : 'never'}:${stuckDates || 'none'}:${missingKey}`;
  const { data: already } = await supabase.from('agent_knowledge').select('id').eq('key', dedupKey).limit(1);
  if (already && already.length) { console.log(`REVI cross-checks: alert already sent for ${dedupKey}.`); return { alerts, deduped: true }; }

  const message = `*REVI HEALTH CHECK*\n\n${alerts.join('\n\n')}`;
  await slack.client.chat.postMessage({ channel: RON_SLACK_ID, text: message });
  await upsertKnowledge('alert', dedupKey, message, 'revi-cross-check');
  console.log(`REVI cross-checks: ${alerts.length} alert(s) DMed to Ron.`);
  return { alerts };
}

// ── Metric registry — single source of truth for what gets scraped ──────────
const METRIC_REGISTRY = [
  // Marketing — per-funnel CPL signals (blended CPL is meaningless across two funnels)
  { name: 'meta_form_cpl_today',          domain: 'marketing',   scrape: _scrapeMetaFormCplToday,          label: 'Meta Form CPL (today)' },
  { name: 'meta_cac_today',               domain: 'marketing',   scrape: _scrapeMetaCacToday,               label: 'Meta CAC via Meta Purchase events (today)' },
  { name: 'meta_cost_per_booking_today',  domain: 'marketing',   scrape: _scrapeMetaCostPerBookingToday,    label: 'Meta cost-per-booking (today)' },
  // Lead volume — lead_posts (GHL lead-intake webhook, Form/WA funnel; VSL
  // self-bookers skip the lead feed and surface as booked appointments below).
  // Key frozen (metric_observations PK with baseline history) — label only.
  { name: 'ghl_new_contacts_today',       domain: 'sales',       scrape: _scrapeGhlNewContactsToday,        label: 'New leads (Form/WA funnel, yesterday)' },
  // Call pipeline — revops_appointments (GHL-native since 2026-07-23) is the truth-source.
  // NOTE: the iclosed_* metric KEYS are historical and frozen ON PURPOSE — they are
  // primary keys in metric_observations with baseline history. Rename = severed baselines.
  { name: 'iclosed_calls_booked_yest',    domain: 'sales',       scrape: _scrapeIclosedCallsBookedYesterday, label: 'Sales calls booked (yesterday)' },
  { name: 'iclosed_calls_held_yest',      domain: 'sales',       scrape: _scrapeIclosedCallsHeldYesterday,   label: 'Sales calls held (yesterday)' },
  { name: 'iclosed_sales_yest',           domain: 'sales',       scrape: _scrapeIclosedSalesYesterday,       label: 'Sales won (yesterday)' },
  { name: 'close_rate_yesterday',         domain: 'sales',       scrape: _scrapeCloseRateYesterday,          label: 'Close rate (yesterday)' },
  { name: 'setter_calls_booked_yest',     domain: 'sales',       scrape: _scrapeSetterCallsBookedYesterday,  label: 'Setter-booked calls (yesterday)' },
  // Fulfillment
  { name: 'phase0_to_phase1_conv_7d',     domain: 'fulfillment', scrape: _scrapePhase0ToPhase1Conv7d,        label: 'Phase 0 → Phase 1 conversion (7d)' },
  { name: 'phase1_cycle_days_p50',        domain: 'fulfillment', scrape: () => _scrapePhaseCycleP50('phase_1'), label: 'Phase 1 cycle days (p50)' },
  { name: 'phase2_cycle_days_p50',        domain: 'fulfillment', scrape: () => _scrapePhaseCycleP50('phase_2'), label: 'Phase 2 cycle days (p50)' },
  { name: 'day7_at_risk_count',           domain: 'fulfillment', scrape: _scrapeDay7AtRiskCount,              label: 'Day 7+ at-risk client count' },
];

async function runAnomalyDetection({ dryRun = false, threshold = ANOMALY_THRESHOLD_SIGMA } = {}) {
  console.log(`Anomaly detection starting (dryRun=${dryRun}, threshold=${threshold}σ)`);
  const results = { scraped: [], skipped: [], anomalies: [], errors: [] };

  // 1. Scrape every metric. One failing scraper does not stop the others.
  for (const m of METRIC_REGISTRY) {
    try {
      const value = await m.scrape();
      if (value === null || value === undefined) {
        results.skipped.push({ metric: m.name, reason: 'no value' });
        continue;
      }
      if (!dryRun) await recordObservation(m.name, m.domain, value, 'anomaly-cron');
      results.scraped.push({ metric: m.name, domain: m.domain, value });
    } catch (err) {
      console.error(`Anomaly scrape error for ${m.name}:`, err.message);
      results.errors.push({ metric: m.name, error: err.message });
    }
  }

  if (dryRun) { console.log('Anomaly dry run — skipping baselines, knowledge, DMs.'); return results; }

  // 2. Recompute baselines for every metric we have observations for.
  for (const m of METRIC_REGISTRY) {
    try { await recomputeBaseline(m.name); }
    catch (err) { console.error(`Anomaly baseline error for ${m.name}:`, err.message); }
  }

  // 3. Detect anomalies and dispatch.
  for (const m of METRIC_REGISTRY) {
    try {
      const snap = await detectAnomaly(m.name, threshold);
      if (!snap || !snap.triggered) continue;
      results.anomalies.push(snap);

      const narration = await narrateAnomaly(snap);
      const direction = snap.z > 0 ? '↑' : '↓';
      const today = new Date().toISOString().slice(0, 10);
      const knowledgeKey = `anomaly:${snap.metric}:${today}`;
      const structured = `${m.label} ${direction} ${snap.value} (baseline ${snap.mean.toFixed(2)} ± ${snap.stdDev.toFixed(2)}, ${snap.z >= 0 ? '+' : ''}${snap.z.toFixed(2)}σ on n=${snap.sampleSize})`;
      const fullMessage = narration ? `${structured}\n${narration}` : structured;

      // Persist to long-term memory so it surfaces in future searches
      await upsertKnowledge('alert', knowledgeKey, fullMessage, 'anomaly-detection', null, 'shared');

      // DM the routed roles
      const recipients = _resolveAnomalyRecipients(snap.domain);
      for (const id of recipients) {
        try {
          await slack.client.chat.postMessage({
            channel: id,
            text: `Anomaly detected — ${m.label}\n\n${fullMessage}`,
          });
        } catch (dmErr) {
          console.error(`Anomaly DM failed for ${id}:`, dmErr.message);
        }
      }
      console.log(`Anomaly fired: ${snap.metric} z=${snap.z.toFixed(2)} → ${recipients.length} recipient(s)`);
    } catch (err) {
      console.error(`Anomaly detection error for ${m.name}:`, err.message);
      results.errors.push({ metric: m.name, error: err.message });
    }
  }

  console.log(`Anomaly detection complete — scraped ${results.scraped.length}, skipped ${results.skipped.length}, anomalies ${results.anomalies.length}, errors ${results.errors.length}`);
  return results;
}

async function queryMetricHistory(metric, days = 30) {
  const since = new Date(Date.now() - days * 24 * 60 * 60 * 1000).toISOString();
  const { data, error } = await supabase
    .from('metric_observations')
    .select('value, observed_at, source')
    .eq('metric', metric)
    .gte('observed_at', since)
    .order('observed_at', { ascending: true });
  if (error) return `Metric history error: ${error.message}`;
  if (!data || !data.length) return `No observations for ${metric} in the last ${days} days.`;
  const lines = data.map(r => `${r.observed_at.slice(0, 10)}  ${r.value}`);
  const baseline = await supabase.from('metric_baselines').select('*').eq('metric', metric).single();
  const baseStr = baseline?.data
    ? `\nBaseline (n=${baseline.data.sample_size}, ${baseline.data.window_days}d): mean ${Number(baseline.data.mean).toFixed(2)}, stddev ${Number(baseline.data.std_dev).toFixed(2)}`
    : '';
  return `${metric} — last ${days} days (${data.length} obs):\n${lines.join('\n')}${baseStr}`;
}

// ─── FILE PROCESSING ──────────────────────────────────────────────────────────
async function downloadSlackFile(fileUrl) {
  const res = await fetch(fileUrl, { headers: { 'Authorization': `Bearer ${process.env.SLACK_BOT_TOKEN}` } });
  if (!res.ok) throw new Error(`Failed to download file: ${res.status}`);
  return Buffer.from(await res.arrayBuffer());
}

async function resizeImageIfNeeded(fileBuffer, mimeType) {
  if (mimeType === 'application/pdf' || mimeType === 'image/gif') return { buffer: fileBuffer, mimeType };
  try {
    const image    = sharp(fileBuffer);
    const metadata = await image.metadata();
    const maxDim   = 1200;
    if (metadata.width <= maxDim && metadata.height <= maxDim) return { buffer: fileBuffer, mimeType };
    const isLandscape = metadata.width > metadata.height;
    const resized = await image.resize(isLandscape ? maxDim : null, isLandscape ? null : maxDim, { fit: 'inside', withoutEnlargement: true }).jpeg({ quality: 85 }).toBuffer();
    console.log(`Image resized from ${metadata.width}x${metadata.height} to fit ${maxDim}px`);
    return { buffer: resized, mimeType: 'image/jpeg' };
  } catch (err) { console.error('Image resize error:', err.message); return { buffer: fileBuffer, mimeType }; }
}

async function processFileWithClaude(fileBuffer, mimeType, userInstruction, systemPrompt, correlationId) {
  let finalBuffer = fileBuffer, finalMimeType = mimeType;
  if (mimeType.startsWith('image/')) {
    const resized = await resizeImageIfNeeded(fileBuffer, mimeType);
    finalBuffer = resized.buffer; finalMimeType = resized.mimeType;
  }
  const base64 = finalBuffer.toString('base64');
  let contentBlock;
  if (finalMimeType === 'application/pdf') {
    contentBlock = { type: 'document', source: { type: 'base64', media_type: 'application/pdf', data: base64 } };
  } else if (finalMimeType.startsWith('image/')) {
    contentBlock = { type: 'image', source: { type: 'base64', media_type: finalMimeType, data: base64 } };
  } else {
    return 'Unsupported file type. I can process images (PNG, JPG, GIF, WEBP) and PDFs.';
  }
  const t0 = Date.now();
  const response = await anthropic.messages.create({
    model: 'claude-sonnet-4-6', max_tokens: 1024, system: systemPrompt,
    messages: [{ role: 'user', content: [contentBlock, { type: 'text', text: userInstruction || 'Analyze this file and provide a useful summary. Extract any action items, key information, or insights relevant to NeuroGrowth operations.' }] }],
  });
  logLlmFromAnthropicResponse(response, Date.now() - t0, correlationId);
  return response.content.filter(b => b.type === 'text').map(b => b.text).join('\n');
}

function getFileMimeType(filename, mimeType) {
  if (mimeType && mimeType !== 'application/octet-stream') return mimeType;
  const ext = filename?.split('.').pop()?.toLowerCase();
  const map = { 'pdf': 'application/pdf', 'png': 'image/png', 'jpg': 'image/jpeg', 'jpeg': 'image/jpeg', 'gif': 'image/gif', 'webp': 'image/webp' };
  return map[ext] || mimeType;
}

// ─── AUDIO TRANSCRIPTION (WHISPER) ───────────────────────────────────────────
const AUDIO_MIME_TYPES = ['audio/webm','audio/mp4','audio/mpeg','audio/mp3','audio/ogg','audio/wav','audio/m4a'];
const AUDIO_EXTENSIONS = ['webm','mp4','mp3','m4a','ogg','wav'];

function isAudioFile(mimeType, filename) {
  if (mimeType && AUDIO_MIME_TYPES.some(t => mimeType.startsWith(t))) return true;
  const ext = filename?.split('.').pop()?.toLowerCase();
  return AUDIO_EXTENSIONS.includes(ext);
}

async function transcribeAudio(fileBuffer, filename) {
  const tmpPath = `/tmp/audio_${Date.now()}_${filename || 'audio.webm'}`;
  fs.writeFileSync(tmpPath, fileBuffer);
  try {
    if (typeof globalThis.File === 'undefined') {
      const { File } = await import('node:buffer');
      globalThis.File = File;
    }
    const transcription = await openai.audio.transcriptions.create({ file: fs.createReadStream(tmpPath), model: 'whisper-1', response_format: 'text' });
    return transcription;
  } finally {
    try { fs.unlinkSync(tmpPath); } catch {}
  }
}

// ─── CLAUDE API WITH RETRY ────────────────────────────────────────────────────
// opts.systemAppend — extra text appended to the system prompt for this call only.
// opts.finalTool    — {name, description, input_schema} added to TOOLS; when the model
//                     calls it, callClaude returns { structured: <tool input> } instead
//                     of text. Used by /agent/consult for schema-constrained verdicts.
// opts.dropTools    — tool names removed from TOOLS for this call (hard ban, not prompt-level).
// opts.onlyTools    — allow-list: ONLY these tools survive (plus finalTool). The inverse of
//                     dropTools, for narrow modes like alert triage where banning ~38 tools
//                     by name would be unmaintainable. [] means finalTool only.
async function callClaude(messages, retries = 3, userId = null, correlationId = null, opts = {}) {
  const correlation_id = correlationId != null && correlationId !== undefined ? correlationId : newCorrelationId();
  // Learned lessons ride every interactive prompt (fetched once, not per retry).
  // Scheduled reports inject their own scoped lessons via getReportLessons.
  let lessonBlock = '';
  try {
    const lessons = await getGlobalLessons();
    if (lessons.length) {
      lessonBlock = `\n\nLESSONS FROM PAST CORRECTIONS (team members corrected Max on these — do not repeat them):\n${lessons.map(l => `- ${(l.value || '').slice(0, 300)}`).join('\n')}`;
    }
  } catch { /* lessons are best-effort — never block a reply */ }
  let lastErr;
  for (let attempt = 0; attempt < retries; attempt++) {
    try {
      // Inject current Costa Rica date/time — built once, reused in ALL calls (initial + follow-ups)
      const nowCR = new Date().toLocaleString('en-US', {
        timeZone: 'America/Costa_Rica',
        weekday: 'long', year: 'numeric', month: 'long', day: 'numeric',
        hour: '2-digit', minute: '2-digit', hour12: true
      });
      const timeContext = `\n\nCURRENT DATE AND TIME: ${nowCR} (Costa Rica time). Use this as your time reference for all date and day-of-week logic. Never assume or guess the date.`;
      const emailProxyGuidance = EMAIL_PROXY_LIVE ? `

EMAIL PROXY (when a setter/closer asks you to send an email on their behalf):
- Mirror the user's language. If they DM in Spanish, respond in Spanish through every step (slot-filling, preview, confirmation). The email body itself stays in whatever language the setter dictated — do NOT translate it.
- Conversationally collect three required fields: recipient email (to), subject, body. cc is optional.
- If any required field is missing, name exactly what is missing and ask for it. Do NOT call draft_outbound_email until to + subject + body are all known.
- Quote values back to the setter for typo-checking before drafting (e.g. "OK so to: acme@x.com, subject: Follow-up — confirm the body before I draft").
- Once you have all three, call draft_outbound_email. The system will show the draft to the setter for review (Stage 1), then route to Ron for final approval (Stage 2). You do not handle the approval flow yourself — just call the tool.
- For replies to active email threads: only call draft_reply_email when the setter is clearly responding to a client message Max forwarded earlier. If they say "never mind", "cancel", a question about something else, or anything ambiguous, respond conversationally — do NOT call the tool.
- Never claim an email was sent unless the system DMs the success notification. The tool call alone does not send anything.` : '';
      const fullSystemPrompt = (userId ? buildRoleSystemPrompt(userId) : SYSTEM_PROMPT) + timeContext + lessonBlock + emailProxyGuidance + (opts.systemAppend || '');

      const TOOLS = [
          { name: 'search_notion',       description: 'Search NeuroGrowth Notion workspace for pages, tasks, client info, and SOPs',           input_schema: { type: 'object', properties: { query: { type: 'string' } }, required: ['query'] } },
          { name: 'get_notion_page',      description: 'Get the content of a specific Notion page by its ID',                                   input_schema: { type: 'object', properties: { page_id: { type: 'string' } }, required: ['page_id'] } },
          { name: 'get_recent_emails',    description: "Get recent unread emails from Ron's Gmail inbox including full email body content",      input_schema: { type: 'object', properties: {} } },
          { name: 'send_email',           description: "Send an email on Ron's behalf. Always confirm before sending.",                          input_schema: { type: 'object', properties: { to: { type: 'string' }, subject: { type: 'string' }, body: { type: 'string' } }, required: ['to','subject','body'] } },
          { name: 'get_calendar_events',  description: 'Get calendar events. daysFromNow: 0=today, 1=tomorrow, -1=yesterday. daysRange: 1=day, 7=week, 14=two weeks.', input_schema: { type: 'object', properties: { daysFromNow: { type: 'number' }, daysRange: { type: 'number' } } } },
          { name: 'search_drive',         description: "Search Ron's Google Drive for files and documents",                                      input_schema: { type: 'object', properties: { query: { type: 'string' } }, required: ['query'] } },
          { name: 'read_google_sheet',    description: 'Read the actual cell data from a Google Sheet. Accepts a Google Sheets URL or file ID. Optionally specify a range. For the Ops Master Tracker, use get_ops_tracker instead — it handles that sheet properly.',                                                input_schema: { type: 'object', properties: { spreadsheetId: { type: 'string', description: 'Google Sheets URL or spreadsheet ID' }, range: { type: 'string', description: 'Optional range e.g. Sheet1!A1:Z100' } }, required: ['spreadsheetId'] } },
          { name: 'get_ops_tracker',      description: "Read the Ops Master Tracker (the fulfillment team's hand-maintained Google Sheet). Tabs: 'infrastructure' = per-client setup state (Sales Navigator, Prosp license, webhook, Prosp tag, NG dashboard, notes); 'change_requests' = client change-request log with priority/owner/status (defaults to OPEN items — pass include_done for history); 'launch_history' = activation/QA/launch dates and days-to-launch per client, TYPED BY HAND by the ops team. For the activation call date and any day count derived from it the dashboard CRM wins — this sheet is not written by any automation and drifts; use it for QA/launch dates and notes, and as a cross-check. If it disagrees with the CRM on an activation date, report both and treat the CRM as the number. Use for any question about a client's Prosp/SN/webhook setup, pending or past change requests, or ops-recorded launch dates. Client filter is fuzzy — use a short name fragment.", input_schema: { type: 'object', properties: { tab: { type: 'string', description: 'infrastructure | change_requests | launch_history' }, client: { type: 'string', description: 'Optional client-name fragment to filter rows.' }, include_done: { type: 'boolean', description: 'change_requests only: include Done items (default false).' } }, required: ['tab'] } },
          { name: 'read_google_doc',      description: 'Read the text content of a Google Doc. Accepts a Google Docs URL or document ID.',      input_schema: { type: 'object', properties: { documentId: { type: 'string', description: 'Google Docs URL or document ID' } }, required: ['documentId'] } },
          { name: 'read_slack_channel',   description: 'Read recent messages from a NeuroGrowth Slack channel. Always use this tool when asked about channel activity — never answer from memory.', input_schema: { type: 'object', properties: { channelName: { type: 'string', description: 'Channel name e.g. ng-fullfillment-ops, ng-sales-goats, ng-ops-management, ng-new-client-alerts, ng-app-and-systems-improvents' }, messageCount: { type: 'number', description: 'Messages to pull, max 20' } }, required: ['channelName'] } },
          { name: 'draft_channel_post',   description: "Prepare a Slack channel post for approval before sending. By default the approval goes back to the person who asked. Set escalate_to_ron=true when the draft matches the escalation criteria (client-facing commitments, pricing, public comms, reputational risk, hiring/firing).", input_schema: { type: 'object', properties: { channelName: { type: 'string' }, message: { type: 'string' }, escalate_to_ron: { type: 'boolean', description: 'Route approval to Ron instead of the originator. Default false. Use true only when escalation criteria apply.' }, escalation_reason: { type: 'string', description: 'Short reason for routing to Ron (only used when escalate_to_ron is true).' } }, required: ['channelName','message'] } },
          { name: 'get_ghl_conversations',description: 'Get recent GHL conversations — prospects and contacts across all channels. Each conversation includes the assigned setter name (or "unassigned" if no owner is set — that is a valid complete answer, not an error). Use this to answer questions about which setter is working a prospect, or whether a prospect is unassigned.',                                                                                                                        input_schema: { type: 'object', properties: { limit: { type: 'number', description: 'Number of conversations to pull, default 20' }, unreadOnly: { type: 'boolean', description: 'Set true to only show unread conversations' } } } },
          { name: 'search_knowledge',     description: "Search the agent's long-term knowledge base for accumulated intelligence about clients, team, processes, and decisions.", input_schema: { type: 'object', properties: { query: { type: 'string' }, category: { type: 'string', description: 'Optional: client, team, process, decision, alert, intel' } }, required: ['query'] } },
          { name: 'save_knowledge',       description: 'Save an important insight to long-term memory. Use when someone on the team shares important context or a pattern emerges. Default visibility is "shared" (whole team sees it). Use "private" when the user explicitly asks to keep it personal or when it is a sensitive personal note that should not surface for other team members.',  input_schema: { type: 'object', properties: { category: { type: 'string', description: 'client, team, process, decision, alert, or intel' }, key: { type: 'string', description: 'Short identifier e.g. Max Valverde or onboarding bottleneck' }, value: { type: 'string', description: 'The knowledge to store' }, visibility: { type: 'string', description: 'shared (default, team-wide) or private (only this user sees it)' } }, required: ['category','key','value'] } },
          { name: 'get_knowledge_category',description: 'Get all knowledge entries for a specific category.',                                    input_schema: { type: 'object', properties: { category: { type: 'string', description: 'client, team, process, decision, alert, or intel' } }, required: ['category'] } },
          { name: 'get_client_status',    description: 'ALWAYS use this tool (NOT Notion) when asked about client onboarding status, client phases, portal status, where a client is in their onboarding, what activities are pending, or what clients are in the system. Queries live Supabase portal database directly.',           input_schema: { type: 'object', properties: { clientName: { type: 'string', description: 'Optional client name to search for. Leave empty to get all clients.' } } } },
          { name: 'get_portal_alerts',    description: 'ALWAYS use this tool (NOT Notion) when asked about launch risks, clients behind on their 14-day window, overdue clients, or who needs attention in fulfillment. Queries live Supabase portal data.',                                                                            input_schema: { type: 'object', properties: {} } },
          { name: 'search_portal_schema', description: "Find portal tables by plain-English keywords. Searches both table and column names (e.g. 'email linkedin client' returns every table that has a matching column, grouped by table). ALWAYS use this first when Ron asks for a field in natural language — do NOT guess table names.", input_schema: { type: 'object', properties: { keywords: { type: 'string', description: 'Space-separated keywords drawn from what Ron asked for.' } }, required: ['keywords'] } },
          { name: 'list_portal_tables',   description: 'List every table and view in the portal Supabase database. Use when Ron explicitly asks what tables exist, or as a fallback if search_portal_schema returns nothing.', input_schema: { type: 'object', properties: {} } },
          { name: 'describe_portal_table',description: 'Show full column list for a specific portal table. Call after search_portal_schema has narrowed things down and you need the complete column set before querying.', input_schema: { type: 'object', properties: { tableName: { type: 'string' } }, required: ['tableName'] } },
          { name: 'query_portal_db',      description: 'Run a read-only SQL query (SELECT or WITH only) against the portal Supabase database. Use for ad-hoc lookups the pre-built tools do not cover — e.g. pulling emails or LinkedIn handles from arbitrary tables. Results capped at 500 rows. For standard onboarding/phase status prefer get_client_status / get_phase0_clients.', input_schema: { type: 'object', properties: { sql: { type: 'string', description: 'A single SELECT or WITH statement. No semicolons, no writes.' } }, required: ['sql'] } },
          { name: 'get_phase0_clients',   description: 'ALWAYS use this tool for Phase 0 (pre-portal onboarding) status — flywheel-ai clients who signed up but have not gone live yet. Covers: portal signup, T&C acceptance, onboarding form, activation call booking, handoff to Phase 1. Use in every fulfillment report to show the Phase 0 pipeline before get_client_status covers Phase 1+.',                                                                                                       input_schema: { type: 'object', properties: {} } },
          { name: 'create_slack_reminder',description: 'Schedule a one-off reminder message in Slack at a specific time. Use for "remind me/someone at X" requests. For recurring reminders use create_scheduled_task instead. Target can be a channel name (#ng-sales-goats) or a user ID (U… for a DM). Compute postAt as an ISO 8601 string in the user\'s timezone (default America/Costa_Rica) based on their natural-language time; must be in the future and within 120 days.',                     input_schema: { type: 'object', properties: { target: { type: 'string', description: 'Channel name like #ng-sales-goats, or a Slack user ID like U08ABBFNGUW for a DM.' }, message: { type: 'string', description: 'The reminder text Max will post at the scheduled time.' }, postAt: { type: 'string', description: 'ISO 8601 datetime with timezone offset, e.g. 2026-04-24T15:00:00-06:00.' } }, required: ['target','message','postAt'] } },
          { name: 'add_calendar_attendees',description: 'Add guests to an existing Google Calendar event and send them invite emails. Use for "add X to the meeting", "forward the invite to Y", or "invite them to tomorrow\'s huddle". Workflow: call get_calendar_events first to find the event ID by summary/date, then call this tool with that ID and the list of attendee emails. Google sends update emails automatically.',                                                                                                                input_schema: { type: 'object', properties: { eventId: { type: 'string', description: 'Google Calendar event ID (returned in square brackets by get_calendar_events).' }, attendees: { type: 'array', items: { type: 'string' }, description: 'Array of email addresses to add as guests.' } }, required: ['eventId','attendees'] } },
          { name: 'create_calendar_event', description: 'Create a new Google Calendar event on Ron\'s primary calendar and send invites to the attendees. Times must be ISO 8601 with timezone offset. Use only when no suitable existing event exists — prefer add_calendar_attendees for existing meetings.',                                                                                                                                                                                                                                    input_schema: { type: 'object', properties: { summary: { type: 'string', description: 'Event title.' }, startISO: { type: 'string', description: 'Start time, ISO 8601 with offset, e.g. 2026-04-24T10:00:00-06:00.' }, endISO: { type: 'string', description: 'End time, ISO 8601 with offset.' }, attendees: { type: 'array', items: { type: 'string' }, description: 'Attendee email addresses.' }, description: { type: 'string', description: 'Optional event description.' }, location: { type: 'string', description: 'Optional location or video link.' } }, required: ['summary','startISO','endISO'] } },
          { name: 'get_sales_intelligence', description: 'Query GHL-native RevOps sales data from Supabase (appointments + outcomes are truth since the 2026-07-23 GHL cutover; EOD self-reports retired; iClosed rows are frozen history). PROVENANCE (state this when asked, never invent people): GHL workflow webhooks POST to the dash.neurogrowth.io portal, which normalizes them into the revops_* tables; setter_claims/lead_posts are written by the ✋ claim flow in #ng-sales-goats — they ARE the channel data in structured form, so never recount from Slack messages. Use for: closer performance (Jonathan, Jose, Ron — calls booked, show rate, sold, revenue, close rate from appointments + outcomes), setter performance (Oscar, William, Sebastian, Josue — calls booked, show rate, qualified attended calls from native setter attribution; Joseph and Debbanny are historical), today\'s calls (with per-call setter — GHL records who booked each appointment), prospect lookup by name, pipeline summary. Also "leads today" — authoritative count of new leads that arrived today and per-setter ownership (from lead_posts + setter_claims, NOT from Slack post text); always use this for the LEADS TODAY section instead of counting channel messages.', input_schema: { type: 'object', properties: { query: { type: 'string', description: 'Natural language query e.g. leads today, who booked the Andres Chavez call, how many calls today, close rate this month, Oscar bookings this week' } }, required: ['query'] } },
          { name: 'closer_monthly_scorecard', description: "Monthly per-closer scorecard from the shared closer_month_scorecard view — the SAME numbers as the portal page /admin/closer-scorecard, so never recompute month stats another way when asked for a closer's month. Returns calls assigned, outcomes logged, pending, showed, no-shows, qualified attended, won/lost/follow-up/DQ, show rate, close rate on shows, revenue, plus a REVI recording reality-check (calls that verifiably happened but were never logged, avg call score, no-show-vs-recording flags) and the month's unattributed outcomes. Months are America/Costa_Rica calendar months anchored on the call's scheduled time. When pending is high, always caveat that show/close rates are unreliable — pending does not mean the call didn't happen.", input_schema: { type: 'object', properties: { month: { type: 'string', description: "Month as YYYY-MM, e.g. 2026-07. For 'last month' compute from today's date in CR time." }, closer: { type: 'string', description: 'Optional closer email or name fragment (jose, ron, jonathan). Omit for all closers.' } }, required: ['month'] } },
          { name: 'log_call_outcome', description: "Log a sales-call outcome to the portal (revops_sales_outcomes) on EXPLICIT human instruction ONLY. Use when a closer or Ron states an outcome in their own words ('won 3500', 'that call was a no show', 'log Marco as lost') — typically replying to an outcome reminder/proposal DM. NEVER call this from your own inference, a REVI read, a transcript, or a report — if a human did not state the outcome in this conversation, do not call this tool. won REQUIRES revenue (the real closed amount; ask if not given — never guess). Writes are first-writer-wins: an existing outcome is never overwritten, the tool will tell you if one exists. Also promotes the prospect's pipeline status per the shared dash contract.", input_schema: { type: 'object', properties: { prospect: { type: 'string', description: 'Prospect email (preferred) or name fragment to find their appointment.' }, date: { type: 'string', description: 'Optional call date YYYY-MM-DD (CR time) to disambiguate when the prospect had multiple calls.' }, outcome: { type: 'string', enum: ['won', 'lost', 'follow_up', 'disqualified', 'no_show'], description: 'The outcome the human stated.' }, revenue: { type: 'number', description: 'Closed revenue in USD — required when outcome is won.' }, note: { type: 'string', description: 'Optional short context, e.g. who instructed it and why.' } }, required: ['prospect', 'outcome'] } },
          { name: 'create_notion_task',   description: 'Create a task in NeuroGrowth Notion. Operational/recurring tasks go to Operations Tracking. Project/strategic tasks go to Project Sprint Tracking.',                                                                                                                               input_schema: { type: 'object', properties: { title: { type: 'string' }, taskType: { type: 'string', description: 'operational (default) or project' }, priority: { type: 'string', description: 'P0 - Critical Customer Impact | P1 - High Business Impact | P2 - Growth & Scalability (default) | P3 - Strategic Initiatives' }, dueDate: { type: 'string', description: 'YYYY-MM-DD format (optional)' }, notes: { type: 'string', description: 'Additional context (optional)' }, customer: { type: 'string', description: 'Customer name (optional)' } }, required: ['title'] } },
          { name: 'create_scheduled_task',description: 'Create a new recurring scheduled task that Max will run automatically.',                  input_schema: { type: 'object', properties: { name: { type: 'string', description: 'Short name for the task' }, schedule: { type: 'string', description: 'Natural language schedule e.g. every Monday at 9am' }, prompt: { type: 'string', description: 'The instruction Max will execute at each scheduled run' }, channel: { type: 'string', description: 'Slack channel to post results to' } }, required: ['name','schedule','prompt'] } },
          { name: 'list_scheduled_tasks', description: 'List all scheduled tasks Max is currently running.',                                     input_schema: { type: 'object', properties: {} } },
          { name: 'clean_duplicate_tasks',description: 'Find and hard-delete duplicate scheduled tasks. Queries ALL rows including inactive. Keeps oldest clean-named version of each task.',                                                                                                                                               input_schema: { type: 'object', properties: {} } },
          { name: 'delete_scheduled_task',description: 'Deactivate and stop a scheduled task by its ID.',                                        input_schema: { type: 'object', properties: { taskId: { type: 'string', description: 'The task ID from list_scheduled_tasks' } }, required: ['taskId'] } },
          { name: 'update_portal_record', description: 'Update specific fields on a portal Supabase record. Whitelisted tables/fields only — use to log notes, correct LinkedIn handles, or update status after completing a task. Allowed tables: client_dashboards (notes, linkedin_handler, customer_status, is_active), customer_onboarding (notes).', input_schema: { type: 'object', properties: { table: { type: 'string', description: 'Table name e.g. client_dashboards' }, id: { type: 'string', description: 'Row UUID' }, fields: { type: 'object', description: 'Key-value pairs to update' } }, required: ['table', 'id', 'fields'] } },
          { name: 'get_meta_ads_summary', description: 'Get NeuroGrowth Meta Ads account-level performance summary — spend, impressions, reach, clicks, CTR, CPC, CPM, leads, and CPL.', input_schema: { type: 'object', properties: { datePreset: { type: 'string', description: 'Date range: today, yesterday, last_7d (default), last_14d, last_30d, last_month, this_month, this_quarter' } } } },
          { name: 'get_meta_campaigns',   description: 'Get Meta Ads campaign-level breakdown.',                                                  input_schema: { type: 'object', properties: { datePreset: { type: 'string', description: 'last_7d (default), last_14d, last_30d, this_month' }, limit: { type: 'number', description: 'Number of campaigns, default 10' } } } },
          { name: 'get_meta_adsets',      description: 'Get Meta Ads ad set level breakdown.',                                                    input_schema: { type: 'object', properties: { campaignId: { type: 'string', description: 'Optional campaign ID filter' }, datePreset: { type: 'string', description: 'last_7d (default), last_14d, last_30d, this_month' } } } },
          { name: 'get_meta_ads',         description: 'Get individual ad-level performance.',                                                    input_schema: { type: 'object', properties: { adSetId: { type: 'string', description: 'Optional ad set ID filter' }, datePreset: { type: 'string', description: 'last_7d (default), last_14d, last_30d, this_month' } } } },
          { name: 'detect_anomalies',     description: 'Run the anomaly-detection pass on demand. Scrapes the 8 tracked metrics, recomputes rolling baselines, and returns any metric currently >= 1.5σ from baseline. By default this is a dry-run (no DMs, no knowledge writes). Use this to answer "what is drifting right now?" without waiting for the daily cron.', input_schema: { type: 'object', properties: { dry_run: { type: 'boolean', description: 'If true (default), do not record observations or fire DMs. If false, runs the full pipeline as if from cron.' } } } },
          { name: 'preview_weekly_recap', description: 'Generate the Weekly Sales & Marketing Recap right now (week-to-date: Monday CR through this moment) and return it as text WITHOUT posting it anywhere. Use when Ron asks to preview, test, or see the weekly recap on demand — the Friday 5 PM cron still posts the real one.', input_schema: { type: 'object', properties: {} } },
          { name: 'get_revi_intelligence', description: "Query REVI (the sales-call coaching + leadership-initiative agent) data. Topics: 'coaching' = per-closer scores, outcomes, and latest coaching focus (query = optional closer name); 'initiatives' = open leadership initiatives from Win Da Week / Management Sync / Product Sync meetings, with owners, next steps, and days stalled; 'deals' = recent won/lost deals with loss reasons and pattern tags; 'prospect' = REVI's scored calls + buying signals for one prospect (query = email or name, required); 'scoreboard' = all-closer comparison. Use for any question about call coaching, closer performance quality, why deals are lost, or what initiatives are open/stalled. RON-ONLY — for team-accessible client call summaries use get_revi_client_context.", input_schema: { type: 'object', properties: { topic: { type: 'string', description: 'coaching | initiatives | deals | prospect | scoreboard' }, query: { type: 'string', description: 'Prospect email/name (topic=prospect) or closer name (topic=coaching). Optional otherwise.' }, days: { type: 'number', description: 'Lookback window in days. Defaults: coaching 14, deals 30, scoreboard 7.' } }, required: ['topic'] } },
          { name: 'get_revi_client_context', description: "Query REVI's client-facing call intelligence — open to the whole team. Topics: 'calls' = quicksync + activation-call report summaries (what was reviewed, conclusions, risks, health status, session number, PDF + recording links), auto-ingested from Fathom recordings — use for 'what was discussed with client X', 'how did the last MINDLIFT quicksync go', 'when was client X's activation call reviewed'; 'roster' = REVI's client list with status. Coaching teardowns, call scores, deal transcripts, and leadership initiatives are NOT here — those are Ron-only via get_revi_intelligence.", input_schema: { type: 'object', properties: { topic: { type: 'string', description: 'calls | roster' }, client: { type: 'string', description: 'Optional client-name fragment (aliases resolve automatically).' }, days: { type: 'number', description: 'topic=calls lookback window in days, default 60.' } }, required: ['topic'] } },
          { name: 'query_metric_history', description: 'Return the time series for a tracked metric so the user can see trend, baseline, and recent observations. Use when someone asks "show me CPL over the last 30 days" or "how has close rate trended?". Available metrics: meta_cpl_today, close_rate_yesterday, setter_calls_booked_yest, phase0_to_phase1_conv_7d, phase1_cycle_days_p50, phase2_cycle_days_p50, day7_at_risk_count, ghl_response_time_p50_min.', input_schema: { type: 'object', properties: { metric: { type: 'string', description: 'Exact metric name from the registry.' }, days: { type: 'number', description: 'Window of history to return, default 30, max 90.' } }, required: ['metric'] } },
          { name: 'set_appointment_status', description: "Set a call's ATTENDANCE (Showed / No-Show / Cancelled) on the GHL appointment — this is Paso 1, separate from the deal outcome. Use ONLY when a human answers Max's '❔ did they show?' question, or states attendance in their own words ('she never showed', 'that one got cancelled'). Max normally sets Showed automatically from a REVI recording, so this tool exists for the calls REVI could not read. NEVER call it from your own inference. Setting the deal outcome (won/lost/no fit/open deal) is log_call_outcome instead — do not confuse the two.", input_schema: { type: 'object', properties: { prospect: { type: 'string', description: 'Prospect email (preferred) or name fragment.' }, date: { type: 'string', description: 'Optional call date YYYY-MM-DD (CR time) to disambiguate multiple calls.' }, status: { type: 'string', enum: ['showed', 'noshow', 'cancelled'], description: 'The attendance the human stated.' } }, required: ['prospect', 'status'] } },
          { name: 'draft_outbound_email', description: "Use this when a setter/closer asks you to send a NEW email on their behalf to a client (proposals, follow-ups, scheduling). Conversationally collect to + subject + body first (cc optional). Do NOT call this tool until you have all three required fields. Once called, the draft is shown to the setter for review, then routed to Ron for final approval before sending from ronny.duarte@neurogrowth.io with Ron's signature. Always confirm field values back to the user before drafting so they can correct typos. Mirror the user's language (English/Spanish) in your conversation.", input_schema: { type: 'object', properties: { to: { type: 'string', description: 'Recipient email address.' }, subject: { type: 'string', description: 'Email subject line.' }, body: { type: 'string', description: 'Email body in the language the setter dictated. Plaintext only — no markdown, no HTML.' }, cc: { type: 'string', description: 'Optional comma-separated cc recipients.' }, contact_name: { type: 'string', description: 'Optional contact display name for context.' } }, required: ['to','subject','body'] } },
          { name: 'draft_reply_email', description: 'Use this only when the setter is replying to a client message that Max forwarded to them earlier from an active email thread. Body is the setter-dictated reply. Routes through the same setter-review then Ron-approval flow as draft_outbound_email. If the setter has multiple active threads, ask which one before calling.', input_schema: { type: 'object', properties: { body: { type: 'string', description: 'Reply body, plaintext, in whatever language the setter dictated.' }, thread_id: { type: 'string', description: 'Optional email_threads row id; if omitted Max will use the setter\'s single active thread.' } }, required: ['body'] } },
          { name: 'make_list_dlqs', description: 'List the incomplete executions (DLQ) queued on a Make scenario: id, when it failed, and the error reason. Read-only. Use this before make_get_dlq. Incomplete executions are runs that errored and piled up while Make still reports the scenario as active.', input_schema: { type: 'object', properties: { scenario_id: { type: 'string', description: 'Make scenario id, e.g. 5148796' }, limit: { type: 'number', description: 'Default 10, max 25.' } }, required: ['scenario_id'] } },
          { name: 'make_get_dlq',   description: 'Get one Make incomplete execution: which module failed, the error, and the input bundle that caused it. The bundle carries the real contact/appointment ids, so use it to name who was affected. Read-only.', input_schema: { type: 'object', properties: { dlq_id: { type: 'string' } }, required: ['dlq_id'] } },
      ].filter(t => EMAIL_PROXY_LIVE || (t.name !== 'draft_outbound_email' && t.name !== 'draft_reply_email'))
       .filter(t => !(opts.dropTools || []).includes(t.name))
       .filter(t => !opts.onlyTools || opts.onlyTools.includes(t.name))
       .filter(t => process.env.MAKE_API_TOKEN || !t.name.startsWith('make_'));
      if (opts.finalTool) TOOLS.push(opts.finalTool);

      // ── Tool dispatcher — shared across initial and all follow-up rounds ──────
      async function dispatchTool(toolUse) {
        // Gate: Ron-only tools refuse for non-Ron users
        if (userId && userId !== RON_SLACK_ID && RON_ONLY_TOOLS.has(toolUse.name)) {
          return {
            type: 'tool_result',
            tool_use_id: toolUse.id,
            content: JSON.stringify(`BLOCKED: ${toolUse.name} is Ron-only. ${RON_ONLY_REASONS[toolUse.name] || RON_ONLY_DEFAULT_REASON} Ask Ron to run this, or let me help a different way.`),
          };
        }
        let result;
        if      (toolUse.name === 'search_notion')          result = await searchNotion(toolUse.input.query);
        else if (toolUse.name === 'get_notion_page')        result = await getNotionPage(toolUse.input.page_id);
        else if (toolUse.name === 'get_recent_emails')      result = await getRecentEmails();
        else if (toolUse.name === 'send_email')             { const r = await sendEmail(toolUse.input.to, toolUse.input.subject, toolUse.input.body); result = r.message || `Email sent to ${toolUse.input.to}`; }
        else if (toolUse.name === 'get_calendar_events')    result = await getCalendarEvents(toolUse.input.daysFromNow || 0, toolUse.input.daysRange || 1);
        else if (toolUse.name === 'search_drive')           { const r = await searchDrive(toolUse.input.query); result = r.length > 4000 ? r.substring(0, 4000) + '...[trimmed]' : r; }
        else if (toolUse.name === 'read_google_sheet')      result = await readGoogleSheet(extractGoogleFileId(toolUse.input.spreadsheetId), toolUse.input.range || null);
        else if (toolUse.name === 'get_ops_tracker')        result = await getOpsTracker(toolUse.input.tab, toolUse.input.client || null, toolUse.input.include_done || false);
        else if (toolUse.name === 'read_google_doc')        result = await readGoogleDoc(extractGoogleFileId(toolUse.input.documentId));
        else if (toolUse.name === 'read_slack_channel')     result = await readSlackChannel(toolUse.input.channelName, toolUse.input.messageCount || 20);
        else if (toolUse.name === 'draft_channel_post')     {
          const escalate = toolUse.input.escalate_to_ron ? '1' : '0';
          const reason = (toolUse.input.escalation_reason || '').replace(/[|\n\r]/g, ' ');
          result = `APPROVAL_NEEDED|${toolUse.input.channelName}|${escalate}|${userId || RON_SLACK_ID}|${reason}|${toolUse.input.message}`;
        }
        else if (toolUse.name === 'get_ghl_conversations')  result = await getGHLConversations(toolUse.input.limit || 20, toolUse.input.unreadOnly || false);
        else if (toolUse.name === 'search_knowledge')       result = await searchKnowledge(toolUse.input.query, toolUse.input.category, userId);
        else if (toolUse.name === 'save_knowledge')         result = await upsertKnowledge(toolUse.input.category, toolUse.input.key, toolUse.input.value, 'conversation', userId, toolUse.input.visibility || 'shared');
        else if (toolUse.name === 'get_knowledge_category') result = await getAllKnowledgeByCategory(toolUse.input.category, userId);
        else if (toolUse.name === 'get_client_status')      result = await getClientStatus(toolUse.input.clientName || null);
        else if (toolUse.name === 'get_portal_alerts')      result = await getPortalAlerts();
        else if (toolUse.name === 'get_phase0_clients')     result = await getPhase0Clients();
        else if (toolUse.name === 'search_portal_schema')   result = await searchPortalSchema(toolUse.input.keywords);
        else if (toolUse.name === 'list_portal_tables')     result = await listPortalTables();
        else if (toolUse.name === 'describe_portal_table')  result = await describePortalTable(toolUse.input.tableName);
        else if (toolUse.name === 'query_portal_db')        result = await queryPortalDb(toolUse.input.sql);
        else if (toolUse.name === 'create_slack_reminder')  result = await createSlackReminder(toolUse.input.target, toolUse.input.message, toolUse.input.postAt);
        else if (toolUse.name === 'add_calendar_attendees') result = await addCalendarAttendees(toolUse.input.eventId, toolUse.input.attendees);
        else if (toolUse.name === 'create_calendar_event')  result = await createCalendarEvent(toolUse.input.summary, toolUse.input.startISO, toolUse.input.endISO, toolUse.input.attendees || [], toolUse.input.description || '', toolUse.input.location || '');
        else if (toolUse.name === 'get_sales_intelligence') result = await getSalesIntelligence(toolUse.input.query);
        else if (toolUse.name === 'closer_monthly_scorecard') result = await getCloserMonthlyScorecard(toolUse.input.month, toolUse.input.closer);
        else if (toolUse.name === 'log_call_outcome')       result = await logCallOutcomeTool(toolUse.input);
        else if (toolUse.name === 'set_appointment_status') result = await setAppointmentStatusTool(toolUse.input);
        else if (toolUse.name === 'create_notion_task')     result = await createNotionTask(toolUse.input.title, toolUse.input.taskType || 'operational', toolUse.input.priority || 'P2 - Growth & Scalability', toolUse.input.dueDate, toolUse.input.notes, toolUse.input.customer);
        else if (toolUse.name === 'create_scheduled_task')  result = await createScheduledTask(toolUse.input.name, toolUse.input.schedule, toolUse.input.prompt, toolUse.input.channel, userId);
        else if (toolUse.name === 'list_scheduled_tasks')   result = await listScheduledTasks();
        else if (toolUse.name === 'clean_duplicate_tasks')  result = await cleanDuplicateTasks();
        else if (toolUse.name === 'delete_scheduled_task')  result = await deleteScheduledTask(toolUse.input.taskId);
        else if (toolUse.name === 'update_portal_record')   result = await updatePortalRecord(toolUse.input.table, toolUse.input.id, toolUse.input.fields);
        else if (toolUse.name === 'get_meta_ads_summary')   result = await getMetaAdsSummary(toolUse.input.datePreset || 'last_7d');
        else if (toolUse.name === 'get_meta_campaigns')     result = await getMetaCampaigns(toolUse.input.datePreset || 'last_7d', toolUse.input.limit || 10);
        else if (toolUse.name === 'get_meta_adsets')        result = await getMetaAdSets(toolUse.input.campaignId || null, toolUse.input.datePreset || 'last_7d');
        else if (toolUse.name === 'get_meta_ads')           result = await getMetaAds(toolUse.input.adSetId || null, toolUse.input.datePreset || 'last_7d');
        else if (toolUse.name === 'detect_anomalies')       {
          const dryRun = toolUse.input.dry_run !== false;
          const out = await runAnomalyDetection({ dryRun });
          result = `Anomaly check (${dryRun ? 'dry-run' : 'live'}) — scraped ${out.scraped.length}, skipped ${out.skipped.length}, anomalies ${out.anomalies.length}, errors ${out.errors.length}\n\n` +
            (out.anomalies.length
              ? out.anomalies.map(a => `${a.metric} (${a.domain}): value=${a.value}, mean=${a.mean.toFixed(2)}, ${a.z >= 0 ? '+' : ''}${a.z.toFixed(2)}σ`).join('\n')
              : 'No metrics currently outside baseline thresholds.') +
            (out.skipped.length ? `\n\nSkipped: ${out.skipped.map(s => s.metric + ' (' + s.reason + ')').join(', ')}` : '') +
            (out.errors.length ? `\n\nErrors: ${out.errors.map(e => e.metric + ': ' + e.error).join('; ')}` : '');
        }
        else if (toolUse.name === 'preview_weekly_recap')   result = await runWeeklySalesMarketingRecap(null, { preview: true });
        else if (toolUse.name === 'query_metric_history')   result = await queryMetricHistory(toolUse.input.metric, Math.min(toolUse.input.days || 30, 90));
        else if (toolUse.name === 'get_revi_intelligence')  result = await queryReviIntelligence(toolUse.input.topic, toolUse.input.query || null, toolUse.input.days);
        else if (toolUse.name === 'get_revi_client_context') result = await queryReviClientContext(toolUse.input.topic, toolUse.input.client || null, toolUse.input.days);
        else if (toolUse.name === 'draft_outbound_email')   result = await draftOutboundEmail(toolUse.input, userId);
        else if (toolUse.name === 'draft_reply_email')      result = await draftReplyEmail(toolUse.input, userId);
        else if (toolUse.name === 'make_list_dlqs')         result = await listMakeDlqs(toolUse.input.scenario_id, toolUse.input.limit);
        else if (toolUse.name === 'make_get_dlq')           result = await getMakeDlqDetail(toolUse.input.dlq_id);
        return { type: 'tool_result', tool_use_id: toolUse.id, content: JSON.stringify(result) };
      }

      // ── Initial call ─────────────────────────────────────────────────────────
      const tInitial = Date.now();
      let response = await anthropic.messages.create({
        model: 'claude-sonnet-4-6',
        max_tokens: 4096,
        system: fullSystemPrompt,
        messages,
        tools: TOOLS,
      });
      logLlmFromAnthropicResponse(response, Date.now() - tInitial, correlation_id);

      // ── Multi-round tool loop (max 5 rounds to prevent infinite chains) ──────
      // 7 rounds: rule #6 mandates data-map → schema search → query before any
      // "doesn't exist" claim; stacked with search_knowledge + a status tool a
      // realistic chain exceeds the old cap of 5.
      const MAX_TOOL_ROUNDS = 7;
      let currentMessages = [...messages];

      for (let round = 0; round < MAX_TOOL_ROUNDS; round++) {
        if (response.stop_reason !== 'tool_use') break;

        const toolUses = response.content.filter(b => b.type === 'tool_use');

        // finalTool short-circuit: the structured submission IS the answer — return its input
        if (opts.finalTool) {
          const fin = toolUses.find(t => t.name === opts.finalTool.name);
          if (fin) return { structured: fin.input };
        }

        const toolResults = await Promise.all(toolUses.map(async (toolUse) => {
          const tTool = Date.now();
          let errored = false;
          let err = null;
          let res;
          try {
            res = await dispatchTool(toolUse);
          } catch (e) {
            errored = true;
            err = e;
            res = { type: 'tool_result', tool_use_id: toolUse.id, content: JSON.stringify(`Error running tool ${toolUse.name}: ${e.message}`) };
          }
          const duration_ms = Date.now() - tTool;
          const resultSummary = res && res.content != null ? String(res.content).slice(0, 2000) : '';
          logActivity({
            event_type: 'tool_call',
            event_source: 'internal',
            action: toolUse.name,
            tool_name: toolUse.name,
            input: toolUse.input,
            output: { summary: resultSummary },
            status: errored ? 'error' : 'ok',
            error_message: errored && err && err.message ? err.message.slice(0, 2000) : null,
            duration_ms,
            correlation_id,
          });
          return res;
        }));

        // Check for approval draft before continuing — pass sentinel through verbatim
        const draftResult = toolResults.find(r => { try { const v = JSON.parse(r.content); return typeof v === 'string' && (v.startsWith('APPROVAL_NEEDED|') || v.startsWith('SETTER_REVIEW_NEEDED|')); } catch { return false; } });
        if (draftResult) {
          return JSON.parse(draftResult.content);
        }

        // Advance message chain and call Claude again with same fullSystemPrompt (preserves time context)
        currentMessages = [...currentMessages, { role: 'assistant', content: response.content }, { role: 'user', content: toolResults }];

        let nextResponse = null;
        for (let fuAttempt = 0; fuAttempt < 3; fuAttempt++) {
          try {
            const tFollow = Date.now();
            nextResponse = await anthropic.messages.create({
              model: 'claude-sonnet-4-6',
              max_tokens: 4096,
              system: fullSystemPrompt,
              messages: currentMessages,
              tools: TOOLS,
            });
            logLlmFromAnthropicResponse(nextResponse, Date.now() - tFollow, correlation_id);
            break;
          } catch (fuErr) {
            if ((fuErr.status === 529 || fuErr.status === 503) && fuAttempt < 2) {
              const wait = (fuAttempt + 1) * 10000;
              console.log(`followUp overloaded (round ${round + 1}), retrying in ${wait/1000}s...`);
              await new Promise(r => setTimeout(r, wait));
            } else { throw fuErr; }
          }
        }

        if (!nextResponse) break;
        response = nextResponse;
      }

      // finalTool never fired (model ended in text or exhausted rounds) — force the submission.
      // If the last response still has pending tool_use blocks (round cap hit), drop that turn:
      // appending it without tool_results would be an invalid message chain.
      if (opts.finalTool) {
        const forceMsgs = [...currentMessages];
        if (response.stop_reason !== 'tool_use') forceMsgs.push({ role: 'assistant', content: response.content });
        const nudge = `Submit your final answer now by calling ${opts.finalTool.name}.`;
        const lastMsg = forceMsgs[forceMsgs.length - 1];
        if (lastMsg && lastMsg.role === 'user') {
          const blocks = Array.isArray(lastMsg.content) ? lastMsg.content : [{ type: 'text', text: String(lastMsg.content) }];
          forceMsgs[forceMsgs.length - 1] = { role: 'user', content: [...blocks, { type: 'text', text: nudge }] };
        } else {
          forceMsgs.push({ role: 'user', content: nudge });
        }
        const tForce = Date.now();
        const forced = await anthropic.messages.create({
          model: 'claude-sonnet-4-6',
          max_tokens: 4096,
          system: fullSystemPrompt,
          messages: forceMsgs,
          tools: TOOLS,
          tool_choice: { type: 'tool', name: opts.finalTool.name },
        });
        logLlmFromAnthropicResponse(forced, Date.now() - tForce, correlation_id);
        const fin = forced.content.find(b => b.type === 'tool_use' && b.name === opts.finalTool.name);
        if (fin) return { structured: fin.input };
      }

      const responseText = response.content.filter(b => b.type === 'text').map(b => b.text).join('\n');
      return responseText || null;

    } catch (err) {
      lastErr = err;
      if (err.status === 529 || err.status === 503 || err.status === 500) {
        const wait = err.status === 529 ? (attempt + 1) * 10000 : (attempt + 1) * 4000;
        console.log(`API overloaded (attempt ${attempt + 1}/${retries}), retrying in ${wait / 1000}s...`);
        await new Promise(r => setTimeout(r, wait));
      } else { throw err; }
    }
  }
  throw lastErr;
}

// ─── SLACK HELPERS ────────────────────────────────────────────────────────────
async function postToSlack(channel, text, threadTs = null) {
  if (!text || !text.trim()) { console.error('postToSlack called with empty text, skipping.'); return; }
  const channelName = channel.startsWith('#') ? channel.slice(1) : channel;
  const payload = { channel: channelName, text };
  if (threadTs) payload.thread_ts = threadTs;
  await slack.client.chat.postMessage(payload);
}

// `say` is absent on cron paths — there is no originating Slack conversation to
// reply into. Every say() here must therefore be guarded: an unguarded one threw a
// TypeError immediately AFTER the channel post succeeded, and the catch below then
// called say() again and threw a second time, propagating out. Callers with a real
// `say` (the approval flow) are unaffected.
async function executeChannelPost(channelName, message, say, correlationId) {
  const reply = async (text) => {
    if (typeof say === 'function') await say(text);
    else console.log(`executeChannelPost: ${text}`);
  };
  try {
    const channels = await getCachedChannelList();
    const channel  = channels.find(c => c.name === channelName.replace('#', ''));
    if (!channel) { await reply(`Could not find channel ${channelName}.`); }
    else {
      await slack.client.chat.postMessage({ channel: channel.id, text: message });
      if (correlationId) {
        logActivity({ event_type: 'slack_message', event_source: 'slack', action: 'outbound', channel_id: channel.id, output: { text: String(message).slice(0, 2000) }, correlation_id: correlationId });
      }
      await reply(`Posted to ${channelName}.`);
      if (correlationId) {
        logActivity({ event_type: 'slack_message', event_source: 'slack', action: 'outbound', output: { text: `Posted to ${channelName}.`.slice(0, 2000) }, correlation_id: correlationId });
      }
    }
  } catch (err) { await reply(`Something went wrong posting: ${err.message}`); }
}

// ─── EMAIL PROXY: tool handlers ───────────────────────────────────────────────
// These tool handlers are called by Claude. They DO NOT send anything — they
// stage a draft for setter review (Stage 1), then return a sentinel that the
// tool loop intercepts so the conversation pauses. The setter's "looks good"
// promotes the draft to pendingApprovals[RON_SLACK_ID] (Stage 2).

async function draftOutboundEmail(input, setterSlackId) {
  const to = String(input.to || '').trim();
  const subject = String(input.subject || '').trim();
  const body = String(input.body || '').trim();
  if (!to || !subject || !body) {
    return 'Missing one of to / subject / body — collect the missing piece from the setter before calling this tool again.';
  }
  if (!/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(to)) {
    return `That doesn't look like a valid email address: "${to}". Ask the setter to confirm.`;
  }
  const cc = input.cc ? String(input.cc).trim() : '';
  pendingDrafts[setterSlackId] = {
    kind: 'email_outbound',
    to, cc, subject, body,
    contactName: input.contact_name || null,
    createdAt: Date.now(),
  };
  return `SETTER_REVIEW_NEEDED|outbound|${setterSlackId}`;
}

async function draftReplyEmail(input, setterSlackId) {
  const body = String(input.body || '').trim();
  if (!body) return 'Reply body is empty — ask the setter what they want to say.';

  // Resolve thread: explicit thread_id wins; else look up the setter's active threads.
  let thread = null;
  if (input.thread_id) {
    const { data, error } = await supabase
      .from('email_threads')
      .select('*')
      .eq('id', input.thread_id)
      .maybeSingle();
    if (error) return `Couldn't load that thread: ${error.message}. Contact Ron/admin.`;
    if (!data) return `No email thread found with id ${input.thread_id}.`;
    thread = data;
  } else {
    const { data, error } = await supabase
      .from('email_threads')
      .select('*')
      .eq('initiated_by_slack_id', setterSlackId)
      .eq('active', true)
      .order('last_message_at', { ascending: false });
    if (error) return `Couldn't load your active threads: ${error.message}. Contact Ron/admin.`;
    if (!data || data.length === 0) return `You don't have an active email thread to reply to. Use draft_outbound_email to start a new one.`;
    if (data.length > 1) {
      const list = data.map(t => `• ${t.subject} → ${(t.to_addresses || []).join(', ')} (id: ${t.id})`).join('\n');
      return `You have ${data.length} active threads. Ask which one and call draft_reply_email again with thread_id:\n${list}`;
    }
    thread = data[0];
  }

  pendingDrafts[setterSlackId] = {
    kind: 'email_reply',
    thread,
    body,
    createdAt: Date.now(),
  };
  return `SETTER_REVIEW_NEEDED|reply|${setterSlackId}`;
}

// Stage 1 → Stage 2 promotion. Called from checkApproval when the setter says
// "looks good". Stages the draft into pendingApprovals[RON_SLACK_ID] and DMs
// Ron with the preview.
async function promoteDraftToRon(setterSlackId, say) {
  const draft = pendingDrafts[setterSlackId];
  if (!draft) return false;

  const setter = getMemberContext(setterSlackId);
  let to, cc, subject, body, threadMeta;
  if (draft.kind === 'email_outbound') {
    ({ to, cc, subject, body } = draft);
    threadMeta = null;
  } else {
    const t = draft.thread;
    to = (t.to_addresses || []).join(', ');
    cc = (t.cc_addresses || []).join(', ');
    subject = t.subject.toLowerCase().startsWith('re:') ? t.subject : `Re: ${t.subject}`;
    body = draft.body;
    threadMeta = {
      gmailThreadId: t.gmail_thread_id,
      inReplyTo: t.last_rfc822_message_id,
      references: ([...(t.rfc822_message_id_chain || []), t.last_rfc822_message_id]).filter(Boolean).join(' '),
      threadRowId: t.id,
    };
  }

  delete pendingDrafts[setterSlackId];

  // ── Autosend bypass: trusted setters skip Ron's Stage-2 approval ──────────
  // Setter still gets the success/failure DM directly from executeEmailSend;
  // Ron gets the audit-trail confirmation (or technical error) via the
  // say-wrapper below so he has visibility into autosent emails without
  // having to approve each one.
  if (isAutosendUser(setterSlackId)) {
    await say(`Listo, enviando ahora.`);
    const syntheticPending = {
      kind: 'email',
      requestedBy: setterSlackId,
      email: { to, cc, subject, body, threadMeta },
      createdAt: Date.now(),
      autosent: true,
    };
    await executeEmailSend(syntheticPending, async (msg) => {
      try {
        await slack.client.chat.postMessage({ channel: RON_SLACK_ID, text: `[autosend · ${setter.displayName}] ${msg}` });
      } catch {}
    });
    return true;
  }

  // ── Standard flow: stage for Ron's approval ───────────────────────────────
  pendingApprovals[RON_SLACK_ID] = {
    kind: 'email',
    requestedBy: setterSlackId,
    email: { to, cc, subject, body, threadMeta },
    createdAt: Date.now(),
  };

  // DM the setter — handoff confirmation.
  await say(`Got it — sending to Ron for final approval. I'll DM you when it goes out (or if Ron pushes back).`);

  // DM Ron — preview with setter attribution.
  const ccLine = cc ? `\nCc: ${cc}` : '';
  const tR = `📧 Email approval requested by *${setter.displayName}*\n\nTo: ${to}${ccLine}\nSubject: ${subject}\n\n${body}\n\nReply *yes* to send (from ronny.duarte@neurogrowth.io with your signature) or *no* to cancel.`;
  try {
    await slack.client.chat.postMessage({ channel: RON_SLACK_ID, text: tR });
  } catch (err) {
    console.error('Ron approval DM failed:', err.message);
    await say(`⚠️ Couldn't reach Ron's DM (${err.message}). Contact Ron/admin to investigate.`);
  }
  return true;
}

// Build the actual email + persist to email_threads. Called from checkApproval
// when Ron says "yes" to a kind=email pending approval.
async function executeEmailSend(pending, say) {
  const { to, cc, subject, body, threadMeta } = pending.email;
  const setterId = pending.requestedBy;
  const setter = getMemberContext(setterId);

  try {
    const opts = {};
    if (cc) opts.cc = cc;
    if (threadMeta) {
      opts.threadId = threadMeta.gmailThreadId;
      opts.inReplyTo = threadMeta.inReplyTo;
      opts.references = threadMeta.references;
    }
    const sendRes = await sendEmail(to, subject, body, opts);

    // Persist or update email_threads row.
    if (threadMeta) {
      // Reply: update existing row.
      const newChain = [
        ...(threadMeta.references ? threadMeta.references.split(/\s+/).filter(Boolean) : []),
      ];
      // Append the prior last id if not already in chain.
      if (threadMeta.inReplyTo && !newChain.includes(threadMeta.inReplyTo)) newChain.push(threadMeta.inReplyTo);
      await supabase.from('email_threads').update({
        last_our_message_id: sendRes.gmailMessageId,
        last_rfc822_message_id: sendRes.rfc822MessageId,
        rfc822_message_id_chain: newChain,
        last_message_at: new Date().toISOString(),
      }).eq('id', threadMeta.threadRowId);
    } else {
      await supabase.from('email_threads').insert({
        gmail_thread_id: sendRes.gmailThreadId,
        last_our_message_id: sendRes.gmailMessageId,
        last_rfc822_message_id: sendRes.rfc822MessageId,
        rfc822_message_id_chain: [],
        to_addresses: to.split(',').map(s => s.trim()).filter(Boolean),
        cc_addresses: cc ? cc.split(',').map(s => s.trim()).filter(Boolean) : [],
        subject,
        initiated_by_slack_id: setterId,
        last_message_at: new Date().toISOString(),
        active: true,
      });
    }

    // DM setter — success.
    await slack.client.chat.postMessage({
      channel: setterId,
      text: `✅ Email sent to ${to} — subject: "${subject}". I'll DM you here when they reply.`,
    });
    // Confirmation back to Ron.
    await say(`✅ Sent on behalf of ${setter.displayName} to ${to}.`);
  } catch (err) {
    console.error('executeEmailSend failed:', err);
    const short = (err.response?.data?.error?.message || err.message || 'unknown error').slice(0, 200);
    try {
      await slack.client.chat.postMessage({
        channel: setterId,
        text: `❌ Send failed: ${short}. Please contact Ron/admin to investigate.`,
      });
    } catch {}
    await say(`🔴 Email send failed for ${setter.displayName} → ${to}. Error: ${short}`);
  }
}

const SETTER_APPROVE_PHRASES = /^(looks good|send it|approved|go ahead|👍|ok|okay|yes|yep|yup|se ve bien|envíalo|enviarlo|envialo|aprobado|listo|dale|sí|si)\b/i;
const SETTER_CANCEL_PHRASES  = /^(cancel|cancelar|cancela|never mind|nevermind|olvídalo|olvidalo|stop|abort|no)\b/i;

async function checkApproval(message, say, userId) {
  const text = (typeof message === 'string' ? message : message.text || '').trim();
  const lower = text.toLowerCase();

  // ── Stage 1: setter has a pending email draft ───────────────────────────────
  if (pendingDrafts[userId]) {
    if (SETTER_APPROVE_PHRASES.test(lower)) {
      await promoteDraftToRon(userId, say);
      return true;
    }
    if (SETTER_CANCEL_PHRASES.test(lower)) {
      delete pendingDrafts[userId];
      await say('❌ Cancelled. Nothing was sent.');
      return true;
    }
    // Anything else → fall through; let Claude treat it as edit instructions
    // (the next turn will see a fresh prompt and the setter can clarify).
  }

  const pending = pendingApprovals[userId];
  if (!pending) return false;
  const approvalCid = newCorrelationId();

  // ── Stage 2 (email): Ron approves an email send ─────────────────────────────
  if (pending.kind === 'email') {
    if (['yes','send it','approved','go ahead','👍'].includes(lower)) {
      await executeEmailSend(pending, say);
      delete pendingApprovals[userId];
      return true;
    }
    if (['no','cancel','stop'].includes(lower)) {
      const setterId = pending.requestedBy;
      try {
        await slack.client.chat.postMessage({
          channel: setterId,
          text: `❌ Ron didn't approve the draft for ${pending.email?.to}. Contact Ron/admin if you need to discuss.`,
        });
      } catch {}
      await say('Cancelled. Email not sent.');
      delete pendingApprovals[userId];
      return true;
    }
    return false;
  }

  // ── Stage 2 (legacy channel post) ───────────────────────────────────────────
  if (['yes','send it','approved','go ahead','👍'].includes(lower)) {
    await executeChannelPost(pending.channelName, pending.message, say, approvalCid);
    // Notify originator if the approver was Ron acting on someone else's draft
    if (pending.requestedBy && pending.requestedBy !== userId) {
      try {
        const t = `Ron approved the draft for ${pending.channelName}. It has been posted.`;
        await slack.client.chat.postMessage({
          channel: pending.requestedBy,
          text: t,
        });
        logActivity({ event_type: 'slack_message', event_source: 'slack', action: 'outbound', channel_id: pending.requestedBy, output: { text: t.slice(0, 2000) }, correlation_id: approvalCid });
      } catch (notifyErr) { console.error('Originator notify error:', notifyErr.message); }
    }
    delete pendingApprovals[userId];
    return true;
  }
  if (['no','cancel','stop'].includes(lower)) {
    const cancelT = 'Cancelled. Nothing was posted.';
    await say(cancelT);
    logActivity({ event_type: 'slack_message', event_source: 'slack', action: 'outbound', output: { text: cancelT.slice(0, 2000) }, correlation_id: approvalCid });
    if (pending.requestedBy && pending.requestedBy !== userId) {
      try {
        const t2 = `Ron held the draft for ${pending.channelName} — want to revise and try again?`;
        await slack.client.chat.postMessage({
          channel: pending.requestedBy,
          text: t2,
        });
        logActivity({ event_type: 'slack_message', event_source: 'slack', action: 'outbound', channel_id: pending.requestedBy, output: { text: t2.slice(0, 2000) }, correlation_id: approvalCid });
      } catch (notifyErr) { console.error('Originator notify error:', notifyErr.message); }
    }
    delete pendingApprovals[userId];
    return true;
  }
  return false;
}

// Approval sentinel format:
//   APPROVAL_NEEDED|<channelName>|<escalate '0'|'1'>|<originUserId>|<reason>|<message...>
// message is everything after the 5th pipe so it may itself contain pipes.
function handleDraftReply(reply, userId, say, correlationId) {
  if (reply.startsWith('SETTER_REVIEW_NEEDED|')) return handleSetterReview(reply, userId, say);
  if (!reply.startsWith('APPROVAL_NEEDED|')) return false;
  const parts = reply.split('|');
  const channelName = parts[1];
  let escalate = false;
  let originUserId = userId;
  let reason = '';
  let draftMessage;

  // New 6-part format with escalate/origin/reason
  if (parts.length >= 6 && (parts[2] === '0' || parts[2] === '1')) {
    escalate     = parts[2] === '1';
    originUserId = parts[3] || userId;
    reason       = parts[4] || '';
    draftMessage = parts.slice(5).join('|');
  } else {
    // Legacy 3-part format — treat as originator self-approval
    draftMessage = parts.slice(2).join('|');
  }

  const origin = getMemberContext(originUserId);
  const approver = escalate ? RON_SLACK_ID : originUserId;
  pendingApprovals[approver] = {
    channelName,
    message: draftMessage,
    requestedBy: originUserId,
    createdAt: Date.now(),
  };

  if (escalate && approver !== originUserId) {
    // Tell the originator we're routing to Ron
    const tO = `This one needs Ron's call${reason ? ` — ${reason}` : ''}. I've routed the draft to him and I'll let you know when he signs off.\n\nFor ${channelName}:\n\n"${draftMessage}"`;
    say(tO);
    if (correlationId) logActivity({ event_type: 'slack_message', event_source: 'slack', action: 'outbound', output: { text: tO.slice(0, 2000) }, correlation_id: correlationId });
    // DM Ron with the attributed draft
    const tR = `Escalation from ${origin.displayName}${reason ? ` — ${reason}` : ''}\n\nDraft for ${channelName}:\n\n"${draftMessage}"\n\nReply "send it" to post or "cancel" to discard.`;
    slack.client.chat.postMessage({ channel: RON_SLACK_ID, text: tR })
      .then(() => {
        if (correlationId) logActivity({ event_type: 'slack_message', event_source: 'slack', action: 'outbound', channel_id: RON_SLACK_ID, output: { text: tR.slice(0, 2000) }, correlation_id: correlationId });
      })
      .catch(err => console.error('Escalation DM to Ron failed:', err.message));
  } else {
    const tA = `Here is what I would post to *${channelName}*:\n\n"${draftMessage}"\n\nReply *yes* to send it or *no* to cancel.`;
    say(tA);
    if (correlationId) logActivity({ event_type: 'slack_message', event_source: 'slack', action: 'outbound', output: { text: tA.slice(0, 2000) }, correlation_id: correlationId });
  }
  return true;
}

// ─── EMAIL PROXY: active-thread hint for the DM handler ──────────────────────
// Returns a short system-style note for the user message if the setter has
// active email threads, so Claude knows when to call draft_reply_email.
async function getActiveThreadHint(setterSlackId) {
  if (!EMAIL_PROXY_LIVE) return '';
  try {
    const { data, error } = await supabase
      .from('email_threads')
      .select('id, subject, to_addresses, last_message_at')
      .eq('initiated_by_slack_id', setterSlackId)
      .eq('active', true)
      .order('last_message_at', { ascending: false })
      .limit(5);
    if (error || !data || data.length === 0) return '';
    if (data.length === 1) {
      const t = data[0];
      return `\n\n[CONTEXT: This user has 1 active email thread: subject "${t.subject}" with ${(t.to_addresses || []).join(', ')}. If their message reads like a reply intended for that client, call draft_reply_email. If it is meta (cancel, never mind, a question about something else), respond conversationally.]`;
    }
    const list = data.map(t => `  • id=${t.id} — "${t.subject}" with ${(t.to_addresses || []).join(', ')}`).join('\n');
    return `\n\n[CONTEXT: This user has ${data.length} active email threads:\n${list}\nIf they want to reply to one, ask which thread first, then call draft_reply_email with thread_id.]`;
  } catch (err) {
    console.error('getActiveThreadHint error:', err.message);
    return '';
  }
}

// ─── EMAIL PROXY: setter-review sentinel handler ──────────────────────────────
// Sentinel format: SETTER_REVIEW_NEEDED|<outbound|reply>|<setterSlackId>
// Reads the staged draft from pendingDrafts and posts a preview to the setter.
function handleSetterReview(reply, userId, say) {
  if (!reply.startsWith('SETTER_REVIEW_NEEDED|')) return false;
  const draft = pendingDrafts[userId];
  if (!draft) {
    say("Draft expired. Please start over.").catch(() => {});
    return true;
  }

  let preview;
  if (draft.kind === 'email_outbound') {
    const ccLine = draft.cc ? `\nCc: ${draft.cc}` : '';
    preview = `📝 Draft email — review before I send to Ron for approval:\n\nTo: ${draft.to}${ccLine}\nSubject: ${draft.subject}\n\n${draft.body}\n\nReply *looks good* (or *se ve bien*) to send for Ron's approval, *cancel* to drop, or tell me what to change.`;
  } else {
    const t = draft.thread;
    const subj = t.subject.toLowerCase().startsWith('re:') ? t.subject : `Re: ${t.subject}`;
    preview = `📝 Draft reply on "${t.subject}" — review before I send to Ron for approval:\n\nTo: ${(t.to_addresses || []).join(', ')}\nSubject: ${subj}\n\n${draft.body}\n\nReply *looks good* (or *se ve bien*) to send for Ron's approval, *cancel* to drop, or tell me what to change.`;
  }
  say(preview).catch(err => console.error('setter-review preview DM failed:', err.message));
  return true;
}

// ─── SHARED FILE HANDLER ──────────────────────────────────────────────────────
async function handleFileMessage(message, say, userId, threadReply = false) {
  const correlation_id = newCorrelationId();
  const fActor = getMemberContext(userId);
  logActivity({
    event_type: 'slack_message',
    event_source: 'slack',
    action: 'inbound',
    actor_user_id: userId,
    actor_name: fActor.displayName,
    channel_id: message.channel,
    thread_ts: message.thread_ts,
    input: { text: (message.text || '').slice(0, 2000) },
    correlation_id,
  });

  const file        = message.files[0];
  const instruction = message.text || null;
  const mimeType    = getFileMimeType(file.name, file.mimetype);

  if (isAudioFile(mimeType, file.name)) {
    const ack = threadReply ? { text: '🎙️ Got the voice note. Transcribing...', thread_ts: message.thread_ts || message.ts } : '🎙️ Got the voice note. Transcribing...';
    await say(ack);
    logActivity({ event_type: 'slack_message', event_source: 'slack', action: 'outbound', channel_id: message.channel, thread_ts: message.thread_ts, output: { text: (typeof ack === 'string' ? ack : ack.text).slice(0, 2000) }, correlation_id });
    try {
      const fileBuffer = await downloadSlackFile(file.url_private);
      const transcript = await transcribeAudio(fileBuffer, file.name);
      if (!transcript || !transcript.trim()) {
        const errMsg = "Couldn't make out anything in that audio. Try again?";
        await say(threadReply ? { text: errMsg, thread_ts: message.thread_ts || message.ts } : errMsg);
        logActivity({ event_type: 'slack_message', event_source: 'slack', action: 'outbound', channel_id: message.channel, thread_ts: message.thread_ts, output: { text: errMsg.slice(0, 2000) }, correlation_id });
        return;
      }
      console.log(`Audio transcribed (${file.name}): ${transcript.substring(0, 100)}...`);
      const transcriptNotice = `_Transcript:_ "${transcript.substring(0, 200)}${transcript.length > 200 ? '...' : ''}"`;
      await say(threadReply ? { text: transcriptNotice, thread_ts: message.thread_ts || message.ts } : transcriptNotice);
      logActivity({ event_type: 'slack_message', event_source: 'slack', action: 'outbound', channel_id: message.channel, thread_ts: message.thread_ts, output: { text: transcriptNotice.slice(0, 2000) }, correlation_id });
      const history = await loadHistory(userId);
      history.push({ role: 'user', content: `[Voice note transcript]: ${transcript}` });
      let reply = await callClaude(history, 3, userId, correlation_id);
      if (!reply || !reply.trim()) reply = await callClaude(history, 2, userId, correlation_id);
      if (!reply || !reply.trim()) return;
      if (handleDraftReply(reply, userId, say, correlation_id)) return;
      await saveMessage(userId, 'user', `[Voice note]: ${transcript}`);
      await saveMessage(userId, 'assistant', reply);
      await say(threadReply ? { text: reply, thread_ts: message.thread_ts || message.ts } : reply);
      logActivity({ event_type: 'slack_message', event_source: 'slack', action: 'outbound', channel_id: message.channel, thread_ts: message.thread_ts, output: { text: String(reply).slice(0, 2000) }, correlation_id });
    } catch (err) {
      console.error('Audio processing error:', err);
      const errMsg = `Had trouble with that audio — ${err.message}`;
      await say(threadReply ? { text: errMsg, thread_ts: message.thread_ts || message.ts } : errMsg);
      logActivity({ event_type: 'slack_message', event_source: 'slack', action: 'outbound', channel_id: message.channel, thread_ts: message.thread_ts, output: { text: errMsg.slice(0, 2000) }, correlation_id });
    }
    return;
  }

  const supported = ['application/pdf','image/png','image/jpeg','image/gif','image/webp'];
  if (!supported.includes(mimeType)) {
    const errMsg = `I can process images (PNG, JPG, GIF, WEBP), PDFs, and audio files. This file type (${mimeType}) isn't supported yet.`;
    await say(threadReply ? { text: errMsg, thread_ts: message.thread_ts || message.ts } : errMsg);
    logActivity({ event_type: 'slack_message', event_source: 'slack', action: 'outbound', channel_id: message.channel, thread_ts: message.thread_ts, output: { text: errMsg.slice(0, 2000) }, correlation_id });
    return;
  }
  const ackMsg = `Got the ${mimeType.includes('pdf') ? 'PDF' : 'image'}. Give me a moment to analyze it...`;
  await say(threadReply ? { text: ackMsg, thread_ts: message.thread_ts || message.ts } : ackMsg);
  logActivity({ event_type: 'slack_message', event_source: 'slack', action: 'outbound', channel_id: message.channel, thread_ts: message.thread_ts, output: { text: ackMsg.slice(0, 2000) }, correlation_id });
  try {
    const fileBuffer = await downloadSlackFile(file.url_private);
    const result     = await processFileWithClaude(fileBuffer, mimeType, instruction, buildRoleSystemPrompt(userId), correlation_id);
    await saveMessage(userId, 'user', `[File: ${file.name}] ${instruction || 'analyze this'}`);
    await saveMessage(userId, 'assistant', result);
    await say(threadReply ? { text: result, thread_ts: message.thread_ts || message.ts } : result);
    logActivity({ event_type: 'slack_message', event_source: 'slack', action: 'outbound', channel_id: message.channel, thread_ts: message.thread_ts, output: { text: String(result).slice(0, 2000) }, correlation_id });
  } catch (err) {
    console.error('File processing error:', err);
    const errMsg = `Had trouble processing that file — ${err.message}`;
    await say(threadReply ? { text: errMsg, thread_ts: message.thread_ts || message.ts } : errMsg);
    logActivity({ event_type: 'slack_message', event_source: 'slack', action: 'outbound', channel_id: message.channel, thread_ts: message.thread_ts, output: { text: errMsg.slice(0, 2000) }, correlation_id });
  }
}

// ─── SLACK HANDLERS ───────────────────────────────────────────────────────────

// ─── THREAD AWARENESS ─────────────────────────────────────────────────────────
// Max used to be blind to Slack threads unless he was @mentioned: the DM handler
// ignored thread_ts entirely, so a reply inside a thread got answered from the
// flat DM history and posted at channel level. On 2026-08-19 Ron asked "is this
// still true?" inside the outcome-escalation thread and got an answer about an
// unrelated conversation happening in the main DM.
//
// These helpers are the one shared thread path used by the DM, channel and
// @mention handlers. The pure ones sit together in this block so
// test/thread-context.test.js can slice them out of the monolith.

const THREAD_ROOT_LIMIT  = 1200;  // the root is usually a Max report — don't gut it
const THREAD_REPLY_LIMIT = 300;
const THREAD_FETCH_LIMIT = 50;
const EVENT_CLAIM_TTL_MS = 60_000;

// A message is Max's if Slack tagged it as a bot post or it came from his user.
function isMaxMessage(m, botUserId) {
  return Boolean(m && (m.bot_id || (botUserId && m.user === botUserId)));
}

// True when Max is already part of a thread — he posted the root or replied in
// it. This is the gate for channels he does not otherwise answer in: he carries
// on his own conversations, he does not barge into other people's.
function isMaxThread(messages, botUserId) {
  return (messages || []).some(m => isMaxMessage(m, botUserId));
}

// Renders a thread as a readable transcript. Senders resolve through the roster
// via `nameFor` — the old inline version printed `user:U0AM…` for everyone
// except the person tagging, which is a large part of why Max could not follow
// a thread with more than two people in it. The root gets a bigger budget than
// the replies because it is normally the report being asked about.
function formatThreadTranscript(messages, opts = {}) {
  const { botUserId, nameFor, rootLimit = THREAD_ROOT_LIMIT, replyLimit = THREAD_REPLY_LIMIT } = opts;
  const label = (id) => (nameFor ? nameFor(id) : `user:${id}`);
  return (messages || []).map((m, i) => {
    const time = new Date(parseFloat(m.ts) * 1000).toLocaleString('en-US', { timeZone: 'America/Costa_Rica', month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit' });
    const sender = isMaxMessage(m, botUserId) ? 'Max' : label(m.user);
    const limit  = i === 0 ? rootLimit : replyLimit;
    const text   = (m.text || '').replace(/<@([A-Z0-9]+)>/g, (_, id) => `@${label(id)}`);
    return `[${time}] ${sender}: ${text.length > limit ? `${text.slice(0, limit)}…` : text}`;
  }).join('\n');
}

// Decides whether a non-DM message is Max's to answer. #ng-pm-agent is his
// channel and behaves as before. Anywhere else he only answers inside a thread
// he is already part of — top-level channel chatter stays none of his business.
function shouldAnswerChannelMessage({ channelName, isThreadReply, maxIsInThread }) {
  if ((channelName || '').includes('ng-pm-agent')) return true;
  return Boolean(isThreadReply && maxIsInThread);
}

// One-reply-per-Slack-event guard. Answering thread replies in every channel
// means an @mention inside a thread now matches both `app_mention` and
// `slack.message`; whichever lands first claims the event and the other becomes
// a no-op. Both produce the same thread-aware in-thread answer, so either
// winning the race is correct.
const _claimedEvents = new Map();
function claimEvent(channel, ts, now = Date.now()) {
  for (const [k, expiry] of _claimedEvents) if (expiry <= now) _claimedEvents.delete(k);
  const key = `${channel}:${ts}`;
  if (_claimedEvents.has(key)) return false;
  _claimedEvents.set(key, now + EVENT_CLAIM_TTL_MS);
  return true;
}
// ─── end pure thread helpers (test/thread-context.test.js slices to here) ─────

// Fetches a thread and builds the context block Max reads before answering.
// `tagged` (an @mention) additionally runs the learning side effects that used
// to live inline in the @mention handler — knowledge save, report-lesson
// extraction, correction capture, client-context extraction. Those stay tied to
// an explicit tag on purpose: two of them are LLM calls, and firing them on
// every thread reply everywhere would multiply cost and manufacture "lessons"
// out of ordinary follow-up questions.
async function buildThreadContext({ channel, threadTs, excludeTs, userId, userText = '', tagged = false, knownChannelName }) {
  const empty = { context: '', maxIsInThread: false, channelName: null, messages: [] };
  if (!threadTs) return empty;
  try {
    const threadResult = await slack.client.conversations.replies({ channel, ts: threadTs, limit: THREAD_FETCH_LIMIT });
    const messages = (threadResult.messages || []).filter(m => m.ts !== excludeTs);
    if (messages.length === 0) return empty;

    const botUserId   = process.env.SLACK_BOT_USER_ID;
    const channelInfo = knownChannelName === undefined
      ? await slack.client.conversations.info({ channel }).catch(() => null)
      : null;
    const channelName = knownChannelName !== undefined ? (knownChannelName || null) : (channelInfo?.channel?.name || null);
    const where       = channelName ? `#${channelName}` : 'this DM';
    const nameFor     = (id) => (id && id === botUserId ? 'Max' : getMemberContext(id).displayName);
    const maxIsInThread = isMaxThread(messages, botUserId);

    let context = `\n\nTHREAD CONTEXT — you are replying inside a Slack thread in ${where}. This thread is the subject of the question and OUTRANKS the recent conversation history, which is a different conversation that happened elsewhere. Answer about this thread. If the thread does not contain what was asked, say so plainly — do not answer from the other conversation.\n${formatThreadTranscript(messages, { botUserId, nameFor })}`;

    if (tagged) {
      const rootMessage = messages[0];
      await upsertKnowledge('process', `thread:${channel}:${threadTs}`,
        `Thread tagged for Max on ${new Date().toLocaleDateString('en-US', { timeZone: 'America/Costa_Rica' })} in #${channelName || 'unknown'}. Last action: "${userText.substring(0, 200)}"`,
        'thread-mention'
      );

      // If the thread root was posted by Max (a report), extract the lesson from
      // the user's feedback before responding — so Max can acknowledge it.
      const isMaxBotPost = rootMessage && !rootMessage.user && rootMessage.bot_id;
      if (isMaxBotPost) {
        try {
          const lesson = await extractAndSaveReportLesson(rootMessage.text || '', userText, channelName || 'unknown channel', userId, newCorrelationId());
          if (lesson) {
            context += `\n\nIMPORTANT: This person is giving feedback on a report Max posted. A lesson has been extracted and saved: "${lesson}". Acknowledge this in your reply — confirm what you learned and that you will apply it to future reports for this channel. Keep it to 1-2 sentences, plain text.`;
          }
        } catch (lessonErr) {
          console.error('Lesson extraction error:', lessonErr.message);
        }
      } else {
        // Thread not rooted in a Max report, but Max may have spoken earlier in
        // it — if this mention corrects him, capture the lesson too.
        const lastMaxMsg = [...messages].reverse().find(m => m.bot_id);
        if (lastMaxMsg) detectAndSaveCorrection(userText, lastMaxMsg.text || '', userId).catch(() => {});
      }

      // Client-specific context from any tagged thread (not just Max bot posts)
      try {
        const clientCtx = await extractClientContext(messages, userText, channelName || 'unknown channel', userId);
        if (clientCtx) {
          const key = `client:${clientCtx.client.toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/(^-|-$)/g, '')}:${new Date().toISOString().slice(0, 10)}`;
          await upsertKnowledge('client', key, clientCtx.context, 'thread-context', userId, 'shared');
          console.log(`Client context saved for "${clientCtx.client}" from #${channelName || 'unknown'}`);
        }
      } catch (ctxErr) {
        console.error('Client context extraction error:', ctxErr.message);
      }
    }

    return { context, maxIsInThread, channelName, messages };
  } catch (err) {
    console.error('Thread context fetch error:', err.message);
    return empty;
  }
}

// DM handler
slack.message(async ({ message, say }) => {
  if (message.bot_id) return;
  if (message.channel_type !== 'im') return;
  // A DM inside a thread is answered *in that thread*. Top-level DMs stay
  // top-level — replying with a thread_ts there would start a thread nobody
  // asked for.
  const dmThreadTs = (message.thread_ts && message.thread_ts !== message.ts) ? message.thread_ts : null;
  const dmSay = (text) => (dmThreadTs ? say({ text, thread_ts: dmThreadTs }) : say(text));
  const isApproval = await checkApproval(message, say, message.user);
  if (isApproval) return;
  const userId = message.user;
  if (!isRosterMember(userId)) { await dmSay("You're not on Max's roster yet — ping Ron and he'll add you."); return; }
  if (!claimEvent(message.channel, message.ts)) return;  // app_mention got there first
  if (message.subtype === 'file_share' && message.files?.length > 0) { await handleFileMessage(message, say, userId, Boolean(dmThreadTs)); return; }
  if (message.subtype) return;
  if (isRateLimited(userId)) { await dmSay('Slow down a bit — you are sending messages too fast. Give me a moment.'); return; }

  // ── Outcome-card thread replies (deterministic — the card knows its call) ──
  let cardThreadHint = '';
  if (message.thread_ts && message.thread_ts !== message.ts) {
    const cardHandled = await handleOutcomeCardThreadReply(message, say);
    if (cardHandled === true) return;
    if (typeof cardHandled === 'string') cardThreadHint = cardHandled;
  }

  // ── Campaign DM commands (deterministic, Ron-only — no LLM needed) ─────────
  if (userId === RON_SLACK_ID && typeof message.text === 'string') {
    const campaignMatch = message.text.match(/^campaign\s*:?\s*(.+)$/i);
    if (campaignMatch) {
      const raw = campaignMatch[1].trim();
      const contactIds = raw.split(/[,\s\n]+/).map(s => s.trim()).filter(s => /^[a-zA-Z0-9]{15,30}$/.test(s));
      if (contactIds.length === 0) {
        await say('No valid GHL contact IDs found. Format: `campaign: <id1>, <id2>, ...`');
        return;
      }
      if (contactIds.length > 50) {
        await say(`Capped at 50 contacts per campaign. You sent ${contactIds.length}. Run again with a smaller batch.`);
        return;
      }
      const camCid = newCorrelationId();
      await say(`Generating drafts for ${contactIds.length} contacts. Posting one DM per draft — react ✅ to send, ❌ to skip.`);
      runRecoverableLeadsCampaign(contactIds, userId, camCid).catch(err => {
        console.error('runRecoverableLeadsCampaign failed:', err);
        slack.client.chat.postMessage({ channel: userId, text: `Campaign run errored: ${err.message}` }).catch(() => {});
      });
      return;
    }

    const reviseMatch = message.text.match(/^revise\s+([a-zA-Z0-9]{15,30})\s*:\s*(.+)$/is);
    if (reviseMatch) {
      const [, contactId, newText] = reviseMatch;
      const trimmed = newText.trim();
      if (trimmed.length < 5) { await say('Revised message is too short. Send `revise <contact_id>: <new text>` with at least 5 chars.'); return; }
      if (trimmed.length > 500) { await say('Revised message is too long (max 500 chars).'); return; }
      const revCid = newCorrelationId();
      const result = await sendCampaignMessage({
        contactId, contactName: null, draftText: trimmed,
        approverSlackId: userId, correlationId: revCid, isRevised: true,
      });
      if (result.ok) {
        await say(`✉️ Sent revised message to contact \`${contactId}\`. Message ID: \`${result.messageId}\``);
      } else {
        await say(`❌ Send failed for \`${contactId}\`: ${result.error}`);
      }
      return;
    }
  }

  // ── Stalled-followup pause/unpause commands (all roster setters) ────────────
  if (typeof message.text === 'string') {
    const pauseMatch = message.text.match(/^pause\s+([a-zA-Z0-9]{15,30})\s*$/i);
    if (pauseMatch) {
      const [, contactId] = pauseMatch;
      try {
        await supabase.from('agent_knowledge').upsert(
          { category: 'setter_pref', key: `pause:${contactId}`, value: userId, visibility: 'shared', source: 'setter_dm', updated_at: new Date().toISOString() },
          { onConflict: 'category,key' }
        );
        await say(`⏸️ Auto-followups paused for contact \`${contactId}\`. DM \`unpause ${contactId}\` to resume.`);
      } catch (err) { await say(`Couldn't save the pause: ${err.message}`); }
      return;
    }
    const unpauseMatch = message.text.match(/^unpause\s+([a-zA-Z0-9]{15,30})\s*$/i);
    if (unpauseMatch) {
      const [, contactId] = unpauseMatch;
      try {
        await supabase.from('agent_knowledge').delete().eq('category', 'setter_pref').eq('key', `pause:${contactId}`);
        await say(`▶️ Auto-followups resumed for contact \`${contactId}\`.`);
      } catch (err) { await say(`Couldn't remove the pause: ${err.message}`); }
      return;
    }
  }
  // ───────────────────────────────────────────────────────────────────────────

  const correlation_id = newCorrelationId();
  const dmCtx = getMemberContext(userId);
  logActivity({
    event_type: 'slack_message',
    event_source: 'slack',
    action: 'inbound',
    actor_user_id: userId,
    actor_name: dmCtx.displayName,
    channel_id: message.channel,
    thread_ts: message.thread_ts,
    input: { text: (message.text || '').slice(0, 2000) },
    correlation_id,
  });
  const history = await loadHistory(userId);
  // Correction learning: if this DM corrects something Max just said, capture
  // the lesson (fire-and-forget — never blocks the reply).
  const lastAssistant = [...history].reverse().find(m => m.role === 'assistant');
  detectAndSaveCorrection(message.text, typeof lastAssistant?.content === 'string' ? lastAssistant.content : '', userId).catch(() => {});
  const threadHint = await getActiveThreadHint(userId);
  const dmThread = await buildThreadContext({ channel: message.channel, threadTs: dmThreadTs, excludeTs: message.ts, userId, userText: message.text || '' });
  const dmHints = cardThreadHint + threadHint;
  history.push({ role: 'user', content: dmThread.context
    ? `${dmThread.context}\n\nMY TASK (what they just asked me, inside that thread): ${message.text || ''}${dmHints}`
    : (message.text || '') + dmHints });
  try {
    let reply = await callClaude(history, 3, userId, correlation_id);
    if (!reply || !reply.trim()) { console.error('Empty reply, retrying for user:', userId); reply = await callClaude(history, 2, userId, correlation_id); }
    if (!reply || !reply.trim()) return;
    if (handleDraftReply(reply, userId, say, correlation_id)) return;
    await saveMessage(userId, 'user', message.text);
    await saveMessage(userId, 'assistant', reply);
    await dmSay(reply);
    logActivity({ event_type: 'slack_message', event_source: 'slack', action: 'outbound', channel_id: message.channel, thread_ts: dmThreadTs, output: { text: String(reply).slice(0, 2000) }, correlation_id });
  } catch (err) { console.error('Claude API error (DM):', err); await dmSay('Got turned around for a second — go ahead and ask again.'); }
});

// (The iClosed booking relay that lived here — threaded setter reveals on
// iClosed's "Strategy call booked" posts — was deleted 2026-08-03. iClosed
// was decommissioned 2026-07-23; booked alerts now come from Make scenario
// 5148796 with different text, so the handler could never match again.)

// @mention handler
slack.event('app_mention', async ({ event, say }) => {
  if (event.bot_id) return;
  const cleanText = event.text.replace(/<@[A-Z0-9]+>/g, '').trim();
  if (!cleanText) return;
  const userId = event.user;
  if (!isRosterMember(userId)) { await say({ text: "You're not on Max's roster yet — ping Ron and he'll add you.", thread_ts: event.thread_ts || event.ts }); return; }

  if (!claimEvent(event.channel, event.ts)) return;

  // If this mention is inside a thread, read the whole thread first.
  const mentionThread = await buildThreadContext({
    channel: event.channel,
    threadTs: (event.thread_ts && event.thread_ts !== event.ts) ? event.thread_ts : null,
    excludeTs: event.ts,
    userId,
    userText: cleanText,
    tagged: true,
  });
  const threadContext = mentionThread.context;

  const history = await loadHistory(userId);
  const fullMessage = threadContext ? `${threadContext}\n\nMY TASK (what I was just tagged to do): ${cleanText}` : cleanText;
  const mentionChannelInfo = await slack.client.conversations.info({ channel: event.channel }).catch(() => null);
  const mentionCid = newCorrelationId();
  const menCtx = getMemberContext(userId);
  logActivity({
    event_type: 'slack_message',
    event_source: 'slack',
    action: 'inbound',
    actor_user_id: userId,
    actor_name: menCtx.displayName,
    channel_id: event.channel,
    channel_name: mentionChannelInfo?.channel?.name,
    thread_ts: event.thread_ts,
    input: { text: (event.text || '').slice(0, 2000) },
    correlation_id: mentionCid,
  });
  history.push({ role: 'user', content: fullMessage });
  try {
    let reply = await callClaude(history, 3, userId, mentionCid);
    if (!reply || !reply.trim()) { console.error('Empty reply on mention, retrying for user:', userId); reply = await callClaude(history, 2, userId, mentionCid); }
    if (!reply || !reply.trim()) return;
    if (handleDraftReply(reply, userId, say, mentionCid)) return;
    await saveMessage(userId, 'user', cleanText);
    await saveMessage(userId, 'assistant', reply);
    await say({ text: reply, thread_ts: event.thread_ts || event.ts });
    logActivity({ event_type: 'slack_message', event_source: 'slack', action: 'outbound', channel_id: event.channel, thread_ts: event.thread_ts || event.ts, output: { text: String(reply).slice(0, 2000) }, correlation_id: mentionCid });
  } catch (err) { console.error('Claude API error (mention):', err); await say({ text: 'Got turned around — try again.', thread_ts: event.thread_ts || event.ts }); }
});

// Channel handler — #ng-pm-agent, plus thread replies anywhere else Max is
// already in the thread (public, private, group DM).
slack.message(async ({ message, say }) => {
  if (message.subtype || message.bot_id) return;
  if (message.channel_type === 'im') return;
  const isThreadReply = Boolean(message.thread_ts && message.thread_ts !== message.ts);
  let channelInfo;
  try { channelInfo = await slack.client.conversations.info({ channel: message.channel }); } catch { return; }
  const channelName = channelInfo.channel?.name || '';
  const inMaxChannel = channelName.includes('ng-pm-agent');
  // Top-level chatter outside his own channel is still none of Max's business —
  // only a thread he is already part of pulls him in.
  if (!inMaxChannel && !isThreadReply) return;

  // Read the thread before deciding anything: outside #ng-pm-agent, Max only
  // answers threads he is already in. This gate must come before checkApproval
  // and the roster rejection so neither fires in a channel he stays out of.
  const chThread = await buildThreadContext({ channel: message.channel, threadTs: isThreadReply ? message.thread_ts : null, excludeTs: message.ts, userId: message.user, userText: message.text || '', knownChannelName: channelName });
  if (!shouldAnswerChannelMessage({ channelName, isThreadReply, maxIsInThread: chThread.maxIsInThread })) return;

  const isApproval = await checkApproval(message, say, message.user);
  if (isApproval) return;
  const userId = message.user;
  if (!isRosterMember(userId)) { await say({ text: "You're not on Max's roster yet — ping Ron and he'll add you.", thread_ts: message.thread_ts || message.ts }); return; }
  if (message.subtype === 'file_share' && message.files?.length > 0) { await handleFileMessage(message, say, userId, true); return; }
  if (message.subtype) return;
  if (isRateLimited(userId)) { await say({ text: 'Slow down a bit — too many messages at once. Give me a moment.', thread_ts: message.thread_ts || message.ts }); return; }

  if (!claimEvent(message.channel, message.ts)) return;
  const chCid = newCorrelationId();
  const chCtx = getMemberContext(userId);
  logActivity({
    event_type: 'slack_message',
    event_source: 'slack',
    action: 'inbound',
    actor_user_id: userId,
    actor_name: chCtx.displayName,
    channel_id: message.channel,
    channel_name: channelName,
    thread_ts: message.thread_ts,
    input: { text: (message.text || '').slice(0, 2000) },
    correlation_id: chCid,
  });
  const history = await loadHistory(userId);
  history.push({ role: 'user', content: chThread.context
    ? `${chThread.context}\n\nMY TASK (what they just asked me, inside that thread): ${message.text || ''}`
    : message.text });
  try {
    let reply = await callClaude(history, 3, userId, chCid);
    if (!reply || !reply.trim()) { console.error('Empty reply on channel, retrying for user:', userId); reply = await callClaude(history, 2, userId, chCid); }
    if (!reply || !reply.trim()) return;
    if (handleDraftReply(reply, userId, say, chCid)) return;
    await saveMessage(userId, 'user', message.text);
    await saveMessage(userId, 'assistant', reply);
    await say({ text: reply, thread_ts: message.thread_ts || message.ts });
    logActivity({ event_type: 'slack_message', event_source: 'slack', action: 'outbound', channel_id: message.channel, thread_ts: message.thread_ts || message.ts, output: { text: String(reply).slice(0, 2000) }, correlation_id: chCid });
  } catch (err) { console.error('Claude API error (channel):', err); }
});

// ─── CRON JOBS ────────────────────────────────────────────────────────────────
// All scheduled jobs run as dynamic tasks loaded from Supabase scheduled_tasks table.
// Internal system functions (nightly learning, portal trends, gap detection, proactive alerts)
// are still wired to their schedules below — these are infrastructure-level and not
// configurable via Slack, so they stay hardcoded.

// ─── SALES CALL PREP ──────────────────────────────────────────────────────────
// Runs hourly Mon–Fri. Queries revops_appointments for calls in the next 3.5–5h,
// fetches GHL conversation history for the prospect, and DMs the assigned closer
// with a prep brief. Deduplicates via agent_knowledge so each call gets one brief.
// closer_id in revops_appointments is stored as the roster email (GHL rows are
// mapped upstream in dash via admin_users; legacy iClosed rows used the same emails).
const CLOSER_SLACK = {
  'jonathan.madriz.neurogrowth@gmail.com': 'U0APYAE0999', // Jonathan
  'jose.neurogrowth@gmail.com':            'U0AMTEKDCPN', // Jose
  'ronny.duarte@neurogrowth.io':           'U05HXGX18H3', // Ron (when he's the closer)
  // Raw GHL user ID fallbacks (unmapped rows)
  'gqymykpddltdxvbkfl2c': 'U0APYAE0999', 'gqYMYkpDDlTdxvBkfl2C': 'U0APYAE0999',
  'izlta0jy5orkymvyitjv': 'U0AMTEKDCPN', 'izLTA0jy5OrKyMvyItjV': 'U0AMTEKDCPN',
  'zogw530idnpofqqnfssc': 'U05HXGX18H3', 'zoGW530iDnPOFqQNfssc': 'U05HXGX18H3', // Ron
};

// (fetchIClosedIntakeForProspect deleted 2026-08-03 — orphaned once call prep
// went GHL-native; iclosed_webhook_deliveries is frozen history.)

// Pull the prospect's GHL booking intake from ghl_webhook_deliveries. The
// qualification answers arrive as ROOT-LEVEL payload keys named with the literal
// Spanish questions (contain "¿" or end with "?"); the booking setter (when a
// setter relayed the webhook) rides in payload.user.
async function fetchGhlIntakeForProspect(prospectId) {
  if (!prospectId) return null;
  try {
    const { data, error } = await portalSupabase
      .from('ghl_webhook_deliveries')
      .select('normalized_event_type, payload, created_at')
      .eq('prospect_id', prospectId)
      .eq('normalized_event_type', 'ghl.call_booked')
      .order('created_at', { ascending: false })
      .limit(5);
    if (error || !data || !data.length) return null;

    const isQuestionKey = (k) => k.includes('¿') || /\?\s*$/.test(k);
    const extractQa = (p) => {
      const qa = [];
      for (const [k, v] of Object.entries(p || {})) {
        if (!isQuestionKey(k)) continue;
        const a = (typeof v === 'string' ? v : '').trim();
        if (a) qa.push({ question: k.trim(), answer: a });
      }
      return qa;
    };
    // Prefer the delivery that actually carries Q&A (workflow config varied early on).
    const withQa = data.find(r => extractQa(r.payload).length);
    const pick = withQa || data[0];
    const p = pick.payload || {};
    const qa = extractQa(p);

    const setterName = [p.user?.firstName, p.user?.lastName].filter(Boolean).join(' ').trim() || null;
    return {
      eventName:  null,
      bookedFrom: null,
      setterName,
      qa,
      timezone:   p['Invitee Timezone'] || p.timezone || null,
      country:    p.country || null,
      phone:      p.phone || null,
    };
  } catch (err) {
    console.error('fetchGhlIntakeForProspect error:', err.message);
    return null;
  }
}

async function fetchGHLConvoForContact(contactId) {
  const locationId = process.env.GHL_LOCATION_ID;
  const apiKey     = process.env.GHL_API_KEY;
  const headers    = { 'Authorization': `Bearer ${apiKey}`, 'Version': '2021-07-28', 'Content-Type': 'application/json' };
  // Get conversations for this contact
  const convoRes  = await fetch(`https://services.leadconnectorhq.com/conversations/search?locationId=${locationId}&contactId=${contactId}&limit=1`, { headers });
  const convoData = await convoRes.json();
  const convoCount = (convoData.conversations || []).length;
  const convoId    = convoData.conversations?.[0]?.id;
  console.log(`[GHL] fetchConvo contactId=${contactId} status=${convoRes.status} conversations=${convoCount} convoId=${convoId || 'none'}`);
  if (!convoId) return null;
  // Get recent messages
  const msgRes  = await fetch(`https://services.leadconnectorhq.com/conversations/${convoId}/messages?limit=12`, { headers });
  const msgData = await msgRes.json();
  const messages = msgData.messages || msgData.messages?.messages || [];
  const msgCount = Array.isArray(messages) ? messages.length : 0;
  console.log(`[GHL] fetchConvo convoId=${convoId} status=${msgRes.status} messages=${msgCount}`);
  return Array.isArray(messages) ? messages : [];
}

async function searchGHLContact(email, name) {
  const locationId = process.env.GHL_LOCATION_ID;
  const apiKey     = process.env.GHL_API_KEY;
  const headers    = { 'Authorization': `Bearer ${apiKey}`, 'Version': '2021-07-28', 'Content-Type': 'application/json' };

  // 1. Exact email match (preferred — most reliable)
  if (email) {
    try {
      const exactRes  = await fetch(`https://services.leadconnectorhq.com/contacts/search/duplicate?locationId=${locationId}&email=${encodeURIComponent(email)}`, { headers });
      if (exactRes.ok) {
        const exactData = await exactRes.json();
        const contact = exactData.contact || exactData.contacts?.[0];
        if (contact) {
          console.log(`[GHL] searchContact strategy=email-exact email=${email} -> ${contact.id}`);
          return contact;
        }
        console.log(`[GHL] searchContact strategy=email-exact email=${email} -> no match`);
      } else {
        console.log(`[GHL] searchContact email-exact failed status=${exactRes.status}`);
      }
    } catch (e) {
      console.log(`[GHL] searchContact email-exact threw: ${e.message}`);
    }
  }

  // 2. Fuzzy query fallback (email-or-name)
  const query = email || name;
  if (!query) {
    console.log('[GHL] searchContact called with no email or name');
    return null;
  }
  const res  = await fetch(`https://services.leadconnectorhq.com/contacts/?locationId=${locationId}&query=${encodeURIComponent(query)}&limit=5`, { headers });
  const data = await res.json();
  const contacts = data.contacts || [];
  console.log(`[GHL] searchContact strategy=fuzzy query="${query}" status=${res.status} returned=${contacts.length}`);

  // Prefer email match within fuzzy results before falling to first
  if (email) {
    const emailLower = email.toLowerCase();
    const emailMatch = contacts.find(c => (c.email || '').toLowerCase() === emailLower);
    if (emailMatch) {
      console.log(`[GHL] searchContact fuzzy result matched email -> ${emailMatch.id}`);
      return emailMatch;
    }
  }
  return contacts[0] || null;
}

// Determine whether a booked call came from the Appointment Setting Pipeline
// (setter-booked) or VSL self-booking. Truth source = GHL opportunity in the
// setter pipeline, NOT contact.assignedTo (which is unreliable).
// Returns { source: 'appointment-setting'|'vsl', setter: string|null }.
async function resolveSetterForContact(email, name) {
  try {
    const contact = await searchGHLContact(email, name);
    if (!contact) return { source: 'vsl', setter: null };

    const locationId = process.env.GHL_LOCATION_ID;
    const apiKey     = process.env.GHL_API_KEY;
    const headers    = { 'Authorization': `Bearer ${apiKey}`, 'Version': '2021-07-28', 'Content-Type': 'application/json' };
    const setterPipelineIds = (process.env.GHL_SETTER_PIPELINE_IDS || 'KH1IQuaN8aNB1lfRpvP4')
      .split(',').map(s => s.trim()).filter(Boolean);

    const oppsRes  = await fetch(
      `https://services.leadconnectorhq.com/opportunities/search?location_id=${locationId}&contact_id=${contact.id}`,
      { headers }
    );
    const oppsData = oppsRes.ok ? await oppsRes.json() : { opportunities: [] };
    const opps = (oppsData.opportunities || []).filter(o => setterPipelineIds.includes(o.pipelineId));
    if (!opps.length) return { source: 'vsl', setter: null };

    // Most recent opp wins
    opps.sort((a, b) => new Date(b.updatedAt || b.createdAt || 0) - new Date(a.updatedAt || a.createdAt || 0));
    const opp = opps[0];
    const assigneeId = opp.assignedTo || contact.assignedTo || null;
    const resolved = assigneeId ? resolveSalesMember(assigneeId) : null;
    // resolveSalesMember returns the raw id when unmapped — treat that as unresolved
    const setter = (resolved && resolved !== assigneeId) ? resolved : null;
    return { source: 'appointment-setting', setter };
  } catch (err) {
    console.error('resolveSetterForContact error:', err.message);
    return { source: 'vsl', setter: null };
  }
}

async function runSalesCallPrep(_correlationId) {
  console.log('Running sales call prep...');
  try {
    const now       = Date.now();
    const windowMin = now + (3.5 * 60 * 60 * 1000); // 3.5h from now
    const windowMax = now + (5   * 60 * 60 * 1000); // 5h from now

    // Query upcoming appointments in the window (flywheel-only)
    const { data: upcomingRaw, error } = await portalSupabase
      .from('revops_appointments')
      .select(`
        id, closer_id, setter_id, scheduled_start, meeting_type, booked_at, iclosed_call_id, ghl_appointment_id, source, prospect_id, qualification_snapshot,
        prospect:prospect_id ( full_name, company, email, lead_source )
      `)
      .gte('scheduled_start', new Date(windowMin).toISOString())
      .lte('scheduled_start', new Date(windowMax).toISOString())
      .is('attended', null); // only upcoming (not yet attended/no-showed)

    if (error) throw error;
    const excludeIds = await getNonFlywheelCallIds();
    const upcoming = filterFlywheelAppts(upcomingRaw, excludeIds);
    if (!upcoming.length) {
      console.log('Sales call prep: no calls in window.');
      return;
    }

    for (const appt of upcoming) {
      // Dedup — skip if brief already sent for this appointment
      const prepKey = `call-prep-${appt.id}`;
      const { data: existing } = await supabase.from('agent_knowledge').select('id').eq('key', prepKey).limit(1);
      if (existing && existing.length) {
        console.log(`Call prep already sent for appointment ${appt.id}`);
        continue;
      }

      const prospect    = appt.prospect || {};
      const prospectId   = appt.prospect_id;
      const prospectName = prospect.full_name || 'Unknown prospect';
      const company     = prospect.company   || '';
      const email       = prospect.email     || '';
      const leadSource  = prospect.lead_source || '';
      const closerName  = resolveSalesMember(appt.closer_id);
      const closerSlack = CLOSER_SLACK[appt.closer_id] || CLOSER_SLACK[(appt.closer_id || '').toLowerCase()];
      const callTime    = formatICTime(appt.scheduled_start, { weekday: 'short', month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit' });
      const hoursOut    = Math.round((new Date(appt.scheduled_start).getTime() - now) / (1000 * 60 * 60) * 10) / 10;

      // GHL conversation lookup + setter resolution.
      // convoSection stays null until something real is found — no placeholder
      // text lives here, so a brief with nothing to show simply omits the
      // section (see brief assembly below) instead of printing a dead line.
      let convoSection = null;
      let sourceLine = null;
      let intake = null; // lazy — fetched at most once, reused by the fallback below

      // Native setter attribution: GHL records who booked the appointment
      // (createdBy → setter_id upstream in dash). NULL means widget self-booked
      // or closer-booked, by design. (The legacy iClosed opportunity-owner +
      // booking-payload heuristics that lived here were deleted 2026-08-03 —
      // every upcoming appointment is GHL-sourced since the 2026-07-23 cutover,
      // so the legacy arm could never fire again.)
      if (appt.setter_id) {
        sourceLine = `👤 Booked by: ${resolveSalesMember(appt.setter_id)}`;
      } else {
        sourceLine = `🟢 Source: self-booked (widget or closer-booked, no setter)`;
      }

      try {
        const ghlContact = await searchGHLContact(email, prospectName);
        if (ghlContact) {
          const messages = await fetchGHLConvoForContact(ghlContact.id);
          if (messages && messages.length) {
            const msgLines = messages
              .filter(m => m.body || m.text)
              .slice(-8) // last 8 messages
              .map(m => {
                const dir  = m.direction === 'inbound' ? '← Prospect' : '→ Team';
                const time = m.dateAdded ? new Date(m.dateAdded).toLocaleString('en-US', { timeZone: 'America/Costa_Rica', month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit' }) : '';
                return `[${time}] ${dir}: ${(m.body || m.text || '').substring(0, 200)}`;
              });
            convoSection = msgLines.join('\n');
          }
        }
      } catch (ghlErr) {
        console.error(`GHL lookup failed for ${prospectName}:`, ghlErr.message);
      }

      // No GHL conversation found — fall back to the booking intake (the
      // prospect's own Q&A answers) from ghl_webhook_deliveries. If that's
      // empty too, the section is simply left out of the brief. (The iClosed
      // intake fallback was deleted 2026-08-03 — the frozen
      // iclosed_webhook_deliveries table can never hold intake for an
      // upcoming call, so it was a guaranteed-null query per brief.)
      if (!convoSection) {
        if (!intake) intake = await fetchGhlIntakeForProspect(prospectId);
        if (intake && (intake.eventName || intake.qa.length)) {
          const lines = ['📋 No GHL message history — here\'s what they shared when booking:'];
          if (intake.eventName) lines.push(`• Event: ${intake.eventName}`);
          for (const { question, answer } of intake.qa) {
            lines.push(`• ${question.replace(/[.?!:]+$/, '')}: "${answer}"`);
          }
          const meta = [];
          if (intake.bookedFrom) meta.push(`Booked via: ${intake.bookedFrom.toLowerCase().replace(/_/g, ' ')}`);
          if (intake.timezone)   meta.push(`Timezone: ${intake.timezone}`);
          if (intake.phone)      meta.push(`Phone: ${intake.phone}`);
          if (meta.length) lines.push(`• ${meta.join(' · ')}`);
          convoSection = lines.join('\n');
        }
      }

      // REVI intel — prior scored calls for this prospect (repeat prospects) plus
      // the closer's own current coaching focus. Cross-schema read of REVI's data;
      // non-fatal by design: the brief must still send if revi.* is unreachable.
      // Closer-facing surface → closer_draft_text only, never Ron-only teardowns.
      let reviSection = null;
      try {
        const reviLines = [];
        const priorCalls = email ? await reviFindCallsByProspect(email, 3) : [];
        for (const pc of priorCalls) {
          const when = pc.call_date ? formatICTime(pc.call_date, { month: 'short', day: 'numeric' }) : '';
          const sig = pc.prospect_signals || {};
          const bits = [`${when} with ${pc.closer && pc.closer.full_name ? pc.closer.full_name.split(' ')[0] : 'us'}`];
          if (pc.overall_score != null)       bits.push(`scored ${pc.overall_score}`);
          if (pc.deal_outcome && pc.deal_outcome !== 'pending') bits.push(`outcome: ${pc.deal_outcome}`);
          if (sig.buying_signal_strength)     bits.push(`buying signal ${sig.buying_signal_strength}`);
          if (sig.objection_type)             bits.push(`objection: ${sig.objection_type}`);
          if (sig.stated_timeline)            bits.push(`timeline: ${sig.stated_timeline}`);
          if (sig.decision_maker_status)      bits.push(`DM status: ${sig.decision_maker_status}`);
          reviLines.push(`• Prior call ${bits.join(' · ')}${pc.coaching_doc_url ? `\n  ${pc.coaching_doc_url}` : ''}`);
        }
        if (closerSlack) {
          // First-name match — Max's display names and REVI's full_name differ in
          // suffixes; closers are first-name-unique (Jose, Jonathan).
          const [coachSum] = await reviGetCoachingSummary((closerName || '').split(' ')[0], 14, true);
          if (coachSum && coachSum.latest_coaching_focus) {
            reviLines.push(`🎯 Your current coaching focus (${coachSum.latest_coaching_date}): ${coachSum.latest_coaching_focus.slice(0, 250)}`);
          }
        }
        if (reviLines.length) reviSection = reviLines.join('\n');
      } catch (reviErr) {
        console.error(`REVI intel lookup failed for ${prospectName}:`, reviErr.message);
      }

      // Lead quality score — parsed by dash from the GHL booking survey
      // (qualification_snapshot.parsed, 0-100, threshold 50). Only rendered
      // when present; thin bookings without a survey simply omit the line.
      const leadScore = appt.qualification_snapshot?.parsed?.lead_quality_score;
      const leadScoreLine = (leadScore !== null && leadScore !== undefined)
        ? `🎯 Lead quality score: ${leadScore}/100`
        : '';

      // Build the brief — GHL section only appears when there's real content.
      const briefLines = [
        `📞 *CALL PREP — ${prospectName}* | in ${hoursOut}h (${callTime} CR)`,
        company     ? `🏢 Company: ${company}` : '',
        email       ? `📧 Email: ${email}` : '',
        leadSource  ? `🔗 Lead source: ${leadSource}` : '',
        leadScoreLine,
        sourceLine,
        ``,
      ];
      if (convoSection) {
        briefLines.push(`*GHL CONVERSATION HISTORY:*`, convoSection, ``);
      }
      if (reviSection) {
        briefLines.push(`*REVI INTEL:*`, reviSection, ``);
      }
      briefLines.push(`Good luck on the call ${closerName.split(' ')[0]}. Let me know if you need anything before you jump on.`);
      const brief = briefLines.filter(l => l !== null && l !== undefined).join('\n');

      if (!closerSlack) {
        // Closer not mapped — send to Ron
        await slack.client.chat.postMessage({ channel: RON_SLACK_ID, text: `⚠️ Call prep for ${prospectName} — closer ID "${appt.closer_id}" has no Slack mapping. Brief:\n\n${brief}` });
      } else {
        await slack.client.chat.postMessage({ channel: closerSlack, text: brief });
        console.log(`Call prep DM sent to ${closerName} for ${prospectName} (${callTime})`);
      }

      // Mark as sent in knowledge base
      await upsertKnowledge('intel', prepKey, `Call prep sent to ${closerName} for ${prospectName} on ${callTime}`, 'system');
    }
  } catch (err) {
    console.error('Sales call prep error:', err.message);
    await slack.client.chat.postMessage({ channel: RON_SLACK_ID, text: `⚠️ Sales call prep cron failed: ${err.message}` });
  }
}

// ─── FULFILLMENT MORNING STANDUP ──────────────────────────────────────────────
// Fires 9:00 AM CR Mon–Fri. DMs each fulfillment team member with their
// specific priorities for the day — no meeting needed.
async function runFulfillmentStandup(_correlationId) {
  console.log('Running fulfillment morning standup DMs...');
  try {
    const now = Date.now();
    const today = new Date().toLocaleDateString('en-US', { weekday: 'long', month: 'long', day: 'numeric', timeZone: 'America/Costa_Rica' });

    // ── Fetch all active client data once ──────────────────────────────────────
    const { data: dashboards } = await portalSupabase
      .from('client_dashboards')
      .select('id, client_name, email, customer_status, customer_type, created_at, stabilization_started_at, linkedin_handler')
      .eq('is_active', true);

    const { data: templates } = await portalSupabase
      .from('customer_activity_templates')
      .select('id, title, order_index');
    const tMap = {};
    (templates || []).forEach(t => { tMap[t.id] = t.title; });

    const { data: allActivities } = await portalSupabase
      .from('customer_activities')
      .select('customer_id, template_id, status, assigned_to, completed_at, notes')
      .in('status', ['phase_1', 'phase_2', 'blocked']);

    const actsByClient = {};
    (allActivities || []).forEach(a => {
      (actsByClient[a.customer_id] = actsByClient[a.customer_id] || []).push(a);
    });

    const { data: phase0 } = await portalSupabase
      .from('v_phase0_fulfillment')
      .select('email, first_name, last_name, company, phase0_step, days_in_phase0')
      .order('days_in_phase0', { ascending: false });

    // Day anchors: the status-filtered allActivities fetch above excludes completed
    // rows, so fetch completed activities separately (one batched query) to find
    // each client's activation call — the true 14-day SLA anchor. Previously this
    // anchored on created_at only, which skewed Day-7/Day-14 flags for any client
    // whose portal row predates their activation call.
    const { data: anchorActs } = await portalSupabase
      .from('customer_activities')
      .select('customer_id, template_id, completed_at')
      .not('completed_at', 'is', null);
    const anchorsByClient = {};
    (anchorActs || []).forEach(a => { (anchorsByClient[a.customer_id] = anchorsByClient[a.customer_id] || []).push(a); });
    const activationFor = await loadActivationDates();
    function getDayAnchor(dash) {
      return resolveDayAnchor(dash, anchorsByClient[dash.id] || [], id => tMap[id], activationFor(dash));
    }
    function getDayCount(dash) {
      return getDayAnchor(dash).daysSince;
    }

    // Helper: phase label
    const phaseLabel = { phase_1: 'Phase 1', phase_2: 'Phase 2', phase_3: 'Phase 3', live: 'Live', blocked: 'BLOCKED' };

    const clients = dashboards || [];
    const blocked   = clients.filter(d => d.customer_status === 'blocked');
    const phase1    = clients.filter(d => d.customer_status === 'phase_1');
    const phase2    = clients.filter(d => d.customer_status === 'phase_2');
    const phase3    = clients.filter(d => d.customer_status === 'phase_3');
    const hitting14Today = clients.filter(d => getDayCount(d) === 14);
    const hitting7Today  = clients.filter(d => getDayCount(d) === 7);

    // ── DM Josue — pipeline owner, activation calls, overall ops ──────────────
    const josueSnap = await getYesterdayStandupSnapshot('josue');
    const josueLines = [`Good morning Josue! Here's your ${today} ops brief:\n`];

    const blockedNames    = blocked.map(d => d.client_name);
    const hitting14Names  = hitting14Today.map(d => d.client_name);
    const hitting7Names   = hitting7Today.map(d => d.client_name);
    const needsCallItems  = (phase0||[]).filter(r => r.phase0_step === '4_awaiting_activation_call');
    const handoffItems    = (phase0||[]).filter(r => r.phase0_step === '5_ready_for_handoff');
    const needsCallNames  = needsCallItems.map(r => [r.first_name, r.last_name].filter(Boolean).join(' ') || r.email);
    const handoffNames    = handoffItems.map(r => [r.first_name, r.last_name].filter(Boolean).join(' ') || r.email);

    const dBlocked   = diffStandupList(blockedNames,   josueSnap?.blocked);
    const dH14       = diffStandupList(hitting14Names, josueSnap?.hitting14);
    const dH7        = diffStandupList(hitting7Names,  josueSnap?.hitting7);
    const dNeedsCall = diffStandupList(needsCallNames, josueSnap?.phase0Needing);
    const dHandoff   = diffStandupList(handoffNames,   josueSnap?.phase0Handoff);

    josueLines.push(...renderDelta('Blocked', dBlocked.new, dBlocked.resolved, dBlocked.unchanged, name => {
      const d = blocked.find(x => x.client_name === name);
      const acts = d ? (actsByClient[d.id] || []).filter(a => a.status === 'blocked').map(a => tMap[a.template_id] || 'Unknown').join(', ') : '';
      const anchor = d ? getDayAnchor(d) : null;
      return `${name} — Day ${anchor?.daysSince ?? '?'}${anchor?.anchorDate ? ` ${anchor.label} (${anchor.anchorDate})` : ''}${acts ? ` | blocked on: ${acts}` : ''}`;
    }));
    josueLines.push(...renderDelta('Day 14 TODAY', dH14.new, dH14.resolved, dH14.unchanged, name => {
      const d = hitting14Today.find(x => x.client_name === name);
      const anchor = d ? getDayAnchor(d) : null;
      return `${name} (${d ? phaseLabel[d.customer_status] || d.customer_status : ''})${anchor?.anchorDate ? ` — ${anchor.label} ${anchor.anchorDate}` : ''} — must launch today`;
    }));
    josueLines.push(...renderDelta('Day 7 at-risk', dH7.new, dH7.resolved, dH7.unchanged, name => {
      const d = hitting7Today.find(x => x.client_name === name);
      return `${name} (${d ? phaseLabel[d.customer_status] || d.customer_status : ''})`;
    }));
    josueLines.push(...renderDelta('Activation calls needed', dNeedsCall.new, dNeedsCall.resolved, dNeedsCall.unchanged, name => {
      const r = needsCallItems.find(x => ([x.first_name, x.last_name].filter(Boolean).join(' ') || x.email) === name);
      return `${name}${r?.company ? ` (${r.company})` : ''} — Day ${r?.days_in_phase0 ?? '?'}`;
    }));
    josueLines.push(...renderDelta('Ready for Phase 1 handoff', dHandoff.new, dHandoff.resolved, dHandoff.unchanged, name => {
      const r = handoffItems.find(x => ([x.first_name, x.last_name].filter(Boolean).join(' ') || x.email) === name);
      return `${name}${r?.company ? ` (${r.company})` : ''}`;
    }));

    josueLines.push(`📊 *Pipeline:* ${phase1.length} Phase 1 | ${phase2.length} Phase 2 | ${phase3.length} Phase 3 | ${blocked.length} Blocked | ${(phase0||[]).length} Phase 0`);
    josueLines.push(`\nAnything blocking you today? Flag it here and I'll help unblock.`);

    for (const id of (slackIdsByRole('tech_ops').length ? slackIdsByRole('tech_ops') : ['U08ABBFNGUW'])) {
      await slack.client.chat.postMessage({ channel: id, text: josueLines.join('\n') });
    }
    await saveStandupSnapshot('josue', { blocked: blockedNames, hitting14: hitting14Names, hitting7: hitting7Names, phase0Needing: needsCallNames, phase0Handoff: handoffNames, counts: { phase1: phase1.length, phase2: phase2.length, phase3: phase3.length, blocked: blocked.length, phase0: (phase0||[]).length } });
    console.log('Standup DM sent to tech_ops role');

    // ── DM fulfillment role — delivery docs, Phase 1 ──────────────────────────
    const valeriaSnap = await getYesterdayStandupSnapshot('valeria');
    const valeriaLines = [];

    const phase1Names   = phase1.map(d => d.client_name);
    const stalledP1     = phase1.filter(d => (getDayCount(d) || 0) >= 4).map(d => d.client_name);
    const vBlockedNames = blockedNames;

    const dP1      = diffStandupList(phase1Names,   valeriaSnap?.phase1Clients?.map(c => c.name));
    const dStalledP1 = diffStandupList(stalledP1,   valeriaSnap?.stalledGe4);
    const dVBlocked  = diffStandupList(vBlockedNames, valeriaSnap?.blocked);

    if (phase1.length === 0 && !valeriaSnap?.phase1Clients?.length) {
      valeriaLines.push(`✅ No clients in Phase 1 right now.\n`);
    } else {
      valeriaLines.push(...renderDelta('Phase 1 clients', dP1.new, dP1.resolved, dP1.unchanged, name => {
        const d = phase1.find(x => x.client_name === name);
        if (!d) return name;
        const acts = actsByClient[d.id] || [];
        const pending = acts.filter(a => a.status === 'phase_1').slice(0, 2).map(a => tMap[a.template_id] || 'Unknown').join(', ');
        const day = getDayCount(d);
        const urgency = day >= 10 ? ' ⚠️ urgent' : day >= 7 ? ' 👀 watch' : day >= 4 ? ' ⚡ stalled' : '';
        return `${name} — Day ${day}${urgency}${pending ? ` | next: ${pending}` : ''}`;
      }));
      valeriaLines.push(...renderDelta('Phase 1 stalled (>= Day 4)', dStalledP1.new, dStalledP1.resolved, [], name => `${name} — no activity in 4+ days`));
    }

    valeriaLines.push(...renderDelta('Blocked', dVBlocked.new, dVBlocked.resolved, dVBlocked.unchanged, name => `${name} — check if docs are holding this up`));
    valeriaLines.push(`Any docs blocked or waiting on client input? Let Josue know so he can follow up.`);

    for (const id of (slackIdsByRole('fulfillment').length ? slackIdsByRole('fulfillment') : ['U09Q3BXJ18B'])) {
      const recipientName = getMemberContext(id).name;
      const greeting = `Good morning ${recipientName}! Here's your ${today} delivery brief:\n`;
      await slack.client.chat.postMessage({ channel: id, text: [greeting, ...valeriaLines].join('\n') });
    }
    await saveStandupSnapshot('valeria', { phase1Clients: phase1.map(d => ({ name: d.client_name, day: getDayCount(d) })), stalledGe4: stalledP1, blocked: vBlockedNames });
    console.log('Standup DM sent to fulfillment role');

    // ── DM Felipe — campaigns, Phase 2 ───────────────────────────────────────
    const felipeSnap = await getYesterdayStandupSnapshot('felipe');
    const felipeLines = [`Good morning Felipe! Here's your ${today} campaign brief:\n`];

    const phase2Names  = phase2.map(d => d.client_name);
    const stalledP2    = phase2.filter(d => (getDayCount(d) || 0) >= 4).map(d => d.client_name);
    const phase3Names  = phase3.map(d => d.client_name);

    const dP2       = diffStandupList(phase2Names,  felipeSnap?.phase2Clients?.map(c => c.name));
    const dStalledP2 = diffStandupList(stalledP2,   felipeSnap?.stalledGe4);
    const dP3       = diffStandupList(phase3Names,  felipeSnap?.phase3Clients?.map(c => c.name));

    if (phase2.length === 0 && !felipeSnap?.phase2Clients?.length) {
      felipeLines.push(`✅ No clients in Phase 2 right now.\n`);
    } else {
      felipeLines.push(...renderDelta('Phase 2 clients', dP2.new, dP2.resolved, dP2.unchanged, name => {
        const d = phase2.find(x => x.client_name === name);
        if (!d) return name;
        const acts = actsByClient[d.id] || [];
        const pending = acts.filter(a => a.status === 'phase_2').slice(0, 2).map(a => tMap[a.template_id] || 'Unknown').join(', ');
        const day = getDayCount(d);
        const urgency = day >= 10 ? ' ⚠️ urgent' : day >= 7 ? ' 👀 watch' : day >= 4 ? ' ⚡ stalled' : '';
        return `${name} — Day ${day}${urgency}${pending ? ` | next: ${pending}` : ''}`;
      }));
      felipeLines.push(...renderDelta('Phase 2 stalled (>= Day 4)', dStalledP2.new, dStalledP2.resolved, [], name => `${name} — no activity in 4+ days`));
    }

    if (phase3.length || felipeSnap?.phase3Clients?.length) {
      felipeLines.push(...renderDelta('Phase 3 stabilization', dP3.new, dP3.resolved, dP3.unchanged, name => {
        const d = phase3.find(x => x.client_name === name);
        if (!d) return name;
        const anchor = d.stabilization_started_at ? new Date(d.stabilization_started_at) : new Date(d.created_at);
        const stabDay = Math.floor((now - anchor.getTime()) / (1000 * 60 * 60 * 24));
        return `${name} — Stabilization Day ${stabDay}`;
      }));
    }

    felipeLines.push(`Any campaign setup blocked or waiting on Valeria's docs? Flag it in #ng-fullfillment-ops so Josue can sequence it.`);

    for (const id of (slackIdsByRole('campaigns').length ? slackIdsByRole('campaigns') : ['U09TNMVML3F'])) {
      await slack.client.chat.postMessage({ channel: id, text: felipeLines.join('\n') });
    }
    await saveStandupSnapshot('felipe', { phase2Clients: phase2.map(d => ({ name: d.client_name, day: getDayCount(d) })), stalledGe4: stalledP2, phase3Clients: phase3.map(d => ({ name: d.client_name })) });
    console.log('Standup DM sent to Felipe');

    // ── DM client success — Phase 0 owner, SLA enforcer, Phase 3 client success
    const taniaSnap = await getYesterdayStandupSnapshot('tania');
    const taniaLines = [`Here's your ${today} client success brief:\n`];

    const { data: phase0All } = await portalSupabase
      .from('v_phase0_fulfillment')
      .select('email, first_name, last_name, company, phase0_step, days_in_phase0')
      .order('phase0_step', { ascending: true })
      .order('days_in_phase0', { ascending: false });

    const p0StepLabels = {
      '1_awaiting_signup':          'awaiting portal signup',
      '2_awaiting_terms':           'awaiting T&C acceptance',
      '3_awaiting_form':            'awaiting onboarding form',
      '4_awaiting_activation_call': 'awaiting activation call booking',
      '5_ready_for_handoff':        'ready for Phase 1 → Josue to kick off',
    };

    // Phase 0 delta
    const p0Names = (phase0All || []).map(r => [r.first_name, r.last_name].filter(Boolean).join(' ') || r.email);
    const dP0 = diffStandupList(p0Names, taniaSnap?.phase0Clients);
    if (phase0All?.length || taniaSnap?.phase0Clients?.length) {
      taniaLines.push(...renderDelta('Phase 0 pipeline', dP0.new, dP0.resolved, dP0.unchanged, name => {
        const r = (phase0All || []).find(x => ([x.first_name, x.last_name].filter(Boolean).join(' ') || x.email) === name);
        if (!r) return name;
        const days = r.days_in_phase0 ?? 0;
        const flag = days >= 14 ? ' 🔴 OVERDUE' : days >= 7 ? ' ⚠️ at risk' : '';
        return `${name}${r.company ? ` (${r.company})` : ''} — ${p0StepLabels[r.phase0_step] || r.phase0_step} | Day ${days}${flag}`;
      }));
    } else {
      taniaLines.push(`📋 *Phase 0:* No clients in pre-portal onboarding.\n`);
    }

    // SLA watch delta
    const slaDueToday  = clients.filter(d => getDayCount(d) === 14);
    const slaOverdue   = clients.filter(d => (getDayCount(d) || 0) > 14 && ['phase_1','phase_2'].includes(d.customer_status));
    const slaNames     = [...slaDueToday, ...slaOverdue].map(d => d.client_name);
    const dSla = diffStandupList(slaNames, taniaSnap?.slaWatch);
    if (slaNames.length || taniaSnap?.slaWatch?.length) {
      taniaLines.push(...renderDelta('SLA watch', dSla.new, dSla.resolved, dSla.unchanged, name => {
        const due  = slaDueToday.find(d => d.client_name === name);
        const over = slaOverdue.find(d => d.client_name === name);
        if (due)  return `${name} — Day 14 TODAY | ${phaseLabel[due.customer_status] || due.customer_status} | must activate by EOD`;
        if (over) {
          const anchor = getDayAnchor(over);
          return `${name} — Day ${anchor.daysSince}${anchor.anchorDate ? ` ${anchor.label} (${anchor.anchorDate})` : ''} | ${phaseLabel[over.customer_status] || over.customer_status} | past 14-day SLA`;
        }
        return name;
      }));
    } else {
      taniaLines.push(`✅ *SLA watch:* No clients at or past the 14-day activation deadline today.\n`);
    }

    // Phase 3 stabilization delta
    const phase3Clients = clients.filter(d => d.customer_status === 'phase_3');
    const p3StabNames   = phase3Clients.map(d => d.client_name);
    const dP3Stab = diffStandupList(p3StabNames, taniaSnap?.phase3StabClients);
    if (phase3Clients.length || taniaSnap?.phase3StabClients?.length) {
      taniaLines.push(...renderDelta('Phase 3 stabilization', dP3Stab.new, dP3Stab.resolved, dP3Stab.unchanged, name => {
        const d = phase3Clients.find(x => x.client_name === name);
        if (!d) return name;
        const anchor  = d.stabilization_started_at ? new Date(d.stabilization_started_at) : new Date(d.created_at);
        const stabDay = Math.floor((now - anchor.getTime()) / (1000 * 60 * 60 * 24));
        const flag    = stabDay >= 20 ? ' 🔴 1:1 overdue — schedule now' : stabDay >= 18 ? ' 📅 1:1 due in ~2 days' : '';
        return `${name} — Stabilization Day ${stabDay}${flag}`;
      }));
    } else {
      taniaLines.push(`📈 *Phase 3:* No clients in stabilization.\n`);
    }

    // Blocked delta
    const dTBlocked = diffStandupList(blockedNames, taniaSnap?.blocked);
    taniaLines.push(...renderDelta('Blocked — needs client-side outreach', dTBlocked.new, dTBlocked.resolved, dTBlocked.unchanged, name => {
      const d = blocked.find(x => x.client_name === name);
      return `${name}${d ? ` — Day ${getDayCount(d)}` : ''}`;
    }));

    taniaLines.push(`Anything you need from me to move any of these forward? I can draft client emails, schedule 1:1 reminders, or pull activity details on any client.`);

    for (const id of slackIdsByRole('client_success')) {
      const firstName = getMemberContext(id).name;
      const personalized = [`Good morning ${firstName}! ${taniaLines[0]}`, ...taniaLines.slice(1)].join('\n');
      await slack.client.chat.postMessage({ channel: id, text: personalized });
    }
    await saveStandupSnapshot('tania', { phase0Clients: p0Names, slaWatch: slaNames, phase3StabClients: p3StabNames, blocked: blockedNames });
    console.log('Standup DM sent to client_success');

    console.log('Fulfillment standup DMs complete.');
  } catch (err) {
    console.error('Fulfillment standup error:', err.message);
    await slack.client.chat.postMessage({ channel: RON_SLACK_ID, text: `⚠️ Fulfillment standup cron failed: ${err.message}` });
  }
}

// ─── SALES MORNING STANDUP ────────────────────────────────────────────────────
// Fires 9:00 AM CR Mon–Fri. DMs each setter with GHL pipeline context and each
// closer with today's call deck + unlogged outcomes. No approval flow.
async function runSalesStandup(_correlationId) {
  console.log('Running sales morning standup DMs...');
  try {
    const now      = Date.now();
    const today    = new Date().toLocaleDateString('en-US', { weekday: 'long', month: 'long', day: 'numeric', timeZone: 'America/Costa_Rica' });
    // ── Fetch GHL conversations for setter brief filtering ─────────────────
    let ghlConvos = [];
    try {
      const locationId = process.env.GHL_LOCATION_ID;
      const apiKey     = process.env.GHL_API_KEY;
      const ghlRes  = await fetch(`https://services.leadconnectorhq.com/conversations/search?locationId=${locationId}&limit=100`, {
        headers: { 'Authorization': `Bearer ${apiKey}`, 'Version': '2021-07-28', 'Content-Type': 'application/json' }
      });
      const ghlData = await ghlRes.json();
      ghlConvos = ghlData.conversations || [];
    } catch (ghlErr) {
      console.error('Sales standup — GHL fetch error:', ghlErr.message);
    }

    const fortyEightHMs = 48 * 60 * 60 * 1000;
    const twentyFourHMs = 24 * 60 * 60 * 1000;

    const needsFollowUp = ghlConvos.filter(c =>
      c.lastMessageDirection === 'inbound' && (now - c.lastMessageDate) > fortyEightHMs
    );
    const newLeads = ghlConvos.filter(c =>
      (now - c.lastMessageDate) < twentyFourHMs && c.unreadCount > 0
    );

    // ── DM each setter ─────────────────────────────────────────────────────
    const setters = [
      { slackId: 'U0B1S1UMH9P', name: 'Oscar' },
      { slackId: 'U0B16P6DQ2F', name: 'William' },
      { slackId: 'U0BFA4SRVQC', name: 'Sebastian' },
    ];

    // Yesterday + 7-day setter stats, outcome-derived from GHL-native
    // appointments and setter_claims. The EOD self-report tables this used to
    // read were retired at the 2026-07-23 cutover (frozen history, 0 new rows)
    // and were rendering "0" for every number here. Non-fatal: if the stats
    // lookup fails, the DM sends without the stats block.
    const crDateStr = d => d.toLocaleDateString('en-CA', { timeZone: 'America/Costa_Rica' });
    const yStr      = crDateStr(new Date(now - 24 * 60 * 60 * 1000));
    const yStartIso = `${yStr}T06:00:00.000Z`; // CR midnight (UTC-6)
    const yEndIso   = new Date(Date.parse(yStartIso) + 24 * 60 * 60 * 1000).toISOString();

    let setterYesterday = null, setterWeek = null;
    try {
      setterYesterday = await getSetterWeeklyStats(yStartIso, yEndIso);
      setterWeek      = await getSetterWeeklyStats(new Date(now - 7 * 24 * 60 * 60 * 1000).toISOString(), new Date(now).toISOString());
    } catch (statsErr) {
      console.error('Sales standup — setter stats failed:', statsErr.message);
    }
    const sumField = (stats, f) => stats ? Object.values(stats).reduce((s, r) => s + (r[f] || 0), 0) : 0;
    const totalScheduled = sumField(setterYesterday, 'calls_booked');
    const totalClaimed   = sumField(setterYesterday, 'leads_claimed');
    const weeklyScheduled = sumField(setterWeek, 'calls_booked');

    const setterLessons = await getReportLessons('sales-standup-setter');
    const setterLessonNote = setterLessons.length
      ? `[Corrections applied from feedback]\n${setterLessons.map(l => `• ${l.value}`).join('\n')}\n\n`
      : '';

    for (const setter of setters) {
      try {
        const lines = [`${setterLessonNote}Good morning ${setter.name}! Here's your setter brief for ${today}:\n`];

        // Yesterday stats — omitted entirely when the stats lookup failed
        if (setterYesterday) {
          lines.push(`📊 Yesterday (team): ${totalScheduled} calls booked | ${totalClaimed} leads claimed`);
          lines.push(`📈 Last 7 days: ${weeklyScheduled} calls booked total`);
          lines.push('');
        }

        // Needs follow-up
        if (needsFollowUp.length) {
          lines.push(`🔥 Needs your reply (${needsFollowUp.length} prospects waiting >48h):`);
          needsFollowUp.slice(0, 10).forEach(c => {
            const name    = c.contactName || c.fullName || 'Unknown';
            const preview = (c.lastMessageBody || '').substring(0, 80);
            const daysAgo = Math.floor((now - c.lastMessageDate) / (24 * 60 * 60 * 1000));
            lines.push(`• ${name} | last: "${preview}" (${daysAgo}d ago)`);
          });
          lines.push('');
        }

        // New leads
        if (newLeads.length) {
          lines.push(`📥 New leads to work (${newLeads.length} unread, last 24h):`);
          newLeads.slice(0, 10).forEach(c => {
            const name    = c.contactName || c.fullName || 'Unknown';
            const preview = (c.lastMessageBody || '').substring(0, 80);
            lines.push(`• ${name} | "${preview}"`);
          });
          lines.push('');
        }

        lines.push('See something off? Thread on this message and tag @Max with the correction.');
        await slack.client.chat.postMessage({ channel: setter.slackId, text: lines.join('\n') });
        console.log(`Sales standup DM sent to setter ${setter.name}`);
      } catch (setterErr) {
        console.error(`Sales standup — DM to ${setter.name} failed:`, setterErr.message);
      }
    }

    // ── DM each closer ─────────────────────────────────────────────────────
    const todayStart = new Date(); todayStart.setHours(0, 0, 0, 0);
    const todayEnd   = new Date(); todayEnd.setHours(23, 59, 59, 999);
    const todayStartISO = todayStart.toISOString();
    const todayEndISO   = todayEnd.toISOString();

    // Today's appointments (flywheel-only — excludes partner-consulting 1:1s)
    const { data: todayCallsRaw } = await portalSupabase
      .from('revops_appointments')
      .select('id, closer_id, scheduled_start, iclosed_call_id, ghl_appointment_id, prospect:prospect_id(full_name)')
      .gte('scheduled_start', todayStartISO)
      .lte('scheduled_start', todayEndISO)
      .order('scheduled_start', { ascending: true });
    const excludeIds = await getNonFlywheelCallIds();
    const todayCalls = filterFlywheelAppts(todayCallsRaw, excludeIds);

    // Yesterday's closer stats — outcome-derived (same truth as the weekly
    // leaderboard). The retired closer EOD table this used to read had 0 new
    // rows since the cutover, so every closer got "0 calls | 0 closes" daily.
    let closerYesterday = null;
    try {
      closerYesterday = await getCloserWeeklyStats(yStartIso, yEndIso);
    } catch (statsErr) {
      console.error('Sales standup — closer stats failed:', statsErr.message);
    }

    const closers = Object.entries(CLOSER_SLACK)
      .filter(([email, slackId]) => slackId && email.includes('@') && slackId !== RON_SLACK_ID)
      .map(([email, slackId]) => ({ email, slackId, name: resolveSalesMember(email) }));

    const closerLessons = await getReportLessons('sales-standup-closer');
    const closerLessonNote = closerLessons.length
      ? `[Corrections applied from feedback]\n${closerLessons.map(l => `• ${l.value}`).join('\n')}\n\n`
      : '';

    for (const closer of closers) {
      try {
        const cs            = closerYesterday ? (closerYesterday[closer.name] || null) : null;
        const heldCalls     = cs ? cs.attended : 0;
        const closes        = cs ? cs.sold     : 0;
        const noShows       = cs ? cs.no_shows : 0;
        const pendingCt     = cs ? cs.pending  : 0;
        const closeRatePct  = heldCalls > 0 ? Math.round((closes / heldCalls) * 100) : 0;

        const myTodayCalls = (todayCalls || []).filter(a => a.closer_id === closer.email);

        const lines = [`${closerLessonNote}Good morning ${closer.name.split(' ')[0]}! Here's your closer brief for ${today}:\n`];

        // Yesterday stats — omitted when the stats lookup failed; "awaiting
        // outcome" keeps a 0-held day with pending calls from reading as dead.
        if (closerYesterday) {
          const pendingStr = pendingCt ? ` | ${pendingCt} awaiting outcome` : '';
          lines.push(`📊 Yesterday: ${heldCalls} calls held | ${closes} closes | ${closeRatePct}% close rate | ${noShows} no-shows${pendingStr}`);
          lines.push('');
        }

        // Today on deck
        if (myTodayCalls.length) {
          lines.push(`📞 Today on deck (${myTodayCalls.length} calls):`);
          myTodayCalls.forEach(a => {
            const pName  = a.prospect?.full_name || 'Unknown';
            const timeStr = formatICTime(a.scheduled_start, { hour: '2-digit', minute: '2-digit' });
            lines.push(`• ${pName} — ${timeStr} CR time`);
          });
          lines.push('');
        }

        lines.push('See something off? Thread on this message and tag @Max with the correction.');
        await slack.client.chat.postMessage({ channel: closer.slackId, text: lines.join('\n') });
        console.log(`Sales standup DM sent to closer ${closer.name}`);
      } catch (closerErr) {
        console.error(`Sales standup — DM to ${closer.name} failed:`, closerErr.message);
      }
    }

    console.log('Sales standup DMs complete.');
  } catch (err) {
    console.error('Sales standup error:', err.message);
    await slack.client.chat.postMessage({ channel: RON_SLACK_ID, text: `⚠️ Sales standup cron failed: ${err.message}` });
  }
}

// ─── OUTCOME WRITE CONTRACT (mirrored from dash) ─────────────────────────────
// Duplicated contract #2 with dash.neurogrowth.io (like the 36h REVI join in
// the scorecard): mirrors src/lib/integrations/revops-ingest/status-map.ts +
// outcome-map.ts. If dash changes RANK, the terminal set, or outcome→status
// targets, change BOTH places. Validated empirically during the Aug-12 replay.
const OUTCOME_STATUS_RANK = {
  prospect: 10, qualified: 20, appointment_booked: 30, nurture: 35,
  appointment_held: 40, won: 100, lost: 100, disqualified: 100, converted: 100, merged: 100,
};
const OUTCOME_TERMINAL_STATUSES = new Set(['won', 'lost', 'disqualified', 'converted', 'merged']);
const OUTCOME_TO_STATUS = {
  won: 'won', lost: 'lost', follow_up: 'nurture', nurture: 'nurture',
  disqualified: 'disqualified', no_show: 'appointment_held', rescheduled: 'appointment_booked',
};
// Status to write on the prospect when `outcome` is logged, or null for no
// change. Never regresses rank; never leaves a terminal status for a
// non-terminal one (mirrors mergeProspectStatus + prospectStatusFromOutcome).
function nextProspectStatusForOutcome(outcome, current) {
  const incoming = OUTCOME_TO_STATUS[outcome];
  if (!incoming || incoming === current) return null;
  if (OUTCOME_TERMINAL_STATUSES.has(current) && !OUTCOME_TERMINAL_STATUSES.has(incoming)) return null;
  if (OUTCOME_TERMINAL_STATUSES.has(incoming)) return incoming;
  if ((OUTCOME_STATUS_RANK[incoming] || 0) > (OUTCOME_STATUS_RANK[current] || 0)) return incoming;
  return null;
}

// ─── GHL VOCABULARY CONTRACT ─────────────────────────────────────────────────
// Closers act on the GHL opportunity card, so Max must speak its stage names —
// never the internal enum. The two pipelines are NOT symmetric: VSL merges
// Lost and No Fit into one stage, so on a VSL call Max must not offer a
// distinction the closer physically cannot set. Stage ids from
// ghl_pipelines_list (verified 2026-08-18); the Appointment Setting ids are the
// same ones already listed in APPT_PIPELINE_STAGE_LABELS.
const GHL_PIPELINE = {
  APPT_SETTING: 'KH1IQuaN8aNB1lfRpvP4',
  VSL:          '7KU0NBdMhhVifszmT9jo',
};
const GHL_OUTCOME_STAGES = {
  [GHL_PIPELINE.APPT_SETTING]: {
    follow_up:    { id: '63d30181-4ec0-4daa-8832-a8eebe1afbeb', label: 'Open Deal' },
    won:          { id: 'a6c8ecb1-a9b5-46a5-8683-f6a9720cfcc9', label: 'Closed' },
    disqualified: { id: '49cd4227-204c-4052-af94-78b961e96fab', label: 'No Fit' },
    lost:         { id: '8eff3fbb-3cc0-474f-bbe7-df4704e0a668', label: 'Lost' },
    no_show:      { id: '6432cf1d-a07f-4276-8b85-77de2e57a512', label: 'No show / Rescheduling' },
  },
  [GHL_PIPELINE.VSL]: {
    follow_up:    { id: '1134d60f-d2c3-43d1-81bd-6c113b46b693', label: 'Follow up - Open Deal' },
    won:          { id: '8bf0b768-b9f8-481c-b423-48893976033c', label: 'Won / Closed' },
    // Both map to the SAME stage — VSL has no Lost-vs-No-Fit distinction.
    disqualified: { id: '2efc59ef-2fd2-4370-aed4-ed79480c07c6', label: 'Lost / No Fit' },
    lost:         { id: '2efc59ef-2fd2-4370-aed4-ed79480c07c6', label: 'Lost / No Fit' },
    no_show:      { id: 'd83e3213-e24b-40c4-b42c-7d0eb69ea37d', label: 'No-Show / Rescheduling' },
  },
};
// The GHL stage for an outcome, or null when the pipeline is unknown (never
// guess a stage id — a wrong PUT moves a real card).
function outcomeStageFor(pipelineId, outcome) {
  return (GHL_OUTCOME_STAGES[pipelineId] || {})[outcome] || null;
}
// Stages that represent a finished deal. A card a human already closed out is
// never reopened by an automated move.
const GHL_TERMINAL_STAGE_IDS = new Set(
  Object.values(GHL_OUTCOME_STAGES).flatMap(s => [s.won.id, s.disqualified.id, s.lost.id]),
);
// The distinct actions a closer can be offered on this pipeline. On VSL,
// lost and disqualified collapse to one button because they are one stage.
// no_show is included: it is the one attendance answer Max cannot derive, and
// folding it into the outcome card is what lets Paso 1 disappear entirely.
function outcomeActionsFor(pipelineId) {
  const stages = GHL_OUTCOME_STAGES[pipelineId] || GHL_OUTCOME_STAGES[GHL_PIPELINE.APPT_SETTING];
  const seen = new Set();
  return ['follow_up', 'lost', 'disqualified', 'no_show'].filter((o) => {
    const s = stages[o];
    if (!s || seen.has(s.id)) return false;
    seen.add(s.id);
    return true;
  }).map(o => ({ outcome: o, label: stages[o].label, say: OUTCOME_REPLY_TOKEN[o] }));
}
// What the closer types. Kept short and stable across pipelines — the stage
// LABEL can be "No show / Rescheduling", which nobody is going to type.
const OUTCOME_REPLY_TOKEN = {
  follow_up: 'open deal', lost: 'lost', disqualified: 'no fit', no_show: 'no show', won: 'won',
};

// Parse a closer's typed reply on an outcome card into {outcome, revenue}.
// Deterministic on purpose: a reply in a card's thread is an answer to THAT
// card, so no LLM should be guessing which prospect it is about. Accepts the
// GHL words the card offers plus the legacy aliases. Returns null when the
// text is conversational (falls through to the LLM with the card as context).
function parseOutcomeReply(text) {
  const t = String(text || '').toLowerCase().replace(/\s+/g, ' ').trim();
  if (!t || t.length > 80) return null; // long texts are conversation, not commands
  const won = t.match(/^won\b[\s:$]*([\d.,]+)?\s*(k)?\s*$/i);
  if (won) {
    let revenue = null;
    if (won[1]) {
      revenue = Number(won[1].replace(/,/g, ''));
      if (won[2]) revenue *= 1000;
      if (!Number.isFinite(revenue) || revenue <= 0) revenue = null;
    }
    return { outcome: 'won', revenue };
  }
  if (/^(open deal|follow ?up|nurture)$/.test(t))            return { outcome: 'follow_up', revenue: null };
  if (/^(no ?fit|dq|disqualified?|lost ?\/ ?no ?fit)$/.test(t)) return { outcome: 'disqualified', revenue: null };
  if (/^(no ?-?show(ed)?)$/.test(t))                          return { outcome: 'no_show', revenue: null };
  if (/^lost$/.test(t))                                       return { outcome: 'lost', revenue: null };
  return null;
}

// ─── REVI EVIDENCE → OUTCOME PROPOSAL ────────────────────────────────────────
// REVI's `deal_outcome` column is 4-valued and 118 of 130 calls sit in two of
// them (stall 91, pending 27), so reading it alone collapses ~70% of calls to
// "follow up". The real signal is coaching_json.deal.status (alive/dead) plus
// icp_fit, which is what separates a No Fit from a Lost.
// prospect_signals is an object on 129 of 130 rows and a double-encoded JSON
// string on one — parse defensively rather than silently rendering nothing.
function parseReviSignals(raw) {
  let v = raw;
  for (let i = 0; i < 2 && typeof v === 'string'; i += 1) {
    try { v = JSON.parse(v); } catch (_) { return {}; }
  }
  if (Array.isArray(v)) v = v[0];
  return (v && typeof v === 'object') ? v : {};
}

const REVI_LOW_ICP = /^\s*(bajo|low)\b/i;

// Tier 1 — the rule tier. Only these earn a one-tap ✅; everything softer is
// shown as a read the closer must type. `won` is NEVER proposable: a verbal
// close is not a payment, so the closer types `won <amount>` themselves.
// Returns { outcome, confidence, wonHint, revenueHint, rule }.
function computeOutcomeProposal(evidence = {}) {
  const rec = evidence.recording;
  const dealStatus = String(evidence.dealStatus || '').toLowerCase();
  const dealOutcome = String(rec?.dealOutcome || '').toLowerCase();
  const funnelClass = String(evidence.funnelClass || '').toLowerCase();
  const cash = Number(evidence.cashCollected);
  // GHL stage evidence stands on its own — the closer moved that card by hand,
  // so it is worth reading even when REVI never saw the call.

  // A close signal from ANY source is a hint, never a proposal.
  if (dealOutcome === 'won' || funnelClass === 'won' || Number.isFinite(cash) && cash > 0) {
    return {
      outcome: null, confidence: 'high', wonHint: true,
      revenueHint: Number.isFinite(cash) && cash > 0 ? cash : null,
      rule: funnelClass === 'won' ? 'ghl_stage_won' : 'revi_won',
    };
  }
  // GHL's own pipeline already says the deal died — the strongest evidence there is.
  if (funnelClass === 'lost') {
    return { outcome: 'lost', confidence: 'high', wonHint: false, revenueHint: null, rule: 'ghl_stage_lost' };
  }
  // The closer moved the card to "No show / Rescheduling" themselves — good
  // evidence, but that stage deliberately merges two different answers, and a
  // reschedule is NOT a no-show. Propose it, make them confirm by typing.
  if (funnelClass === 'noshow') {
    return { outcome: 'no_show', confidence: 'medium', wonHint: false, revenueHint: null, rule: 'ghl_stage_noshow' };
  }
  if (dealStatus === 'dead') {
    return REVI_LOW_ICP.test(String(evidence.icpFit || ''))
      ? { outcome: 'disqualified', confidence: 'high', wonHint: false, revenueHint: null, rule: 'revi_dead_low_icp' }
      : { outcome: 'lost',         confidence: 'high', wonHint: false, revenueHint: null, rule: 'revi_dead' };
  }
  if (dealStatus === 'alive') {
    // The safe outcome: non-terminal, and nextProspectStatusForOutcome refuses
    // to regress or to leave a terminal status, so a wrong tap costs little.
    return { outcome: 'follow_up', confidence: 'high', wonHint: false, revenueHint: null, rule: 'revi_alive' };
  }
  // A recording exists but REVI has no deal read — still worth a card, but the
  // closer types the answer.
  if (rec) return { outcome: null, confidence: 'low', wonHint: false, revenueHint: null, rule: 'recording_only' };
  return { outcome: null, confidence: 'none', wonHint: false, revenueHint: null, rule: 'no_evidence' };
}

// Renders one outcome card. Pure — callers pass pre-formatted strings — so the
// exact copy a closer sees is asserted in tests. `pipelineId` decides the
// vocabulary: the card must never name a stage this prospect's card does not
// have (VSL has no separate Lost vs No Fit).
function buildOutcomeCardText({ prospectName, whenStr, heldDays, rec, proposal, funnel, pipelineId, nudgeCount }) {
  const stages = GHL_OUTCOME_STAGES[pipelineId] || GHL_OUTCOME_STAGES[GHL_PIPELINE.APPT_SETTING];
  const lines = [`📋 *${prospectName}* — ${whenStr} CR — held ${heldDays}d ago${nudgeCount > 1 ? ` · nudged ${nudgeCount}×` : ''}`];

  if (rec) {
    const dur = rec.durationMin ? `${rec.durationMin} min` : 'recorded';
    const score = rec.score != null ? `, score ${rec.score}/100` : '';
    lines.push(`✅ Marked *Showed* automatically — REVI recorded ${dur}${score}${rec.url ? ` <${rec.url}|↗>` : ''}`);
  } else {
    lines.push('❔ No REVI recording found for this call.');
  }

  const sig = rec?.signals || {};
  const evidence = [rec?.dealRecovery, sig.objection_type, sig.stated_timeline]
    .map(s => String(s || '').trim()).filter(Boolean)[0];
  if (evidence) lines.push(`🧠 ${truncateOneLine(evidence, 340)}`);
  const sigBits = [
    sig.buying_signal_strength && `buying signal: ${sig.buying_signal_strength}`,
    rec?.icpFit && `ICP fit: ${truncateOneLine(rec.icpFit, 90)}`,
  ].filter(Boolean);
  if (sigBits.length) lines.push(`🔎 ${sigBits.join(' · ')}`);

  if (funnel?.to_stage_name) {
    lines.push(`📊 GHL: ${funnel.to_stage_name}${funnel.detected_on ? ` since ${funnel.detected_on}` : ''}`);
  }

  // Actions. `won` is never a reaction — the closer types the real amount.
  const wonLabel = stages.won.label;
  const alt = outcomeActionsFor(pipelineId)
    .filter(a => a.outcome !== proposal.outcome)
    .map(a => `\`${a.say}\``)
    .join(' / ');
  if (proposal.wonHint) {
    const amt = proposal.revenueHint ? ` (REVI heard $${proposal.revenueHint})` : '';
    lines.push('');
    lines.push(`🎉 This sounded like a close${amt} — I never log *${wonLabel}* on my own.`);
    lines.push(`→ Reply \`won <amount>\` once the money is real · or ${alt} if it landed differently.`);
  } else if (proposal.outcome && proposal.confidence === 'high') {
    const target = stages[proposal.outcome];
    lines.push('');
    lines.push(`→ ✅ move to *${target.label}* · ❌ dismiss · \`won <amount>\` · or ${alt}`);
  } else if (proposal.outcome) {
    // Good evidence, but not good enough for one tap — show the read and make
    // the closer type it, so an ambiguous stage can't rubber-stamp a metric.
    const target = stages[proposal.outcome];
    lines.push('');
    lines.push(`→ Looks like *${target.label}*, but I'm not certain enough to log it on a tap.`);
    lines.push(`   Reply \`${OUTCOME_REPLY_TOKEN[proposal.outcome]}\` to confirm · \`won <amount>\` · or ${alt}`);
  } else {
    lines.push('');
    lines.push(`→ I can't tell from here. Reply \`won <amount>\` · ${alt || `\`${OUTCOME_REPLY_TOKEN.follow_up}\``}`);
  }
  return lines.join('\n');
}

// Collapse a multi-line REVI narrative into one readable Slack line. REVI's
// coaching_json survives a round of JSON escaping, so the text arrives with
// LITERAL backslash-n sequences as well as real newlines — strip both, or the
// card renders "...jueves.\n\nPlan de recuperación:" verbatim to the closer.
function truncateOneLine(s, max) {
  const flat = String(s || '')
    .replace(/\\r\\n|\\n|\\r/g, ' ')
    .replace(/\s*[\r\n]+\s*/g, ' ')
    .replace(/\s{2,}/g, ' ')
    .trim();
  return flat.length > max ? `${flat.slice(0, max - 1).trimEnd()}…` : flat;
}

// ─── REVI PROSPECT NOTE (GHL) ────────────────────────────────────────────────
// The same read that powers the closer's outcome card, written as a note on
// the GHL contact so the WHOLE sales team sees it where they already work —
// setters place their pre-call notes on the same surface. Pure builder; the
// sweep that posts it lives with the other crons. Spanish on purpose: REVI's
// narratives are Spanish and so is the team.
function buildReviProspectNote(rec, { callDateStr } = {}) {
  if (!rec) return null;
  const lines = ['🧠 REVI — lectura de la llamada'];
  const meta = [
    callDateStr && `📅 ${callDateStr}`,
    rec.durationMin && `${rec.durationMin} min`,
    rec.score != null && `score ${rec.score}/100`,
  ].filter(Boolean).join(' · ');
  if (meta) lines.push(meta);

  const st = String(rec.dealStatus || '').toLowerCase();
  if (st) lines.push(`Estado del deal: ${st === 'alive' ? 'VIVO' : st === 'dead' ? 'MUERTO' : st}`);

  const sig = rec.signals || {};
  if (sig.buying_signal_strength) lines.push(`Señal de compra: ${sig.buying_signal_strength}`);
  if (sig.objection_type)      lines.push(`Objeción: ${truncateOneLine(sig.objection_type, 300)}`);
  if (sig.stated_timeline)     lines.push(`Timeline: ${truncateOneLine(sig.stated_timeline, 200)}`);
  if (sig.stated_budget_fit)   lines.push(`Presupuesto: ${truncateOneLine(sig.stated_budget_fit, 250)}`);
  if (sig.decision_maker_status) lines.push(`Decisor: ${truncateOneLine(sig.decision_maker_status, 250)}`);
  if (rec.icpFit)              lines.push(`ICP fit: ${truncateOneLine(rec.icpFit, 200)}`);
  if (rec.dealRecovery)        lines.push(`\nPróximos pasos:\n${truncateOneLine(rec.dealRecovery, 900)}`);
  if (rec.url)                 lines.push(`\n🎙 Grabación: ${rec.url}`);
  lines.push('\n(Generado por REVI a partir de la grabación — verificar antes de citar al prospecto.)');
  return lines.join('\n');
}

// Posts one REVI note per scored call, once ever (agent_knowledge
// `revi-note:{fathom_recording_id}`). Sweeps recent closer_call_scores and
// resolves the GHL contact via the portal prospect's email. Ships in dry-run:
// REVI_NOTES_MODE=live arms it, and is the kill switch.
// GHL's own contact lookup by email. The portal's ghl_contact_id was only
// backfilled for GHL-native prospects, so pre-cutover rows have a real GHL
// contact that the portal simply doesn't know about. Returns null on anything
// unexpected — a missing contact must skip, never guess.
async function ghlFindContactByEmail(email) {
  const em = String(email || '').trim();
  if (!em) return null;
  try {
    const res = await ghlFetch(
      `https://services.leadconnectorhq.com/contacts/search/duplicate?locationId=${process.env.GHL_LOCATION_ID}&email=${encodeURIComponent(em)}`,
      { headers: { 'Authorization': `Bearer ${process.env.GHL_API_KEY}`, 'Version': '2021-07-28' } },
      { label: 'contact-by-email' },
    );
    if (!res.ok) return null;
    return (await res.json())?.contact?.id || null;
  } catch (_) { return null; }
}

async function runReviProspectNotesSync(_correlationId) {
  const dryRun = process.env.REVI_NOTES_MODE !== 'live';
  console.log(`Running REVI prospect-notes sync (${dryRun ? 'DRY RUN' : 'LIVE'})...`);
  // Lookback is a knob so a BACKFILL is just a wider run of this same proven
  // path — no second code path to get subtly different. Cap + throttle keep a
  // wide run from hammering GHL; the dedup marker makes it resumable, so a
  // capped run simply drains over the following days.
  const lookbackDays = Math.max(1, Number(process.env.REVI_NOTES_LOOKBACK_DAYS) || 7);
  const maxPerRun    = Math.max(1, Number(process.env.REVI_NOTES_MAX_PER_RUN) || 40);
  const tally = { candidates: 0, posted: 0, noContact: 0, failed: 0, capped: 0 };
  try {
    const sinceIso = new Date(Date.now() - lookbackDays * 24 * 3600 * 1000).toISOString();
    const { data: scores } = await reviSupabase
      .from('closer_call_scores')
      .select('fathom_recording_id, prospect_email, prospect_name, call_date, duration_min, overall_score, recording_url, prospect_signals, coaching_json')
      .gte('call_date', sinceIso);
    // Newest first: on a capped backfill the most recent (most useful) calls
    // land first rather than starting from April.
    const rows = (scores || [])
      .filter(s => s.fathom_recording_id && s.prospect_email)
      .sort((a, b) => String(b.call_date).localeCompare(String(a.call_date)));
    if (!rows.length) { console.log('REVI prospect-notes: nothing scored in window.'); return tally; }
    if (lookbackDays !== 7) console.log(`REVI prospect-notes: BACKFILL mode — ${lookbackDays}d lookback, ${rows.length} scored call(s), cap ${maxPerRun}/run.`);

    for (const s of rows) {
      const noteKey = `revi-note:${s.fathom_recording_id}`;
      const { data: done } = await supabase.from('agent_knowledge').select('value').eq('key', noteKey).limit(1);
      if (done && done.length) continue;
      if (tally.posted >= maxPerRun) { tally.capped += 1; continue; }
      tally.candidates += 1;

      const { data: prospects } = await portalSupabase
        .from('revops_prospects')
        .select('id, ghl_contact_id, full_name')
        .ilike('email', s.prospect_email)
        .limit(1);
      // 92 of 127 pre-cutover prospects have no ghl_contact_id in the portal
      // (it was only backfilled for GHL-native rows), but GHL itself can find
      // them by email — so ask GHL rather than skipping a real contact.
      let contactId = prospects?.[0]?.ghl_contact_id || null;
      if (!contactId) contactId = await ghlFindContactByEmail(s.prospect_email);
      if (!contactId) {
        tally.noContact += 1;
        console.log(`REVI prospect-notes: no GHL contact for ${s.prospect_email} — skipped.`);
        continue;
      }

      // Bind the note to the call it describes. GHL has no notes endpoint on
      // opportunities (verified 404) — notes live on the contact and the
      // opportunity view surfaces them — but a note CAN relate to an
      // appointment, which is the shape GHL itself writes when someone adds a
      // note from the appointment page. Best-effort: no appointment match still
      // posts a contact-only note.
      let apptId = null;
      if (prospects?.[0]?.id) {
        const { data: appts } = await portalSupabase
          .from('revops_appointments')
          .select('ghl_appointment_id, scheduled_start')
          .eq('prospect_id', prospects[0].id);
        apptId = nearestAppointmentToCall(appts, Date.parse(s.call_date));
      }

      const coaching = s.coaching_json || {};
      const deal = typeof coaching.deal === 'string'
        ? (() => { try { return JSON.parse(coaching.deal); } catch (_) { return {}; } })()
        : (coaching.deal || {});
      const note = buildReviProspectNote({
        durationMin: s.duration_min == null ? null : Math.round(Number(s.duration_min)),
        score: s.overall_score == null ? null : Number(s.overall_score),
        url: s.recording_url || null,
        signals: parseReviSignals(s.prospect_signals),
        dealStatus: deal.status || null,
        dealRecovery: deal.recovery || null,
        icpFit: coaching.icp_fit || null,
      }, { callDateStr: formatICTime(s.call_date, { month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit' }) });
      if (!note) continue;

      if (dryRun) {
        console.log(`[dry-run] would post REVI note for ${s.prospect_name || s.prospect_email} (${note.length} chars) on contact ${contactId}${apptId ? ` + appointment ${apptId}` : ' (no appointment match — contact only)'}`);
        tally.posted += 1;
        continue;
      }
      try {
        const res = await ghlFetch(`https://services.leadconnectorhq.com/contacts/${contactId}/notes`, {
          method: 'POST',
          headers: { 'Authorization': `Bearer ${process.env.GHL_API_KEY}`, 'Version': '2021-07-28', 'Content-Type': 'application/json' },
          body: JSON.stringify({
            body: note,
            relations: [
              ...(apptId ? [{ objectKey: 'appointment', recordId: apptId }] : []),
              { objectKey: 'contact', recordId: contactId },
            ],
          }),
        }, { label: `note ${contactId}` });
        if (!res.ok) throw new Error(`note POST → ${res.status}: ${(await res.text()).slice(0, 150)}`);
        await upsertKnowledge('process', noteKey, `posted|${new Date().toISOString().slice(0, 10)}|${contactId}${apptId ? `|${apptId}` : ''}`, 'revi-note');
        tally.posted += 1;
        console.log(`REVI note posted for ${s.prospect_name || s.prospect_email}`);
        await new Promise(r => setTimeout(r, 250)); // be kind to GHL on a wide backfill
      } catch (postErr) {
        tally.failed += 1;
        console.error(`REVI note for ${s.prospect_email} failed:`, postErr.message);
      }
    }
    const capNote = tally.capped ? ` — ${tally.capped} left for the next run (cap ${maxPerRun})` : '';
    const verdict = tally.failed
      ? `⚠️ ISSUES — candidates ${tally.candidates}, posted ${tally.posted}, no-contact ${tally.noContact}, failures ${tally.failed}${capNote}`
      : `✅ ALL GREEN — candidates ${tally.candidates}, posted ${tally.posted}, no-contact ${tally.noContact}${capNote}`;
    console.log(`REVI prospect-notes sync complete. ${verdict}`);
    return tally;
  } catch (err) {
    console.error('REVI prospect-notes sync error:', err.message);
    return tally;
  }
}

// ─── WON HANDOFF NOTES (fulfillment CRM) ─────────────────────────────────────
// When a deal is WON, fulfillment onboards the client with zero context from
// the closing call. REVI already scored that call (coaching_json); this sweep
// condenses it into a Spanish handoff note and lands it in the portal CRM via
// ONE guarded RPC (ng_apply_won_handoff_note): the outcome row's notes, an
// append-only closing_call_summary activity, and — once fulfillment provisions
// the client — customer_closing_data.internal_notes, the field they already
// read during onboarding. Keys off revops_sales_outcomes rather than the
// moment of logging, so it catches wons from BOTH writers (closer-typed via
// Max and GHL webhook via dash). Ships in dry-run: WON_HANDOFF_MODE=live arms
// it (also the kill switch).

const WON_HANDOFF_MARKER = 'RESUMEN LLAMADA DE CIERRE (REVI';
const WON_HANDOFF_WINDOW_DAYS = 30;        // wons older than this age out of the sweep
const WON_HANDOFF_RECORDING_WAIT_DAYS = 7; // stop waiting for a REVI recording after this

// Pure step decision over the agent_knowledge marker `won-handoff:{appointment_id}`.
// States: placed / already_placed / gave_up are terminal; prospect_only means
// the note exists but the client record didn't yet; anything else re-attempts
// the recording match.
function decideWonHandoffStep(markerValue) {
  const state = String(markerValue || '').split('|')[0];
  if (state === 'placed' || state === 'already_placed' || state === 'gave_up') return 'skip';
  if (state === 'prospect_only') return 'retry_placement';
  return 'match';
}

// Assembles the final CRM note. The header doubles as the idempotency marker
// the RPC checks before appending to the shared internal_notes field — every
// note MUST contain WON_HANDOFF_MARKER or the RPC rejects it.
function buildWonHandoffNote({ summaryText, callDateStr, closerName, recordingUrl }) {
  const body = String(summaryText || '').trim();
  if (!body) return null;
  const lines = [
    '--- ' + WON_HANDOFF_MARKER + ' · Max) — ' + (callDateStr || 'fecha desconocida') + ' · closer ' + (closerName || 'desconocido') + ' ---',
    body,
  ];
  if (recordingUrl) lines.push('🎙 Grabación: ' + recordingUrl);
  return lines.join('\n');
}

// The prompt is a pure function of its own so its rules can be asserted in
// test/won-handoff-notes.test.js. A prompt reachable only through a live API
// call is a prompt that drifts silently — the first live run (2026-08-19)
// broke two rules at once and nothing caught it but a human reading the notes.
//
// Two things the first run got wrong, both encoded below:
//
// 1. TENSE. REVI scored 3 of the 4 matched calls as `stall`, because the sale
//    usually closes on a later follow-up REVI never recorded. The summaries
//    faithfully described the call — "decisión aún no tomada", "el deal está
//    vivo pero en pausa", "riesgo: la competencia puede ganarlo" — about
//    clients who had already paid. Fulfillment could not tell client from
//    prospect. The analysed call is the call that SOLD the deal, not the
//    moment of the close, and the model has to be told so.
//
// 2. MONEY. The blanket "no money amounts" rule was too blunt to hold: it let
//    "depósito de 500 USD" (our revenue, on a team-visible field) through
//    while the client's own ticket range and MRR goal — real onboarding
//    context — were banned by the same clause. Split it: our deal economics
//    never, the client's business figures always.
//
// Still excluded, unchanged: coaching critique and call scores (source
// sensitivity ≤ surface audience — this lands on a team-visible CRM field).
function buildWonHandoffPrompt({ prospectName, coaching }) {
  const c = coaching || {};
  const material = {
    prospect_name: c.prospect_name || prospectName || null,
    company: c.company || null,
    icp_fit: c.icp_fit || null,
    context: c.context || null,
    general_read: c.general_read || null,
    deal: c.deal || null,
    opportunities: c.opportunities || null,
  };
  return [
    'You prepare internal handoff notes for the fulfillment team at NeuroGrowth (B2B LinkedIn organic growth agency).',
    'This client HAS ALREADY BOUGHT. The deal is won and onboarding is starting — that is settled fact, not something the note may question.',
    'Below is the analysis of the sales call. That call is the conversation that SOLD the deal, not the moment of the close: deals here routinely close on a later follow-up that was never recorded. So the analysis may still describe the prospect as undecided, the payment as pending, or the deal as at risk of being lost. All of that is HISTORY and has since been resolved. Write the executive summary the onboarding team needs to serve this client well.',
    '',
    'Rules:',
    '- Write in Spanish (the analysis and the team are Spanish-speaking).',
    '- 6 to 10 short lines, each starting with "• ". Plain text only, no markdown.',
    '- Cover, in this order when the material allows: who the client is and what their business does; why they bought / the goal they stated; pains discussed on the call; promises or expectations set on the call; risks or watch-outs for fulfillment; agreed next steps.',
    '- NEVER cast doubt on whether the client bought. No "decisión pendiente", no "el deal está en pausa", no "el pago no se procesó", no "pendiente de confirmación", no competitor threats, no urgency about closing. Write about a client, never about a prospect.',
    '- "Risks" means risks to DELIVERING for this client: expectations that will be hard to meet, a promise made on the call, a partner or spouse who joins onboarding without having heard the pitch. Never the risk of losing the sale — that risk is gone.',
    '- "Next steps" means what ONBOARDING does next. Drop the closer\'s follow-up actions (send the proposal, collect the balance, call on Wednesday); they are done or they belong to sales.',
    '- NEVER include money we are paid: deposits, balances, contract value, payment amounts or dates, discounts, commissions. Those live in the closing-data columns and this note is team-visible.',
    '- DO include the CLIENT\'s own business figures when they describe the business — their offer or ticket price, their revenue or MRR goal, client counts, sales per month. That is context fulfillment needs.',
    '- NEVER include coaching feedback about the closer, call scores, or anything phrased as advice to the sales team. This note is about the CLIENT.',
    '- If a section has no material, skip it silently. Never invent facts.',
    '',
    'CALL ANALYSIS JSON:',
    JSON.stringify(material),
  ].join('\n');
}

// One-shot condensation of REVI's coaching_json into a fulfillment-voice
// executive summary.
async function generateWonHandoffSummary({ prospectName, coaching }) {
  const prompt = buildWonHandoffPrompt({ prospectName, coaching });
  const response = await anthropic.messages.create({
    model: 'claude-sonnet-4-6',
    max_tokens: 700,
    messages: [{ role: 'user', content: prompt }],
  });
  const text = (response.content?.[0]?.text || '').trim();
  if (!text) throw new Error('empty summary from model');
  return text;
}

// The closing context lives on the LAST real call before the win — NOT on the
// call the outcome row happens to point at. Deals routinely close on a
// follow-up days after the call that did the actual selling, and dash attaches
// an outcome to the prospect's LATEST appointment, so the appointment's date is
// not evidence of when the closing call happened. Live proof: Fernando Corella
// won on 2026-08-06 against an appointment dated 2026-08-12, while the call
// carrying the context was 2026-08-04. The 36h appointment join (right for
// proving attendance) therefore threw that context away.
//
// So: scope by prospect, order by recency, take the newest call at or before
// the win. Same rule covers same-day closes, follow-up closes weeks later, and
// synthetic `opp:` rows with no appointment time — no special cases.
const WON_HANDOFF_LOOKBACK_DAYS = 60;
// A close is sometimes logged hours before the call wraps or gets scored, so a
// small forward pad keeps that call eligible without letting a LATER,
// post-sale call (onboarding, quicksync) masquerade as the closing call.
const WON_HANDOFF_FORWARD_PAD_MS = 2 * 86400000;

// Pure picker so the selection rule is testable without touching REVI.
function pickClosingCall(rows, outcomeAtMs) {
  const usable = (rows || []).filter(r =>
    r && r.call_date && r.coaching_json && Object.keys(r.coaching_json).length);
  if (!usable.length) return null;
  const cutoff = outcomeAtMs + WON_HANDOFF_FORWARD_PAD_MS;
  const eligible = usable.filter(r => new Date(r.call_date).getTime() <= cutoff);
  if (!eligible.length) return null;
  eligible.sort((a, b) => new Date(b.call_date).getTime() - new Date(a.call_date).getTime());
  return eligible[0];
}

async function findClosingCallRecording(email, outcomeAtMs, label) {
  if (!email) return null;
  try {
    const lo = new Date(outcomeAtMs - WON_HANDOFF_LOOKBACK_DAYS * 86400000).toISOString();
    const hi = new Date(outcomeAtMs + WON_HANDOFF_FORWARD_PAD_MS).toISOString();
    const { data: scores } = await reviSupabase
      .from('closer_call_scores')
      .select('prospect_email, call_date, recording_url, coaching_json')
      .ilike('prospect_email', email)
      .gte('call_date', lo)
      .lte('call_date', hi);
    const pick = pickClosingCall(scores, outcomeAtMs);
    if (!pick) return null;
    const total = (scores || []).length;
    if (total > 1) {
      console.log(`${label}: ${total} scored calls for ${email} — using the latest before the win (${pick.call_date}) as the closing call.`);
    }
    return { url: pick.recording_url || null, coaching: pick.coaching_json || {}, callDate: pick.call_date };
  } catch (err) {
    console.error(`${label}: closing-call lookup failed:`, err.message);
    return null;
  }
}

async function applyWonHandoffNote(appointmentId, note) {
  const { rows } = await portalWriterPg.query(
    'SELECT public.ng_apply_won_handoff_note($1, $2) AS result',
    [appointmentId, note]
  );
  return rows?.[0]?.result || 'error';
}

async function runWonHandoffNotes(_correlationId, { windowDays = WON_HANDOFF_WINDOW_DAYS } = {}) {
  const dryRun = process.env.WON_HANDOFF_MODE !== 'live';
  console.log(`Running won-handoff notes sweep (${dryRun ? 'DRY RUN' : 'LIVE'})...`);
  const tally = { wons: 0, generated: 0, placed: 0, prospectOnly: 0, noRecording: 0, gaveUp: 0, failed: 0 };
  try {
    if (!portalPg) { console.log('Won-handoff notes: portal read pool not configured, skipping.'); return tally; }
    if (!dryRun && !portalWriterPg) { console.log('Won-handoff notes: PORTAL_WRITER_DATABASE_URL not set, skipping.'); return tally; }
    const now = Date.now();
    const sinceIso = new Date(now - windowDays * 86400000).toISOString();
    const { rows: wons } = await portalPg.query(
      `SELECT o.appointment_id, o.created_at AS outcome_at,
              a.scheduled_start, a.closer_id,
              p.full_name, lower(coalesce(p.email, '')) AS email
         FROM revops_sales_outcomes o
         JOIN revops_appointments a ON a.id = o.appointment_id
         JOIN revops_prospects p ON p.id = a.prospect_id
        WHERE o.outcome = 'won' AND o.created_at >= $1
        ORDER BY o.created_at ASC`,
      [sinceIso]
    );
    if (!wons.length) { console.log('Won-handoff notes: no won deals in window.'); return tally; }
    tally.wons = wons.length;

    const todayISO = new Date().toISOString().slice(0, 10);

    for (const won of wons) {
      const apptId = won.appointment_id;
      const markerKey = `won-handoff:${apptId}`;
      const { data: markerRows } = await supabase.from('agent_knowledge').select('value').eq('key', markerKey).limit(1);
      const markerValue = markerRows?.[0]?.value || '';
      const step = decideWonHandoffStep(markerValue);
      if (step === 'skip') continue;

      try {
        if (step === 'retry_placement') {
          if (dryRun) { console.log(`[dry-run] would retry closing-data placement for ${won.full_name}`); tally.prospectOnly += 1; continue; }
          const result = await applyWonHandoffNote(apptId, null);
          if (result === 'placed' || result === 'already_placed') {
            await upsertKnowledge('process', markerKey, `${result}|${todayISO}`, 'won-handoff');
            tally.placed += 1;
            console.log(`Won-handoff note placed for ${won.full_name} (${result}).`);
          } else {
            tally.prospectOnly += 1; // client record still doesn't exist — keep waiting
          }
          continue;
        }

        // step === 'match' — find the call that carries the closing context
        const outcomeAtMs = new Date(won.outcome_at).getTime();
        const rec = await findClosingCallRecording(won.email, outcomeAtMs, 'Won-handoff notes');
        if (!rec || !rec.coaching || !Object.keys(rec.coaching).length) {
          // A dry run must never decide a deal is permanently hopeless: these
          // markers are terminal, so writing them without a live run would
          // silently seal deals out of the first real sweep.
          if ((now - outcomeAtMs) > WON_HANDOFF_RECORDING_WAIT_DAYS * 86400000) {
            if (!dryRun) await upsertKnowledge('process', markerKey, `gave_up|${todayISO}`, 'won-handoff');
            tally.gaveUp += 1;
            console.log(`Won-handoff notes: no REVI recording for ${won.full_name} after ${WON_HANDOFF_RECORDING_WAIT_DAYS}d — giving up.`);
          } else {
            const prevCount = parseInt(String(markerValue).split('|')[1], 10) || 0;
            if (!dryRun) await upsertKnowledge('process', markerKey, `no_recording|${prevCount + 1}`, 'won-handoff');
            tally.noRecording += 1;
          }
          continue;
        }

        const summary = await generateWonHandoffSummary({ prospectName: won.full_name, coaching: rec.coaching });
        const note = buildWonHandoffNote({
          summaryText: summary,
          // The RECORDING's date — the appointment's may belong to a later
          // follow-up, and a header dated Aug 12 over an Aug 4 call is a lie
          // to whoever reads the note.
          callDateStr: formatICTime(rec.callDate || won.scheduled_start || won.outcome_at, { month: 'short', day: 'numeric', year: 'numeric' }),
          closerName: resolveSalesMember(won.closer_id),
          recordingUrl: rec.url,
        });
        if (!note) { tally.failed += 1; continue; }
        tally.generated += 1;
        if (dryRun) {
          console.log(`[dry-run] would apply won-handoff note for ${won.full_name} (${note.length} chars):\n${note}`);
          continue;
        }
        const result = await applyWonHandoffNote(apptId, note);
        if (result === 'placed' || result === 'already_placed') {
          await upsertKnowledge('process', markerKey, `${result}|${todayISO}`, 'won-handoff');
          tally.placed += 1;
        } else if (result === 'prospect_only') {
          await upsertKnowledge('process', markerKey, `prospect_only|${todayISO}`, 'won-handoff');
          tally.prospectOnly += 1;
        } else {
          tally.failed += 1;
          console.error(`Won-handoff note for ${won.full_name}: RPC returned ${result}`);
        }
      } catch (oneErr) {
        tally.failed += 1;
        console.error(`Won-handoff note for ${won.full_name} failed:`, oneErr.message);
      }
    }

    // Never a silent zero: skips are counted and the summary line names them.
    const parts = [];
    if (tally.generated) parts.push(`${tally.generated} ${tally.generated === 1 ? 'summary' : 'summaries'} generated`);
    if (tally.placed) parts.push(`${tally.placed} placed in closing data`);
    if (tally.prospectOnly) parts.push(`${tally.prospectOnly} waiting for the client record`);
    if (tally.noRecording) parts.push(`${tally.noRecording} waiting on a REVI recording`);
    if (tally.gaveUp) parts.push(`${tally.gaveUp} had no recording after ${WON_HANDOFF_RECORDING_WAIT_DAYS}d — no note`);
    if (tally.failed) parts.push(`${tally.failed} FAILED`);
    if (parts.length) {
      const line = `📋 Won-handoff notes${dryRun ? ' (dry-run)' : ''}: ${parts.join(' · ')}.`;
      console.log(line);
      if (!dryRun || tally.failed) await postToSlack('#ng-pm-agent', line);
    } else {
      console.log('Won-handoff notes: nothing to do.');
    }
    return tally;
  } catch (err) {
    console.error('Won-handoff notes sweep error:', err.message);
    await slack.client.chat.postMessage({ channel: RON_SLACK_ID, text: `⚠️ Won-handoff notes cron failed: ${err.message}` }).catch(() => {});
    return tally;
  }
}

// ─── APPOINTMENT STATUS (Paso 1) ─────────────────────────────────────────────
// Show/no-show is a FACT, not a judgment, and closers were logging it ~2% of
// the time (40 of 44 August appointments still sat at the default `confirmed`).
// REVI knows the answer: a scored recording is positive proof the call happened.
// The inverse does NOT hold — REVI can fail to join — so absence is never
// treated as a no-show; it is the one case that reaches a human.
// Measured over every post-cutover call a human judged (2026-08-18):
//   REVI recording + held   = 16      REVI recording + no-show = 0
//   no recording  + held    = 25      no recording  + no-show  = 2
// So a recording PROVES a show — not one no-show has ever had one — but its
// absence proves nothing: 25 of 27 recording-less calls were actually held.
// REVI covers ~39% of held calls today. That asymmetry is the whole design:
// write `showed` on proof, and never ask a separate "did they show?" question,
// which would have fired on ~60% of calls and just recreated the SOP in Slack.
// Attendance for the rest is DEFERRED to the outcome card — a closer who says
// `lost` or `open deal` has told us the call happened, and `no show` sets both.
//
// Resolution cascade, in order. Pure: callers supply the evidence.
// Returns { status, action, reason, conflict }.
//   action 'skip'  — already dispositioned or not ours to answer
//   action 'write' — Max PUTs this status to GHL
//   action 'defer' — unknowable from data; the outcome card will settle it
const APPT_TERMINAL_STATUSES = new Set(['showed', 'noshow', 'cancelled', 'invalid']);
function resolveAppointmentStatus(evidence = {}) {
  const current = String(evidence.ghlStatus || '').toLowerCase();
  const hasRecording = !!evidence.recording;

  if (APPT_TERMINAL_STATUSES.has(current)) {
    // GHL REUSES the appointment row on reschedule, so an attendance answer
    // set for an earlier booking rides along onto the new date. Attendance
    // written BEFORE the call started cannot be describing that call — it is
    // a leftover, not an answer, and must not count as dispositioned.
    // (Cancelling before a call is normal, so `cancelled` is exempt.)
    // Real case: Daniela Bruno — no-showed Jul 24, rebooked to Aug 4, the
    // Jul 28 `noshow` followed her onto the Aug 4 row, and she then sat
    // through a 74-minute recorded call. 3 of 16 post-cutover terminal
    // statuses were stale this way, all of them rescheduled rows.
    const staleAttendance = (current === 'showed' || current === 'noshow')
      && Number.isFinite(evidence.statusWrittenAtMs)
      && Number.isFinite(evidence.startMs)
      && evidence.statusWrittenAtMs < evidence.startMs;

    if (!staleAttendance) {
      // A recording alongside a status a human set AFTER the call is a real
      // disagreement. Never silently flip it.
      if (hasRecording && (current === 'noshow' || current === 'cancelled')) {
        return { status: null, action: 'skip', reason: 'conflict_recording_vs_status', conflict: true };
      }
      return { status: null, action: 'skip', reason: `already_${current}`, conflict: false };
    }
    // Stale: fall through and let the evidence below answer for the real call.
    if (hasRecording) {
      return { status: 'showed', action: 'write', reason: 'stale_status_superseded_by_recording', conflict: false };
    }
    return { status: null, action: 'defer', reason: 'stale_status_no_evidence', conflict: false };
  }
  // Cancelled / rescheduled are already answered upstream by the GHL
  // "Outcome: Cancelled" workflow → dash chain. Consume it, never re-ask.
  if (evidence.cancelled)   return { status: null, action: 'skip', reason: 'cancelled_upstream', conflict: false };
  if (evidence.rescheduled) return { status: null, action: 'skip', reason: 'rescheduled', conflict: false };

  if (hasRecording) return { status: 'showed', action: 'write', reason: 'revi_recording', conflict: false };

  return { status: null, action: 'defer', reason: 'outcome_card_will_settle_it', conflict: false };
}

// Attendance implied by a deal outcome. Every outcome except no_show requires
// the call to have happened — you cannot lose, disqualify or follow up on a
// conversation nobody had. Mirrors classifyOutcome's attendance semantics, so
// one answer from the closer settles Paso 1 and Paso 2 together.
function appointmentStatusForOutcome(outcome) {
  if (!outcome) return null;
  return outcome === 'no_show' ? 'noshow' : 'showed';
}

// Nearest unmatched REVI recording for the same prospect email within the
// 36h join window (same contract as the scorecard overlay). Marks the
// returned recording matched so one recording never vouches for two calls.
const OUTCOME_MATCH_PAD_MS = 36 * 3600 * 1000;
function matchRecordingToCall(recordings, email, apptMs) {
  const em = String(email || '').toLowerCase();
  if (!em || !Number.isFinite(apptMs)) return null;
  let best = null;
  for (const r of recordings) {
    if (r.matched || r.email !== em) continue;
    const dist = Math.abs(r.at - apptMs);
    if (dist > OUTCOME_MATCH_PAD_MS) continue;
    if (!best || dist < Math.abs(best.at - apptMs)) best = r;
  }
  if (best) best.matched = true;
  return best;
}

// The reverse of matchRecordingToCall: given a REVI call, which appointment was
// it? Same 36h contract. Used to bind a REVI note to the specific call it
// describes — without this, a prospect with three calls accumulates three notes
// on one contact with nothing saying which call each is about.
// Ignores rows with no writable GHL event (pre-cutover, or `opp:` synthetics).
function nearestAppointmentToCall(appointments, callMs) {
  if (!Number.isFinite(callMs)) return null;
  let best = null;
  for (const a of (appointments || [])) {
    const gid = a.ghl_appointment_id;
    if (!gid || String(gid).startsWith('opp:')) continue;
    const at = Date.parse(a.scheduled_start || '');
    if (!Number.isFinite(at)) continue;
    if (Math.abs(at - callMs) > OUTCOME_MATCH_PAD_MS) continue;
    if (!best || Math.abs(at - callMs) < Math.abs(Date.parse(best.scheduled_start) - callMs)) best = a;
  }
  return best ? best.ghl_appointment_id : null;
}

const VALID_LOGGABLE_OUTCOMES = new Set(['won', 'lost', 'follow_up', 'disqualified', 'no_show']);

// Writes one outcome row + the matching prospect-status promotion in a single
// transaction. NEVER overwrites: the unique index on appointment_id makes the
// insert first-writer-wins, so a human-logged outcome can never be clobbered.
async function logOutcomeToPortal({ appointmentId, outcome, source, notes, closedRevenue }) {
  if (!portalWriterPg) return { ok: false, reason: 'not_configured', message: 'PORTAL_WRITER_DATABASE_URL not set.' };
  if (!VALID_LOGGABLE_OUTCOMES.has(outcome)) return { ok: false, reason: 'bad_outcome', message: `Outcome must be one of: ${[...VALID_LOGGABLE_OUTCOMES].join(', ')}` };
  if (outcome === 'won' && !(Number(closedRevenue) > 0)) return { ok: false, reason: 'won_needs_revenue', message: 'Logging won requires the closed revenue amount.' };
  const client = await portalWriterPg.connect();
  try {
    await client.query('BEGIN');
    const ins = await client.query(
      `INSERT INTO revops_sales_outcomes (appointment_id, outcome, notes, source, closed_revenue, close_date)
       VALUES ($1, $2, $3, $4, $5, CASE WHEN $2 = 'won' THEN (now() AT TIME ZONE 'America/Costa_Rica')::date END)
       ON CONFLICT (appointment_id) DO NOTHING
       RETURNING id`,
      [appointmentId, outcome, notes || null, source, closedRevenue == null ? null : Number(closedRevenue)]
    );
    if (!ins.rowCount) {
      await client.query('ROLLBACK');
      const { rows } = await client.query('SELECT outcome, source FROM revops_sales_outcomes WHERE appointment_id = $1', [appointmentId]);
      return { ok: false, reason: 'exists', existing: rows[0] || null };
    }
    let statusChange = null;
    let opportunityId = null;
    let ghlContactId = null;
    let ghlAppointmentId = null;
    const { rows: prow } = await client.query(
      `SELECT p.id, p.status, p.ghl_opportunity_id, p.ghl_contact_id, a.ghl_appointment_id FROM revops_prospects p JOIN revops_appointments a ON a.prospect_id = p.id WHERE a.id = $1`,
      [appointmentId]
    );
    if (prow.length) {
      opportunityId = prow[0].ghl_opportunity_id || null;
      ghlContactId = prow[0].ghl_contact_id || null;
      ghlAppointmentId = prow[0].ghl_appointment_id || null;
      const next = nextProspectStatusForOutcome(outcome, prow[0].status);
      if (next) {
        await client.query('UPDATE revops_prospects SET status = $1, updated_at = now() WHERE id = $2', [next, prow[0].id]);
        statusChange = `${prow[0].status} → ${next}`;
      }
    }
    await client.query('COMMIT');

    // Portal first, GHL second, and deliberately OUTSIDE the transaction: the
    // portal row is what every report reads, so a GHL failure must never roll
    // back a logged outcome. Before this, "I'll log it for you" left the
    // closer's opportunity card sitting in Call Booked forever.
    let stageMove = null;
    // dash only populates ghl_opportunity_id on ~half of prospects (58 of 111
    // post-cutover on 2026-08-18 — Aura Bonilla's Open-Deal ✅ hit this), but
    // nearly all carry ghl_contact_id, and GHL can find the opp from that.
    if (!opportunityId && ghlContactId) {
      opportunityId = await ghlFindSalesOpportunityByContact(ghlContactId);
    }
    if (opportunityId) {
      try {
        stageMove = await ghlMoveOpportunityForOutcome(opportunityId, outcome);
      } catch (moveErr) {
        stageMove = { ok: false, message: moveErr.message };
      }
    } else {
      stageMove = { ok: false, message: 'no GHL opportunity found for the prospect' };
    }

    // Paso 1 falls out of Paso 2 for free: an outcome implies attendance, so
    // the closer never answers a separate "did they show?" question. Only
    // fills a blank — a status a human already set is never overwritten.
    let attendance = null;
    const impliedStatus = appointmentStatusForOutcome(outcome);
    if (impliedStatus && ghlAppointmentId && !String(ghlAppointmentId).startsWith('opp:')) {
      try {
        const cur = await ghlGetAppointmentStatus(ghlAppointmentId);
        if (cur && APPT_TERMINAL_STATUSES.has(cur)) {
          attendance = { ok: true, already: true, status: cur };
        } else {
          await ghlSetAppointmentStatus(ghlAppointmentId, impliedStatus);
          attendance = { ok: true, status: impliedStatus };
        }
      } catch (attErr) {
        attendance = { ok: false, message: attErr.message };
      }
    }
    return { ok: true, outcomeId: ins.rows[0].id, statusChange, stageMove, attendance };
  } catch (err) {
    await client.query('ROLLBACK').catch(() => {});
    return { ok: false, reason: 'error', message: err.message };
  } finally {
    client.release();
  }
}

// Agent-tool wrapper: resolve a prospect to their appointment, then write the
// outcome the human stated. Resolution is read-only via portalPg; the write
// goes through logOutcomeToPortal (first-writer-wins, never overwrites).
async function logCallOutcomeTool({ prospect, date, outcome, revenue, note }) {
  if (!portalPg) return 'Portal read-only DB not configured. Set PORTAL_READONLY_DATABASE_URL in .env.';
  const frag = `%${String(prospect || '').trim().toLowerCase()}%`;
  if (frag === '%%') return 'ERROR: prospect (name or email) is required.';
  const { rows } = await portalPg.query(
    `SELECT a.id, a.scheduled_start, a.closer_id, p.full_name, p.email, o.outcome AS existing_outcome, o.source AS existing_source
       FROM revops_appointments a
       JOIN revops_prospects p ON p.id = a.prospect_id
       LEFT JOIN revops_sales_outcomes o ON o.appointment_id = a.id
      WHERE (lower(coalesce(p.email,'')) LIKE $1 OR lower(p.full_name) LIKE $1)
        AND ($2::date IS NULL OR (a.scheduled_start AT TIME ZONE 'America/Costa_Rica')::date = $2::date)
        AND a.scheduled_start <= now()
      ORDER BY a.scheduled_start DESC
      LIMIT 5`,
    [frag, date || null]
  );
  if (!rows.length) return `No past appointment found for "${prospect}"${date ? ` on ${date}` : ''}. Check the name/email or give me the call date.`;
  const fmt = r => `${r.full_name} <${r.email || 'no email'}> — ${formatICTime(r.scheduled_start, { month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit' })} CR — closer ${r.closer_id}${r.existing_outcome ? ` — ALREADY LOGGED: ${r.existing_outcome}` : ''}`;
  const target = rows[0];
  if (rows.length > 1 && !date) {
    const sameDay = rows.filter(r => String(r.scheduled_start) === String(target.scheduled_start));
    if (sameDay.length > 1 || rows.slice(1).some(r => !r.existing_outcome)) {
      return `Ambiguous — ${rows.length} past appointments match "${prospect}". Tell me which date:\n${rows.map(fmt).join('\n')}`;
    }
  }
  if (target.existing_outcome) {
    return `That call already has an outcome: ${target.existing_outcome} (source: ${target.existing_source}). I never overwrite outcomes — if it's wrong, tell Ron; a correction needs a deliberate fix, not a silent replace.`;
  }
  const result = await logOutcomeToPortal({
    appointmentId: target.id,
    outcome,
    source: 'closer',
    notes: note ? `Logged via Max: ${note}` : 'Logged via Max on explicit closer/Ron instruction',
    closedRevenue: revenue,
  });
  if (!result.ok) {
    if (result.reason === 'won_needs_revenue') return 'Logging won needs the amount — e.g. revenue: 3500.';
    if (result.reason === 'exists') return `Already logged as ${result.existing?.outcome} — no change (first writer wins).`;
    if (result.reason === 'not_configured') return 'Outcome write path not configured (PORTAL_WRITER_DATABASE_URL missing on ng-pm-MAX). Log it in GHL instead and tell Ron.';
    return `Write failed: ${result.message || result.reason}`;
  }
  return `Logged ${outcome}${revenue ? ` ($${revenue})` : ''} for ${target.full_name} (${formatICTime(target.scheduled_start, { month: 'short', day: 'numeric' })} call).${result.statusChange ? ` Prospect status: ${result.statusChange}.` : ''} Scorecard + portal reflect it immediately.`;
}

// Paso 1 by hand: set attendance on the GHL appointment itself. Only reached
// for calls REVI could not read — Max sets `showed` automatically otherwise.
// Writes to GHL only; dash's Outcome workflows carry it back into the portal's
// `attended`, so this never writes revops directly and can't fight ingestion.
async function setAppointmentStatusTool({ prospect, date, status }) {
  if (!portalPg) return 'Portal read-only DB not configured. Set PORTAL_READONLY_DATABASE_URL in .env.';
  const allowed = new Set(['showed', 'noshow', 'cancelled']);
  if (!allowed.has(status)) return `status must be one of: ${[...allowed].join(', ')}`;
  const frag = `%${String(prospect || '').trim().toLowerCase()}%`;
  if (frag === '%%') return 'ERROR: prospect (name or email) is required.';
  const { rows } = await portalPg.query(
    `SELECT a.id, a.ghl_appointment_id, a.scheduled_start, p.full_name, p.email
       FROM revops_appointments a
       JOIN revops_prospects p ON p.id = a.prospect_id
      WHERE (lower(coalesce(p.email,'')) LIKE $1 OR lower(p.full_name) LIKE $1)
        AND ($2::date IS NULL OR (a.scheduled_start AT TIME ZONE 'America/Costa_Rica')::date = $2::date)
        AND a.scheduled_start <= now()
      ORDER BY a.scheduled_start DESC
      LIMIT 5`,
    [frag, date || null]
  );
  if (!rows.length) return `No past appointment found for "${prospect}"${date ? ` on ${date}` : ''}.`;
  if (rows.length > 1 && !date) {
    return `Ambiguous — ${rows.length} past appointments match "${prospect}". Tell me which date:\n`
      + rows.map(r => `${r.full_name} — ${formatICTime(r.scheduled_start, { month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit' })} CR`).join('\n');
  }
  const target = rows[0];
  if (!target.ghl_appointment_id || String(target.ghl_appointment_id).startsWith('opp:')) {
    return `${target.full_name}'s call has no GHL appointment I can write to (pre-cutover or opportunity-only row) — set it in GHL by hand.`;
  }
  try {
    await ghlSetAppointmentStatus(target.ghl_appointment_id, status);
  } catch (err) {
    return `Couldn't set it in GHL: ${err.message}. Set it on the appointment by hand.`;
  }
  await upsertKnowledge('process', `appt-status-ask:${target.id}`, `answered|${new Date().toISOString().slice(0, 10)}`, 'appt-status-ask');
  return `Set ${target.full_name}'s ${formatICTime(target.scheduled_start, { month: 'short', day: 'numeric' })} call to *${status}* in GHL.`;
}

// ─── UNLOGGED OUTCOME REMINDERS (GHL) ────────────────────────────────────────
// REVI recordings covering a set of call times — same 36h contract as the
// scorecard overlay. Best-effort by design: a REVI outage degrades every
// caller to "no read", never blocks the run.
// coaching_json is the field that makes a real proposal possible: deal.status
// (alive/dead) + icp_fit separate a No Fit from a Lost, which deal_outcome
// alone cannot do (it reads `stall` on 91 of 130 calls).
async function fetchReviRecordings(startTimesMs, label) {
  const starts = (startTimesMs || []).filter(Number.isFinite);
  if (!starts.length) return [];
  try {
    const lo = new Date(Math.min(...starts) - OUTCOME_MATCH_PAD_MS).toISOString();
    const hi = new Date(Math.max(...starts) + OUTCOME_MATCH_PAD_MS).toISOString();
    const [{ data: reviClosers }, { data: scores }] = await Promise.all([
      reviSupabase.from('revi_closers').select('id, fathom_host_email'),
      reviSupabase.from('closer_call_scores')
        .select('closer_id, prospect_email, call_date, duration_min, overall_score, deal_outcome, recording_url, prospect_signals, coaching_json')
        .gte('call_date', lo).lte('call_date', hi),
    ]);
    const closerByReviId = {};
    for (const rc of (reviClosers || [])) {
      if (rc.fathom_host_email) closerByReviId[rc.id] = rc.fathom_host_email.toLowerCase();
    }
    return (scores || [])
      .filter(s => s.prospect_email && s.call_date)
      .map((s) => {
        const coaching = s.coaching_json || {};
        const deal = typeof coaching.deal === 'string'
          ? (() => { try { return JSON.parse(coaching.deal); } catch (_) { return {}; } })()
          : (coaching.deal || {});
        return {
          email: s.prospect_email.toLowerCase(),
          closer: closerByReviId[s.closer_id] || null,
          at: new Date(s.call_date).getTime(),
          durationMin: s.duration_min == null ? null : Math.round(Number(s.duration_min)),
          score: s.overall_score == null ? null : Number(s.overall_score),
          dealOutcome: s.deal_outcome || null,
          url: s.recording_url || null,
          signals: parseReviSignals(s.prospect_signals),
          dealStatus: deal.status || null,
          dealRecovery: deal.recovery || null,
          icpFit: coaching.icp_fit || null,
          cashCollected: coaching.cash_collected_usd == null ? null : Number(coaching.cash_collected_usd),
          matched: false,
        };
      });
  } catch (err) {
    console.error(`${label}: REVI overlay unavailable, falling back to plain nudges:`, err.message);
    return [];
  }
}

// Latest GHL opportunity stage per prospect email, from the funnel-event stream
// REVI already mirrors. This is a second, independent outcome signal: if the
// card has already moved to a won/lost stage, nobody needs to be asked.
async function fetchGhlFunnelStateByEmail(emails) {
  const wanted = [...new Set((emails || []).map(e => String(e || '').toLowerCase()).filter(Boolean))];
  if (!wanted.length) return {};
  try {
    const { data } = await reviSupabase
      .from('funnel_events')
      .select('contact_email, detected_on, to_class, to_stage_name, pipeline_id')
      .in('contact_email', wanted)
      .order('detected_on', { ascending: true });
    const byEmail = {};
    for (const r of (data || [])) {
      const em = String(r.contact_email || '').toLowerCase();
      if (em) byEmail[em] = r; // ordered ascending, so the last write wins
    }
    return byEmail;
  } catch (err) {
    console.error('GHL funnel state unavailable (degrading to REVI-only reads):', err.message);
    return {};
  }
}

// Shared by the outcome reminders (Paso 2) and the appointment-status sweep
// (Paso 1): the set of past sales calls a human could actually act on.
//
// Only surface calls a closer can actually resolve. Outcomes are logged on the
// GHL opportunity card, and the webhook attaches them to the prospect's LATEST
// appointment — so two classes of row are permanently unfixable and must never
// be nagged (chasing impossible items is how a reminder system gets ignored):
//   1. Pre-cutover calls (before GHL went live) — pre-migration history.
//      Self-expiring: they fall out of the window on their own.
//   2. Orphaned rows — the prospect rebooked, so any outcome the closer logs
//      lands on the newer appointment, never on this one.
// NOTE: iClosed-BOOKED calls scheduled after the cutover ARE fulfillable —
// the prospect has a live GHL opp card, so source is irrelevant here.
const GHL_CUTOVER_ISO = '2026-07-23T00:00:00.000Z';
async function fetchDueSalesCalls({ cutoffIso, sinceIso, label }) {
  const floorIso = sinceIso > GHL_CUTOVER_ISO ? sinceIso : GHL_CUTOVER_ISO;

  // `attended` is almost always null so it is NOT a gate; we exclude cancelled
  // calls and require a call identity.
  const { data: pastCalls } = await portalSupabase
    .from('revops_appointments')
    .select('id, prospect_id, closer_id, scheduled_start, iclosed_call_id, ghl_appointment_id, source, attended, qualification_snapshot, prospect:prospect_id(full_name, email, ghl_contact_id, ghl_opportunity_id)')
    .lte('scheduled_start', cutoffIso)
    .gte('scheduled_start', floorIso);

  // Orphan detection: any later appointment for the same prospect means an
  // outcome logged now attaches there, not here.
  const prospectIds = [...new Set((pastCalls || []).map(a => a.prospect_id).filter(Boolean))];
  const latestStartByProspect = {};
  if (prospectIds.length) {
    const { data: allAppts } = await portalSupabase
      .from('revops_appointments')
      .select('prospect_id, scheduled_start')
      .in('prospect_id', prospectIds);
    for (const r of (allAppts || [])) {
      const cur = latestStartByProspect[r.prospect_id];
      if (!cur || String(r.scheduled_start) > cur) latestStartByProspect[r.prospect_id] = String(r.scheduled_start);
    }
  }

  // Flywheel-only: exclude partner-consulting 1:1s before any state tracking.
  const excludeIds = await getNonFlywheelCallIds();
  let orphanSkipped = 0;
  const rescheduledIds = new Set();
  const dueCalls = (pastCalls || []).filter((a) => {
    if (!(a.iclosed_call_id || a.ghl_appointment_id)) return false;
    if (isAppointmentCancelled(a)) return false;
    if (excludeIds.has(a.iclosed_call_id)) return false;
    const latest = a.prospect_id ? latestStartByProspect[a.prospect_id] : null;
    if (latest && latest > String(a.scheduled_start)) {
      orphanSkipped += 1;
      rescheduledIds.add(a.id);
      return false;
    }
    return true;
  });
  if (orphanSkipped) {
    console.log(`${label}: skipped ${orphanSkipped} orphaned call(s) — prospect rebooked, outcome would attach to the newer appointment.`);
  }
  return { dueCalls, rescheduledIds, orphanSkipped };
}

// dash writes the GHL cancellation flag at the TOP level of
// qualification_snapshot ({cancelled: true, reason: 'deleted_in_ghl'}), not
// under .ghl — that sub-object only ever carries opportunity_source. The old
// `qualification_snapshot.ghl.cancelled` check therefore never matched a single
// row, so cancelled calls were still being nagged. Verified against the portal
// 2026-08-18: 87 post-cutover rows, .ghl keys = {opportunity_source} only.
function isAppointmentCancelled(appt) {
  const qs = appt?.qualification_snapshot || {};
  return qs.cancelled === true || qs.iclosed?.cancelled === true || qs.ghl?.cancelled === true;
}

// Fires 4 PM CR every day. DMs the owning closer for any call >24h old that
// still has no outcome logged in GHL. Re-nudges daily (de-duped via
// agent_knowledge) and escalates to Ron once unlogged 3+ days despite reminders.
// Every unlogged call now gets its OWN card carrying Max's read of the REVI
// evidence, phrased in the GHL stage names the closer would set by hand: ✅
// logs the proposal and moves the opportunity, a text reply corrects it.
// Facts auto-surface; judgments always get a human ✅.
async function runUnloggedOutcomeReminders(_correlationId) {
  console.log('Running unlogged-outcome reminders...');
  try {
    const now    = Date.now();
    const since  = new Date(now - 14 * 24 * 60 * 60 * 1000).toISOString(); // 14d floor
    const cutoff = new Date(now - 24 * 60 * 60 * 1000).toISOString();      // >24h old

    const { dueCalls } = await fetchDueSalesCalls({
      cutoffIso: cutoff, sinceIso: since, label: 'Unlogged-outcome reminders',
    });

    if (!dueCalls.length) {
      console.log('Unlogged-outcome reminders: no due calls in window.');
      return;
    }

    // Calls that already have an outcome logged. revops_sales_outcomes is
    // reliable since the dash.neurogrowth.io ingestion fix (PR #3, 2026-05-19),
    // so we read it directly by appointment_id instead of scanning raw webhook
    // deliveries. Verified equivalent to the old webhook scan (0 mismatches over
    // 125 appts / 30d). One outcome row per appointment_id (unique index).
    const dueIds = dueCalls.map(a => a.id);
    const { data: outcomeRows } = await portalSupabase
      .from('revops_sales_outcomes')
      .select('appointment_id')
      .in('appointment_id', dueIds);
    const loggedApptIds = new Set((outcomeRows || []).map(o => o.appointment_id));
    const unlogged = dueCalls.filter(a => !loggedApptIds.has(a.id));

    if (!unlogged.length) {
      console.log('Unlogged-outcome reminders: all attended calls logged.');
      return;
    }

    // De-dup + escalation state per appointment ───────────────────────────────
    const todayISO = new Date().toISOString().slice(0, 10);
    const unloggedByCloser = {};
    const escalations = []; // { appt, closerName, count } where 3+ days unlogged

    for (const a of unlogged) {
      const dedupKey = `outcome-reminder:${a.id}`;
      let firstReminded = todayISO;
      let count = 1;
      const { data: existing } = await supabase
        .from('agent_knowledge')
        .select('value')
        .eq('key', dedupKey)
        .limit(1);
      if (existing && existing.length && existing[0].value) {
        const [prevDate, prevCount] = String(existing[0].value).split('|');
        if (prevDate) firstReminded = prevDate;
        count = (parseInt(prevCount, 10) || 0) + 1;
      }
      await upsertKnowledge('process', dedupKey, `${firstReminded}|${count}`, 'outcome-reminder');

      const daysSinceFirst = Math.floor((now - new Date(firstReminded).getTime()) / 86400000);
      const entry = { appt: a, count, daysSinceFirst };
      (unloggedByCloser[a.closer_id] = unloggedByCloser[a.closer_id] || []).push(entry);
      if (daysSinceFirst >= 3) {
        escalations.push({ appt: a, closerName: resolveSalesMember(a.closer_id), count });
      }
    }

    const reviRecordings = await fetchReviRecordings(
      unlogged.map(a => new Date(a.scheduled_start).getTime()),
      'Unlogged-outcome reminders',
    );
    const funnelByEmail = await fetchGhlFunnelStateByEmail(
      unlogged.map(a => (a.prospect?.email || '')),
    );

    // Feedback-loop lessons ───────────────────────────────────────────────────
    const lessons = await getReportLessons('unlogged-outcome-reminder');
    const lessonNote = lessons.length
      ? `[Corrections applied from feedback]\n${lessons.map(l => `• ${l.value}`).join('\n')}\n\n`
      : '';

    // DM each closer: pre-filled proposal per recording-verified call (capped),
    // then the classic aggregate list for the rest ────────────────────────────
    const PROPOSALS_PER_CLOSER_PER_RUN = 5;
    // After this many days a card stops being bumped in-thread; Ron's 3-day
    // escalation has already fired by then, so continuing to poke the closer
    // adds noise without adding pressure.
    const OUTCOME_CARD_MAX_BUMPS = 4;
    for (const [closerEmail, entries] of Object.entries(unloggedByCloser)) {
      const slackId = CLOSER_SLACK[closerEmail] || CLOSER_SLACK[(closerEmail || '').toLowerCase()];
      if (!slackId) {
        console.warn(`Unlogged-outcome reminders: no Slack ID for closer ${closerEmail}`);
        continue;
      }
      const closerName = resolveSalesMember(closerEmail);
      const firstName  = (typeof closerName === 'string' ? closerName : '').split(' ')[0] || 'there';

      let proposalsSent = 0;
      const aggregate = []; // entries that get (or stay on) the classic list

      for (const entry of entries) {
        const { appt } = entry;
        const email = (appt.prospect?.email || '').toLowerCase();
        const pName = appt.prospect?.full_name || 'Unknown';
        const proposalKey = `outcome-proposal:${appt.id}`;

        // One card per appointment, ever. A still-open card gets BUMPED in its
        // own thread rather than re-DM'd — a fresh DM every afternoon is how a
        // reminder reaches "nudged 14×" and stops being read.
        const { data: sentBefore } = await supabase
          .from('agent_knowledge').select('value').eq('key', proposalKey).limit(1);
        if (sentBefore && sentBefore.length) {
          const [state, , cardChannel, cardTs] = String(sentBefore[0].value || '').split('|');
          const stillOpen = state === 'proposed';
          if (stillOpen && cardChannel && cardTs && entry.count <= OUTCOME_CARD_MAX_BUMPS) {
            try {
              await slack.client.chat.postMessage({
                channel: cardChannel,
                thread_ts: cardTs,
                text: `⏳ Still open — day ${entry.daysSinceFirst || 1}. Tap an action above and I'll log it plus move the GHL card.`,
              });
            } catch (bumpErr) {
              console.error(`Outcome card bump for ${pName} failed:`, bumpErr.message);
            }
          } else if (stillOpen) {
            aggregate.push({ ...entry, proposalPending: true });
          }
          continue;
        }

        // Every unlogged call earns a card now. Previously a card required a
        // matched recording AND a deal_outcome in {stall, lost, won}, which is
        // why calls like Aura Bonilla's (deal_outcome `pending`, deposit agreed
        // on the call) only ever appeared as a bullet in a flat list.
        if (proposalsSent >= PROPOSALS_PER_CLOSER_PER_RUN) {
          aggregate.push(entry);
          continue;
        }

        const rec = matchRecordingToCall(reviRecordings, email, new Date(appt.scheduled_start).getTime());
        const funnel = funnelByEmail[email] || null;
        const pipelineId = funnel?.pipeline_id || GHL_PIPELINE.APPT_SETTING;
        const proposal = computeOutcomeProposal({
          recording: rec,
          dealStatus: rec?.dealStatus,
          icpFit: rec?.icpFit,
          cashCollected: rec?.cashCollected,
          funnelClass: funnel?.to_class,
        });

        const dStr = formatICTime(appt.scheduled_start, { month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit' });
        const heldDays = Math.floor((now - new Date(appt.scheduled_start).getTime()) / 86400000);
        const cardText = buildOutcomeCardText({
          prospectName: pName, whenStr: dStr, heldDays, rec, proposal, funnel, pipelineId,
          nudgeCount: entry.count,
        });

        try {
          const posted = await slack.client.chat.postMessage({
            channel: slackId,
            text: cardText,
            metadata: {
              event_type: 'outcome_proposal',
              event_payload: {
                appointment_id: appt.id,
                prospect_name: pName,
                proposed_outcome: proposal.outcome || '',
                won_hint: !!proposal.wonHint,
                confidence: proposal.confidence,
                rule: proposal.rule,
                pipeline_id: pipelineId,
                closer_email: closerEmail,
              },
            },
          });
          // Store where the card lives so tomorrow's nudge bumps THIS thread
          // instead of opening a new DM — "reminded 14×" is what a re-DM loop
          // looks like from the closer's side.
          await upsertKnowledge('process', proposalKey, `proposed|${todayISO}|${slackId}|${posted.ts || ''}`, 'outcome-proposal');
          proposalsSent += 1;
        } catch (propErr) {
          console.error(`Outcome card DM to ${closerName} failed:`, propErr.message);
          aggregate.push(entry);
        }
      }
      if (proposalsSent) console.log(`Outcome proposals sent to ${closerName}: ${proposalsSent}`);

      if (aggregate.length) {
        try {
          const lines = [`${lessonNote}Hey ${firstName} — these calls are still missing an outcome in GHL:\n`];
          lines.push(`⚠️ Outcome not logged (${aggregate.length}):`);
          aggregate.forEach(({ appt, count, proposalPending }) => {
            const pName = appt.prospect?.full_name || 'Unknown';
            const dStr  = formatICTime(appt.scheduled_start, { month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit' });
            const heldDays = Math.floor((now - new Date(appt.scheduled_start).getTime()) / 86400000);
            const nudge = count > 1 ? ` · reminded ${count}×` : '';
            const pend  = proposalPending ? ' · ✅ the proposal above or reply to it' : '';
            lines.push(`• ${pName} — ${dStr} CR — held ${heldDays}d ago${nudge}${pend}`);
          });
          lines.push('');
          lines.push('Set the outcome on the opportunity card in GHL, or just reply here (e.g. `won 3500`, `lost`, `follow up`) and I\'ll log it for you.');
          lines.push('');
          lines.push('See something off? Thread on this message and tag @Max with the correction.');
          await slack.client.chat.postMessage({ channel: slackId, text: lines.join('\n') });
          console.log(`Unlogged-outcome reminder sent to ${closerName} (${aggregate.length} calls)`);
        } catch (closerErr) {
          console.error(`Unlogged-outcome reminder to ${closerName} failed:`, closerErr.message);
        }
      }
    }

    // Escalate 3+ day stragglers to Ron ───────────────────────────────────────
    if (escalations.length) {
      const eLines = ['🚨 Outcome-logging escalation — 3+ days unlogged despite reminders:\n'];
      escalations.forEach(({ appt, closerName, count }) => {
        const pName = appt.prospect?.full_name || 'Unknown';
        const dStr  = formatICTime(appt.scheduled_start, { month: 'short', day: 'numeric' });
        eLines.push(`• ${pName} — ${dStr} — closer: ${closerName} — reminded ${count}×`);
      });
      await slack.client.chat.postMessage({ channel: RON_SLACK_ID, text: eLines.join('\n') });
      console.log(`Unlogged-outcome escalation sent to Ron (${escalations.length} calls)`);
    }

    console.log('Unlogged-outcome reminders complete.');
  } catch (err) {
    console.error('Unlogged-outcome reminders error:', err.message);
    await slack.client.chat.postMessage({ channel: RON_SLACK_ID, text: `⚠️ Unlogged-outcome reminder cron failed: ${err.message}` });
  }
}

// ─── PASO 1 SWEEP: APPOINTMENT STATUS FROM REVI ──────────────────────────────
// The SOP asked closers to open every appointment in GHL and set Showed /
// No-Show / Cancelled by hand. Compliance was ~2%: on 2026-08-18, 40 of 44
// August appointments on the main sales calendar were still sitting at the
// default `confirmed`. That is why revops_appointments.attended is null almost
// everywhere and show rate has to be derived from outcomes instead of measured.
//
// A scored REVI recording is positive proof the call happened, so Max sets
// `showed` himself. The inverse is NOT true — REVI can fail to join a call —
// so a missing recording is never written as a no-show; it is the only case
// that reaches a human, and only after cancellation and reschedule have been
// ruled out from data that already exists.
const GHL_SALES_CALENDAR_IDS = (process.env.GHL_SALES_CALENDAR_IDS
  || 'fYQJCzbk4hvV0brpJqoE,HXLeEjxpa0gdiTPNiAzc,KRTGx8XteIJSCcKAShHS').split(',').map(s => s.trim()).filter(Boolean);

// Current appointmentStatus for every sales-calendar event in a window, keyed
// by GHL appointment id. Bulk by calendar (3 calls) rather than per contact.
async function fetchGhlAppointmentStatuses(fromMs, toMs) {
  const byId = {};
  const locationId = process.env.GHL_LOCATION_ID;
  for (const calendarId of GHL_SALES_CALENDAR_IDS) {
    try {
      const res = await ghlFetch(
        `https://services.leadconnectorhq.com/calendars/events?locationId=${locationId}`
        + `&calendarId=${calendarId}&startTime=${fromMs}&endTime=${toMs}`,
        { headers: { 'Authorization': `Bearer ${process.env.GHL_API_KEY}`, 'Version': '2021-07-28' } },
        { label: `calendar ${calendarId}` },
      );
      if (!res.ok) {
        console.warn(`Appointment-status sweep: calendar ${calendarId} → ${res.status}`);
        continue;
      }
      const events = (await res.json()).events || [];
      for (const e of events) {
        if (e.deleted) continue;
        byId[e.id] = {
          status: String(e.appointmentStatus || e.appoinmentStatus || '').toLowerCase(),
          contactId: e.contactId || null,
          rescheduledAt: e.rescheduledAt || null,
          // When the status was last written vs when the call actually starts —
          // together these expose a status left over from an earlier booking of
          // this same (reused) row.
          statusWrittenAtMs: Date.parse(e.dateUpdated || '') || null,
          startMs: Date.parse(e.startTime || '') || null,
        };
      }
    } catch (err) {
      console.warn(`Appointment-status sweep: calendar ${calendarId} fetch failed: ${err.message}`);
    }
  }
  return byId;
}

// Current attendance on one appointment, or null if it can't be read. Used to
// make the implied-attendance write fill-only — never clobber a human's answer.
async function ghlGetAppointmentStatus(eventId) {
  try {
    const res = await ghlFetch(`https://services.leadconnectorhq.com/calendars/events/appointments/${eventId}`, {
      headers: { 'Authorization': `Bearer ${process.env.GHL_API_KEY}`, 'Version': '2021-07-28' },
    }, { label: `appt-read ${eventId}` });
    if (!res.ok) return null;
    const body = await res.json();
    const appt = body.appointment || body.event || body;
    const st = appt.appointmentStatus || appt.appoinmentStatus;
    return st ? String(st).toLowerCase() : null;
  } catch (_) {
    return null;
  }
}

async function ghlSetAppointmentStatus(eventId, appointmentStatus) {
  const res = await ghlFetch(`https://services.leadconnectorhq.com/calendars/events/appointments/${eventId}`, {
    method: 'PUT',
    headers: {
      'Authorization': `Bearer ${process.env.GHL_API_KEY}`,
      'Version': '2021-07-28',
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({ appointmentStatus }),
  }, { label: `appt-status ${eventId}` });
  if (!res.ok) throw new Error(`appt PUT ${eventId} → ${res.status}: ${(await res.text()).slice(0, 150)}`);
  return true;
}

async function runAppointmentStatusSync(_correlationId) {
  const dryRun = process.env.APPT_STATUS_SYNC_MODE !== 'live';
  console.log(`Running appointment-status sync (${dryRun ? 'DRY RUN' : 'LIVE'})...`);
  const tally = { considered: 0, showed: 0, skipped: 0, conflicts: 0, deferred: 0, failed: 0 };
  try {
    const now    = Date.now();
    const since  = new Date(now - 14 * 24 * 60 * 60 * 1000).toISOString();
    const cutoff = new Date(now - 2 * 60 * 60 * 1000).toISOString(); // ended ≥2h ago

    const { dueCalls } = await fetchDueSalesCalls({
      cutoffIso: cutoff, sinceIso: since, label: 'Appointment-status sync',
    });
    if (!dueCalls.length) {
      console.log('Appointment-status sync: no due calls in window.');
      return tally;
    }

    const [statusById, reviRecordings] = await Promise.all([
      fetchGhlAppointmentStatuses(Date.parse(since), now),
      fetchReviRecordings(dueCalls.map(a => new Date(a.scheduled_start).getTime()), 'Appointment-status sync'),
    ]);

    const conflicts = [];

    for (const appt of dueCalls) {
      // Only GHL-native rows carry an event we can write to.
      const eventId = appt.ghl_appointment_id;
      if (!eventId || String(eventId).startsWith('opp:')) continue;
      tally.considered += 1;

      const ghl = statusById[eventId];
      const rec = matchRecordingToCall(reviRecordings, (appt.prospect?.email || '').toLowerCase(), new Date(appt.scheduled_start).getTime());
      const verdict = resolveAppointmentStatus({
        ghlStatus: ghl?.status,
        recording: rec,
        cancelled: isAppointmentCancelled(appt),
        rescheduled: false, // orphans are already dropped by fetchDueSalesCalls
        statusWrittenAtMs: ghl?.statusWrittenAtMs,
        startMs: ghl?.startMs ?? Date.parse(appt.scheduled_start),
      });

      if (verdict.conflict) {
        tally.conflicts += 1;
        conflicts.push({ appt, ghlStatus: ghl?.status, rec });
        continue;
      }
      if (verdict.action === 'skip')  { tally.skipped += 1; continue; }
      // Deferred, not asked: the outcome card already puts one question to the
      // closer, and any answer it gets settles attendance too.
      if (verdict.action === 'defer') { tally.deferred += 1; continue; }

      if (dryRun) {
        console.log(`[dry-run] would set ${eventId} (${appt.prospect?.full_name}) → showed (${verdict.reason})`);
        tally.showed += 1;
        continue;
      }
      try {
        await ghlSetAppointmentStatus(eventId, 'showed');
        tally.showed += 1;
        console.log(`Appointment ${eventId} (${appt.prospect?.full_name}) → showed`);
      } catch (writeErr) {
        tally.failed += 1;
        console.error(`Appointment-status write failed for ${eventId}:`, writeErr.message);
      }
    }

    // A recording that contradicts a human's answer is never auto-resolved.
    if (conflicts.length) {
      const lines = ['⚠️ *Appointment status conflicts* — REVI recorded a call that GHL says did not happen:\n'];
      conflicts.forEach(({ appt, ghlStatus, rec }) => {
        lines.push(`• ${appt.prospect?.full_name || 'Unknown'} — ${formatICTime(appt.scheduled_start, { month: 'short', day: 'numeric' })} — GHL: *${ghlStatus}*, REVI: ${rec?.durationMin || '?'} min recording`);
      });
      lines.push('');
      lines.push('I did not change these. Someone should decide which is right.');
      await slack.client.chat.postMessage({ channel: RON_SLACK_ID, text: lines.join('\n') }).catch(() => {});
    }

    const verdict = tally.failed || tally.conflicts
      ? `⚠️ ISSUES — considered ${tally.considered}, showed ${tally.showed}, conflicts ${tally.conflicts}, write failures ${tally.failed}, deferred ${tally.deferred}`
      : `✅ ALL GREEN — considered ${tally.considered}, marked showed ${tally.showed}, skipped ${tally.skipped}, deferred to the outcome card ${tally.deferred}`;
    console.log(`Appointment-status sync complete. ${verdict}`);

    // On 2026-08-19 the first live run wrote 0 of 6 (a GHL auth blink) and said
    // so only in a log line nobody reads — the exact silent-failure shape this
    // repo keeps re-learning. A run that writes NOTHING while having work to do
    // now reaches a human. Threshold, not >0, so one transient blink stays quiet.
    if (tally.failed >= 3 || (tally.failed > 0 && tally.showed === 0)) {
      await slack.client.chat.postMessage({
        channel: RON_SLACK_ID,
        text: `⚠️ Appointment-status sync: ${tally.failed} write(s) failed, ${tally.showed} succeeded (of ${tally.considered} considered).\nAttendance was NOT marked for those calls. Check the Railway logs for the status code — a GHL 401 blink is retried once and usually clears; a persistent one means the token lost its calendar scope.`,
      }).catch(() => {});
    }
    return tally;
  } catch (err) {
    console.error('Appointment-status sync error:', err.message);
    await slack.client.chat.postMessage({ channel: RON_SLACK_ID, text: `⚠️ Appointment-status sync failed: ${err.message}` }).catch(() => {});
    return tally;
  }
}


// ─── STALE UNCLAIMED LEADS ────────────────────────────────────────────────────
// A lead posted to #ng-sales-goats with no time-based fallback if nobody reacts
// sat unclaimed for 11 days before anyone noticed (2026-07-02 → 2026-07-13,
// Adrian RM / excelenciaenbombas@hotmail.com). These two jobs close that gap:
// a 2h in-thread nag, and a daily digest to Ron of everything still open.

// A lead whose GHL contact already carries a terminal disposition was worked —
// it just never got a ✋ in Slack, because claiming is only needed for leads
// nobody has picked up yet. SANTIAGO LONDONO U (no-fit + cancelled, self-booked
// with Jose on 2026-07-28) sat in the daily sweep for 22 straight days as a
// result. That is worse than useless: a sweep that cries wolf trains the reader
// to scroll past it, which is exactly how the original 11-day Adrian RM lead was
// missed. Tag names verified against the live location tag list on 2026-08-19 —
// `cancelled` is deliberately NOT terminal on its own (a cancelled call still
// deserves a human), it only lands here alongside `no-fit`.
const TERMINAL_LEAD_TAGS = new Set(
  (process.env.STALE_LEAD_TERMINAL_TAGS || 'won-deal,no-fit,generic-lost,activation-done,call-showed')
    .split(',').map(s => s.trim().toLowerCase()).filter(Boolean),
);

function hasTerminalLeadTag(contact) {
  if (!contact || !Array.isArray(contact.tags)) return false;
  return contact.tags.some(t => TERMINAL_LEAD_TAGS.has(String(t).trim().toLowerCase()));
}

// One GHL read per unclaimed lead, capped so a broken claim flow (which would
// leave hundreds unclaimed) cannot turn a daily cron into hundreds of GHL calls
// and trip the rate limiter. Leads are checked oldest-first; anything past the
// cap is still REPORTED, just not tag-checked, and the skip is logged rather
// than swallowed.
const TERMINAL_TAG_LOOKUP_CAP = 50;

async function dropDispositionedLeads(unclaimed) {
  const kept = [];
  let looked = 0, hidden = 0, failed = 0;
  for (const lead of unclaimed) {
    if (!lead.contactId || looked >= TERMINAL_TAG_LOOKUP_CAP) { kept.push(lead); continue; }
    looked += 1;
    let contact = null;
    try {
      contact = await ghlGetContact(lead.contactId);
    } catch (err) {
      // Fail SAFE, in the direction of noise: a GHL outage must never be able to
      // hide a genuinely unclaimed lead. Keep it and say why.
      failed += 1;
      console.error(`Stale-lead tag check: GHL lookup threw for ${lead.contactId} (${err.message}) — keeping the lead.`);
      kept.push(lead);
      continue;
    }
    if (!contact) { failed += 1; kept.push(lead); continue; }
    if (hasTerminalLeadTag(contact)) { hidden += 1; continue; }
    kept.push(lead);
  }
  if (unclaimed.length > TERMINAL_TAG_LOOKUP_CAP) {
    console.log(`Stale-lead tag filter: ${unclaimed.length - TERMINAL_TAG_LOOKUP_CAP} lead(s) past the ${TERMINAL_TAG_LOOKUP_CAP}-lookup cap were NOT tag-checked — still reported.`);
  }
  if (hidden || failed) {
    console.log(`Stale-lead tag filter: ${hidden} already-dispositioned lead(s) filtered out, ${failed} lookup failure(s) kept.`);
  }
  return kept;
}

// Shared query: leads posted since `sinceMs` with no matching setter_claims row.
// Mirrors the leads-today report's join pattern (two queries + diff in JS —
// supabase-js has no LEFT JOIN, and nothing else in this file uses raw SQL for it).
async function getUnclaimedLeads(sinceMs) {
  const { data: leadRows, error: leadErr } = await supabase
    .from('lead_posts')
    .select('slack_message_ts, slack_channel_id, contact_id, full_name, source, posted_at')
    .gte('posted_at', new Date(sinceMs).toISOString())
    .order('posted_at', { ascending: true });
  // Throw rather than swallow-and-return-[] — both callers treat an empty
  // result as "genuinely nothing unclaimed" (nag check: nothing to nag;
  // sweep: no DM to Ron). A swallowed query error would look identical to
  // "all clear" and silently mask exactly the kind of failure this feature
  // exists to catch. Let the caller's own try/catch (which already DMs Ron
  // on any thrown error) handle it instead.
  if (leadErr) throw new Error(`getUnclaimedLeads: lead_posts query failed: ${leadErr.message}`);

  // Group by slack_message_ts, not contact_id — the FB→WhatsApp dup-skip path
  // in handleGHLWebhook gives a duplicate contact its own lead_posts row that
  // reuses the ORIGINAL lead's slack_message_ts. Without grouping, a dup's
  // contact_id (which never appears in setter_claims) would false-positive an
  // already-claimed lead as unclaimed.
  const byTs = new Map(); // ts → { slackChannelId, fullName, source, postedAt (earliest), contactIds:Set }
  for (const r of (leadRows || [])) {
    if (!r.slack_message_ts) continue;
    let g = byTs.get(r.slack_message_ts);
    if (!g) {
      g = { slackChannelId: r.slack_channel_id, fullName: r.full_name, source: r.source, postedAt: r.posted_at, contactIds: new Set() };
      byTs.set(r.slack_message_ts, g);
    }
    if (r.contact_id) g.contactIds.add(r.contact_id);
  }

  const allContactIds = [...new Set([...byTs.values()].flatMap(g => [...g.contactIds]))];
  // Chunk the .in() lookup — the daily sweep's 30-day window can pull 700+
  // distinct contact IDs (confirmed live 2026-07-15: 705 over 30 days), and
  // cramming all of them into one filter produces a URL long enough to fail
  // at the fetch/network layer (raw "TypeError: fetch failed", not a normal
  // Postgrest error) before Supabase ever sees the request. 200/chunk keeps
  // each request comfortably short regardless of how much lead volume grows.
  const CLAIM_LOOKUP_CHUNK_SIZE = 200;
  const claimedContactIds = new Set();
  for (let i = 0; i < allContactIds.length; i += CLAIM_LOOKUP_CHUNK_SIZE) {
    const chunk = allContactIds.slice(i, i + CLAIM_LOOKUP_CHUNK_SIZE);
    const { data: claimRows, error: claimErr } = await supabase
      .from('setter_claims')
      .select('ghl_contact_id')
      .in('ghl_contact_id', chunk);
    if (claimErr) throw new Error(`getUnclaimedLeads: setter_claims query failed: ${claimErr.message}`);
    for (const c of (claimRows || [])) claimedContactIds.add(c.ghl_contact_id);
  }

  const unclaimed = [];
  for (const [ts, g] of byTs.entries()) {
    const isClaimed = [...g.contactIds].some(cid => claimedContactIds.has(cid));
    if (isClaimed) continue;
    unclaimed.push({
      slackMessageTs: ts,
      slackChannelId: g.slackChannelId,
      contactId: [...g.contactIds][0] || null,
      fullName: g.fullName,
      source: g.source,
      postedAt: g.postedAt,
    });
  }
  unclaimed.sort((a, b) => new Date(a.postedAt) - new Date(b.postedAt));
  // Oldest-first before the tag filter, so the lookup cap spends its budget on
  // the leads that have been sitting longest.
  return await dropDispositionedLeads(unclaimed);
}

// Resolves the @setters Slack user group to its mention string at runtime
// rather than hardcoding a subteam ID that could go stale if the group is
// ever recreated. Cached after first successful lookup. Falls back to
// <!channel> (and logs loudly) if the group can't be found, so the nag never
// silently fails to notify anyone.
let _cachedSetterMention = null;
async function resolveSetterUsergroupMention() {
  if (_cachedSetterMention) return _cachedSetterMention;
  try {
    const res = await slack.client.usergroups.list();
    const group = (res.usergroups || []).find(g => g.handle === 'setters');
    if (group) {
      _cachedSetterMention = `<!subteam^${group.id}|@setters>`;
      return _cachedSetterMention;
    }
    console.warn('resolveSetterUsergroupMention: no usergroup with handle "setters" found — falling back to <!channel>.');
  } catch (err) {
    console.error('resolveSetterUsergroupMention: usergroups.list failed:', err.message, '— falling back to <!channel>.');
  }
  return '<!channel>';
}

// Nag check — every 30 min, 7 AM–9 PM CR (see cron registration below). Posts
// a threaded reminder the first time a lead crosses 2h unclaimed, then keeps
// re-nagging the SAME thread every additional hour it stays unclaimed (2026-07-16:
// changed from one-shot per Ron — a single nag wasn't enough pressure, wanted
// escalating hourly pings on the lead's own thread, not just the once-daily
// Ron digest). agent_knowledge tracks last-nagged-at per lead, not just
// ever-nagged, so the gate is "≥1h since last nag" rather than "never nagged."
async function runStaleLeadNagCheck(_correlationId) {
  console.log('Running stale-lead nag check...');
  try {
    const leads = await getUnclaimedLeads(Date.now() - 24 * 60 * 60 * 1000);
    const staleLeads = leads.filter(l => Date.now() - new Date(l.postedAt).getTime() >= 2 * 60 * 60 * 1000);
    if (!staleLeads.length) { console.log('Stale-lead nag check: nothing past the 2h threshold.'); return; }

    const mention = await resolveSetterUsergroupMention();

    for (const lead of staleLeads) {
      if (!lead.contactId) continue;
      const dedupKey = `stale-lead-nag:${lead.contactId}`;
      const { data: existing } = await supabase
        .from('agent_knowledge')
        .select('value')
        .eq('key', dedupKey)
        .limit(1);
      const lastNaggedAt = existing && existing.length ? new Date(existing[0].value).getTime() : null;
      if (lastNaggedAt && Date.now() - lastNaggedAt < 60 * 60 * 1000) continue; // nagged within the last hour — wait for the next hourly window

      const hoursUnclaimed = Math.floor((Date.now() - new Date(lead.postedAt).getTime()) / (60 * 60 * 1000));
      try {
        await slack.client.chat.postMessage({
          channel: lead.slackChannelId,
          thread_ts: lead.slackMessageTs,
          text: `${mention} this lead has been unclaimed for ${hoursUnclaimed}+ hours — ${lead.fullName || 'Unknown'}. React ✅ to claim.`,
        });
        await upsertKnowledge('process', dedupKey, new Date().toISOString(), 'stale-lead-nag');
        console.log(`Stale-lead nag posted for contact ${lead.contactId} (${lead.fullName}), ${hoursUnclaimed}h unclaimed.`);
      } catch (postErr) {
        // Don't write the dedupe key if the post failed — better to retry
        // next tick than silently suppress a lead that was never actually nagged.
        console.error(`Stale-lead nag: post failed for contact ${lead.contactId}:`, postErr.message);
      }
    }
    console.log('Stale-lead nag check complete.');
  } catch (err) {
    console.error('Stale-lead nag check error:', err.message);
    await slack.client.chat.postMessage({ channel: RON_SLACK_ID, text: `⚠️ Stale-lead nag check cron failed: ${err.message}` });
  }
}

// Daily sweep — 6 PM CR every day. ONE DM to Ron listing every lead still
// unclaimed at sweep time. Leads persist on this list every day until claimed
// (no "seen it once" suppression) — that persistence is the actual fix for
// the 11-day blind spot this feature exists to close.
async function runStaleLeadDailySweep(_correlationId) {
  console.log('Running stale-lead daily sweep...');
  try {
    const leads = await getUnclaimedLeads(Date.now() - 30 * 24 * 60 * 60 * 1000);
    if (!leads.length) { console.log('Stale-lead daily sweep: nothing unclaimed.'); return; }

    const formatElapsed = (postedAt) => {
      const ms = Date.now() - new Date(postedAt).getTime();
      const hours = Math.floor(ms / (60 * 60 * 1000));
      if (hours < 24) return `${hours}h ago`;
      const days = Math.floor(hours / 24);
      return `${days}d ${hours % 24}h ago`;
    };

    const lines = [
      `UNCLAIMED LEADS — END OF DAY SWEEP`,
      '',
      `${leads.length} lead(s) still unclaimed as of 6 PM CR:`,
      '',
      ...leads.map(l => `• ${l.fullName || 'Unknown'} — posted ${formatElapsed(l.postedAt)} (${l.source || 'Unknown source'})`),
    ];
    await slack.client.chat.postMessage({ channel: RON_SLACK_ID, text: lines.join('\n') });
    console.log(`Stale-lead daily sweep sent to Ron (${leads.length} unclaimed).`);
  } catch (err) {
    console.error('Stale-lead daily sweep error:', err.message);
    await slack.client.chat.postMessage({ channel: RON_SLACK_ID, text: `⚠️ Stale-lead daily sweep cron failed: ${err.message}` });
  }
}

// ─── WEEKLY SALES & MARKETING RECAP ──────────────────────────────────────────
// Fires Friday 5 PM CR. DMs Ron only. Every number is computed LIVE at report
// time from the authoritative sources (lead_posts, revops_appointments +
// revops_sales_outcomes, Meta insights with an exact matching date window) —
// never from summing daily metric_observations snapshots, which is how the old
// version reported "0 leads / 0.1% close rate" for months.

// CR-anchored recap window: Monday 00:00 CR → now, plus the same elapsed
// window one week earlier for WoW deltas. Meta windows are CR calendar dates
// (Graph API time_range is date-granular).
function crRecapBounds(now = new Date()) {
  const toDateStr = d => d.toLocaleDateString('en-CA', { timeZone: 'America/Costa_Rica' });
  const todayStr   = toDateStr(now);
  const todayStart = new Date(`${todayStr}T06:00:00Z`); // CR midnight (UTC-6, no DST)
  const crDow      = new Date(`${todayStr}T00:00:00Z`).getUTCDay(); // 0=Sun..6=Sat
  const monOffset  = crDow === 0 ? 6 : crDow - 1;
  const weekStart  = new Date(todayStart.getTime() - monOffset * 24 * 60 * 60 * 1000);
  const weekAgoMs  = 7 * 24 * 60 * 60 * 1000;
  const label = d => d.toLocaleDateString('en-US', { month: 'long', day: 'numeric', timeZone: 'America/Costa_Rica' });
  return {
    startIso:     weekStart.toISOString(),
    endIso:       now.toISOString(),
    prevStartIso: new Date(weekStart.getTime() - weekAgoMs).toISOString(),
    prevEndIso:   new Date(now.getTime() - weekAgoMs).toISOString(),
    metaSince:     toDateStr(weekStart),
    metaUntil:     todayStr,
    prevMetaSince: toDateStr(new Date(weekStart.getTime() - weekAgoMs)),
    prevMetaUntil: toDateStr(new Date(now.getTime() - weekAgoMs)),
    monLabel: label(weekStart),
    endLabel: label(now),
  };
}

// Deduped Form/WA-funnel leads in window, per-source — same one-logical-lead-
// per-slack_message_ts dedup as the LEADS TODAY branch of getSalesIntelligence
// (FB→WA dup contacts share the original post's ts). Throws on Supabase error.
async function getWeeklyLeadStats(startIso, endIso) {
  const { data, error } = await supabase
    .from('lead_posts')
    .select('slack_message_ts, source, posted_at')
    .gte('posted_at', startIso)
    .lt('posted_at', endIso);
  if (error) throw new Error(error.message);
  const byTs = new Map(); // ts → source (earliest row wins)
  for (const r of (data || [])) {
    if (!r.slack_message_ts || byTs.has(r.slack_message_ts)) continue;
    byTs.set(r.slack_message_ts, r.source || 'Unknown');
  }
  const bySource = {};
  for (const src of byTs.values()) bySource[src] = (bySource[src] || 0) + 1;
  return { total: byTs.size, bySource };
}

// One-cohort weekly pipeline stats, flywheel-filtered, GHL-native.
// Show/close/revenue numbers all come from the scheduled_start cohort so they
// are internally consistent; "booked" (booked_at cohort) is a separate
// booking-velocity number and is never a show-rate denominator.
async function getWeeklySalesStats(startIso, endIso) {
  const excludeIds = await getNonFlywheelCallIds();

  const { data: bookedRaw, error: bookedErr } = await portalSupabase
    .from('revops_appointments')
    .select('id, setter_id, iclosed_call_id, ghl_appointment_id')
    .gte('booked_at', startIso)
    .lt('booked_at', endIso);
  if (bookedErr) throw new Error(bookedErr.message);
  const booked = filterFlywheelAppts(bookedRaw, excludeIds);

  const { data: schedRaw, error: schedErr } = await portalSupabase
    .from('revops_appointments')
    .select('id, scheduled_start, attended, iclosed_call_id, ghl_appointment_id')
    .gte('scheduled_start', startIso)
    .lt('scheduled_start', endIso);
  if (schedErr) throw new Error(schedErr.message);
  const appts = filterFlywheelAppts(schedRaw, excludeIds);

  let outcomesById = {};
  const apptIds = appts.map(a => a.id);
  if (apptIds.length) {
    const { data: outcomes, error: outErr } = await portalSupabase
      .from('revops_sales_outcomes')
      .select('appointment_id, outcome, closed_revenue')
      .in('appointment_id', apptIds);
    if (outErr) throw new Error(outErr.message);
    outcomesById = Object.fromEntries((outcomes || []).map(o => [o.appointment_id, o]));
  }

  let held = 0, noShows = 0, pending = 0, rescheduled = 0, won = 0, revenue = 0;
  const nowMs = Date.now();
  for (const a of appts) {
    // Outcome on a call that hasn't happened yet is a reschedule leftover
    // (GHL reuses the appointment row) — treat as pending, same as the
    // closer/setter weekly stats.
    const o = new Date(a.scheduled_start).getTime() > nowMs ? null : outcomesById[a.id];
    const c = classifyOutcome(o);
    if (a.attended === true || c.showed) held += 1;
    if (c.noShow)      noShows += 1;
    if (c.pending)     pending += 1;
    if (c.rescheduled) rescheduled += 1;
    if (o) {
      if ((o.outcome || '').toLowerCase() === 'won') won += 1;
      revenue += Number(o.closed_revenue || 0);
    }
  }

  // Wins LOGGED in the window regardless of when the call happened — a deal
  // closed this week on a call scheduled two weeks ago shows here, not in
  // `won` (which is cohort-scoped). Same created_at basis as the daily
  // iclosed_sales_yest scraper, so the recap can't contradict the anomaly DMs.
  const { data: loggedRows, error: loggedErr } = await portalSupabase
    .from('revops_sales_outcomes')
    .select('id, closed_revenue, created_at')
    .eq('outcome', 'won')
    .gte('created_at', startIso)
    .lt('created_at', endIso);
  if (loggedErr) throw new Error(loggedErr.message);

  // Show rate excludes pending + rescheduled from the denominator (house
  // convention — fresh bookings with no outcome yet must not drag it down).
  const decided = held + noShows;
  return {
    bookedInWindow:     booked.length,
    selfBookedInWindow: booked.filter(a => !a.setter_id).length, // VSL self-book proxy
    scheduled: appts.length,
    held, noShows, pending, rescheduled, won, revenue,
    wonLogged:     (loggedRows || []).length,
    revenueLogged: (loggedRows || []).reduce((s, r) => s + Number(r.closed_revenue || 0), 0),
    showRatePct:  decided > 0 ? Math.round((held / decided) * 100) : null,
    closeRatePct: held > 0 ? Math.round((won / held) * 100) : null,
  };
}

// '(▲ +3 WoW)' / '(▼ −2 WoW)' / '(= flat WoW)'; '' when either side is missing.
function fmtDelta(cur, prev, { money = false } = {}) {
  if (cur == null || prev == null) return '';
  const d = cur - prev;
  const mag = money ? `$${Math.abs(d).toLocaleString()}` : `${Math.abs(d)}`;
  if (d === 0) return ' (= flat WoW)';
  return d > 0 ? ` (▲ +${mag} WoW)` : ` (▼ −${mag} WoW)`;
}

// First line of a knowledge value, trimmed to a sentence boundary — never cut
// mid-sentence like the old substring(0, 120) truncation.
function firstSentence(text, max = 200) {
  const line = (text || '').split('\n')[0].trim();
  if (line.length <= max) return line;
  const cut = line.slice(0, max);
  const lastStop = Math.max(cut.lastIndexOf('. '), cut.lastIndexOf('! '), cut.lastIndexOf('? '));
  if (lastStop > 40) return cut.slice(0, lastStop + 1);
  return cut.slice(0, cut.lastIndexOf(' ')) + '…';
}

// 2–3 sentence READ paragraph from the deterministic stat block. This is the
// one place report-feedback lessons are actually APPLIED (the old version
// prepended them as text without changing anything). Non-fatal: '' on failure
// and the recap posts without a READ section.
async function composeRecapNarrative(statText, lessons) {
  try {
    const lessonBlock = (lessons || []).length
      ? `\n\nApply these standing corrections from Ron's past feedback:\n${lessons.map(l => `- ${l.value}`).join('\n')}`
      : '';
    const res = await anthropic.messages.create({
      model: 'claude-haiku-4-5-20251001',
      max_tokens: 300,
      messages: [{ role: 'user', content: `You are Max, NeuroGrowth's ops agent, writing the executive takeaway for Ron's weekly sales & marketing recap. Below is the final stat block (all numbers are verified — copy any number verbatim, never recompute or rephrase a stat's meaning).\n\n${statText}${lessonBlock}\n\nWrite the takeaway: 2–3 SHORT sentences, 60 words max, plain text. Sentence 1: the single most important thing that happened this week. Sentence 2: the one thing to watch or act on. Restate at most two numbers total. No preamble, no markdown, no bullet points, no run-on sentences.` }],
    });
    return res.content.filter(b => b.type === 'text').map(b => b.text).join('').trim();
  } catch (err) {
    console.error('composeRecapNarrative error:', err.message);
    return '';
  }
}

async function runWeeklySalesMarketingRecap(_correlationId, { preview = false } = {}) {
  console.log('Running weekly sales & marketing recap...');
  try {
    const b = crRecapBounds();

    // Each fetch degrades to null on failure so one broken source costs its
    // section, not the whole report.
    const safe = (fn, label) => fn().catch(err => { console.error(`Recap ${label} error:`, err.message); return null; });

    const [leads, prevLeads, sales, prevSales, meta, prevMeta, anomalyRows, opsFlagRows, clientRows, lessons] = await Promise.all([
      safe(() => getWeeklyLeadStats(b.startIso, b.endIso), 'leads'),
      safe(() => getWeeklyLeadStats(b.prevStartIso, b.prevEndIso), 'prev-leads'),
      safe(() => getWeeklySalesStats(b.startIso, b.endIso), 'sales'),
      safe(() => getWeeklySalesStats(b.prevStartIso, b.prevEndIso), 'prev-sales'),
      getMetaAdsRange(b.metaSince, b.metaUntil),          // already null on error
      getMetaAdsRange(b.prevMetaSince, b.prevMetaUntil),
      // σ-anomalies only — source-scoped so nightly-learning ops items, REVI
      // infra health, and confidential rows can never appear as "anomalies".
      safe(async () => {
        const { data, error } = await supabase
          .from('agent_knowledge')
          .select('key, value, updated_at')
          .eq('category', 'alert')
          .eq('source', 'anomaly-detection')
          .ilike('key', 'anomaly:%')
          .gte('updated_at', b.startIso)
          .order('updated_at', { ascending: false });
        if (error) throw new Error(error.message);
        return data || [];
      }, 'anomalies'),
      safe(async () => {
        const { data, error } = await supabase
          .from('agent_knowledge')
          .select('key')
          .eq('category', 'alert')
          .in('source', ['nightly-learning', 'revi-cross-check'])
          .not('key', 'ilike', 'confidential:%')
          .gte('updated_at', b.startIso);
        if (error) throw new Error(error.message);
        return data || [];
      }, 'ops-flags'),
      safe(async () => {
        const { data, error } = await supabase
          .from('agent_knowledge')
          .select('key, value, updated_at')
          .eq('category', 'client')
          .gte('updated_at', b.startIso)
          .order('updated_at', { ascending: false })
          .limit(30);
        if (error) throw new Error(error.message);
        return data || [];
      }, 'client-updates'),
      safe(() => getReportLessons('weekly-sales-marketing-recap'), 'lessons'),
    ]);

    const parts = [`📈 Weekly Sales & Marketing Recap — ${b.monLabel} to ${b.endLabel} (week through report time)`];

    // ── LEADS ─────────────────────────────────────────────────────────────
    parts.push('', '`LEADS`');
    if (leads) {
      parts.push(`Form/WA funnel (GHL lead feed): *${leads.total}*${fmtDelta(leads.total, prevLeads?.total)}`);
      const srcLine = Object.entries(leads.bySource).sort((a, z) => z[1] - a[1]).map(([s, c]) => `${s}: ${c}`).join(' | ');
      if (srcLine) parts.push(`  ${srcLine}`);
    } else {
      parts.push('Lead feed unavailable this run.');
    }
    if (sales) parts.push(`VSL self-booked calls: *${sales.selfBookedInWindow}*${fmtDelta(sales.selfBookedInWindow, prevSales?.selfBookedInWindow)}`);

    // ── SALES PIPELINE ────────────────────────────────────────────────────
    // Two DIFFERENT cohorts share this block, so both are labelled explicitly.
    // "Booked" counts calls whose booked_at fell in the window (booking
    // velocity); everything below it counts calls whose scheduled_start fell in
    // the window (what actually happened). They legitimately disagree — a call
    // booked Monday for next month is in the first and not the second. The
    // first live run printed "Booked: 14" directly above "Held: 7 … 13 awaiting
    // outcome" (7+13=20), which reads as an arithmetic error unless the two
    // cohorts and the scheduled total are named on the page.
    parts.push('', '`SALES PIPELINE` (flywheel only)');
    if (sales) {
      const waiting = [];
      if (sales.pending)     waiting.push(`${sales.pending} awaiting outcome`);
      if (sales.rescheduled) waiting.push(`${sales.rescheduled} rescheduled`);
      parts.push(`New bookings made this week: *${sales.bookedInWindow}*${fmtDelta(sales.bookedInWindow, prevSales?.bookedInWindow)} (booking velocity — may be for later dates)`);
      parts.push(`Calls on the calendar this week: *${sales.scheduled}*${fmtDelta(sales.scheduled, prevSales?.scheduled)}`);
      // Show rate denominator is decided calls only (held + no-shows); pending
      // and rescheduled are excluded, so say what it was computed on.
      const decided = sales.held + sales.noShows;
      parts.push(
        `  Held: *${sales.held}* | No-shows: ${sales.noShows} | Show rate: *${sales.showRatePct == null ? 'n/a' : sales.showRatePct + '%'}*` +
        (sales.showRatePct == null ? '' : ` (of ${decided} decided)`) +
        (waiting.length ? ` · ${waiting.join(', ')}` : '')
      );
      parts.push(
        `  Closes: *${sales.won}* | Close rate: *${sales.closeRatePct == null ? 'n/a' : sales.closeRatePct + '%'}* | Revenue: *$${sales.revenue.toLocaleString()}*` +
        fmtDelta(sales.revenue, prevSales?.revenue, { money: true })
      );
      // Close rate on a thin decided base is a discipline signal, not a
      // performance one — say so rather than letting "0%" read as lost deals.
      if (sales.pending > 0 && sales.pending >= sales.held) {
        parts.push(`  ⚠️ ${sales.pending} of ${sales.scheduled} calls have no outcome logged — close rate is computed on the ${sales.held} held call(s) that do.`);
      }
      // A win logged this week on an older call is real revenue that the
      // cohort line above won't show — surface it so "Closes: 0" can't
      // contradict a "sale won yesterday" anomaly DM.
      if (sales.wonLogged !== sales.won) {
        parts.push(`Wins logged this week (incl. calls from earlier weeks): *${sales.wonLogged}* | $${sales.revenueLogged.toLocaleString()}`);
      }
    } else {
      parts.push('Pipeline data unavailable this run.');
    }

    // ── META ADS ──────────────────────────────────────────────────────────
    parts.push('', `\`META ADS\` (${b.metaSince} → ${b.metaUntil})`);
    if (meta) {
      parts.push(`Spend: *$${meta.spend.toFixed(2)}*${fmtDelta(Math.round(meta.spend), prevMeta ? Math.round(prevMeta.spend) : null, { money: true })} | CTR: ${meta.ctr.toFixed(2)}% | CPC: $${meta.cpc.toFixed(2)} | CPM: $${meta.cpm.toFixed(2)}`);
      parts.push(`Form leads: ${meta.leads} | Form CPL: *${meta.formCpl == null ? 'n/a' : '$' + meta.formCpl.toFixed(2)}*`);
      parts.push(`Sales (Meta Purchase events): ${meta.purchases} | CAC: *${meta.cac == null ? 'n/a' : '$' + meta.cac.toFixed(2)}*`);
      if (leads) {
        const delta = leads.total - meta.leads;
        const flag = meta.leads > 0 && Math.abs(delta) / meta.leads > 0.2 ? ' — over 20% apart, worth a look' : '';
        parts.push(`Reconciliation: Meta ${meta.leads} form leads vs ${leads.total} in GHL lead feed (Δ ${delta >= 0 ? '+' : ''}${delta})${flag}`);
      }
    } else {
      parts.push('Meta data unavailable this run.');
    }

    // ── ANOMALIES (σ-detection only) ──────────────────────────────────────
    parts.push('', '`ANOMALIES` (σ-detection, this week)');
    if (anomalyRows && anomalyRows.length) {
      // Latest per metric: key = anomaly:<metric>:<date>, rows arrive newest-first.
      const seen = new Set();
      let shown = 0;
      for (const a of anomalyRows) {
        const metric = a.key.split(':')[1] || a.key;
        if (seen.has(metric)) continue;
        seen.add(metric);
        parts.push(`• ${firstSentence(a.value)}`);
        if (++shown >= 5) break;
      }
      const hidden = seen.size < anomalyRows.length ? anomalyRows.length - shown : 0;
      if (hidden > 0) parts.push(`(+${hidden} repeat firings of the same metrics)`);
    } else {
      parts.push('No σ-anomalies this week.');
    }
    if (opsFlagRows && opsFlagRows.length) {
      parts.push(`${opsFlagRows.length} ops/client flags logged this week (already DMed as they fired).`);
    }

    // ── CLIENT UPDATES (latest per client) ────────────────────────────────
    if (clientRows && clientRows.length) {
      parts.push('', '`CLIENT UPDATES`');
      const seenClients = new Set();
      let shown = 0;
      for (const r of clientRows) {
        const slug = r.key.split(':')[1] || r.key;
        if (seenClients.has(slug)) continue;
        seenClients.add(slug);
        // Nightly learning occasionally files a report/digest title as a
        // "client" — real client slugs are short; skip implausible ones.
        if (slug.length > 40 || /reporte|report|semana|resumen/i.test(slug)) continue;
        parts.push(`• ${slug.replace(/-/g, ' ').toUpperCase()}: ${firstSentence(r.value)}`);
        if (++shown >= 6) break;
      }
    }

    // ── READ (narrative — where feedback lessons are actually applied) ────
    const statText = parts.join('\n');
    const narrative = await composeRecapNarrative(statText, lessons);
    if (narrative) parts.splice(1, 0, '', '`READ`', narrative);

    parts.push('', 'See something off? Reply to this message tagging @Max with the correction.');
    const msg = parts.join('\n');

    if (preview) return msg;
    await slack.client.chat.postMessage({ channel: RON_SLACK_ID, text: msg });
    console.log('Weekly sales & marketing recap sent to Ron.');
  } catch (err) {
    console.error('Weekly sales & marketing recap error:', err.message);
    if (preview) return `⚠️ Recap preview failed: ${err.message}`;
    await slack.client.chat.postMessage({ channel: RON_SLACK_ID, text: `⚠️ Weekly sales & marketing recap cron failed: ${err.message}` });
  }
}

// Nightly learning — 11:30 PM CR (infrastructure — reads all channels, saves knowledge)
cron.schedule('30 5 * * *',  wrapCronJob('runNightlyLearning', runNightlyLearning),     { timezone: 'America/Costa_Rica' });

// Weekly portal trend analysis — Friday 4:30 PM CR (infrastructure — saves intel to knowledge base)
cron.schedule('30 22 * * 5', wrapCronJob('runWeeklyPortalTrends', async (c) => { await runWeeklyPortalTrends(c); }),  { timezone: 'America/Costa_Rica' });

// Weekly sales & marketing recap — Friday 5:00 PM CR (DMs Ron with 7-day sales + marketing summary)
cron.schedule('0 17 * * 5',  wrapCronJob('runWeeklySalesMarketingRecap', async (c) => { await runWeeklySalesMarketingRecap(c); }), { timezone: 'America/Costa_Rica' });

// Monday gap detection — 8:00 AM CR (infrastructure — posts to ops channel)
cron.schedule('0 14 * * 1',  wrapCronJob('runMondayGapDetection', async (c) => { await runMondayGapDetection(c); }),  { timezone: 'America/Costa_Rica' });

// Proactive alerts disabled 2026-05-07 — redundant with Monday gap detection,
// daily standups, anomaly detection, and Friday weekly recap. Posts were recycling
// the same stale-alert list every day with no fresh signal.
// cron.schedule('0 15 * * *',  wrapCronJob('runProactiveAlerts', runProactiveAlerts),     { timezone: 'America/Costa_Rica' });
// cron.schedule('0 20 * * *',  wrapCronJob('runProactiveAlerts', runProactiveAlerts),     { timezone: 'America/Costa_Rica' });

// Proactive team DMs — 8:00 AM CR Mon–Fri (infrastructure — DMs Josue, Valeria, Felipe, client_success based on client status)
// runProactiveDMs merged into runFulfillmentStandup (9 AM) — stalled flags now inline per role
// cron.schedule('0 8 * * 1-5', wrapCronJob('runProactiveDMs', async (c) => { await runProactiveDMs(c); }),        { timezone: 'America/Costa_Rica' });

// Phase 1 anomaly detection: daily 6am Costa Rica. Scrapes 13 metrics (v2 — per-funnel), recomputes
// rolling baselines, fires DMs at >= 1.5σ deltas. See ANOMALY_ROUTING for who gets pinged.
cron.schedule('0 6 * * *',   async () => {
  try { await runAnomalyDetection(); }
  catch (err) {
    console.error('Anomaly cron hard failure:', err.message);
    try { await slack.client.chat.postMessage({ channel: RON_SLACK_ID, text: `Anomaly cron crashed: ${err.message}` }); }
    catch (_) {}
  }
}, { timezone: 'America/Costa_Rica' });

// REVI cross-checks — 6:30 AM CR Tue–Sat (each run covers the prior business day).
// Heartbeat on REVI's ingestion + delivery; alert-only, silent when healthy.
cron.schedule('30 6 * * 2-6', wrapCronJob('runReviCrossChecks', async (c) => { await runReviCrossChecks(c); }), { timezone: 'America/Costa_Rica' });

// Fulfillment morning standup — 9:00 AM CR Mon–Fri (DMs Josue, Valeria, Felipe with daily priorities)
cron.schedule('0 9 * * 1-5', wrapCronJob('runFulfillmentStandup', async (c) => { await runFulfillmentStandup(c); }),  { timezone: 'America/Costa_Rica' });

// Sales morning standup — 9:00 AM CR Mon–Fri (DMs setters and closers with role-specific briefs)
cron.schedule('0 9 * * 1-5', wrapCronJob('runSalesStandup', async (c) => { await runSalesStandup(c); }),              { timezone: 'America/Costa_Rica' });

// Sales call prep — every hour Mon–Fri (DMs closer 4h before any strategy call)
cron.schedule('0 * * * 1-5',  wrapCronJob('runSalesCallPrep', async (c) => { await runSalesCallPrep(c); }),       { timezone: 'America/Costa_Rica' });

// Unlogged GHL outcome reminders — 4 PM CR every day (DMs closers, escalates to Ron after 3d).
// "Logged?" is read directly from revops_sales_outcomes (by appointment_id) —
// reliable since the dash.neurogrowth.io ingestion fix (PR #3, 2026-05-19).
cron.schedule('0 16 * * *',   wrapCronJob('runUnloggedOutcomeReminders', async (c) => { await runUnloggedOutcomeReminders(c); }), { timezone: 'America/Costa_Rica' });

// Appointment-status sweep (Paso 1) — 3 PM CR, one hour BEFORE the outcome
// reminders so today's calls are already marked Showed by the time their
// outcome card goes out. Ships in dry-run: set APPT_STATUS_SYNC_MODE=live to
// let it write to GHL. Kill switch is that same env var.
cron.schedule('0 15 * * *',   wrapCronJob('runAppointmentStatusSync', async (c) => { await runAppointmentStatusSync(c); }), { timezone: 'America/Costa_Rica' });

// REVI prospect notes — 2 PM CR daily, before the status sweep and the cards,
// so the note is already on the contact when a closer opens it from either.
// Ships in dry-run: REVI_NOTES_MODE=live arms it (also the kill switch).
cron.schedule('0 14 * * *',   wrapCronJob('runReviProspectNotesSync', async (c) => { await runReviProspectNotesSync(c); }), { timezone: 'America/Costa_Rica' });

// Won-deal handoff notes — 9 AM + 5 PM CR daily. Condenses the REVI read of a
// won closing call into a Spanish executive summary for fulfillment onboarding
// and lands it in the portal CRM via the ng_apply_won_handoff_note RPC
// (outcome row + prospect activity + customer_closing_data.internal_notes once
// the client exists). Ships in dry-run: WON_HANDOFF_MODE=live arms it (also
// the kill switch).
cron.schedule('0 9,17 * * *', wrapCronJob('runWonHandoffNotes', async (c) => { await runWonHandoffNotes(c); }), { timezone: 'America/Costa_Rica' });

// Stale-lead nag check — every 30 min, 7 AM–9 PM CR (business hours only). Nags
// the #ng-sales-goats thread, tagging @setters, once a lead has sat unclaimed 2+ hours,
// then re-nags the SAME thread every additional hour until claimed (agent_knowledge
// dedupe key stale-lead-nag:<contact_id> gates on "≥1h since last nag", not one-shot).
cron.schedule('*/30 7-20 * * *', wrapCronJob('runStaleLeadNagCheck', async (c) => { await runStaleLeadNagCheck(c); }), { timezone: 'America/Costa_Rica' });

// Stale-lead daily sweep — 6:00 PM CR every day (no weekend exclusion — leads
// can go unclaimed on weekends too). DMs Ron ONE summary of every lead still
// unclaimed at sweep time; repeat appearances are intentional, not deduped.
cron.schedule('0 18 * * *', wrapCronJob('runStaleLeadDailySweep', async (c) => { await runStaleLeadDailySweep(c); }), { timezone: 'America/Costa_Rica' });

// Stalled prospect follow-ups — 11 AM CR Mon–Fri. Dry-run DMs setters their stalled list;
// live auto-send activates only when STALLED_FOLLOWUPS_LIVE='true' (deferred until dry-run validated).
cron.schedule('0 11 * * 1-5', wrapCronJob('runStalledProspectFollowups', async (c) => { await runStalledProspectFollowups(c); }), { timezone: 'America/Costa_Rica' });

// Auto strike mover — every 2h, 7 AM–9 PM CR. Advances setter pipeline cards on
// real human WhatsApp follow-ups so nobody has to drag them. Every 2h (not
// hourly) because a full sweep costs ~1 API call per candidate card and the 20h
// debounce means a card can only move once a day anyway. Defaults to DRY RUN —
// set STRIKE_MOVER_MODE=live on Railway to arm it.
// 30 min, not the 10-min default: this one legitimately runs long. Over 147 sweeps
// it averaged 6.4 min and peaked at 17.4 min, throttled across ~1500 contacts. The
// default would abandon a perfectly healthy sweep. Every other cron peaks under 80s.
cron.schedule('0 7-21/2 * * *', wrapCronJob('runAutoStrikeMover', async (c) => { await runAutoStrikeMover(c); }, { timeoutMs: 30 * 60 * 1000 }), { timezone: 'America/Costa_Rica' });

// Daily strike-mover digest to the sales channel — 9:30 PM CR, right after the
// day's last sweep, so the 24h window covers exactly today's 8 sweeps.
cron.schedule('30 21 * * *', wrapCronJob('runStrikeSalesDigest', async (c) => { await runStrikeSalesDigest(c); }), { timezone: 'America/Costa_Rica' });

// Setter attribution reconcile cron RETIRED 2026-07-26 — GHL records the booker
// natively in revops_appointments.setter_id; the leaderboard reads it directly.

// NOTE: the standalone Wed+Sat "SETTER LEADERBOARD" post (runSetterLeaderboard, EOD-only,
// MTD) was retired 2026-06-02 — its content is now part of the single unified weekly
// LEADERBOARD ("Weekly Closer Comparison" scheduled task), which renders both setters and
// closers from the shared GHL-first stat source so the numbers can never diverge.

// Email proxy reply poller — top of every hour, 8am–8pm CR Mon–Fri.
// Polls Gmail for new replies on active email_threads rows and DMs the setter.
if (EMAIL_PROXY_LIVE) {
  cron.schedule('0 8-20 * * 1-5', wrapCronJob('runEmailReplyPoller', async (c) => { await runEmailReplyPoller(c); }), { timezone: 'America/Costa_Rica' });
}

// ─── GHL LEAD WEBHOOK ─────────────────────────────────────────────────────────
// ROSTER 2026-07-29 (Ron): active setters are Sebastian, Oscar and William only.
// Joseph and Debbanny have rolled off.
//
// Deliberate split — departed staff stay in the NAME maps but are removed from the
// ACTION maps. Name resolution is retrospective: leaderboards and weekly reports
// resolve setter_id on historical rows, and dropping a departed setter's name there
// would silently relabel their past calls "Unknown" rather than removing them.
// The action maps below decide who gets DM'd, nudged and can claim leads — that is
// where a stale entry does damage.
const GHL_USER_NAMES = {
  'cuttpcov7ztlvyjkhdx8': 'Joseph Salazar', 'cUTTPGov7ZTLvyjKHdX8': 'Joseph Salazar', // historical — rolled off 2026-07
  'zcmdiz2eerapd80w2zop': 'Oscar M',         'ZcmdIz2EEraPd80W2zop': 'Oscar M',
  'n8mvtuhbbby7qppqnmr7': 'William B',       'N8mvtuHbbbY7QppqNMr7': 'William B',
  'wdjte1temxfr0lpi5rgv': 'Sebastian S',     'Wdjte1temxfR0lpi5RGV': 'Sebastian S',
  '5orsahkh2joujb5fczrp': 'Debbanny',        '5OrSaHkh2joUjB5FCZrP': 'Debbanny', // historical — rolled off 2026-05-03
  'gqymykpddltdxvbkfl2c': 'Jonathan Madriz', 'gqYMYkpDDlTdxvBkfl2C': 'Jonathan Madriz',
  'izlta0jy5orkymsyltjv': 'Jose Carranza',   'izLTA0jy5OrKyMvyltjV': 'Jose Carranza',
  'zogw530idnpofqqnfssc': 'Ron Duarte',      'zoGW530iDnPOFqQNfssc': 'Ron Duarte',
};

// ACTION map — routes DMs and nudges. Departed setters MUST NOT appear here.
const GHL_TO_SLACK = {
  'oscar': 'U0B1S1UMH9P', 'oscar m': 'U0B1S1UMH9P', 'oscar neurogrowth': 'U0B1S1UMH9P',
  'william': 'U0B16P6DQ2F', 'william b': 'U0B16P6DQ2F', 'william neurogrowth': 'U0B16P6DQ2F',
  'sebastian': 'U0BFA4SRVQC', 'sebastian s': 'U0BFA4SRVQC', 'sebastian serrano': 'U0BFA4SRVQC', 'sebastian neurogrowth': 'U0BFA4SRVQC',
  'jonnathan': 'U0APYAE0999', 'jonathan': 'U0APYAE0999', 'jonathan madriz': 'U0APYAE0999',
  'jose': 'U0AMTEKDCPN', 'jose carranza': 'U0AMTEKDCPN',
  'zcmdiz2eerapd80w2zop': 'U0B1S1UMH9P', 'n8mvtuhbbby7qppqnmr7': 'U0B16P6DQ2F',
  'gqymykpddltdxvbkfl2c': 'U0APYAE0999', 'izlta0jy5orkymsyltjv': 'U0AMTEKDCPN',
  'wdjte1temxfr0lpi5rgv': 'U0BFA4SRVQC',
};

// ACTION map — lead-claim flow: Slack user → GHL user ID (reaction_added handler).
// Active staff only: Debbanny rolled off 2026-05-03, Joseph 2026-07.
const SLACK_TO_GHL_USER = {
  'U0B1S1UMH9P': 'ZcmdIz2EEraPd80W2zop', // Oscar M
  'U0B16P6DQ2F': 'N8mvtuHbbbY7QppqNMr7', // William B
  'U0BFA4SRVQC': 'Wdjte1temxfR0lpi5RGV', // Sebastian Serrano
  'U0APYAE0999': 'gqYMYkpDDlTdxvBkfl2C', // Jonathan Madriz
  'U0AMTEKDCPN': 'izLTA0jy5OrKyMvyltjV', // Jose Carranza
  'U05HXGX18H3': 'zoGW530iDnPOFqQNfssc', // Ron Duarte (testing)
};

// ACTION map — fallback: GHL ships payload.user.email reliably even when
// customData.assignedTo is empty/broken. Used by /webhook/ghl-claim when the GHL
// token doesn't resolve. Active staff only.
const EMAIL_TO_GHL_USER_ID = {
  'oscar.neurogrowth@gmail.com':  'ZcmdIz2EEraPd80W2zop',
  'william.neurogrowth@gmail.com': 'N8mvtuHbbbY7QppqNMr7',
  'sebastian.neurogrowth@gmail.com': 'Wdjte1temxfR0lpi5RGV',
  'jonathan.neurogrowth@gmail.com': 'gqYMYkpDDlTdxvBkfl2C',
  'jose.neurogrowth@gmail.com': 'izLTA0jy5OrKyMvyltjV',
  'ronny.duarte@neurogrowth.io': 'zoGW530iDnPOFqQNfssc',
};

const LEAD_CHANNEL_ID = 'C0AJANQBYUE'; // #ng-sales-goats

// ─── SELF-SERVE (VSL / landing page) LEADS ───────────────────────────────────
// Prospects who book themselves off the VSL land on their own calendar. Nobody
// set them, so they must NOT go out as a claimable lead card: a setter reacting
// ✋ takes ownership of an organic booking and the setter/leaderboard numbers
// stop being true. These get an FYI threaded under the Make booking card instead.
const SELF_SERVE_CALENDAR_ID = 'HXLeEjxpa0gdiTPNiAzc'; // "LinkedIn Flywheel - Self Serving"
const SELF_SERVE_SOURCE_RE   = /self[\s._-]?serv/i;
// The Make booking card lands ~16s after the booking, but ordering vs. this
// webhook is not guaranteed — wait it out before looking for the parent post.
const SELF_SERVE_FYI_DELAY_MS = 90 * 1000;

const LEAD_CLAIM_EMOJIS = new Set(['raised_hand', 'hand', 'white_check_mark', 'heavy_check_mark']);
const LEAD_CLAIMED_EMOJI = 'white_check_mark';

function resolveSetterSlackId(assignedUser) {
  if (!assignedUser) return null;
  const lower = assignedUser.toLowerCase().trim();
  if (GHL_TO_SLACK[lower]) return GHL_TO_SLACK[lower];
  for (const [key, slackId] of Object.entries(GHL_TO_SLACK)) {
    if (lower.includes(key) || key.includes(lower)) return slackId;
  }
  return null;
}

// Phone normalization for our three markets: CR (8-digit national), US/CA
// (10-digit NANP), and MX (10-digit, post-2019 IFT unification). CR is
// unambiguous on length. US vs MX collide at 10 digits, so when no country
// code is present we require a country hint (GHL contact's `country` field)
// to classify confidently — without a hint we display raw and refuse to
// write back to GHL, since auto-tagging an MX lead as +1 (or vice versa)
// would corrupt the CRM record.
function normalizePhone(raw, countryHint) {
  if (!raw) return null;
  const digits = String(raw).replace(/\D/g, '');
  if (!digits) return null;
  const hint = (countryHint || '').toString().trim().toUpperCase();

  // If an explicit country code is present, trust it (this is the only way to
  // safely disambiguate a bare 10-digit US vs MX number).
  let cc = null;
  let national = digits;
  if (digits.length === 11 && digits.startsWith('1'))        { cc = '1';   national = digits.slice(1); }
  else if (digits.length === 12 && digits.startsWith('52'))  { cc = '52';  national = digits.slice(2); }
  else if (digits.length === 13 && digits.startsWith('521')) { cc = '52';  national = digits.slice(3); }
  else if (digits.length >= 11  && digits.startsWith('506')) { cc = '506'; national = digits.slice(3); }

  if (cc) {
    const ok = (cc === '506' && national.length === 8) || ((cc === '1' || cc === '52') && national.length === 10);
    if (!ok) return { e164: null, display: String(raw).trim(), confident: false };
    return { e164: `+${cc}${national}`, display: formatPhoneDisplay(cc, national), confident: true };
  }

  // No prefix — judge by length, falling back to the country hint when 10
  // digits are ambiguous between US/CA and MX.
  if (national.length === 8) {
    return { e164: `+506${national}`, display: formatPhoneDisplay('506', national), confident: true };
  }
  if (national.length === 10) {
    if (hint === 'US' || hint === 'CA') cc = '1';
    else if (hint === 'MX')             cc = '52';
    else return { e164: null, display: String(raw).trim(), confident: false };
    return { e164: `+${cc}${national}`, display: formatPhoneDisplay(cc, national), confident: true };
  }
  return { e164: null, display: String(raw).trim(), confident: false };
}

function formatPhoneDisplay(cc, national) {
  if (cc === '1')   return `+1 (${national.slice(0, 3)}) ${national.slice(3, 6)}-${national.slice(6)}`;
  if (cc === '52')  return `+52 ${national.slice(0, 3)} ${national.slice(3, 6)} ${national.slice(6)}`;
  if (cc === '506') return `+506 ${national.slice(0, 4)} ${national.slice(4)}`;
  return `+${cc}${national}`;
}

// Post the self-serve FYI as a thread reply under the Make booking card for the
// same contact. Falls back to a top-level post when no parent is found, so the
// lead is never silently swallowed. Returns the ts the lead was recorded under.
async function postSelfServeLeadFyi({ text, contactId, correlationId }) {
  let threadTs = null;
  try {
    const history = await slack.client.conversations.history({ channel: LEAD_CHANNEL_ID, limit: 30 });
    const parent = (history.messages || []).find(m =>
      typeof m.text === 'string' && m.text.includes(`/contacts/detail/${contactId}`) && !m.thread_ts);
    if (parent) threadTs = parent.ts;
  } catch (histErr) {
    console.error('self-serve FYI: history lookup failed:', histErr.message);
  }
  const posted = await slack.client.chat.postMessage({
    channel: LEAD_CHANNEL_ID,
    text,
    ...(threadTs ? { thread_ts: threadTs } : {}),
  });
  logActivity({ event_type: 'slack_message', event_source: 'slack', action: 'outbound', channel_id: LEAD_CHANNEL_ID, output: { text: text.slice(0, 2000) }, correlation_id: correlationId });
  console.log(threadTs
    ? `self-serve FYI threaded under booking card ${threadTs} (contact ${contactId})`
    : `self-serve FYI posted top-level — no booking card found for contact ${contactId}`);
  return threadTs || posted.ts;
}

async function handleGHLWebhook(req, res) {
  // Auth check — reject requests that don't include the correct secret header
  // Set GHL_WEBHOOK_SECRET in env vars and configure GHL to send it as x-ghl-secret
  const secret = process.env.GHL_WEBHOOK_SECRET;
  if (secret) {
    const provided = req.headers['x-ghl-secret'] || req.headers['x-webhook-secret'];
    if (provided !== secret) {
      console.warn('GHL webhook rejected — invalid or missing secret header');
      res.writeHead(401, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: 'Unauthorized' }));
      return;
    }
  }
  try {
    let body = '';
    req.on('data', chunk => { body += chunk.toString(); });
    req.on('end', async () => {
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ received: true }));
      try {
        const payload = JSON.parse(body);
        console.log('GHL webhook received:', JSON.stringify(payload).substring(0, 300));
        console.log('GHL raw payload keys:', Object.keys(payload).join(', '));
        if (payload.contact)         console.log('GHL contact object:', JSON.stringify(payload.contact).substring(0, 300));
        if (payload.customData)      console.log('GHL customData:', JSON.stringify(payload.customData).substring(0, 300));
        if (payload.attributionSource) console.log('GHL attributionSource:', JSON.stringify(payload.attributionSource).substring(0, 200));

        const cd = payload.customData || payload.custom_data || {};
        const ct = payload.contact || {};
        const fullName   = cd.fullName || payload.fullName || payload.full_name || `${payload.first_name || ct.firstName || ''} ${payload.last_name || ct.lastName || ''}`.trim() || ct.name || payload.name || 'Unknown';
        const email      = cd.email      || payload.email      || ct.email    || '';
        const phone      = cd.phone      || payload.phone      || ct.phone    || '';
        const phoneCountry = cd.country  || ct.country         || payload.country || '';
        const phoneInfo  = normalizePhone(phone, phoneCountry);
        const phoneDisplay = phoneInfo ? phoneInfo.display : phone;
        const contactAttr = (payload.contact && payload.contact.attributionSource) || {};
        const attrSource  = payload.attributionSource || {};
        const sourceRaw   = cd.source || payload.source || payload.contact_source || ct.source || contactAttr.sessionSource || contactAttr.medium || attrSource.medium || payload.triggerData?.source || '';
        const sourceMap   = { 'whatsapp_coex':'WhatsApp','whatsapp':'WhatsApp','fb':'Facebook','facebook':'Facebook','instagram':'Instagram','organic':'Organic','paid':'Paid Ads','email':'Email','sms':'SMS','referral':'Referral' };
        const source      = sourceMap[sourceRaw.toLowerCase()] || sourceRaw;
        const assignedTo  = cd.assignedTo || cd['opportunity.assignedTo'] || payload.assignedTo || payload['opportunity.assignedTo'] || payload.assigned_user || payload.contact_owner || ct.assignedTo || ct.assigned_user || '';
        const contactId   = cd.contactId  || payload.contactId  || payload.contact_id || ct.id || payload.id || '';
        const locationId  = payload.locationId || payload.location_id || process.env.GHL_LOCATION_ID || '';
        const leadContext = cd.context || payload.context || '';

        let resolvedAssignedTo = assignedTo;
        // Fetched once and reused: the webhook payload alone can't tell us whether
        // this lead booked itself (self-serve) — the contact's source/attribution can.
        let ghlContact = null;
        if (contactId) {
          try {
            const contactRes = await fetch(`https://services.leadconnectorhq.com/contacts/${contactId}`, { headers: { 'Authorization': `Bearer ${process.env.GHL_API_KEY}`, 'Version': '2021-07-28' } });
            const contactData = await contactRes.json();
            ghlContact = contactData.contact || contactData || null;
          } catch (apiErr) { console.error('GHL contact lookup error:', apiErr.message); }
        }
        if (!resolvedAssignedTo && ghlContact) {
          try {
            const assignedUser = ghlContact.assignedTo || '';
            if (assignedUser) {
              try {
                const usersRes  = await fetch(`https://services.leadconnectorhq.com/users/?locationId=${process.env.GHL_LOCATION_ID}`, { headers: { 'Authorization': `Bearer ${process.env.GHL_API_KEY}`, 'Version': '2021-07-28' } });
                const usersData = await usersRes.json();
                const users     = usersData.users || usersData || [];
                const matched   = users.find(u => u.id === assignedUser);
                resolvedAssignedTo = matched ? (matched.name || matched.firstName || matched.email) : assignedUser;
              } catch (userErr) { resolvedAssignedTo = assignedUser; }
              const displayName = GHL_USER_NAMES[resolvedAssignedTo] || GHL_USER_NAMES[resolvedAssignedTo.toLowerCase()];
              if (displayName) resolvedAssignedTo = displayName;
              console.log(`GHL resolved assignedTo: ${resolvedAssignedTo}`);
            }
          } catch (apiErr) { console.error('GHL assignedTo resolve error:', apiErr.message); }
        }

        // Self-serve = booked straight off the VSL/landing page. Three independent
        // signals because each one alone has a hole: the webhook source is missing
        // on some payloads, and a contact created long before the VSL launched keeps
        // its original source but still books on the self-serve calendar.
        const attributionMediumIds = [ghlContact?.attributionSource?.mediumId, ghlContact?.lastAttributionSource?.mediumId].filter(Boolean);
        const isSelfServe = SELF_SERVE_SOURCE_RE.test(sourceRaw || '')
          || SELF_SERVE_SOURCE_RE.test(ghlContact?.source || '')
          || attributionMediumIds.includes(SELF_SERVE_CALENDAR_ID);
        if (isSelfServe) console.log(`GHL lead ${contactId} detected as SELF-SERVE (source="${sourceRaw}", contactSource="${ghlContact?.source || ''}") — no setter DM, no claim card`);

        console.log('GHL parsed:', { fullName, email, phone, source, assignedTo: resolvedAssignedTo, contactId });

        // Write the corrected E.164 number back to GHL so the CRM stops
        // mis-flagging country codes and the team no longer fixes it by hand.
        // Conservative on purpose: only when we're confident about the country
        // (8-digit CR / 10-digit US) AND the stored value actually differs.
        if (contactId && phoneInfo?.confident && phoneInfo.e164
            && phoneInfo.e164.replace(/\D/g, '') !== String(phone).replace(/\D/g, '')) {
          try {
            const ghlAuth = { 'Authorization': `Bearer ${process.env.GHL_API_KEY}`, 'Version': '2021-07-28', 'Content-Type': 'application/json' };
            const putRes  = await fetch(`https://services.leadconnectorhq.com/contacts/${contactId}`, {
              method: 'PUT', headers: ghlAuth, body: JSON.stringify({ phone: phoneInfo.e164 }),
            });
            if (putRes.ok) {
              console.log(`GHL phone normalized: "${phone}" → ${phoneInfo.e164} (contact ${contactId})`);
              logActivity({ event_type: 'ghl_update', event_source: 'ghl', action: 'phone_normalized', correlation_id: newCorrelationId(), output: { contact_id: contactId, from: String(phone), to: phoneInfo.e164 } });
            } else {
              console.warn(`GHL phone PUT failed ${putRes.status}: ${(await putRes.text()).slice(0, 200)}`);
            }
          } catch (phoneErr) { console.error('GHL phone normalize error:', phoneErr.message); }
        }

        const ghlLink      = contactId ? `https://app.gohighlevel.com/v2/location/${locationId}/contacts/detail/${contactId}` : 'https://app.gohighlevel.com';

        // Cross-contact dedup. GHL has multiple workflows that each create a
        // contact off the same Meta Lead Ads event (FB Lead Form + Paid Social),
        // so the same human shows up twice within ~30s with different contact_ids.
        // Look back 30 min on normalized phone/email and, if matched, drop a threaded
        // note on the original post instead of spamming a second top-level lead post.
        const phoneLast10 = (phone.match(/\d/g) || []).join('').slice(-10) || null;
        const emailLower  = (email || '').trim().toLowerCase() || null;
        const firstNameLower = (fullName || '').trim().toLowerCase().split(/\s+/)[0] || '';
        const namePrefix3    = firstNameLower.slice(0, 3) || null;
        let dupOriginal = null;
        if (contactId && (phoneLast10 || emailLower)) {
          try {
            const dupSince = new Date(Date.now() - 30 * 60 * 1000).toISOString();
            const orFilter = [
              phoneLast10 ? `phone_last10.eq.${phoneLast10}` : null,
              emailLower  ? `email_lower.eq.${emailLower}`  : null,
            ].filter(Boolean).join(',');
            const { data: dupRows, error: dupErr } = await supabase
              .from('lead_posts')
              .select('contact_id, slack_message_ts, slack_channel_id, posted_at')
              .neq('contact_id', contactId)
              .gte('posted_at', dupSince)
              .or(orFilter)
              .order('posted_at', { ascending: false })
              .limit(1);
            if (dupErr) console.error('lead_posts dedup lookup failed:', dupErr.message);
            else if (dupRows && dupRows.length) dupOriginal = dupRows[0];
          } catch (dupLookupErr) {
            console.error('lead_posts dedup lookup threw:', dupLookupErr.message);
          }
        }

        // FB-Lead-Form → WhatsApp signature: GHL auto-creates a second contact
        // when the lead clicks the "Start WhatsApp chat" CTA on the post-submit
        // page. The auto-created contact has a different phone (their WA account),
        // no email, and a partial name — so phone/email dedup above misses it.
        // Fuzzy-match instead: same name first-3-chars, within 5 min, original
        // came in with Source=Facebook, incoming is non-Facebook social. Tight
        // filters to keep false-positive rate low.
        if (!dupOriginal && contactId && namePrefix3 && namePrefix3.length === 3 && !emailLower) {
          const incomingSource = (source || '').toLowerCase();
          const looksLikeWaSpawned = incomingSource && !incomingSource.includes('facebook') && !incomingSource.includes('vsl');
          if (looksLikeWaSpawned) {
            try {
              const fuzzySince = new Date(Date.now() - 5 * 60 * 1000).toISOString();
              const { data: fuzzyRows, error: fuzzyErr } = await supabase
                .from('lead_posts')
                .select('contact_id, slack_message_ts, slack_channel_id, posted_at, source, full_name')
                .neq('contact_id', contactId)
                .gte('posted_at', fuzzySince)
                .eq('name_prefix3', namePrefix3)
                .order('posted_at', { ascending: false })
                .limit(1);
              if (fuzzyErr) console.error('lead_posts fuzzy dedup lookup failed:', fuzzyErr.message);
              else if (fuzzyRows && fuzzyRows.length) {
                const candidate = fuzzyRows[0];
                const candSource = (candidate.source || '').toLowerCase();
                if (candSource.includes('facebook')) {
                  console.log(`GHL webhook: fuzzy FB→WA dup match — incoming ${contactId} ("${fullName}", ${source}) matches recent ${candidate.contact_id} ("${candidate.full_name}", ${candidate.source})`);
                  dupOriginal = candidate;
                }
              }
            } catch (fuzzyErrThrow) {
              console.error('lead_posts fuzzy dedup lookup threw:', fuzzyErrThrow.message);
            }
          }
        }

        if (dupOriginal) {
          console.log(`GHL webhook: contact ${contactId} matches recent lead_posts row (orig contact ${dupOriginal.contact_id}, ts ${dupOriginal.slack_message_ts}). Skipping top-level post.`);
          const dupNote = [
            `⚠️ GHL spawned a second contact for this lead — *${fullName || 'unknown'}*`,
            source ? `Source on duplicate: ${source}` : null,
            phone  ? `Phone: ${phone}` : null,
            email  ? `Email: ${email}` : null,
            `🔗 ${ghlLink}`,
            `_Original contact above is the one to work; this duplicate is junk and can be ignored / merged in GHL._`,
          ].filter(Boolean).join('\n');
          try {
            await slack.client.chat.postMessage({
              channel: dupOriginal.slack_channel_id || LEAD_CHANNEL_ID,
              thread_ts: dupOriginal.slack_message_ts,
              text: dupNote,
            });
          } catch (dupPostErr) {
            console.error('lead_posts dedup thread reply failed:', dupPostErr.message);
          }
          try {
            await supabase.from('lead_posts').upsert({
              contact_id: contactId,
              slack_message_ts: dupOriginal.slack_message_ts,
              slack_channel_id: dupOriginal.slack_channel_id || LEAD_CHANNEL_ID,
              phone_last10: phoneLast10,
              email_lower: emailLower,
              source: source || null,
              full_name: fullName || null,
              name_prefix3: namePrefix3,
            }, { onConflict: 'contact_id' });
          } catch (lpErr) {
            console.error('lead_posts dup-row upsert failed:', lpErr.message);
          }
          return;
        }

        const ghlCorr = newCorrelationId();

        // Self-serve: the prospect already booked, so there is nothing to claim and
        // no setter to brief. Announce it as a thread reply on the booking card.
        if (isSelfServe) {
          const fyiText = [
            `🌱 *Self-serve (orgánico)* — ${fullName} agendó por su cuenta desde el VSL. Sin setter, no hay nada que reclamar.`,
            email        ? `📧 ${email}` : null,
            phoneDisplay ? `📱 ${phoneDisplay}` : null,
            contactId    ? `🔗 ${ghlLink}` : null,
          ].filter(Boolean).join('\n');
          setTimeout(async () => {
            try {
              const ts = await postSelfServeLeadFyi({ text: fyiText, contactId, correlationId: ghlCorr });
              if (contactId && ts) {
                await supabase.from('lead_posts').upsert({
                  contact_id: contactId,
                  slack_message_ts: ts,
                  slack_channel_id: LEAD_CHANNEL_ID,
                  phone_last10: phoneLast10,
                  email_lower: emailLower,
                  source: source || null,
                  full_name: fullName || null,
                  name_prefix3: namePrefix3,
                }, { onConflict: 'contact_id' });
              }
            } catch (fyiErr) {
              console.error('self-serve FYI post failed:', fyiErr.message);
            }
          }, SELF_SERVE_FYI_DELAY_MS);
          return;
        }

        const setterSlackId = resolveSetterSlackId(resolvedAssignedTo);
        const contextLine = leadContext ? `\n- Context: ${leadContext}` : '';
        const actionGuidance = leadContext
          ? `Their first action should reflect the Context above (e.g. if it mentions booking friction, the setter should call/DM the lead now to unblock the booking, not just "reach out").`
          : `Their first action (reach out now, check GHL).`;
        const prompt = `You are Max, the NeuroGrowth PM Agent. A new lead just came in and was assigned to a setter.\n\nLead details:\n- Name: ${fullName}\n- Email: ${email || 'not provided'}\n- Phone: ${phone || 'not provided'}\n- Source: ${source}\n- Assigned to: ${resolvedAssignedTo || 'unassigned'}${contextLine}\n- GHL link: ${ghlLink}\n\nWrite a short, direct Slack DM to the setter (2-3 sentences max) telling them: 1. A new lead came in and was assigned to them. 2. Key lead details. 3. ${actionGuidance} Sound like a colleague, not a bot. No markdown. Include the GHL link.`;
        const tGhl = Date.now();
        const briefingResponse = await anthropic.messages.create({ model: 'claude-sonnet-4-6', max_tokens: 300, messages: [{ role: 'user', content: prompt }] });
        logLlmFromAnthropicResponse(briefingResponse, Date.now() - tGhl, ghlCorr);
        const briefing = briefingResponse.content.filter(b => b.type === 'text').map(b => b.text).join('');
        if (!briefing || !briefing.trim()) { console.error('GHL webhook: empty briefing from Claude'); return; }

        if (setterSlackId) {
          await slack.client.chat.postMessage({ channel: setterSlackId, text: briefing });
          logActivity({ event_type: 'slack_message', event_source: 'slack', action: 'outbound', channel_id: setterSlackId, output: { text: briefing.slice(0, 2000) }, correlation_id: ghlCorr });
          console.log(`GHL lead briefing sent to setter ${assignedTo} (${setterSlackId})`);
        } else {
          console.log(`GHL lead received but setter not resolved. assignedTo: "${assignedTo}". Add to GHL_TO_SLACK map if needed.`);
        }

        const claimHint = !resolvedAssignedTo
          ? `\n_React with ✋ or ✅ to claim — Max will assign you the contact + the opportunity in GHL._`
          : '';
        const channelNote = [
          `🆕 *New Lead* — ${fullName}`,
          email             ? `📧 ${email}`   : null,
          phoneDisplay      ? `📱 ${phoneDisplay}` : null,
          source && source !== 'Unknown channel' ? `📌 Source: ${source}` : null,
          resolvedAssignedTo ? `👤 Assigned to: ${resolvedAssignedTo}` : null,
          leadContext        ? `📝 ${leadContext}` : null,
          contactId          ? `🔗 ${ghlLink}` : null,
        ].filter(Boolean).join('\n') + claimHint;
        const leadPost = await slack.client.chat.postMessage({
          channel: LEAD_CHANNEL_ID,
          text: channelNote,
          metadata: contactId ? {
            event_type: 'ghl_lead',
            event_payload: { contact_id: contactId, location_id: locationId, full_name: fullName, email: email || null, correlation_id: ghlCorr },
          } : undefined,
        });
        logActivity({ event_type: 'slack_message', event_source: 'slack', action: 'outbound', channel_id: LEAD_CHANNEL_ID, output: { text: channelNote.slice(0, 2000) }, correlation_id: ghlCorr });

        // Persist contact_id → message_ts mapping so /webhook/ghl-claim can mirror
        // GHL-direct claims back to this Slack post. Non-fatal on insert failure.
        if (contactId && leadPost?.ts) {
          try {
            await supabase.from('lead_posts').upsert({
              contact_id: contactId,
              slack_message_ts: leadPost.ts,
              slack_channel_id: LEAD_CHANNEL_ID,
              phone_last10: phoneLast10,
              email_lower: emailLower,
              source: source || null,
              full_name: fullName || null,
              name_prefix3: namePrefix3,
            }, { onConflict: 'contact_id' });
          } catch (lpErr) {
            console.error('lead_posts insert failed:', lpErr.message);
          }
        }
      } catch (parseErr) { console.error('GHL webhook parse error:', parseErr.message); }
    });
  } catch (err) { console.error('GHL webhook handler error:', err.message); res.writeHead(500); res.end('error'); }
}

// ─── REVERSE-MIRROR: GHL claim → Slack post update ───────────────────────────
// Setter assigns a contact in GHL UI directly (no Slack ✋/✅) → GHL workflow
// fires this webhook → Max finds the corresponding #ng-sales-goats post via
// lead_posts lookup → adds ✅ + threaded reply mirroring the GHL claim, and
// records a setter_claims row with claim_source='ghl_direct'.
//
// Required GHL workflow body: { contact_id, assigned_to, location_id }
// Required header: x-ghl-secret (reuses GHL_WEBHOOK_SECRET env var)

async function handleGHLClaimWebhook(req, res) {
  const secret = process.env.GHL_WEBHOOK_SECRET;
  if (secret) {
    const provided = req.headers['x-ghl-secret'] || req.headers['x-webhook-secret'];
    if (provided !== secret) {
      console.warn('GHL claim webhook rejected — invalid or missing secret header');
      res.writeHead(401, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: 'Unauthorized' }));
      return;
    }
  }
  try {
    let body = '';
    req.on('data', chunk => { body += chunk.toString(); });
    req.on('end', async () => {
      // Always 200 quickly — GHL retries on non-2xx
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ received: true }));
      try {
        const payload = JSON.parse(body);
        // Log full payload for debugging — strip later once stable.
        console.log('ghl-claim webhook RAW payload:', JSON.stringify(payload).slice(0, 1500));
        const cd = payload.customData || payload.custom_data || {};
        // Strip whitespace from keys (GHL Custom Data sometimes ships keys with trailing spaces — see existing /webhook/ghl-lead workflow)
        const cdNorm = {};
        for (const [k, v] of Object.entries(cd)) cdNorm[String(k).trim()] = v;
        const contactId  = cdNorm.contact_id || cdNorm.contactId  || payload.contact_id  || payload.contactId  || payload.contact?.id || '';
        let assignedTo = cdNorm.assigned_to || cdNorm.assignedTo || cdNorm.assigned_user || cdNorm.user_id || payload.assigned_to || payload.assignedTo || payload.contact?.assignedTo || '';
        let assignedToSource = assignedTo ? 'customData' : '';
        // Fallback: GHL token sometimes ships empty even on legitimate assigns. The top-level user.email
        // arrives reliably, so resolve setter id via EMAIL_TO_GHL_USER_ID. Only known active setters resolve.
        if (!assignedTo) {
          const userEmail = String(payload.user?.email || '').toLowerCase().trim();
          if (userEmail && EMAIL_TO_GHL_USER_ID[userEmail]) {
            assignedTo = EMAIL_TO_GHL_USER_ID[userEmail];
            assignedToSource = `email:${userEmail}`;
          }
        }
        const locationId = payload.location_id || payload.locationId || process.env.GHL_LOCATION_ID || '';
        console.log(`ghl-claim parsed: contactId=${contactId} assignedTo=${assignedTo} source=${assignedToSource} cdKeys=${Object.keys(cd).join('|')}`);
        if (!contactId) { console.warn('ghl-claim webhook: no contact_id in payload'); return; }
        if (!assignedTo) { console.warn(`ghl-claim webhook: no assigned_to for contact ${contactId} (unassign event, or GHL token + user.email both empty/unmapped — user.email='${payload.user?.email || ''}')`); return; }

        const claimCorr = newCorrelationId();
        console.log(`ghl-claim webhook: contact=${contactId} assignedTo=${assignedTo}`);

        // 1. Find the Slack lead post for this contact
        let leadPost;
        try {
          const { data, error } = await supabase
            .from('lead_posts')
            .select('slack_message_ts, slack_channel_id')
            .eq('contact_id', contactId)
            .single();
          if (error) throw error;
          leadPost = data;
        } catch (lookupErr) {
          console.log(`ghl-claim webhook: no lead_posts row for ${contactId} (lead pre-dates reverse-mirror or wasn't from FB Ads). Logged only.`);
          return;
        }
        if (!leadPost?.slack_message_ts) return;

        // 2. Fetch the message + check if Max already added ✅ (race with Slack reaction)
        const channel = leadPost.slack_channel_id;
        const timestamp = leadPost.slack_message_ts;
        const history = await slack.client.conversations.history({
          channel, latest: timestamp, limit: 1, inclusive: true, include_all_metadata: true,
        });
        const msg = history.messages && history.messages[0];
        if (!msg || msg.ts !== timestamp) {
          console.warn(`ghl-claim webhook: message ${timestamp} not found in ${channel}`);
          return;
        }
        const claimReaction = (msg.reactions || []).find(r => r.name === LEAD_CLAIMED_EMOJI);
        if (claimReaction && claimReaction.users && claimReaction.users.includes(process.env.SLACK_BOT_USER_ID)) {
          console.log(`ghl-claim webhook: contact ${contactId} already marked claimed in Slack — skipping`);
          return;
        }

        // 3. Resolve GHL user → Slack ID for the threaded reply
        const setterSlackId = SLACK_TO_GHL_USER && Object.entries(SLACK_TO_GHL_USER).find(([, ghl]) => ghl === assignedTo || ghl?.toLowerCase() === assignedTo.toLowerCase())?.[0];
        const setterName = GHL_USER_NAMES[assignedTo] || GHL_USER_NAMES[assignedTo.toLowerCase()] || assignedTo;
        const setterMention = setterSlackId ? `<@${setterSlackId}>` : `*${setterName}*`;

        // 4. Add ✅ + threaded reply
        try { await slack.client.reactions.add({ channel, timestamp, name: LEAD_CLAIMED_EMOJI }); }
        catch (reactErr) {
          if (!String(reactErr.data?.error || reactErr.message).includes('already_reacted')) {
            console.warn('ghl-claim reactions.add failed:', reactErr.message);
          }
        }
        await slack.client.chat.postMessage({
          channel, thread_ts: timestamp,
          text: `✅ Claimed in GHL by ${setterMention}. Updated outside Slack — synced here.`,
        });

        // 5. Record the claim
        try {
          await supabase.from('setter_claims').insert({
            ghl_contact_id: contactId,
            contact_name: msg.metadata?.event_payload?.full_name || null,
            prospect_email: msg.metadata?.event_payload?.email || null,
            slack_message_ts: timestamp,
            slack_channel_id: channel,
            claimed_by_slack_user_id: setterSlackId || 'unknown',
            claimed_by_setter_name: setterName,
            ghl_user_id: assignedTo,
            opps_reassigned: 0, // GHL did the assignment; Max didn't reassign opps in this path
            seconds_to_claim: Math.max(0, Math.round(Date.now() / 1000 - parseFloat(timestamp))),
            claim_source: 'ghl_direct',
          });
        } catch (insErr) { console.error('ghl-claim setter_claims insert failed:', insErr.message); }

        logActivity({
          event_type: 'ghl_lead_claimed', event_source: 'ghl_webhook', action: 'claim_mirror',
          channel_id: channel,
          output: { contact_id: contactId, ghl_user_id: assignedTo, setter_name: setterName },
          correlation_id: claimCorr,
        });
        console.log(`ghl-claim mirrored to Slack — contact ${contactId} → ${setterName}`);
      } catch (parseErr) {
        console.error('ghl-claim webhook parse/processing error:', parseErr.message);
      }
    });
  } catch (err) { console.error('ghl-claim webhook handler error:', err.message); res.writeHead(500); res.end('error'); }
}

// ─── STALLED PROSPECT FOLLOW-UPS (Initiative 2 — dry-run by default) ─────────
// Daily 11 AM CR Mon–Fri. Detects WhatsApp prospects who went silent ≥ 2 business
// days ago, applies skip gates, then either DMs the assigned setter a dry-run list
// (default) or auto-sends a re-engagement message (when STALLED_FOLLOWUPS_LIVE='true').

const OPT_OUT_PHRASES = [
  // English
  'not interested', 'no thanks', 'no thank you', 'stop messaging', 'unsubscribe',
  'remove me', 'please stop', "don't contact", 'do not contact', 'take me off',
  'wrong number', "i'll pass", 'i pass', 'leave me alone',
  // Spanish
  'no me interesa', 'no gracias', 'ya no', 'bórrame', 'borrame', 'quítame', 'quitame',
  'no más', 'no mas', 'dejen de', 'deja de', 'número equivocado', 'numero equivocado',
  'paso', 'no quiero', 'déjame', 'dejame en paz',
];
const DNC_TAGS = new Set(['dnc', 'blocked', 'do_not_contact', 'do-not-contact', 'unsub', 'unsubscribed', 'opt_out', 'opt-out']);

function businessDaysBetween(fromMs, toMs) {
  if (toMs <= fromMs) return 0;
  let count = 0;
  const oneDay = 24 * 60 * 60 * 1000;
  for (let t = fromMs + oneDay; t <= toMs; t += oneDay) {
    const weekday = new Date(t).toLocaleString('en-US', { timeZone: 'America/Costa_Rica', weekday: 'short' });
    if (weekday !== 'Sat' && weekday !== 'Sun') count += 1;
  }
  return count;
}

function hasOptOutSignal(messages) {
  const recent = messages.slice(-10);
  for (const m of recent) {
    const body = String(m.body || m.message || '').toLowerCase();
    if (!body) continue;
    for (const phrase of OPT_OUT_PHRASES) {
      if (body.includes(phrase)) return phrase;
    }
  }
  return null;
}

// GHL enforces a per-location burst limit, and Max is his own worst offender: the
// hourly runAutoStrikeMover pages every card in four stages and then reads a
// conversation per card. On 2026-08-17 20:00 UTC a setter's ✅ landed 22s into that
// burst, the contact PUT came back 429, and the claim was simply dropped (a second
// claim 40s later went through untouched — purely transient).
//
// Everything on the claim path and the sweep path goes through here now. Returns the
// Response unchanged so callers keep their own error handling; 429/5xx are retried on
// the full ladder, and 401 gets exactly ONE retry — see below.
const GHL_RETRY_DELAYS_MS = [1000, 3000, 8000];

// A 401 "will never come good" is the right default for an auth error, and it is what
// this wrapper originally assumed. The strike_sweep history says otherwise for GHL:
// over 30 days / 95 sweeps, 401s hit 19 distinct contacts and **18 of them 401'd
// exactly once** — a genuine permission or token problem would have failed that same
// contact on all ~95 sweeps. They are also load-independent (never once co-occurring
// with a 429, and `scanned` averaged 1528 on sweeps with a 401 vs 1549 without), so
// they are not the burst limit wearing an auth mask either. It is GHL's auth layer
// blinking, ~1 per sweep, and it clears by itself.
//
// So: one retry, one second, and only one — enough to absorb a blink. A genuinely dead
// token still fails fast (2 calls instead of 1) and is caught by the failure-spike
// thresholds instead, which is where a real auth outage belongs: STRIKE_FAILURE_LIMITS
// alerts at 5 401s in a sweep, and a dead token produces hundreds.
const GHL_UNAUTH_RETRY_DELAY_MS = 1000;

async function ghlFetch(url, init = {}, { retries = GHL_RETRY_DELAYS_MS.length, label = '' } = {}) {
  let unauthRetried = false;
  for (let attempt = 0; ; attempt++) {
    const res = await fetch(url, init);
    if (res.status === 401 && !unauthRetried) {
      unauthRetried = true;
      console.warn(`GHL 401${label ? ` on ${label}` : ''} — single retry in ${GHL_UNAUTH_RETRY_DELAY_MS}ms (transient auth blink; a dead token will trip the failure-spike alert)`);
      await new Promise(r => setTimeout(r, GHL_UNAUTH_RETRY_DELAY_MS));
      attempt--; // the 401 retry is not part of the 429/5xx budget
      continue;
    }
    if (!(res.status === 429 || res.status >= 500) || attempt >= retries) return res;
    // GHL usually omits Retry-After; honour it when present so we back off by their
    // clock rather than ours, but never park a Slack reaction handler for minutes.
    const retryAfter = Number(res.headers.get('retry-after'));
    const wait = Number.isFinite(retryAfter) && retryAfter > 0
      ? Math.min(retryAfter * 1000, 30000)
      : GHL_RETRY_DELAYS_MS[Math.min(attempt, GHL_RETRY_DELAYS_MS.length - 1)];
    console.warn(`GHL ${res.status}${label ? ` on ${label}` : ''} — retry ${attempt + 1}/${retries} in ${wait}ms`);
    await new Promise(r => setTimeout(r, wait));
  }
}

async function ghlGetConversationMessages(conversationId) {
  const res = await ghlFetch(`https://services.leadconnectorhq.com/conversations/${conversationId}/messages?limit=20`, {
    headers: { 'Authorization': `Bearer ${process.env.GHL_API_KEY}`, 'Version': '2021-07-28' },
  });
  if (!res.ok) throw new Error(`GHL messages fetch ${res.status}`);
  const data = await res.json();
  // GHL returns oldest-first or newest-first depending on endpoint version; normalize to oldest-first
  const msgs = (data.messages?.messages || data.messages || []).slice();
  msgs.sort((a, b) => new Date(a.dateAdded || a.createdAt || 0) - new Date(b.dateAdded || b.createdAt || 0));
  return msgs;
}

async function ghlGetContact(contactId) {
  const res = await ghlFetch(`https://services.leadconnectorhq.com/contacts/${contactId}`, {
    headers: { 'Authorization': `Bearer ${process.env.GHL_API_KEY}`, 'Version': '2021-07-28' },
  }, { label: `GET /contacts/${contactId}` });
  if (!res.ok) return null;
  const data = await res.json();
  return data.contact || data || null;
}

// Booking gate for stalled-prospect nudges — reads GHL directly.
//
// Replaces two lookups against `revops_iclosed_bookings` — a table that DOES NOT
// EXIST in the portal database (verified 2026-07-29: `relation does not exist`).
// Every call threw, the bare `catch` returned null, and both gates therefore read as
// "no booking" for every prospect ever checked. Failing open in the dangerous
// direction: a prospect who had already booked a call, or already sat through one,
// was still eligible to be surfaced as a stalled lead and chased.
//
// GHL is now the booking source of truth, and this is keyed on contactId rather than
// fuzzy email/phone matching, so it can't mis-join on a shared or missing email.
// One fetch per candidate, shared by both gates.
async function ghlGetContactAppointments(contactId) {
  try {
    const res = await fetch(`https://services.leadconnectorhq.com/contacts/${contactId}/appointments`, {
      headers: { 'Authorization': `Bearer ${process.env.GHL_API_KEY}`, 'Version': '2021-07-28' },
    });
    if (!res.ok) return [];
    const data = await res.json();
    return (data.events || []).filter(e => !e.deleted);
  } catch (_) { return []; }
}

// GHL returns startTime as "YYYY-MM-DD HH:mm:ss" in UTC with no offset; normalize so
// Date.parse doesn't fall back to the host's local zone.
function parseGhlApptTime(s) {
  const t = Date.parse(String(s || '').replace(' ', 'T') + 'Z');
  return Number.isNaN(t) ? 0 : t;
}

const APPT_DEAD_STATUSES = new Set(['cancelled', 'canceled', 'noshow', 'no-show', 'invalid']);

// Upcoming live appointment ⇒ they are booked, do not nudge.
function findFutureBooking(appointments, now = Date.now()) {
  const upcoming = appointments
    .filter(a => !APPT_DEAD_STATUSES.has(String(a.appointmentStatus || '').toLowerCase()))
    .map(a => ({ event_at: a.startTime, event_status: a.appointmentStatus, ts: parseGhlApptTime(a.startTime) }))
    .filter(a => a.ts > now)
    .sort((a, b) => a.ts - b.ts);
  return upcoming[0] || null;
}

// Already sat through a call ⇒ a different motion than a stalled-prospect chase.
function findAttendedCall(appointments) {
  const attended = appointments
    .filter(a => String(a.appointmentStatus || '').toLowerCase() === 'showed')
    .map(a => ({ event_at: a.startTime, event_status: a.appointmentStatus, ts: parseGhlApptTime(a.startTime) }))
    .sort((a, b) => b.ts - a.ts);
  return attended[0] || null;
}

// ── STAGE + CLIENT GUARDRAILS ───────────────────────────────────────────────────
// The nudge is built on conversations, which say nothing about where a prospect
// actually sits in the pipeline. Without these gates, someone who booked a call — or
// who already became a paying CLIENT — is exactly as eligible for a "you went quiet"
// chase as a cold lead.
const APPT_PIPELINE_STAGE_LABELS = {
  '93de6a09-78a4-4253-bea4-c1528ed6f2b3': 'New Lead',
  '4b936528-794e-40ab-812d-144b9d5e8128': 'Initial Contact',
  '92245916-0622-4f46-aabc-6091b8af5fc0': 'Strike 1',
  '6274c958-8252-4698-acc6-c6818d43a99f': 'Strike 2',
  'e639662d-6b1b-42b5-a89d-7ebd70ca97e3': 'Strike 3',
  'dc1fba03-abeb-4b47-9d29-6c308002b6c1': 'Call Booked',
  '6432cf1d-a07f-4276-8b85-77de2e57a512': 'No show / Rescheduling',
  '63d30181-4ec0-4daa-8832-a8eebe1afbeb': 'Open Deal',
  'a6c8ecb1-a9b5-46a5-8683-f6a9720cfcc9': 'Closed',
  '49cd4227-204c-4052-af94-78b961e96fab': 'No Fit',
  '8eff3fbb-3cc0-474f-bbe7-df4704e0a668': 'Lost',
};

// ALLOW-LIST by design. Every closer stage (Call Booked, No show / Rescheduling,
// Open Deal, Closed, No Fit, Lost) is excluded by omission, so a stage added to the
// pipeline later is excluded until somebody deliberately opts it in — the safe
// default for a gate whose job is to prevent unwanted outreach.
// New Lead is excluded too: a card still sitting there has never been worked, which
// is runStaleLeadNagCheck's job (claim the lead), not re-engagement.
const NUDGEABLE_STAGE_IDS = new Set([
  '4b936528-794e-40ab-812d-144b9d5e8128', // Initial Contact
  '92245916-0622-4f46-aabc-6091b8af5fc0', // Strike 1
  '6274c958-8252-4698-acc6-c6818d43a99f', // Strike 2
  'e639662d-6b1b-42b5-a89d-7ebd70ca97e3', // Strike 3
]);

// Note this only inspects the Appointment Setting pipeline. A VSL self-booked
// opportunity lives in pipeline 7KU0NBdMhhVifszmT9jo and would not appear here — the
// appointment gates above are what catch those, since they read bookings directly
// from GHL regardless of which pipeline the card sits in.
async function findNudgeableOpp(contactId) {
  const locationId = process.env.GHL_LOCATION_ID;
  try {
    const data = await ghlFetchJson(
      `https://services.leadconnectorhq.com/opportunities/search?location_id=${locationId}`
      + `&pipeline_id=${STRIKE_PIPELINE_ID}&contact_id=${contactId}&status=open&limit=5`,
    );
    const opps = data.opportunities || [];
    if (!opps.length) return { opp: null, reason: 'no_open_setter_opp' };
    const match = opps.find(o => NUDGEABLE_STAGE_IDS.has(o.pipelineStageId));
    if (match) return { opp: match, reason: null };
    const label = APPT_PIPELINE_STAGE_LABELS[opps[0].pipelineStageId] || opps[0].pipelineStageId;
    return { opp: null, reason: `stage_not_nudgeable:${label}` };
  } catch (err) {
    // FAIL CLOSED. This gate exists to keep clients and booked prospects out of the
    // chase list; an unverifiable stage must never become an implicit "go ahead".
    return { opp: null, reason: `opp_lookup_failed:${err.message}` };
  }
}

// Paying clients must never be chased as leads. Stage alone cannot guarantee that: a
// client's old setter card may still sit in a strike stage because nobody moved it,
// and duplicate contacts can hold a stray open opp. Portal `client_dashboards` is the
// billing-side source of truth.
//
// Returns null when it cannot be read, and callers treat null as "cannot verify" and
// hold EVERY nudge for that run. Sending nothing for one cycle costs a day; messaging
// a paying client as if they were a cold lead costs trust.
async function loadActiveClientEmails() {
  try {
    const { data, error } = await portalSupabase
      .from('client_dashboards')
      .select('email')
      .eq('is_active', true);
    if (error) throw new Error(error.message);
    const set = new Set((data || []).map(r => String(r.email || '').toLowerCase().trim()).filter(Boolean));
    // Zero rows means the query "worked" but told us nothing useful — treat as
    // unverifiable rather than as "there are no clients".
    if (!set.size) throw new Error('client_dashboards returned zero emails');
    return set;
  } catch (err) {
    console.error('loadActiveClientEmails failed — holding all stalled nudges this run:', err.message);
    return null;
  }
}

// A call that has already happened but is STILL marked 'confirmed' — i.e. the closer
// hasn't logged showed/no-show yet. Outcome logging routinely lags by days, so this
// state is common and ambiguous: the prospect may well have attended. Hold the nudge
// rather than chase someone who just sat through a call. Bounded to a week so a
// permanently unlogged appointment can't mute a prospect forever.
function findAppointmentPendingOutcome(appointments, now = Date.now(), windowDays = 7) {
  const cutoff = now - windowDays * 24 * 60 * 60 * 1000;
  const pending = appointments
    .filter(a => String(a.appointmentStatus || '').toLowerCase() === 'confirmed')
    .map(a => ({ event_at: a.startTime, event_status: a.appointmentStatus, ts: parseGhlApptTime(a.startTime) }))
    .filter(a => a.ts && a.ts <= now && a.ts >= cutoff)
    .sort((a, b) => b.ts - a.ts);
  return pending[0] || null;
}


// Returns true if a setter has DM'd Max `pause <contactId>` for this contact.
// Pause entries live in agent_knowledge under category=setter_pref, key=pause:<contactId>.
async function isStalledFollowupPaused(contactId) {
  try {
    const { data } = await supabase
      .from('agent_knowledge')
      .select('id')
      .eq('category', 'setter_pref')
      .eq('key', `pause:${contactId}`)
      .limit(1);
    return !!(data && data.length);
  } catch (_) { return false; }
}

// Live-send guardrail: returns { lifetime, lastSentMs } for a contact's prospect_followups.
// Used to enforce 14-day cooldown + 2-lifetime cap before auto-sending again.
async function getStalledFollowupHistory(contactId) {
  try {
    const { data } = await supabase
      .from('prospect_followups')
      .select('sent_at, status')
      .eq('contact_id', contactId)
      .eq('status', 'sent')
      .order('sent_at', { ascending: false });
    if (!data || !data.length) return { lifetime: 0, lastSentMs: null };
    return { lifetime: data.length, lastSentMs: new Date(data[0].sent_at).getTime() };
  } catch (_) { return { lifetime: 0, lastSentMs: null }; }
}

async function evaluateStalledCandidate(convo, ghlUserNames, clientEmails) {
  const reasons = [];
  const setterGhlId = (convo.assignedTo || '').toString();
  const setterName  = ghlUserNames[setterGhlId] || ghlUserNames[setterGhlId.toLowerCase()] || null;
  if (!setterName) return { skip: 'no_setter_match', setterSlackId: null };
  const setterSlackId = GHL_TO_SLACK[setterName.toLowerCase()] || GHL_TO_SLACK[setterGhlId.toLowerCase()] || null;
  if (!setterSlackId) return { skip: 'no_setter_slack_map', setterSlackId: null };

  // Re-fetch messages to confirm no outbound after the last inbound
  let messages;
  try { messages = await ghlGetConversationMessages(convo.id); }
  catch (err) { return { skip: `fetch_messages_failed:${err.message}`, setterSlackId }; }
  if (!messages.length) return { skip: 'no_messages', setterSlackId };
  if (messages.length < 3) return { skip: 'thread_too_short', setterSlackId };

  const lastInboundIdx = [...messages].reverse().findIndex(m => m.direction === 'inbound');
  if (lastInboundIdx === -1) return { skip: 'no_inbound', setterSlackId };
  const realIdx = messages.length - 1 - lastInboundIdx;
  const tail = messages.slice(realIdx + 1);
  if (tail.some(m => m.direction === 'outbound')) return { skip: 'setter_already_replied', setterSlackId };

  const lastInbound = messages[realIdx];
  const lastBody = String(lastInbound.body || lastInbound.message || '').trim();
  if (!lastBody || lastBody === '[Voice Note]') return { skip: 'voice_note_or_empty', setterSlackId };
  if (/^[\p{Emoji}\s]+$/u.test(lastBody) && lastBody.length < 6) return { skip: 'emoji_only', setterSlackId };

  const optOut = hasOptOutSignal(messages);
  if (optOut) return { skip: `opt_out:${optOut}`, setterSlackId };

  // Contact-level checks
  const contact = await ghlGetContact(convo.contactId);
  if (!contact) return { skip: 'contact_fetch_failed', setterSlackId };
  if (!contact.phone) return { skip: 'no_phone', setterSlackId };

  const tags = (contact.tags || []).map(t => String(t).toLowerCase());
  for (const t of tags) if (DNC_TAGS.has(t)) return { skip: `dnc_tag:${t}`, setterSlackId };
  // Won deals carry this tag from the Opportunity Won workflow — never chase them.
  if (tags.includes('won-deal')) return { skip: 'won_deal_tag', setterSlackId };

  if (await isStalledFollowupPaused(convo.contactId)) return { skip: 'setter_paused', setterSlackId };

  // Client gate — see loadActiveClientEmails. null means "could not verify", and the
  // safe answer to that is silence.
  if (!clientEmails) return { skip: 'client_check_unavailable', setterSlackId };
  const contactEmail = String(contact.email || '').toLowerCase().trim();
  if (contactEmail && clientEmails.has(contactEmail)) return { skip: 'is_client', setterSlackId };

  // One GHL fetch, both booking gates (see ghlGetContactAppointments — the old
  // revops_iclosed_bookings lookups went dead at the GHL cutover and were failing open).
  const appointments = await ghlGetContactAppointments(convo.contactId);

  const futureBooking = findFutureBooking(appointments);
  if (futureBooking) return { skip: `already_booked:${futureBooking.event_at}`, setterSlackId };

  const attended = findAttendedCall(appointments);
  if (attended) return { skip: `already_attended:${attended.event_status}`, setterSlackId };

  const pendingOutcome = findAppointmentPendingOutcome(appointments);
  if (pendingOutcome) return { skip: `appointment_pending_outcome:${pendingOutcome.event_at}`, setterSlackId };

  // Stage gate last — it is the most expensive check, so let the cheap disqualifiers
  // run first. Only Initial Contact and the strikes may be nudged.
  const { opp, reason: stageReason } = await findNudgeableOpp(convo.contactId);
  if (!opp) return { skip: stageReason, setterSlackId };

  return {
    skip: null,
    setterSlackId,
    setterName,
    contactName: convo.contactName || convo.fullName || contact.firstName || 'Unknown',
    lastBody: lastBody.slice(0, 140),
    ageDays: Math.floor((Date.now() - convo.lastMessageDate) / (24 * 60 * 60 * 1000)),
    contactId: convo.contactId,
    conversationId: convo.id,
    stage: APPT_PIPELINE_STAGE_LABELS[opp.pipelineStageId] || opp.pipelineStageId,
  };
}

async function runStalledProspectFollowups(correlationId) {
  // Modes: 'dry_run' (default) | 'approval' | 'live'
  // STALLED_FOLLOWUPS_MODE wins; STALLED_FOLLOWUPS_LIVE='true' kept as legacy shorthand for 'live'.
  const explicitMode = (process.env.STALLED_FOLLOWUPS_MODE || '').toLowerCase();
  const mode = ['dry_run', 'approval', 'live'].includes(explicitMode)
    ? explicitMode
    : (process.env.STALLED_FOLLOWUPS_LIVE === 'true' ? 'live' : 'dry_run');
  const isLive     = mode === 'live';
  const isApproval = mode === 'approval';
  console.log(`runStalledProspectFollowups starting (mode=${mode.toUpperCase()})`);

  const locationId = process.env.GHL_LOCATION_ID;
  const apiKey     = process.env.GHL_API_KEY;
  const url = `https://services.leadconnectorhq.com/conversations/search?locationId=${locationId}&limit=100`;
  const res = await fetch(url, { headers: { 'Authorization': `Bearer ${apiKey}`, 'Version': '2021-07-28' } });
  if (!res.ok) throw new Error(`GHL conversations.search → ${res.status}`);
  const data = await res.json();
  const convos = data.conversations || [];

  const now = Date.now();
  const candidates = convos.filter(c => {
    if (c.lastMessageType !== 'TYPE_WHATSAPP') return false;
    if (c.lastMessageDirection !== 'inbound') return false;
    if (!c.lastMessageDate) return false;
    return businessDaysBetween(c.lastMessageDate, now) >= 2;
  });

  // Nudge targets = ACTIVE SETTERS ONLY (Ron 2026-07-29: Sebastian, Oscar, William).
  // Closers are intentionally absent: a conversation assigned to a closer is not a
  // setter's stalled prospect, and Joseph/Debbanny have rolled off — a nudge naming
  // them would either DM a departed teammate or silently reach nobody.
  const ghlUserNames = {
    'zcmdiz2eerapd80w2zop': 'Oscar M',         'ZcmdIz2EEraPd80W2zop': 'Oscar M',
    'n8mvtuhbbby7qppqnmr7': 'William B',       'N8mvtuHbbbY7QppqNMr7': 'William B',
    'wdjte1temxfr0lpi5rgv': 'Sebastian S',     'Wdjte1temxfR0lpi5RGV': 'Sebastian S',
  };

  // Loaded once per run rather than per candidate. If this comes back null the
  // client gate can't be evaluated and every candidate is held — see
  // loadActiveClientEmails.
  const clientEmails = await loadActiveClientEmails();
  if (!clientEmails) console.warn('stalled-cron: client list unavailable — every candidate will be held this run.');

  const skipCounts = {};
  const claimable = [];
  for (const c of candidates) {
    const result = await evaluateStalledCandidate(c, ghlUserNames, clientEmails);
    if (result.skip) {
      skipCounts[result.skip.split(':')[0]] = (skipCounts[result.skip.split(':')[0]] || 0) + 1;
      console.log(`stalled-skip ${c.contactId}: ${result.skip}`);
      continue;
    }
    claimable.push(result);
  }

  // Group by setter Slack ID
  const bySetter = {};
  for (const r of claimable) (bySetter[r.setterSlackId] ||= []).push(r);

  // Live-send guardrails (only consulted when isLive). Tunable via env so we can
  // raise the daily cap from 3 → 10 after 2 weeks of clean reply quality.
  const COOLDOWN_DAYS  = 14;
  const LIFETIME_CAP   = 2;
  const DAILY_CAP      = parseInt(process.env.STALLED_FOLLOWUPS_DAILY_CAP || '3', 10);
  const HOUR_LO        = 10; // 10:00 CR inclusive
  const HOUR_HI        = 18; // 18:00 CR exclusive
  const crHour         = parseInt(new Date().toLocaleString('en-US', { timeZone: 'America/Costa_Rica', hour: 'numeric', hour12: false }), 10);
  const businessHoursOk = crHour >= HOUR_LO && crHour < HOUR_HI;

  const liveSent = []; // { contactName, contactId, setterSlackId, message, ageDays }
  const approvalDrafts = []; // { contactName, contactId, setterSlackId, message, ageDays }
  const liveSkips = {};

  // DM each setter their stalled list (dry-run) — or generate + (send|post-for-approval).
  for (const [slackId, list] of Object.entries(bySetter)) {
    if (!isLive && !isApproval) {
      const lines = list.map(r => `• ${r.contactName} (${r.ageDays}d, WhatsApp) — last: "${r.lastBody}" → https://app.gohighlevel.com/v2/location/${locationId}/contacts/detail/${r.contactId}`);
      const text = `👀 *Stalled prospects* — dry-run (no auto-followup sent yet):\n${lines.join('\n')}\n\n_Reply to nudge them yourself, or sit tight — Max will start auto-following up after dry-run watch period._`;
      try {
        await slack.client.chat.postMessage({ channel: slackId, text });
        logActivity({ event_type: 'slack_message', event_source: 'slack', action: 'outbound', channel_id: slackId, output: { text: text.slice(0, 2000) }, correlation_id: correlationId });
      } catch (err) { console.error(`stalled DM to ${slackId} failed:`, err.message); }
      continue;
    }

    // ── LIVE / APPROVAL path ─────────────────────────────────────────────────
    // Both modes apply the same guardrails. The only difference: live POSTs to
    // GHL immediately; approval posts each draft to Ron's DM with `campaign_draft`
    // metadata so he can ✅/❌ each one (the existing reaction handler at the
    // bottom of this file recognizes `source: 'stalled_cron'` and writes to
    // prospect_followups after a successful send).
    if (!businessHoursOk) {
      console.warn(`stalled-cron mode=${mode} outside business hours (${crHour}:00 CR). Skipping.`);
      liveSkips.outside_business_hours = (liveSkips.outside_business_hours || 0) + list.length;
      continue;
    }

    for (const r of list) {
      const actionedSoFar = isLive ? liveSent.length : approvalDrafts.length;
      if (actionedSoFar >= DAILY_CAP) {
        liveSkips.daily_cap_reached = (liveSkips.daily_cap_reached || 0) + 1;
        continue;
      }

      const history = await getStalledFollowupHistory(r.contactId);
      if (history.lifetime >= LIFETIME_CAP) { liveSkips.lifetime_cap = (liveSkips.lifetime_cap || 0) + 1; continue; }
      if (history.lastSentMs && (Date.now() - history.lastSentMs) < COOLDOWN_DAYS * 24 * 60 * 60 * 1000) {
        liveSkips.cooldown_active = (liveSkips.cooldown_active || 0) + 1;
        continue;
      }

      // Generate the draft (reuses the campaign generator — same spec: single
      // sentence, conversation language, ≤25 words, ends with question, no markdown).
      let draftText;
      try {
        const messages    = await ghlGetConversationMessages(r.conversationId);
        const contact     = await ghlGetContact(r.contactId);
        const setterFirst = (r.setterName || '').split(' ')[0] || 'the team';
        draftText = await generateCampaignDraft(contact || { firstName: r.contactName }, messages, setterFirst);
        if (!draftText) throw new Error('empty draft');
      } catch (err) {
        console.error(`stalled draft failed ${r.contactId}: ${err.message}`);
        liveSkips.draft_failed = (liveSkips.draft_failed || 0) + 1;
        continue;
      }

      if (isApproval) {
        // Post the draft to the ASSIGNED SETTER's DM (Ron 2026-07-29 — he does not
        // want to be the approver: "I want the DMs to the setters"). The setter owns
        // the conversation, so they are the right person to judge the draft, and it
        // keeps Ron out of a per-message loop. Ron still receives the run summary.
        // Approval remains human: nothing reaches the prospect without a ✅.
        const ghlLink = `https://app.gohighlevel.com/v2/location/${locationId}/contacts/detail/${r.contactId}`;
        const postText = [
          `📤 *${r.contactName}* — ${r.ageDays}d sin respuesta | ${r.stage} | WhatsApp`,
          `_Último mensaje (prospecto):_ "${r.lastBody}"`,
          ``,
          `*Borrador:*`,
          `> ${draftText}`,
          ``,
          `✅ para enviar · ❌ para descartar · ✏️ editar (DM: \`revise ${r.contactId}: <nuevo texto>\`)`,
          `🔗 ${ghlLink}`,
        ].join('\n');
        try {
          await slack.client.chat.postMessage({
            channel: r.setterSlackId,
            text: postText,
            metadata: {
              event_type: 'campaign_draft',
              event_payload: {
                contact_id: r.contactId,
                contact_name: r.contactName,
                draft_text: draftText,
                correlation_id: correlationId,
                source: 'stalled_cron',
                setter_slack_id: r.setterSlackId,
                attempt_n: history.lifetime + 1,
              },
            },
          });
          approvalDrafts.push({ ...r, message: draftText });
          logActivity({ event_type: 'stalled_followup_draft_posted', event_source: 'cron', action: 'outbound', actor_user_id: r.setterSlackId, channel_id: r.setterSlackId, output: { contact_id: r.contactId, draft_text: draftText.slice(0, 500) }, correlation_id: correlationId });
        } catch (err) {
          console.error(`stalled approval-draft post failed for ${r.contactId}: ${err.message}`);
          liveSkips.post_failed = (liveSkips.post_failed || 0) + 1;
        }
        continue;
      }

      // ── LIVE: send via GHL — same shape as sendCampaignMessage.
      let ghlMessageId = null, ghlConversationId = null, sendErr = null;
      try {
        const sendRes = await fetch('https://services.leadconnectorhq.com/conversations/messages', {
          method: 'POST',
          headers: { 'Authorization': `Bearer ${apiKey}`, 'Version': '2021-07-28', 'Content-Type': 'application/json' },
          body: JSON.stringify({ type: 'WhatsApp', contactId: r.contactId, message: draftText }),
        });
        if (!sendRes.ok) {
          const errBody = await sendRes.text();
          throw new Error(`GHL ${sendRes.status}: ${errBody.slice(0, 200)}`);
        }
        const sendData = await sendRes.json();
        ghlMessageId      = sendData.messageId || null;
        ghlConversationId = sendData.conversationId || r.conversationId || null;
      } catch (err) {
        sendErr = err.message;
      }

      // Audit row in either branch.
      try {
        await supabase.from('prospect_followups').insert({
          contact_id: r.contactId,
          contact_name: r.contactName,
          conversation_id: ghlConversationId,
          channel: 'WhatsApp',
          message: draftText,
          ghl_message_id: ghlMessageId,
          setter_slack_id: r.setterSlackId,
          attempt_n: history.lifetime + 1,
          status: sendErr ? 'failed' : 'sent',
          error_message: sendErr,
          correlation_id: correlationId,
        });
      } catch (err) { console.error(`prospect_followups insert failed for ${r.contactId}: ${err.message}`); }

      if (sendErr) {
        console.error(`stalled send failed ${r.contactId}: ${sendErr}`);
        liveSkips.send_failed = (liveSkips.send_failed || 0) + 1;
        continue;
      }

      liveSent.push({ ...r, message: draftText });
      logActivity({ event_type: 'stalled_followup_sent', event_source: 'cron', action: 'outbound', actor_user_id: r.setterSlackId, output: { contact_id: r.contactId, message: draftText.slice(0, 500) }, correlation_id: correlationId });
    }

    // DM the setter what was actually sent on their behalf (live only).
    if (isLive) {
      const sentForThisSetter = liveSent.filter(s => s.setterSlackId === slackId);
      if (sentForThisSetter.length > 0) {
        const lines = sentForThisSetter.map(s => `• ${s.contactName} (${s.ageDays}d) — sent: "${s.message.slice(0, 140)}"`);
        const text = `🤖 *Auto-followups sent on your behalf* — ${sentForThisSetter.length} prospect(s):\n${lines.join('\n')}\n\n_DM \`pause <contact_id>\` to stop future auto-followups for a specific contact._`;
        try { await slack.client.chat.postMessage({ channel: slackId, text }); }
        catch (err) { console.error(`stalled live-DM to ${slackId} failed:`, err.message); }
      }
    }
  }

  // Cross-post to #ng-sales-goats so the team sees the activity (live mode only).
  if (isLive && liveSent.length > 0) {
    try {
      await slack.client.chat.postMessage({
        channel: LEAD_CHANNEL_ID,
        text: `🔔 Auto-followup sent — ${liveSent.length} prospect${liveSent.length === 1 ? '' : 's'} nudged today.`,
      });
    } catch (err) { console.error('stalled cross-post to #ng-sales-goats failed:', err.message); }
  }

  // Summary DM to Ron
  const totalCandidates = candidates.length;
  const totalSendable   = claimable.length;
  const totalSkipped    = totalCandidates - totalSendable;
  const skipBreakdown   = Object.entries(skipCounts).map(([k, v]) => `${k}: ${v}`).join(', ') || 'none';
  const liveSkipBreakdown = Object.entries(liveSkips).map(([k, v]) => `${k}: ${v}`).join(', ');
  const modeLabel = mode.toUpperCase().replace('_', '-');
  let actionLine;
  if (isLive)          actionLine = `Sent: ${liveSent.length}${liveSkipBreakdown ? ` | Live-skip: ${liveSkipBreakdown}` : ''}`;
  else if (isApproval) actionLine = `Drafts sent to setters for approval: ${approvalDrafts.length}${liveSkipBreakdown ? ` | Skip: ${liveSkipBreakdown}` : ''}`
    + (approvalDrafts.length ? `\n${approvalDrafts.map(d => `• ${d.setterName} → ${d.contactName} (${d.stage}, ${d.ageDays}d)`).join('\n')}` : '')
    + `\n_Each setter approves their own draft in DM (✅ send · ❌ skip · \`revise <contact_id>: <text>\`). Nothing reaches a prospect without a ✅._`;
  else                 actionLine = totalSendable > 0 ? `Setter DMs sent: ${Object.keys(bySetter).length}` : '_(no setter DMs sent — no eligible prospects today)_';
  const summary = [
    `📊 *Stalled prospect ${mode.replace('_', '-')} summary* (${modeLabel})`,
    `Candidates: ${totalCandidates} | Eligible: ${totalSendable} | Skipped: ${totalSkipped}`,
    `Skip breakdown: ${skipBreakdown}`,
    actionLine,
  ].join('\n');
  try {
    await slack.client.chat.postMessage({ channel: RON_SLACK_ID, text: summary });
    logActivity({ event_type: 'slack_message', event_source: 'slack', action: 'outbound', channel_id: RON_SLACK_ID, output: { text: summary }, correlation_id: correlationId });
  } catch (err) { console.error('stalled summary DM to Ron failed:', err.message); }

  console.log(`runStalledProspectFollowups done — ${totalSendable}/${totalCandidates} eligible, skipped ${totalSkipped}, sent ${liveSent.length}, drafts-posted ${approvalDrafts.length} (mode=${mode})`);
}

// ─── AUTO STRIKE MOVER (pipeline cards move themselves on setter follow-ups) ──
// Setters spend real time dragging cards through New Lead → Initial Contact →
// Strike 1/2/3, and mostly don't (hence 1,028 parked in Initial Contact). This
// moves the card for them, so the board becomes a live activity tracker and
// setters only converse.
//
// WHY THIS LIVES IN MAX AND NOT MAKE: GHL's conversation index
// (conversations/search) does NOT track WhatsApp messages sent from the shared
// WhatsApp Business phone — those arrive with userId '' and an unformatted
// number, and lastMessageDate/lastManualMessageDate never move for them
// (verified 2026-07-28: a thread with five setter messages still reported an
// inbound from an hour earlier as its last message). Phone-origin is the
// MAJORITY of real setter follow-ups, so no event-style poll built on that index
// can be trusted. The only reliable source is per-conversation /messages, which
// means sweeping candidate cards — prohibitive on Make's per-operation billing,
// free here.
//
// RULES (Appointment Setting Pipeline only):
//   New Lead → Initial Contact   on the first HUMAN outbound WhatsApp.
//   IC → S1 → S2 → S3            one stage per chase. A chase means the lead has
//                                been silent 24h+; an engaged conversation never
//                                marches toward Strike 3.
//   Watermark   the triggering message must be NEWER than the card's last stage
//               change, so one message can never be counted twice and a setter's
//               own manual drag is never re-counted.
//   Debounce    at most one advance per card per 20h.
//   Never past Strike 3, never backward, never touches Call Booked+ stages.
//   Lost stays manual. Automated sends (source 'workflow') never count — only
//   source 'app', which is the reliable human/automation discriminator (userId
//   is frequently empty on genuine human sends and cannot be used for this).
const STRIKE_PIPELINE_ID = process.env.STRIKE_MOVER_PIPELINE_ID || 'KH1IQuaN8aNB1lfRpvP4';
const STRIKE_STAGE = {
  NEW_LEAD:        '93de6a09-78a4-4253-bea4-c1528ed6f2b3',
  INITIAL_CONTACT: '4b936528-794e-40ab-812d-144b9d5e8128',
  STRIKE_1:        '92245916-0622-4f46-aabc-6091b8af5fc0',
  STRIKE_2:        '6274c958-8252-4698-acc6-c6818d43a99f',
  STRIKE_3:        'e639662d-6b1b-42b5-a89d-7ebd70ca97e3',
};
const STRIKE_STAGE_NAME = {
  [STRIKE_STAGE.NEW_LEAD]:        'New Lead',
  [STRIKE_STAGE.INITIAL_CONTACT]: 'Initial Contact',
  [STRIKE_STAGE.STRIKE_1]:        'Strike 1',
  [STRIKE_STAGE.STRIKE_2]:        'Strike 2',
  [STRIKE_STAGE.STRIKE_3]:        'Strike 3',
};
const STRIKE_NEXT_STAGE = {
  [STRIKE_STAGE.INITIAL_CONTACT]: STRIKE_STAGE.STRIKE_1,
  [STRIKE_STAGE.STRIKE_1]:        STRIKE_STAGE.STRIKE_2,
  [STRIKE_STAGE.STRIKE_2]:        STRIKE_STAGE.STRIKE_3,
};
const STRIKE_SCOPE_STAGES = [
  STRIKE_STAGE.NEW_LEAD, STRIKE_STAGE.INITIAL_CONTACT, STRIKE_STAGE.STRIKE_1, STRIKE_STAGE.STRIKE_2,
];
const STRIKE_DEBOUNCE_MS     = 20 * 60 * 60 * 1000; // one advance per card per 20h
const STRIKE_LEAD_SILENCE_MS = 24 * 60 * 60 * 1000; // lead quiet this long ⇒ outreach is a chase

// contactId → conversationId. Saves one API call per card on every sweep after
// the first. A stale entry self-heals: a failed messages fetch drops the card for
// that run and the id is re-resolved next sweep.
const _strikeConvoCache = new Map();

async function ghlFetchJson(url) {
  const endpoint = url.split('?')[0].split('/').slice(-2).join('/');
  const res = await ghlFetch(url, {
    headers: { 'Authorization': `Bearer ${process.env.GHL_API_KEY}`, 'Version': '2021-07-28' },
  }, { label: endpoint });
  if (!res.ok) throw new Error(`GHL ${res.status} on ${url.split('?')[0].split('/').slice(-2).join('/')}`);
  return res.json();
}

// Pages a whole stage. Uses the meta.startAfter/startAfterId cursor rather than
// meta.nextPageUrl so the location/pipeline filters can't drift between pages.
async function ghlSearchOppsByStage(stageId, { limit = 100, maxPages = 40 } = {}) {
  const locationId = process.env.GHL_LOCATION_ID;
  const out = [];
  let startAfter = null, startAfterId = null;
  for (let page = 0; page < maxPages; page++) {
    let url = `https://services.leadconnectorhq.com/opportunities/search?location_id=${locationId}`
            + `&pipeline_id=${STRIKE_PIPELINE_ID}&pipeline_stage_id=${stageId}&status=open&limit=${limit}`;
    if (startAfter && startAfterId) url += `&startAfter=${startAfter}&startAfterId=${startAfterId}`;
    const data  = await ghlFetchJson(url);
    const batch = data.opportunities || [];
    out.push(...batch);
    if (batch.length < limit) break;
    startAfter   = data.meta?.startAfter;
    startAfterId = data.meta?.startAfterId;
    if (!startAfter || !startAfterId) break;
    // Same throttle the per-card loop in runAutoStrikeMover uses. Four stages of
    // back-to-back paging with no gap is what opened the 2026-08-17 burst that
    // 429'd a setter's claim; a few hundred ms across a sweep that runs for
    // minutes costs nothing.
    const pageThrottleMs = parseInt(process.env.STRIKE_MOVER_THROTTLE_MS || '120', 10);
    if (pageThrottleMs > 0) await new Promise(r => setTimeout(r, pageThrottleMs));
  }
  return out;
}

async function ghlFindConversationId(contactId) {
  if (_strikeConvoCache.has(contactId)) return _strikeConvoCache.get(contactId);
  const locationId = process.env.GHL_LOCATION_ID;
  const data = await ghlFetchJson(
    `https://services.leadconnectorhq.com/conversations/search?locationId=${locationId}&contactId=${contactId}&limit=1`,
  );
  const id = (data.conversations || [])[0]?.id || null;
  if (id) _strikeConvoCache.set(contactId, id);
  return id;
}

async function ghlMoveOpportunityStage(oppId, stageId, pipelineId = STRIKE_PIPELINE_ID) {
  const res = await ghlFetch(`https://services.leadconnectorhq.com/opportunities/${oppId}`, {
    method: 'PUT',
    headers: {
      'Authorization': `Bearer ${process.env.GHL_API_KEY}`,
      'Version': '2021-07-28',
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({ pipelineId, pipelineStageId: stageId }),
  }, { label: `PUT /opportunities/${oppId}` });
  if (!res.ok) throw new Error(`opp PUT ${oppId} → ${res.status}: ${(await res.text()).slice(0, 150)}`);
  return true;
}

// Move a sales opportunity to the stage that matches a logged outcome. Reads
// the opp first so the move stays inside its OWN pipeline (Appointment Setting
// and VSL have different stage ids for the same idea) and so an already-closed
// card is left alone. Never guesses a stage id.
// Find the prospect's sales opportunity when the portal row lacks the id.
// Scoped to the two known sales pipelines; prefers an open card, then newest.
async function ghlFindSalesOpportunityByContact(contactId) {
  try {
    const res = await ghlFetch(
      `https://services.leadconnectorhq.com/opportunities/search?location_id=${process.env.GHL_LOCATION_ID}&contact_id=${contactId}`,
      { headers: { 'Authorization': `Bearer ${process.env.GHL_API_KEY}`, 'Version': '2021-07-28' } },
      { label: `opp-by-contact ${contactId}` },
    );
    if (!res.ok) return null;
    const opps = ((await res.json()).opportunities || [])
      .filter(o => GHL_OUTCOME_STAGES[o.pipelineId]);
    if (!opps.length) return null;
    opps.sort((a, b) => {
      const ao = a.status === 'open' ? 0 : 1, bo = b.status === 'open' ? 0 : 1;
      if (ao !== bo) return ao - bo;
      return String(b.createdAt || '').localeCompare(String(a.createdAt || ''));
    });
    return opps[0].id;
  } catch (_) { return null; }
}

async function ghlMoveOpportunityForOutcome(oppId, outcome) {
  const res = await ghlFetch(`https://services.leadconnectorhq.com/opportunities/${oppId}`, {
    headers: { 'Authorization': `Bearer ${process.env.GHL_API_KEY}`, 'Version': '2021-07-28' },
  }, { label: `opp-read ${oppId}` });
  if (!res.ok) return { ok: false, message: `opp GET ${oppId} → ${res.status}` };
  const opp = (await res.json()).opportunity || {};
  const pipelineId = opp.pipelineId;
  const target = outcomeStageFor(pipelineId, outcome);
  if (!target) return { ok: false, message: `no stage mapping for outcome "${outcome}" in pipeline ${pipelineId || '(unknown)'}` };
  if (opp.pipelineStageId === target.id) return { ok: true, already: true, label: target.label };
  // A card a human already closed out is not ours to reopen.
  if (GHL_TERMINAL_STAGE_IDS.has(opp.pipelineStageId)) {
    return { ok: false, terminal: true, message: `opportunity already in a closed stage (${APPT_PIPELINE_STAGE_LABELS[opp.pipelineStageId] || opp.pipelineStageId})` };
  }
  await ghlMoveOpportunityStage(oppId, target.id, pipelineId);
  return { ok: true, label: target.label };
}

// Pure decision function — all the semantics live here and it does no I/O, so the
// rules can be reasoned about (and tested) without touching GHL.
// `messages` must be oldest-first (ghlGetConversationMessages normalizes to that).
function evaluateStrikeMove(opp, messages, now = Date.now()) {
  const stage = opp.pipelineStageId;
  if (opp.status !== 'open')                 return { skip: 'not_open' };
  if (opp.pipelineId !== STRIKE_PIPELINE_ID) return { skip: 'wrong_pipeline' };
  if (!STRIKE_SCOPE_STAGES.includes(stage))  return { skip: 'stage_out_of_scope' };

  // Activity entries (TYPE_ACTIVITY_OPPORTUNITY — "Opportunity updated" etc.)
  // interleave with real messages; without this filter the mover reads its own
  // stage-change activity as the newest message in the thread.
  const wa = (messages || []).filter(m => m.messageType === 'TYPE_WHATSAPP');
  if (!wa.length) return { skip: 'no_whatsapp' };

  const newest = wa[wa.length - 1];
  if (String(newest.direction || '').toLowerCase() !== 'outbound') return { skip: 'lead_spoke_last' };
  if (String(newest.source    || '').toLowerCase() !== 'app')      return { skip: 'automated_send' };

  const touchTs = Date.parse(newest.dateAdded || newest.createdAt || 0) || 0;
  const stageTs = Date.parse(opp.lastStageChangeAt || opp.createdAt || 0) || 0;
  if (!touchTs)             return { skip: 'no_touch_timestamp' };
  if (!(touchTs > stageTs)) return { skip: 'already_counted' };

  if (stage === STRIKE_STAGE.NEW_LEAD) {
    return { move: STRIKE_STAGE.INITIAL_CONTACT, reason: 'first human touch' };
  }

  if (now - stageTs < STRIKE_DEBOUNCE_MS) return { skip: 'debounced' };

  // Lead silence, not "was the previous message outbound" — setters routinely
  // double-text within one live conversation, and the naive check reads that as
  // a chase and marches an engaged lead toward Strike 3.
  const lastInbound   = [...wa].reverse().find(m => String(m.direction || '').toLowerCase() === 'inbound');
  const lastInboundTs = lastInbound ? (Date.parse(lastInbound.dateAdded || lastInbound.createdAt || 0) || 0) : 0;
  if (lastInboundTs && now - lastInboundTs < STRIKE_LEAD_SILENCE_MS) return { skip: 'lead_engaged' };

  const next = STRIKE_NEXT_STAGE[stage];
  if (!next) return { skip: 'no_next_stage' };
  return { move: next, reason: lastInboundTs ? 'chase — lead silent 24h+' : 'chase — lead never replied' };
}

// Sweep — see cron registration below. Mode is STRIKE_MOVER_MODE:
// 'dry_run' (default — reports what it WOULD do, writes nothing) | 'live'.
async function runAutoStrikeMover(correlationId) {
  const mode       = (process.env.STRIKE_MOVER_MODE || '').toLowerCase() === 'live' ? 'live' : 'dry_run';
  const isLive     = mode === 'live';
  const throttleMs = parseInt(process.env.STRIKE_MOVER_THROTTLE_MS || '120', 10);
  // Stampede guard: the first live sweep over a 1,000+ card backlog could advance
  // a lot of cards at once. Capped per run; the rest are picked up next sweep.
  const maxMoves   = parseInt(process.env.STRIKE_MOVER_MAX_MOVES || '100', 10);
  console.log(`runAutoStrikeMover starting (mode=${mode.toUpperCase()})`);

  const cards = [];
  for (const stageId of STRIKE_SCOPE_STAGES) cards.push(...await ghlSearchOppsByStage(stageId));

  const skipCounts = {};
  const bump = (k) => { skipCounts[k] = (skipCounts[k] || 0) + 1; };
  const moves = [], failures = [];
  let capped = 0;

  for (const opp of cards) {
    try {
      if (!opp.contactId) { bump('no_contact'); continue; }
      const convoId = await ghlFindConversationId(opp.contactId);
      if (!convoId) { bump('no_conversation'); continue; }
      const messages = await ghlGetConversationMessages(convoId);
      const verdict  = evaluateStrikeMove(opp, messages);
      if (verdict.skip) { bump(verdict.skip); continue; }

      if (moves.length >= maxMoves) { capped++; continue; }
      if (isLive) await ghlMoveOpportunityStage(opp.id, verdict.move);
      moves.push({
        name:   opp.contact?.name || opp.name || opp.id,
        from:   STRIKE_STAGE_NAME[opp.pipelineStageId] || opp.pipelineStageId,
        to:     STRIKE_STAGE_NAME[verdict.move] || verdict.move,
        reason: verdict.reason,
      });
    } catch (err) {
      // A stale cached conversation id is the likeliest cause — drop it so the
      // next sweep re-resolves instead of failing this card forever.
      _strikeConvoCache.delete(opp.contactId);
      failures.push(`${opp.contact?.name || opp.id}: ${err.message}`);
    }
    if (throttleMs > 0) await new Promise(r => setTimeout(r, throttleMs));
  }

  // Per-sweep stats persist to agent_activity so the nightly learning report can
  // roll up the day (strikeBuildDailyDigest). Transient GHL 401/503 failures stay
  // in metadata, not status — they self-heal next sweep and shouldn't trip the
  // status<>'ok' error monitoring index.
  logActivity({
    event_type: 'strike_sweep', event_source: 'cron', action: 'runAutoStrikeMover',
    correlation_id: correlationId,
    metadata: { mode, scanned: cards.length, moved: moves.length, capped,
                skips: skipCounts, moves: moves.slice(0, 50), failures: failures.slice(0, 10) },
  });

  // Per-run Slack posts are dry-run-only (testing visibility). Live mode is
  // silent here — the nightly learning report carries the daily summary.
  if (!isLive) {
    const skipBreakdown = Object.entries(skipCounts).sort((a, b) => b[1] - a[1])
      .map(([k, v]) => `${k}=${v}`).join(', ') || 'none';
    const lines = [
      `AUTO STRIKE MOVER — DRY RUN`,
      '',
      `Cards scanned: ${cards.length} | Would move: ${moves.length}${capped ? ` (+${capped} over the ${maxMoves}/run cap, next sweep)` : ''}`,
      '',
      `Skips: ${skipBreakdown}`,
    ];
    if (moves.length) {
      lines.push('');
      lines.push(...moves.slice(0, 25).map(m => `• ${m.name}: ${m.from} → ${m.to} (${m.reason})`));
      if (moves.length > 25) lines.push(`• …and ${moves.length - 25} more.`);
    }
    if (failures.length) {
      lines.push('');
      lines.push(`${failures.length} failure(s): ${failures.slice(0, 3).join(' | ')}`);
    }
    lines.push('', `Set STRIKE_MOVER_MODE=live on Railway to let these moves actually happen.`);
    try {
      await postToSlack(AGENT_CHANNEL, lines.join('\n'));
    } catch (err) { console.error('strike mover summary post failed:', err.message); }
  }

  console.log(`runAutoStrikeMover done — scanned ${cards.length}, ${isLive ? 'moved' : 'would move'} ${moves.length}, failures ${failures.length} (mode=${mode})`);
}

// Daily roll-up of strike_sweep audit rows for the nightly learning report.
// Reads agent_activity through the anon key — needs the scoped select policy
// from migrations/012_strike_sweep_read.sql. Zero sweeps is itself a signal
// (cron dead) and still returns content rather than null.
// ─── FAILURE-SPIKE THRESHOLDS ────────────────────────────────────────────────
// The nightly prompt used to ask the model to flag "a spike in failures" with no
// number attached, which means the bar moves every night. These are the numbers.
//
// Per-status, not a single total, because the classes have genuinely different
// shapes in 30 days of strike_sweep history (95 sweeps, pre-retry):
//
//   429  max 10/sweep, 10/day — BURSTY. All 20 events landed in just 2 sweeps.
//                               Load-driven; a burst is one event, not ten.
//   401  max  2/sweep,  4/day — a steady ~1-per-sweep trickle across 17 sweeps,
//                               never once co-occurring with a 429. Auth blink.
//   5xx  max  3/sweep,  5/day — GHL degraded, sporadic.
//
// A single "failures > N" rule would either cry wolf at a normal 429 burst or sleep
// through a dead token. Limits sit ~2-4x the observed per-status maximum so a real
// regression trips them and known-normal noise does not. They are also deliberately
// generous now that ghlFetch retries 429/5xx and blinks a 401 once: these count
// SURVIVORS, so post-retry the expected steady-state is ~0 and anything reaching
// these numbers is a genuine change in behaviour.
const STRIKE_FAILURE_LIMITS = {
  '401': { perSweep: 5,  perDay: 10, meaning: 'auth — token may be dead or rotating' },
  '429': { perSweep: 8,  perDay: 20, meaning: 'rate limit — throttle is not keeping up' },
  '5xx': { perSweep: 8,  perDay: 20, meaning: 'GHL degraded' },
};
// Below this many sweeps in the window we do not judge at all. A cold start, a
// redeploy, or a half-day of history is not evidence of a spike (recipe standard:
// every numeric threshold carries a warmup).
const STRIKE_FAILURE_WARMUP_SWEEPS = 7;

/**
 * "Ana Lopez: GHL messages fetch 429" → "429". Returns null when unparseable.
 *
 * Failures are stored as `${contactName}: ${err.message}`, and the message puts the
 * status in three different places depending on which call threw:
 *   "GHL messages fetch 429"                 → trailing
 *   "GHL 401 on conversations/search"        → mid-string
 *   "opp PUT abc123 → 429: Too Many Requests" → after an arrow
 * So drop the name prefix, then take the first 4xx/5xx in what remains. Matching only
 * a trailing number silently bucketed every conversations/search 401 as "other", which
 * would have kept a dead token from ever reaching the 401 threshold.
 */
function strikeFailureStatus(msg) {
  const s = String(msg || '');
  const body = s.includes(': ') ? s.slice(s.indexOf(': ') + 2) : s;
  const m = body.match(/\b([45]\d{2})\b/);
  return m ? m[1] : null;
}

/** Bucket raw failure strings into the classes STRIKE_FAILURE_LIMITS is keyed by. */
function strikeBucketFailures(failures = []) {
  const counts = { '401': 0, '429': 0, '5xx': 0, other: 0 };
  for (const f of failures) {
    const s = strikeFailureStatus(f);
    if (s === '401') counts['401']++;
    else if (s === '429') counts['429']++;
    else if (s && /^5\d\d$/.test(s)) counts['5xx']++;
    else counts.other++;
  }
  return counts;
}

/**
 * Machine-checked verdict on a window of sweeps.
 *
 * `perSweepMax` counts the worst SINGLE sweep, not the window total — 10 429s in one
 * sweep is a burst worth naming, while 10 spread over a week is background noise, and
 * summing them would erase exactly that difference.
 */
function strikeAssessFailures(sweeps = []) {
  const windowTotals = { '401': 0, '429': 0, '5xx': 0, other: 0 };
  const perSweepMax  = { '401': 0, '429': 0, '5xx': 0, other: 0 };
  for (const s of sweeps) {
    const c = strikeBucketFailures((s.metadata || {}).failures || []);
    for (const k of Object.keys(windowTotals)) {
      windowTotals[k] += c[k];
      if (c[k] > perSweepMax[k]) perSweepMax[k] = c[k];
    }
  }
  if (sweeps.length < STRIKE_FAILURE_WARMUP_SWEEPS) {
    return { level: 'warmup', reasons: [], windowTotals, perSweepMax, sweeps: sweeps.length };
  }
  const reasons = [];
  for (const [status, limit] of Object.entries(STRIKE_FAILURE_LIMITS)) {
    if (perSweepMax[status] >= limit.perSweep) {
      reasons.push(`${perSweepMax[status]} × ${status} in a single sweep (limit ${limit.perSweep}) — ${limit.meaning}`);
    } else if (windowTotals[status] >= limit.perDay) {
      reasons.push(`${windowTotals[status]} × ${status} across the window (limit ${limit.perDay}) — ${limit.meaning}`);
    }
  }
  return { level: reasons.length ? 'alert' : 'ok', reasons, windowTotals, perSweepMax, sweeps: sweeps.length };
}

async function strikeBuildDailyDigest(hours = 24) {
  const since = new Date(Date.now() - hours * 3600 * 1000).toISOString();
  const { data: sweeps, error } = await portalSupabase.from('agent_activity')
    .select('created_at, metadata')
    .eq('event_type', 'strike_sweep')
    .gte('created_at', since)
    .order('created_at', { ascending: true });
  if (error) throw new Error(`strike sweep query: ${error.message}`);
  if (!sweeps || !sweeps.length) {
    return {
      digestBlock: `0 sweeps ran in the last ${hours}h — the auto strike mover cron may be dead (expected 8/day, 7am-9pm CR). Flag as an alert.`,
      slackLine: `Strike mover: 0 sweeps in last ${hours}h — cron may be down`,
      salesBlock: `⚠️ *PIPELINE AUTO-MOVER* ran 0 sweeps in the last ${hours}h — it may be down. Ron has been flagged; move cards by hand until the all-clear.`,
    };
  }
  const allMoves = [], allFailures = [], skipTotals = {};
  let moved = 0, capped = 0, dryRuns = 0, lastScanned = 0;
  for (const s of sweeps) {
    const m = s.metadata || {};
    if (m.mode !== 'live') dryRuns++;
    moved += m.moved || 0;
    capped += m.capped || 0;
    lastScanned = m.scanned ?? lastScanned;
    for (const [k, v] of Object.entries(m.skips || {})) skipTotals[k] = (skipTotals[k] || 0) + v;
    allMoves.push(...(m.moves || []));
    allFailures.push(...(m.failures || []));
  }
  const lines = [
    `${sweeps.length} sweep(s)${dryRuns ? ` (${dryRuns} dry-run)` : ''} | pipeline cards in scope: ~${lastScanned} | cards moved: ${moved}${capped ? ` | deferred over per-run cap: ${capped}` : ''}`,
  ];
  if (allMoves.length) {
    lines.push('Moves:');
    lines.push(...allMoves.slice(0, 40).map(m => `• ${m.name}: ${m.from} → ${m.to} (${m.reason})`));
    if (allMoves.length > 40) lines.push(`• …and ${allMoves.length - 40} more.`);
  }
  const skipBreakdown = Object.entries(skipTotals).sort((a, b) => b[1] - a[1])
    .map(([k, v]) => `${k}=${v}`).join(', ');
  if (skipBreakdown) lines.push(`Skip totals across sweeps (same card counted each sweep): ${skipBreakdown}`);
  // Deterministic verdict — the model is told the answer rather than asked to eyeball
  // "is this a spike?". Failures reaching here have already survived ghlFetch's retries.
  const failureVerdict = strikeAssessFailures(sweeps);
  if (allFailures.length) {
    const b = failureVerdict.windowTotals;
    const shape = [['401', b['401']], ['429', b['429']], ['5xx', b['5xx']], ['other', b.other]]
      .filter(([, n]) => n > 0).map(([k, n]) => `${k}×${n}`).join(', ');
    lines.push(`${allFailures.length} failure(s) after retries [${shape}]: ${allFailures.slice(0, 5).join(' | ')}`);
  }
  if (failureVerdict.level === 'alert') {
    lines.push(`⚠️ FAILURE SPIKE — ${failureVerdict.reasons.join('; ')}. Report this as an alert.`);
  } else if (failureVerdict.level === 'ok' && allFailures.length) {
    lines.push(`Failure levels are within normal limits — do NOT report these as an anomaly.`);
  } else if (failureVerdict.level === 'warmup') {
    lines.push(`Only ${failureVerdict.sweeps} sweep(s) in window (warmup needs ${STRIKE_FAILURE_WARMUP_SWEEPS}) — not judging failure levels.`);
  }
  // Sales-facing variant: plain language, no skip-key jargon, no sweep mechanics.
  const transitionTallies = {};
  for (const m of allMoves) {
    const key = `${m.from} → ${m.to}`;
    transitionTallies[key] = (transitionTallies[key] || 0) + 1;
  }
  const salesLines = [`*PIPELINE AUTO-MOVER — last ${hours}h*`, `Cards moved: *${moved}*`];
  for (const [transition, n] of Object.entries(transitionTallies).sort((a, b) => b[1] - a[1])) {
    salesLines.push(`• ${transition}: ${n}`);
  }
  if (allMoves.length) {
    salesLines.push('', ...allMoves.slice(0, 20).map(m => `› ${m.name}: ${m.from} → ${m.to}`));
    if (allMoves.length > 20) salesLines.push(`› …and ${allMoves.length - 20} more.`);
  }
  salesLines.push('', '_Cards are NOT moved when: the lead replied last, the last touch was an automated send (only follow-ups typed in GHL count), or the card already moved in the past 20h. Spot a card that should have moved? Reply here with the contact name and Ron will trace it._');

  return {
    digestBlock: lines.join('\n'),
    slackLine: `Strike mover (last ${hours}h): ${sweeps.length} sweeps | ${moved} card(s) moved | ${allFailures.length} transient failure(s)`,
    salesBlock: salesLines.join('\n'),
  };
}

// Daily sales-visible digest of the auto strike mover. Exists because live mode
// went silent on 2026-08-04 (per-run posts removed) and sales concluded the
// mover was broken — movement has to be visible where sales lives.
async function runStrikeSalesDigest(correlationId) {
  const { salesBlock } = await strikeBuildDailyDigest(24);
  await postToSlack(SALES_CHANNEL, salesBlock);
  logActivity({ event_type: 'slack_message', event_source: 'cron', action: 'runStrikeSalesDigest', output: { text: salesBlock.slice(0, 2000) }, correlation_id: correlationId });
}

// ─── RECOVERABLE-LEADS CAMPAIGN (Cycle 4-lite — draft & approve via reactions) ─
// Ron DMs Max `campaign: <contact_id_1>, <contact_id_2>, ...` → Max generates
// re-engagement WhatsApp drafts → posts each as a separate DM with metadata →
// Ron reacts ✅ to send (or ❌ to skip). The reaction_added handler reads the
// `campaign_draft` metadata and calls the GHL send_conversation_message endpoint.
// No daily auto-send, no business-hours gate, no cooldown — Ron eyeballs every draft.

const NG_OFFER_BRIEF = `NeuroGrowth runs B2B LinkedIn organic-growth campaigns for founders. Next step is a 30-min strategy call with the closer team to scope fit.`;

async function generateCampaignDraft(contact, messages, setterFirstName) {
  const tail = messages.slice(-8).map(m => {
    const dir = m.direction === 'inbound' ? 'Prospect' : 'Setter';
    const body = String(m.body || m.message || '').replace(/\s+/g, ' ').slice(0, 200);
    return `${dir}: ${body}`;
  }).join('\n');

  const lastInbound = [...messages].reverse().find(m => m.direction === 'inbound');
  const lastInboundBody = lastInbound ? String(lastInbound.body || lastInbound.message || '').slice(0, 240) : '(none)';
  const ageDays = lastInbound?.dateAdded ? Math.floor((Date.now() - new Date(lastInbound.dateAdded).getTime()) / (24 * 60 * 60 * 1000)) : '?';

  const prompt = `You are writing a single-sentence WhatsApp re-engagement message for a NeuroGrowth prospect who went silent ${ageDays} days ago. Match the conversation's language (Spanish or English).

Conversation tail (oldest first):
${tail || '(no message history available)'}

Last message from prospect: "${lastInboundBody}"
Prospect name: ${contact.firstName || contact.name || 'there'}
Setter on the thread: ${setterFirstName || 'the team'}
Offer brief: ${NG_OFFER_BRIEF}

Write ONE message:
- Match the conversation language exactly
- Reference the LAST thing the prospect said
- One sentence, ≤25 words, end with a question
- No emoji, no markdown, no quotes
- Sound like a setter picking up the thread (not a bot, not a marketer)
- Don't apologize, don't beg, don't offer a discount

Output ONLY the message text. No preamble, no explanation.`;

  const t = Date.now();
  const corr = newCorrelationId();
  const res = await anthropic.messages.create({
    model: 'claude-sonnet-4-6',
    max_tokens: 200,
    messages: [{ role: 'user', content: prompt }],
  });
  logLlmFromAnthropicResponse(res, Date.now() - t, corr);
  const text = res.content.filter(b => b.type === 'text').map(b => b.text).join('').trim();
  return text.replace(/^["']+|["']+$/g, '');
}

async function runRecoverableLeadsCampaign(contactIds, ronSlackId, correlationId) {
  const locationId = process.env.GHL_LOCATION_ID;
  console.log(`runRecoverableLeadsCampaign starting — ${contactIds.length} contacts, requested by ${ronSlackId}`);

  const summary = { drafted: 0, posted: 0, fetch_failed: 0, draft_failed: 0 };

  for (const contactId of contactIds) {
    let contact, messages;
    try {
      contact = await ghlGetContact(contactId);
      if (!contact) throw new Error('contact not found');
      // Find conversation by contact_id (contact object doesn't always carry conversation reference)
      const convRes = await fetch(`https://services.leadconnectorhq.com/conversations/search?locationId=${locationId}&contactId=${contactId}`, {
        headers: { 'Authorization': `Bearer ${process.env.GHL_API_KEY}`, 'Version': '2021-07-28' },
      });
      const convData = convRes.ok ? await convRes.json() : { conversations: [] };
      const convo = (convData.conversations || [])[0];
      messages = convo?.id ? await ghlGetConversationMessages(convo.id) : [];
    } catch (err) {
      console.error(`campaign fetch failed for ${contactId}: ${err.message}`);
      summary.fetch_failed += 1;
      continue;
    }

    let draftText;
    try {
      const setterGhlId = (contact.assignedTo || '').toString();
      const setterName = GHL_USER_NAMES[setterGhlId] || GHL_USER_NAMES[setterGhlId.toLowerCase()] || 'the team';
      const setterFirstName = setterName.split(' ')[0];
      draftText = await generateCampaignDraft(contact, messages || [], setterFirstName);
      if (!draftText) throw new Error('empty draft from LLM');
      summary.drafted += 1;
    } catch (err) {
      console.error(`campaign draft failed for ${contactId}: ${err.message}`);
      summary.draft_failed += 1;
      continue;
    }

    const lastInbound = [...(messages || [])].reverse().find(m => m.direction === 'inbound');
    const lastBody = lastInbound ? String(lastInbound.body || lastInbound.message || '').slice(0, 200) : '(no inbound)';
    const ageDays = lastInbound?.dateAdded ? Math.floor((Date.now() - new Date(lastInbound.dateAdded).getTime()) / (24 * 60 * 60 * 1000)) : '?';
    const fullName = `${contact.firstName || ''} ${contact.lastName || ''}`.trim() || contact.name || 'Unknown';
    const ghlLink = `https://app.gohighlevel.com/v2/location/${locationId}/contacts/detail/${contactId}`;

    const postText = [
      `📤 *${fullName}* — ${ageDays}d stale | WhatsApp`,
      `_Last (prospect):_ "${lastBody}"`,
      ``,
      `*Draft:*`,
      `> ${draftText}`,
      ``,
      `✅ to send · ❌ to skip · ✏️ revise (DM: \`revise ${contactId}: <new text>\`)`,
      `🔗 ${ghlLink}`,
    ].join('\n');

    try {
      await slack.client.chat.postMessage({
        channel: ronSlackId,
        text: postText,
        metadata: {
          event_type: 'campaign_draft',
          event_payload: {
            contact_id: contactId,
            contact_name: fullName,
            draft_text: draftText,
            correlation_id: correlationId,
          },
        },
      });
      summary.posted += 1;
      logActivity({ event_type: 'campaign_draft_posted', event_source: 'slack', action: 'outbound', actor_user_id: ronSlackId, output: { contact_id: contactId, draft_text: draftText.slice(0, 500) }, correlation_id: correlationId });
    } catch (err) {
      console.error(`campaign post failed for ${contactId}: ${err.message}`);
    }
  }

  const summaryText = [
    `🧾 *Campaign drafts ready — ${summary.posted}/${contactIds.length} posted*`,
    `Drafted: ${summary.drafted} | Posted: ${summary.posted}`,
    summary.fetch_failed > 0 ? `Fetch failed: ${summary.fetch_failed}` : null,
    summary.draft_failed > 0 ? `Draft failed: ${summary.draft_failed}` : null,
    ``,
    `React ✅ on each draft to approve and send · ❌ to skip · DM \`revise <contact_id>: <new text>\` to override`,
  ].filter(Boolean).join('\n');
  await slack.client.chat.postMessage({ channel: ronSlackId, text: summaryText });
  console.log(`runRecoverableLeadsCampaign done — ${JSON.stringify(summary)}`);
}

// Direct send helper used by the reaction handler + revise DM. Calls GHL POST /conversations/messages.
async function sendCampaignMessage({ contactId, contactName, draftText, approverSlackId, correlationId, isRevised }) {
  let claimRow;
  try {
    const { data: inserted } = await supabase.from('campaign_sends').insert({
      contact_id: contactId,
      contact_name: contactName,
      channel: 'WhatsApp',
      draft_text: draftText,
      approved_by_slack_id: approverSlackId,
      status: 'pending',
      correlation_id: correlationId,
    }).select('id').single();
    claimRow = inserted;
  } catch (err) {
    console.error('campaign_sends insert failed:', err.message);
  }

  try {
    const res = await fetch('https://services.leadconnectorhq.com/conversations/messages', {
      method: 'POST',
      headers: {
        'Authorization': `Bearer ${process.env.GHL_API_KEY}`,
        'Version': '2021-07-28',
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({ type: 'WhatsApp', contactId, message: draftText }),
    });
    if (!res.ok) {
      const errBody = await res.text();
      throw new Error(`GHL ${res.status}: ${errBody.slice(0, 200)}`);
    }
    const data = await res.json();

    if (claimRow?.id) {
      await supabase.from('campaign_sends').update({
        status: 'sent',
        sent_at: new Date().toISOString(),
        sent_text: draftText,
        ghl_message_id: data.messageId,
        conversation_id: data.conversationId,
      }).eq('id', claimRow.id);
    }

    return { ok: true, messageId: data.messageId, conversationId: data.conversationId };
  } catch (err) {
    if (claimRow?.id) {
      await supabase.from('campaign_sends').update({ status: 'failed', error_message: err.message }).eq('id', claimRow.id);
    }
    return { ok: false, error: err.message };
  }
}

// ─── (RETIRED 2026-06-02) Standalone Wed+Sat SETTER LEADERBOARD ──────────────
// The deterministic Wed+Sat MTD "SETTER LEADERBOARD" post (runSetterLeaderboard)
// was removed. It pulled setter calls/show-rate/qualified from EOD self-report
// only, which silently diverged from the weekly pod. Setters now appear in the
// single unified weekly LEADERBOARD ("Weekly Closer Comparison" task) alongside
// closers, from the shared GHL-first stat source (getSetterWeeklyStats):
// calls attributed via native revops_appointments.setter_id (setter_attributions is frozen
// iClosed-era history), show rate + AQC outcome-derived.

// ─── INNER CIRCLE HUDDLE EVENT LOOKUP (CACHED) ───────────────────────────────
async function getInnerCircleHuddleEvent() {
  const auth     = getGoogleAuth();
  const calendar = google.calendar({ version: 'v3', auth });

  // 1. Try cached event ID first — only if it's still in the future
  const { data: cached } = await supabase
    .from('agent_knowledge')
    .select('value')
    .eq('category', 'config')
    .eq('key', 'inner_circle_huddle_event_id')
    .single();
  if (cached?.value) {
    try {
      const { data: event } = await calendar.events.get({ calendarId: 'primary', eventId: cached.value });
      const start = event?.start?.dateTime || event?.start?.date;
      if (event && start && new Date(start) > new Date()) return event; // Cache hit — future event
    } catch (_) { /* Stale ID — fall through to search */ }
  }

  // 2. Search calendar (next 90 days — huddle is monthly, 30-day window was too short)
  const timeMin = new Date().toISOString();
  const timeMax = new Date(Date.now() + 90 * 24 * 60 * 60 * 1000).toISOString();
  const res     = await calendar.events.list({ calendarId: 'primary', q: 'Inner Circle Huddle', timeMin, timeMax, singleEvents: true, orderBy: 'startTime', maxResults: 5 });
  const event   = (res.data.items || [])[0];
  if (!event) return null;

  // 3. Cache the ID for next time
  await supabase.from('agent_knowledge').upsert(
    { category: 'config', key: 'inner_circle_huddle_event_id', value: event.id, source: 'auto', updated_at: new Date().toISOString() },
    { onConflict: 'category,key' }
  );
  console.log(`Cached Inner Circle Huddle event ID: ${event.id}`);
  return event;
}

// ─── SUPABASE WEBHOOK HANDLER ────────────────────────────────────────────────
async function handlePhase3Transition(record) {
  const { email, client_name } = record;
  if (!email) {
    console.warn('Phase 3 transition webhook: no email on record, skipping.');
    return;
  }
  try {
    const event = await getInnerCircleHuddleEvent();
    if (!event) {
      await slack.client.chat.postMessage({ channel: RON_SLACK_ID, text: `⚠️ ${client_name} just moved to Phase 3 but I couldn't find "Inner Circle Huddle" in your calendar (next 30 days). Add their invite manually: ${email}` });
      return;
    }
    await addCalendarAttendees(event.id, [email]);
    await slack.client.chat.postMessage({ channel: RON_SLACK_ID, text: `✅ ${client_name} moved to Phase 3 — I've added ${email} to *${event.summary}* (${event.start?.dateTime || event.start?.date}). Invite sent automatically.` });
    console.log(`Phase 3 transition: added ${email} to event ${event.id} (${event.summary})`);
  } catch (err) {
    console.error('handlePhase3Transition error:', err.message);
    await slack.client.chat.postMessage({ channel: RON_SLACK_ID, text: `⚠️ ${client_name} moved to Phase 3 but the calendar invite failed: ${err.message}. Add manually: ${email}` });
  }
}

// ─── MONTHLY PHASE 3 RECONCILIATION ──────────────────────────────────────────
async function runPhase3Reconciliation(_correlationId) {
  try {
    // 1. Get all active phase_3 clients from portal
    const { rows } = await portalPg.query(
      `SELECT client_name, email FROM client_dashboards WHERE customer_status = 'phase_3' AND is_active = true AND email IS NOT NULL`
    );
    if (!rows.length) { console.log('Phase 3 reconciliation: no active phase_3 clients.'); return; }

    // 2. Find Inner Circle Huddle (cached)
    const event = await getInnerCircleHuddleEvent();
    if (!event) {
      await slack.client.chat.postMessage({ channel: RON_SLACK_ID, text: `⚠️ Phase 3 reconciliation: couldn't find Inner Circle Huddle in calendar. ${rows.length} phase_3 clients on file — check manually.` });
      return;
    }

    // 3. Get full attendee list from the event
    const auth = getGoogleAuth();
    const calendar = google.calendar({ version: 'v3', auth });
    const { data: fullEvent } = await calendar.events.get({ calendarId: 'primary', eventId: event.id });
    const existing = new Set((fullEvent.attendees || []).map(a => a.email.toLowerCase()));
    const missing  = rows.filter(r => !existing.has(r.email.toLowerCase()));
    if (!missing.length) { console.log('Phase 3 reconciliation: all clients already on Huddle.'); return; }

    // 4. Add missing clients
    const added = [];
    for (const client of missing) {
      const result = await addCalendarAttendees(event.id, [client.email]);
      if (!result.startsWith('Add attendees error')) added.push(client.client_name);
    }
    if (added.length) {
      await slack.client.chat.postMessage({
        channel: RON_SLACK_ID,
        text: `🔁 Monthly Phase 3 reconciliation: added ${added.length} missing client(s) to Inner Circle Huddle — ${added.join(', ')}.`
      });
    }
  } catch (err) {
    console.error('Phase 3 reconciliation error:', err.message);
  }
}

async function handleSupabaseWebhook(req, res) {
  const secret = process.env.SUPABASE_WEBHOOK_SECRET;
  if (secret && req.headers['x-webhook-secret'] !== secret) {
    console.warn('Supabase webhook rejected — invalid or missing secret');
    res.writeHead(401, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ error: 'Unauthorized' }));
    return;
  }
  // Respond 200 immediately so Supabase doesn't retry
  res.writeHead(200, { 'Content-Type': 'application/json' });
  res.end(JSON.stringify({ received: true }));
  try {
    let body = '';
    req.on('data', chunk => { body += chunk.toString(); });
    req.on('end', async () => {
      try {
        const payload    = JSON.parse(body);
        const { table, type, record, old_record } = payload;
        console.log(`Supabase webhook: ${type} on ${table}`);

        // Phase 3 transition — auto-invite to Inner Circle Huddle
        if (table === 'client_dashboards' && type === 'UPDATE') {
          const wasPhase3 = old_record?.customer_status === 'phase_3';
          const isPhase3  = record?.customer_status === 'phase_3';
          if (!wasPhase3 && isPhase3) await handlePhase3Transition(record);
        }
      } catch (parseErr) {
        console.error('Supabase webhook parse error:', parseErr.message);
      }
    });
  } catch (err) {
    console.error('Supabase webhook handler error:', err.message);
  }
}

// ─── AGENT CONSULT (cross-agent API: REVI asks Max to verify initiatives) ────
const SUBMIT_VERDICTS_TOOL = {
  name: 'submit_verdicts',
  description: 'Submit final per-initiative verification verdicts. This ends the consult — call it exactly once, covering every initiative id from the request.',
  input_schema: {
    type: 'object',
    required: ['verdicts'],
    properties: {
      verdicts: {
        type: 'array',
        items: {
          type: 'object',
          required: ['id', 'status', 'confidence'],
          properties: {
            id:         { type: 'string', description: 'The initiative id exactly as given in the request.' },
            status:     { type: 'string', enum: ['confirmed_done', 'progress_found', 'no_evidence', 'unknown'] },
            evidence:   { type: 'string', description: 'One plain-text sentence citing concrete evidence. NO Slack formatting, no asterisks, no emoji.' },
            source:     { type: 'string', description: 'Where the evidence came from, e.g. "slack #ng-fullfillment-ops", "portal db", "ghl", "calendar", "knowledge base".' },
            confidence: { type: 'string', enum: ['high', 'medium', 'low'] },
          },
        },
      },
    },
  },
};

// Hard-banned in consult mode: circular evidence (REVI verifying itself) and all mutating tools.
const CONSULT_DROP_TOOLS = ['get_revi_intelligence', 'get_revi_client_context', 'save_knowledge', 'send_email', 'draft_outbound_email', 'draft_reply_email', 'draft_channel_post', 'create_slack_reminder', 'create_calendar_event', 'add_calendar_attendees', 'create_notion_task', 'create_scheduled_task', 'delete_scheduled_task', 'clean_duplicate_tasks', 'update_portal_record'];

const CONSULT_SYSTEM_APPEND = `

AGENT CONSULT MODE: You are answering an API request from REVI (the sales-call coaching and leadership-initiative agent), not a Slack user. Rules for this request only:
- Do NOT use get_revi_intelligence — REVI's own data is exactly what you are being asked to independently verify. Use Slack channels, the portal database, GHL, Calendar, and the knowledge base instead.
- Evidence must be plain text: no Slack formatting, no backtick headers, no ALL CAPS names, no emoji.
- Do not draft channel posts, DMs, or emails. Do not save knowledge. Read-only investigation.
- Only report evidence you actually found via tools this request. If you find nothing, say no_evidence — never guess.`;

async function handleAgentConsult(req, res) {
  const secret = process.env.AGENT_CONSULT_SECRET;
  const json = (code, obj) => { res.writeHead(code, { 'Content-Type': 'application/json' }); res.end(JSON.stringify(obj)); };
  if (!secret) return json(503, { ok: false, error: 'consult disabled' });
  if (req.headers['x-agent-secret'] !== secret) {
    console.warn('Agent consult rejected — invalid or missing secret');
    return json(401, { ok: false, error: 'Unauthorized' });
  }
  let body = '';
  let tooLarge = false;
  req.on('data', chunk => {
    body += chunk.toString();
    if (body.length > 100_000 && !tooLarge) { tooLarge = true; json(413, { ok: false, error: 'payload too large' }); req.destroy(); }
  });
  req.on('end', async () => {
    if (tooLarge) return;
    let payload;
    try { payload = JSON.parse(body); } catch { return json(400, { ok: false, error: 'invalid JSON' }); }
    const correlationId = newCorrelationId();
    try {
      if (payload.purpose === 'initiative_check' && Array.isArray(payload.initiatives) && payload.initiatives.length) {
        const items = payload.initiatives.slice(0, 15);
        const lines = items.map((i, n) =>
          `${n + 1}. id: ${i.id}\n   title: ${i.title}\n   owner: ${i.owner_name || 'unowned'}\n   committed next step: ${i.next_step || '(none)'}\n   last movement: ${i.last_movement_at || 'unknown'} (${i.days_stalled != null ? i.days_stalled + ' days stalled' : 'age unknown'})\n   context: ${i.description || '(none)'}`
        ).join('\n');
        const msg = `REVI initiative verification request. The following leadership initiatives show NO movement in meetings for 10+ days. For each one, investigate whether real-world progress or completion actually happened outside of meetings — check the relevant Slack channels, the portal database, GHL, Calendar, and your knowledge base. Batch your lookups efficiently. Then call submit_verdicts exactly once, covering every id below.\n\n${lines}`;
        console.log(`Agent consult [${correlationId}]: initiative_check for ${items.length} initiative(s)`);
        const result = await callClaude([{ role: 'user', content: msg }], 3, null, correlationId, {
          systemAppend: CONSULT_SYSTEM_APPEND,
          finalTool: SUBMIT_VERDICTS_TOOL,
          dropTools: CONSULT_DROP_TOOLS,
        });
        const requestedIds = new Set(items.map(i => String(i.id)));
        const raw = (result && result.structured && Array.isArray(result.structured.verdicts)) ? result.structured.verdicts : [];
        const verdicts = raw.filter(v => v && requestedIds.has(String(v.id)));
        const answeredIds = new Set(verdicts.map(v => String(v.id)));
        for (const id of requestedIds) {
          if (!answeredIds.has(id)) verdicts.push({ id, status: 'unknown', evidence: '', source: '', confidence: 'low' });
        }
        console.log(`Agent consult [${correlationId}]: returning ${verdicts.length} verdict(s)`);
        return json(200, { ok: true, checked_at: new Date().toISOString(), verdicts });
      }
      if (payload.purpose === 'question' && typeof payload.question === 'string' && payload.question.trim()) {
        console.log(`Agent consult [${correlationId}]: question`);
        const answer = await callClaude([{ role: 'user', content: payload.question.slice(0, 4000) }], 3, null, correlationId, {
          systemAppend: CONSULT_SYSTEM_APPEND,
          dropTools: CONSULT_DROP_TOOLS,
        });
        return json(200, { ok: true, answer: (typeof answer === 'string' ? answer : '').slice(0, 4000) });
      }
      return json(400, { ok: false, error: 'unknown purpose — expected initiative_check or question' });
    } catch (err) {
      console.error(`Agent consult error [${correlationId}]:`, err.message);
      return json(500, { ok: false, error: 'internal error' });
    }
  });
}

// ─── HEALTH CHECK SERVER ──────────────────────────────────────────────────────
const healthServer = http.createServer((req, res) => {
  if (req.url === '/health' && req.method === 'GET') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ status: 'ok', agent: 'NeuroGrowth PM Agent (Max)', uptime: Math.floor(process.uptime()), timestamp: new Date().toISOString() }));
  } else if (req.url === '/webhook/ghl-lead' && req.method === 'POST') {
    handleGHLWebhook(req, res);
  } else if (req.url === '/webhook/ghl-claim' && req.method === 'POST') {
    handleGHLClaimWebhook(req, res);
  } else if (req.url === '/webhook/supabase' && req.method === 'POST') {
    handleSupabaseWebhook(req, res);
  } else if (req.url === '/agent/consult' && req.method === 'POST') {
    handleAgentConsult(req, res);
  } else {
    res.writeHead(404); res.end('Not found');
  }
});
healthServer.listen(process.env.PORT || 3000, () => {
  console.log(`Health check server listening on port ${process.env.PORT || 3000}`);
});

// ─── SOCKET HEALTH ──────────────────────────────────────────────────────────
// Max receives every inbound Slack event (reaction-based
// lead claiming, @mentions, DMs) over a single Socket Mode WebSocket. Crons post
// outbound via the bot token and keep working even when the socket is dead, so a
// silent socket failure looks like "Max half-works" — exactly what happened when
// inbound went dark from Jun 26 while reports kept posting. These guards make a
// socket failure loud instead of silent.
const SOCKET_ALERT_CHANNEL = process.env.SOCKET_ALERT_CHANNEL || OPS_CHANNEL;

async function alertSocketHealth(text) {
  try {
    await slack.client.chat.postMessage({ channel: SOCKET_ALERT_CHANNEL, text });
  } catch (e) {
    console.error('alertSocketHealth: failed to post alert:', e.message);
  }
}

// Watchdog: if the socket drops and does not recover within the grace window,
// post one alert. Debounced — at most one alert per outage; cleared on reconnect.
function attachSocketWatchdog() {
  const client = slack.receiver && slack.receiver.client;
  if (!client || typeof client.on !== 'function') {
    console.error('Socket watchdog: SocketMode client unavailable — watchdog not attached.');
    return;
  }
  const GRACE_MS = 90 * 1000;
  let downTimer = null;
  let alerted = false;

  client.on('disconnected', () => {
    console.error('[socket] disconnected — starting watchdog grace timer.');
    if (downTimer) return;
    downTimer = setTimeout(() => {
      alerted = true;
      console.error('[socket] still down after grace window — alerting ops.');
      alertSocketHealth('🚨 Max Socket Mode has been disconnected for 90s+ — inbound Slack events (reaction lead-claiming, @mentions, DMs) are NOT being processed. Crons still post. Check Railway logs / SLACK_APP_TOKEN.');
    }, GRACE_MS);
  });

  const onUp = (state) => {
    if (downTimer) { clearTimeout(downTimer); downTimer = null; }
    if (alerted) {
      alerted = false;
      console.log(`[socket] reconnected (${state}) — inbound events flowing again.`);
      alertSocketHealth('✅ Max Socket Mode reconnected — inbound Slack events are flowing again.');
    }
  };
  client.on('authenticated', () => onUp('authenticated'));
  client.on('connected', () => onUp('connected'));
}

// ─── ALERT TRIAGE BRIDGE ────────────────────────────────────────────────────
// Detectors in this file historically format a string, post it to Slack, and stop —
// Max never sees his own alerts, so nothing can act on them. This bridge closes that
// loop: a detector hands over a structured alert plus the remediation actions it is
// willing to authorize, Max investigates with read-only tools, and returns ONE
// structured proposal. Nothing executes here; execution is the reaction route below.
// Same finalTool + systemAppend shape as handleAgentConsult (REVI → Max verdicts).

const REMEDIATION_CHANNEL = process.env.REMEDIATION_CHANNEL || OPS_CHANNEL;
const REMEDIATION_COOLDOWN_MS = Number(process.env.REMEDIATION_COOLDOWN_H || 6) * 60 * 60 * 1000;
// Armed in stages: with this set, proposals still post and ✅ still resolves them,
// but the executor is never reached. Lets the triage reasoning be judged on real
// alerts before it can touch production Make.
const REMEDIATION_DRY_RUN = String(process.env.REMEDIATION_DRY_RUN || '') === 'true';

const TRIAGE_SYSTEM_APPEND = `

ALERT TRIAGE MODE: an automated detector fired and you are diagnosing it, not chatting.
- Investigate with the tools you were given. Every claim must come from a tool result in THIS request. Do not speculate.
- Name the concrete affected records (contact, appointment, booking) when the data contains them. "Some executions failed" is a useless proposal.
- Recommend exactly one action from the allowed list. If the evidence supports none of them, or the real fix is a human code/blueprint change, recommend no_action or escalate and say why.
- risk_notes is mandatory whenever you recommend an action: state plainly what goes wrong if a human approves this and you were wrong.
- Plain text only. No markdown, no asterisks, no emoji, no bullet characters.
- You are PROPOSING. You have no write access here. Never say you fixed, retried, restarted, or resolved anything.`;

function buildTriageTool(actionIds) {
  return {
    name: 'propose_remediation',
    description: 'Submit your single final triage verdict. Call this exactly once; it ends the triage.',
    input_schema: {
      type: 'object',
      required: ['summary', 'root_cause', 'recommended_action', 'confidence'],
      properties: {
        summary:            { type: 'string', description: 'One or two plain sentences a human reads first. Name the real records.' },
        root_cause:         { type: 'string', description: 'What actually broke, citing the evidence you found.' },
        recommended_action: { type: 'string', enum: [...actionIds, 'no_action', 'escalate'] },
        dlq_ids:            { type: 'array', items: { type: 'string' }, description: 'Retry actions only: the specific incomplete-execution ids to retry. Omit otherwise.' },
        confidence:         { type: 'string', enum: ['high', 'medium', 'low'] },
        risk_notes:         { type: 'string', description: 'What breaks if a human approves this and you are wrong.' },
      },
    },
  };
}

// alert = { alertKey, source, title, facts{}, actions[{id,description}], guidance,
//           onlyTools[], correlationId }
async function triageAlert(alert) {
  const factLines   = Object.entries(alert.facts || {}).map(([k, v]) => `  ${k}: ${v}`).join('\n');
  const actionLines = (alert.actions || []).map(a => `  ${a.id} — ${a.description}`).join('\n');
  const msg = [
    `AUTOMATED ALERT from ${alert.source}. Diagnose it, then call propose_remediation exactly once.`,
    '', `Alert: ${alert.title}`, 'Facts:', factLines,
    '', 'Actions a human may approve:', actionLines,
    '  no_action — the alert is benign or self-resolving',
    '  escalate — a human must intervene; no automated action is safe',
    '', alert.guidance || '',
  ].join('\n');

  const result = await callClaude([{ role: 'user', content: msg }], 3, null, alert.correlationId, {
    systemAppend: TRIAGE_SYSTEM_APPEND,
    finalTool:    buildTriageTool((alert.actions || []).map(a => a.id)),
    onlyTools:    alert.onlyTools || [],
  });
  const proposal = result && result.structured;
  if (!proposal || !proposal.recommended_action) return null;  // completeness guard
  return proposal;
}

async function resolveOpsChannelId() {
  try {
    const channels = await getCachedChannelList();
    const match = channels.find(c => c.name === String(REMEDIATION_CHANNEL).replace('#', ''));
    if (match) return match.id;
  } catch (e) {
    console.error('resolveOpsChannelId failed:', e.message);
  }
  return REMEDIATION_CHANNEL;
}

// Restart-safe dedup: the Slack message IS the state, so this survives the redeploys
// that wipe every in-memory Map in this file. Any proposal for this alertKey inside
// the cooldown — pending, executed, OR dismissed — suppresses a new one.
// Fails CLOSED: if history is unreadable we suppress, because the alternative is a
// proposal storm every 10 minutes. The plain watchdog alert still fires regardless.
async function hasOpenRemediationProposal(alertKey) {
  try {
    const channel = await resolveOpsChannelId();
    if (String(channel).startsWith('#')) return true;   // never resolved to an id → cannot dedup
    const hist = await slack.client.conversations.history({
      channel, limit: 200, include_all_metadata: true,
      oldest: String((Date.now() - REMEDIATION_COOLDOWN_MS) / 1000),
    });
    return (hist.messages || []).some(m =>
      m.metadata && m.metadata.event_type === 'remediation_proposal' &&
      m.metadata.event_payload && m.metadata.event_payload.alert_key === alertKey);
  } catch (e) {
    console.error('hasOpenRemediationProposal failed, suppressing to be safe:', e.message);
    return true;
  }
}

async function postRemediationProposal({ alertKey, executor, title, proposal, payload, correlationId }) {
  const channel = await resolveOpsChannelId();
  const dry = REMEDIATION_DRY_RUN;
  const lines = [
    `🛠 *Remediation proposed* — ${title}${dry ? '  _(DRY RUN)_' : ''}`,
    '',
    proposal.summary,
    '',
    `*Root cause:* ${proposal.root_cause}`,
    `*Proposed action:* \`${proposal.recommended_action}\` (confidence: ${proposal.confidence})`,
    proposal.risk_notes ? `*Risk if wrong:* ${proposal.risk_notes}` : null,
    '',
    dry ? '_Dry run — ✅ will explain what would happen, but will not touch Make._'
        : '✅ to execute · ❌ to dismiss. Any NeuroGrowth roster member may approve.',
  ].filter(Boolean);

  const res = await slack.client.chat.postMessage({
    channel, text: lines.join('\n'),
    // Slack metadata must be flat primitives/arrays and small — it is the durable
    // state the reaction handler rehydrates from. Never put a payload bundle here.
    metadata: {
      event_type: 'remediation_proposal',
      event_payload: {
        v: 1, alert_key: alertKey, executor,
        action: proposal.recommended_action,
        dry_run: dry ? 'true' : 'false',
        correlation_id: correlationId || '',
        ...payload,
      },
    },
  });
  logActivity({
    event_type: 'remediation', event_source: 'cron', action: 'remediation_proposed',
    status: proposal.confidence, channel_id: res.channel, thread_ts: res.ts,
    output: { alert_key: alertKey, executor, action: proposal.recommended_action,
              summary: proposal.summary, risk_notes: proposal.risk_notes || null },
    metadata: payload, correlation_id: correlationId,
  });
  return res;
}

// Claim-lock emoji: whoever wins reactions.add executes. Stronger than checking
// existing reactions after the fact (the campaign flow does that, and it leaves a
// real double-execute window when two people react at once).
const REMEDIATION_LOCK_EMOJI = 'hourglass_flowing_sand';

function summariseRemediation(meta, r) {
  if (meta.executor === 'make_scenario_reactivate') {
    return r.already_active ? `${r.scenario} was already active.`
                            : `${r.scenario} restarted — queued webhooks replay automatically.`;
  }
  const extra = [
    r.gone && r.gone.length ? `${r.gone.length} already resolved` : null,
    r.too_old && r.too_old.length ? `${r.too_old.length} skipped (past the CAPI dedup window)` : null,
    r.failed && r.failed.length ? `${r.failed.length} failed` : null,
    r.capped ? `capped at ${MAKE_RETRY_CAP}` : null,
  ].filter(Boolean).join(', ');
  return `retried ${r.retried} incomplete execution(s) on ${r.scenario}${extra ? ` — ${extra}` : ''}.`;
}

// Returns true when the reaction was ours (handled), false to fall through.
async function handleRemediationReaction(event, baseEmoji) {
  const channel = event.item.channel, ts = event.item.ts;
  const hist = await slack.client.conversations.history({
    channel, latest: ts, limit: 1, inclusive: true, include_all_metadata: true,
  });
  const msg = hist.messages && hist.messages[0];
  if (!msg || msg.ts !== ts) return false;
  const meta = msg.metadata && msg.metadata.event_type === 'remediation_proposal'
    ? msg.metadata.event_payload : null;
  if (!meta) return false;   // not a proposal — fall through

  // The proposal lives in a team-visible channel, so a live Make write must not be
  // triggerable by anyone who happens to be in it.
  if (!isRosterMember(event.user)) {
    await slack.client.chat.postMessage({ channel, thread_ts: ts,
      text: `<@${event.user}> you're not on the NeuroGrowth roster, so I can't act on that. Ask Ron.` });
    return true;
  }

  try {
    await slack.client.reactions.add({ channel, timestamp: ts, name: REMEDIATION_LOCK_EMOJI });
  } catch (e) {
    const err = String((e && e.data && e.data.error) || e.message || '');
    if (err.includes('already_reacted')) {
      console.log(`remediation ${meta.alert_key}: already claimed, ignoring duplicate reaction`);
      return true;
    }
    throw e;
  }

  const corr  = meta.correlation_id || newCorrelationId();
  const react = (name) => slack.client.reactions.add({ channel, timestamp: ts, name }).catch(() => {});
  const reply = (text) => slack.client.chat.postMessage({ channel, thread_ts: ts, text });

  if (CAMPAIGN_SKIP_EMOJIS.has(baseEmoji)) {
    await react('no_entry');
    await reply(`Dismissed by <@${event.user}>. Nothing executed. The watchdog keeps alerting, but I won't re-propose for ${Math.round(REMEDIATION_COOLDOWN_MS / 3600000)}h.`);
    logActivity({ event_type: 'remediation', event_source: 'slack', action: 'remediation_rejected',
      actor_user_id: event.user, channel_id: channel, thread_ts: ts,
      output: { alert_key: meta.alert_key, executor: meta.executor, action: meta.action },
      correlation_id: corr });
    return true;
  }

  if (meta.dry_run === 'true') {
    await react('eyes');
    await reply(`Dry run — would have run \`${meta.executor}\` on scenario ${meta.scenario_id}${meta.dlq_ids ? ` for ${meta.dlq_ids.length} queued item(s)` : ''}. Nothing was touched. Unset REMEDIATION_DRY_RUN to arm this.`);
    logActivity({ event_type: 'remediation', event_source: 'slack', action: 'remediation_dry_run',
      actor_user_id: event.user, channel_id: channel, thread_ts: ts,
      output: { alert_key: meta.alert_key, executor: meta.executor }, correlation_id: corr });
    return true;
  }

  let result;
  try {
    if (meta.executor === 'make_dlq_retry')                 result = await retryMakeDlqs(meta.scenario_id, meta.dlq_ids || []);
    else if (meta.executor === 'make_scenario_reactivate')  result = await reactivateMakeScenario(meta.scenario_id);
    else result = { ok: false, error: `Unknown executor "${meta.executor}".` };
  } catch (err) {
    result = { ok: false, error: err.message };
  }

  await react(result.ok ? 'white_check_mark' : 'warning');
  await reply(result.ok
    ? `✅ Executed by <@${event.user}> — ${summariseRemediation(meta, result)}`
    : `❌ Not executed: ${result.error}`);
  logActivity({
    event_type: 'remediation', event_source: 'slack',
    action: result.ok ? 'remediation_executed' : 'remediation_blocked',
    status: result.ok ? 'ok' : 'blocked',
    actor_user_id: event.user, channel_id: channel, thread_ts: ts,
    output: { alert_key: meta.alert_key, executor: meta.executor, result }, correlation_id: corr,
  });
  return true;
}

// ─── GHL APPOINTMENT DELETION SWEEP ─────────────────────────────────────────
// Deleting an appointment in GHL fires NO workflow and NO webhook — the call
// silently vanishes from every report while Slack stays quiet (the 🔴 cancel
// alert only fires on a status change). This sweep probes upcoming ingested
// appointments against the GHL API and routes true deletions through the same
// cancel-alert pipeline (dedupe + outbox + Make scenario 5822303) via the
// portal RPC ng_mark_appt_deleted, which also marks the revops row cancelled
// the way dash's cancelled-mode upsert does. Direction is always "by team":
// clients cannot delete an appointment — they can only cancel (status change),
// which flows through the webhook path with its own by-client/by-team label.
const GHL_APPT_PROBE_BASE = 'https://services.leadconnectorhq.com/calendars/events/appointments/';

// Pure classifier — extracted verbatim by test/appt-deletion-sweep.test.js, keep
// dependency-free. GHL surfaces deletion two ways depending on the endpoint era:
// a 404, or a 200 whose appointment carries deleted:true. A cancelled status is
// NOT a deletion — the webhook path owns that alert.
function classifyGhlApptProbe(httpStatus, body) {
  if (httpStatus === 404) return 'deleted';
  if (httpStatus < 200 || httpStatus >= 300) return 'error';
  const appt = body && body.appointment ? body.appointment : null;
  if (!appt) return 'error';
  if (appt.deleted === true) return 'deleted';
  const st = String(appt.appoinmentStatus || appt.appointmentStatus || '').toLowerCase();
  if (st === 'cancelled' || st === 'canceled') return 'cancelled';
  return 'active';
}

async function runApptDeletionSweep(correlationId) {
  if (!process.env.GHL_API_KEY) return;
  // Yesterday → +45 days: recent enough that a deletion still matters to the
  // team, far enough out to cover everything bookable. Rows already marked
  // cancelled (webhook path or a previous sweep) are skipped client-side —
  // qualification_snapshot json filtering is clearer here than in PostgREST.
  const sinceIso = new Date(Date.now() - 2 * 24 * 3600 * 1000).toISOString();
  const untilIso = new Date(Date.now() + 45 * 24 * 3600 * 1000).toISOString();
  const { data: appts, error } = await portalSupabase
    .from('revops_appointments')
    .select('ghl_appointment_id, scheduled_start, qualification_snapshot')
    .eq('source', 'ghl')
    .not('ghl_appointment_id', 'is', null)
    .gte('scheduled_start', sinceIso)
    .lte('scheduled_start', untilIso)
    .order('scheduled_start', { ascending: true })
    .limit(80);
  if (error) { console.error('apptDeletionSweep: query failed:', error.message); return; }
  const candidates = (appts || []).filter(a => {
    const snap = a.qualification_snapshot;
    return !(snap && (snap.cancelled === true || snap.cancelled === 'true'));
  });

  let probed = 0, deleted = 0, errors = 0;
  for (const a of candidates) {
    let state;
    try {
      const res = await fetch(GHL_APPT_PROBE_BASE + encodeURIComponent(a.ghl_appointment_id), {
        headers: { Authorization: `Bearer ${process.env.GHL_API_KEY}`, Version: '2021-07-28' },
      });
      const body = res.status === 404 ? null : await res.json().catch(() => null);
      state = classifyGhlApptProbe(res.status, body);
    } catch (e) {
      errors++;
      // Three transport failures = GHL (or the network) is down; a half-swept
      // list is fine — every appointment is re-probed on the next run.
      if (errors >= 3) { console.error('apptDeletionSweep: aborting, GHL unreachable:', e.message); return; }
      continue;
    }
    probed++;
    if (state === 'error') { errors++; continue; }
    if (state !== 'deleted') continue;
    const { data: outcome, error: rpcErr } = await portalSupabase
      .rpc('ng_mark_appt_deleted', { p_ghl_appointment_id: a.ghl_appointment_id });
    if (rpcErr) {
      console.error(`apptDeletionSweep: mark failed for ${a.ghl_appointment_id}:`, rpcErr.message);
      errors++;
    } else {
      deleted++;
      console.log(`apptDeletionSweep: ${a.ghl_appointment_id} deleted in GHL → ${outcome}`);
    }
  }
  console.log(`apptDeletionSweep [${correlationId}]: probed ${probed}/${candidates.length}, deleted ${deleted}, errors ${errors}`);
}

// Every 3 hours across the CR working day — deletions are rare, and each run
// re-probes the full window, so a missed run self-heals on the next.
cron.schedule('20 7-19/3 * * *', wrapCronJob('runApptDeletionSweep', async (c) => { await runApptDeletionSweep(c); }), { timezone: 'America/Costa_Rica' });
// ─── end GHL APPOINTMENT DELETION SWEEP ─────────────────────────────────────

// ─── MAKE SCENARIO HEALTH ───────────────────────────────────────────────────
// Make auto-deactivates a scenario when it errors at initialization (an invalid
// blueprint, a broken connection). Incoming webhooks queue rather than vanish, so
// nothing is lost — but CAPI events go stale and setters see no booking alerts
// until someone reads the Make email. That is the blind spot this closes: on
// 2026-08-04 [PROD] GHL Appt Booked sat deactivated for ~7 min and the only
// signal was an email to Ron's inbox.
//
// Watches every scenario whose name starts with [PROD] so new ones are covered
// without a code change. Inert unless MAKE_API_TOKEN is set.
const MAKE_API_TOKEN   = process.env.MAKE_API_TOKEN || null;
const MAKE_API_BASE    = process.env.MAKE_API_BASE  || 'https://us2.make.com/api/v2';
const MAKE_TEAM_ID     = process.env.MAKE_TEAM_ID   || '432699';
const MAKE_ALERT_CHANNEL = process.env.MAKE_ALERT_CHANNEL || OPS_CHANNEL;

// Scenarios that are [PROD]-named but deactivated ON PURPOSE, so the watchdog does
// not cry wolf. 5776020 [PROD] Auto Strike Mover was superseded by Max's own
// runAutoStrikeMover cron and turned off deliberately. Cleaner long-term fix is to
// rename retired scenarios out of the [PROD] prefix (Ron already does this with
// [DISABLED …] / [ARCHIVED …]) and drop them from this list.
const MAKE_WATCHDOG_IGNORE = new Set(
  String(process.env.MAKE_WATCHDOG_IGNORE || '5776020')
    .split(',').map(s => s.trim()).filter(Boolean)
);

// scenarioId -> reason we already alerted on. Cleared when the scenario recovers.
// In-memory by design (matches the socket watchdog): after a Railway restart a
// still-broken scenario re-alerts on the next hourly run, which is the behaviour
// we want — a restart should never silently swallow an open outage.
const makeScenarioAlerted = new Map();

// dlqCount oscillates: Make retries incomplete executions, so the count drops to
// zero and comes back. Testing it raw produced three alert/all-clear pairs in
// twenty minutes on 2026-08-13 (05:00, 05:10, 05:20) — the fastest way to teach
// everyone to mute the channel. Both edges are damped: a DLQ must persist for
// DLQ_CONFIRM_POLLS before it alerts, and must stay clear just as long before the
// all-clear. isActive/isinvalid are unambiguous and stay immediate.
const DLQ_CONFIRM_POLLS = 3; // ~30 min at the */10 cadence
const makeDlqStreak   = new Map(); // scenarioId -> consecutive polls with dlqCount > 0
const makeClearStreak = new Map(); // scenarioId -> consecutive clear polls while alerted

// ── Agentic remediation config ──────────────────────────────────────────────
// Triage is opt-in. Unset → every agentic path below is inert and the watchdog
// behaves exactly as it did before, which is also what keeps the untouched
// test/make-watchdog.test.js green (it builds this block with a fixed set of
// injected params, so an ungated callClaude would ReferenceError).
const MAKE_TRIAGE_ENABLED = String(process.env.MAKE_TRIAGE_ENABLED || '') === 'true';
const MAKE_RETRY_CAP      = Number(process.env.MAKE_RETRY_CAP || 10);
// Meta dedups CAPI events on a deterministic event_id (ghl_schedule_<appointmentId>)
// for 48h. Scenario 5148796 sets event_time to {{now}}, so retrying an execution that
// already fired its CAPI modules AFTER that window can double-count a Schedule /
// Qualified conversion and corrupt ad optimization. Refuse those; a human can still
// force one by hand in Make.
const MAKE_DLQ_MAX_AGE_MS = Number(process.env.MAKE_DLQ_MAX_AGE_H || 44) * 60 * 60 * 1000;

// Thin wrapper on the ghlFetchJson convention: bare fetch, throw on !ok, no retry.
async function makeApi(path, init = {}) {
  const res = await fetch(`${MAKE_API_BASE}${path}`, {
    ...init,
    headers: { Authorization: `Token ${MAKE_API_TOKEN}`, 'Content-Type': 'application/json', ...(init.headers || {}) },
  });
  const body = await res.text().catch(() => '');
  if (!res.ok) throw new Error(`Make ${init.method || 'GET'} ${path} → ${res.status}: ${body.slice(0, 200)}`);
  try { return body ? JSON.parse(body) : {}; } catch { return {}; }
}

// ── DLQ (incomplete executions) reads — exposed to Max as read-only tools ────
async function listMakeDlqs(scenarioId, limit = 10) {
  if (!MAKE_API_TOKEN) return { error: 'MAKE_API_TOKEN not set' };
  try {
    const n = Math.min(Number(limit) || 10, 25);
    const out = await makeApi(`/dlqs?scenarioId=${encodeURIComponent(scenarioId)}&pg%5Blimit%5D=${n}`);
    // Envelope key varies by Make API version — accept either shape rather than
    // silently returning zero rows and reading as "queue is empty".
    const rows = Array.isArray(out.dlqs) ? out.dlqs
               : Array.isArray(out.incompleteExecutions) ? out.incompleteExecutions
               : [];
    return {
      scenario_id: String(scenarioId), count: rows.length,
      dlqs: rows.map(d => ({
        id: String(d.id),
        created: d.created || d.timestamp || d.executionDate || null,
        resolved: d.resolved === undefined ? null : d.resolved,
        reason: d.reason || d.error || null,
        index: d.index === undefined ? null : d.index,
      })),
    };
  } catch (e) {
    return { error: e.message };
  }
}

async function getMakeDlqDetail(dlqId) {
  if (!MAKE_API_TOKEN) return { error: 'MAKE_API_TOKEN not set' };
  const id = encodeURIComponent(String(dlqId));
  const [detail, bundle] = await Promise.all([
    makeApi(`/dlqs/${id}`).catch(e => ({ error: e.message })),
    makeApi(`/dlqs/${id}/bundle`).catch(e => ({ error: e.message })),
  ]);
  // Hard truncate: a bundle is arbitrary customer payload heading into an LLM context.
  return { id: String(dlqId), detail, bundle: JSON.stringify(bundle).slice(0, 6000) };
}

// ── Executors (run only after a human ✅ on a proposal) ──────────────────────
// Allow-list gate shared by both executors. Returns a human-readable "Blocked: …"
// rather than throwing, matching the PORTAL_WRITE_WHITELIST convention.
async function assertRemediableScenario(scenarioId) {
  const s = (await makeApi(`/scenarios/${encodeURIComponent(scenarioId)}`)).scenario;
  if (!s) return { blocked: `Blocked: Make scenario ${scenarioId} not found.` };
  if (!String(s.name || '').startsWith('[PROD]')) {
    return { blocked: `Blocked: "${s.name}" is not a [PROD] scenario — remediation is [PROD]-only.` };
  }
  if (MAKE_WATCHDOG_IGNORE.has(String(s.id))) {
    return { blocked: `Blocked: "${s.name}" is on MAKE_WATCHDOG_IGNORE — it is switched off on purpose.` };
  }
  return { scenario: s };
}

async function retryMakeDlqs(scenarioId, dlqIds) {
  const gate = await assertRemediableScenario(scenarioId);
  if (gate.blocked) return { ok: false, error: gate.blocked };

  const asked = [...new Set((dlqIds || []).map(String))];
  if (!asked.length) return { ok: false, error: 'Blocked: no incomplete-execution ids supplied.' };

  // Re-list live DLQs at EXECUTION time, hours after the proposal was written:
  // drops ids the model hallucinated, ids a human already cleared by hand, and
  // ids now too old for Meta's CAPI dedup window.
  const live = await listMakeDlqs(scenarioId, 25);
  if (live.error) return { ok: false, error: `Blocked: could not re-read the queue (${live.error}).` };
  const byId = new Map((live.dlqs || []).map(d => [String(d.id), d]));
  const tooOld = [], gone = [];
  const ids = asked.filter(id => {
    const d = byId.get(id);
    if (!d) { gone.push(id); return false; }
    const t = d.created ? Date.parse(d.created) : NaN;
    if (Number.isFinite(t) && Date.now() - t > MAKE_DLQ_MAX_AGE_MS) { tooOld.push(id); return false; }
    return true;
  }).slice(0, MAKE_RETRY_CAP);

  if (!ids.length) {
    return { ok: false, gone, too_old: tooOld,
      error: `Blocked: nothing retryable — ${gone.length} already resolved, ${tooOld.length} past the ${Math.round(MAKE_DLQ_MAX_AGE_MS / 3600000)}h CAPI-dedup window.` };
  }

  const results = [];
  for (const id of ids) {
    try {
      await makeApi(`/dlqs/${encodeURIComponent(id)}/retry`, { method: 'POST' });
      results.push({ id, ok: true });
    } catch (e) {
      results.push({ id, ok: false, error: e.message });
    }
  }
  const okIds = results.filter(r => r.ok).map(r => r.id);
  return {
    ok: okIds.length > 0, scenario: gate.scenario.name, retried: okIds.length,
    failed: results.filter(r => !r.ok), gone, too_old: tooOld,
    capped: asked.length > MAKE_RETRY_CAP,
  };
}

async function reactivateMakeScenario(scenarioId) {
  const gate = await assertRemediableScenario(scenarioId);
  if (gate.blocked) return { ok: false, error: gate.blocked };
  const s = gate.scenario;
  // The 2026-08-04 guard, re-checked HERE because hours pass between proposal and ✅:
  // starting a scenario whose blueprint is invalid just auto-deactivates it again on
  // the next incoming event, and looks like the fix worked until the next booking.
  if (s.isinvalid === true) {
    return { ok: false, error: `Blocked: "${s.name}" has an INVALID BLUEPRINT. Reactivating would auto-deactivate again on the next event. Fix the blueprint first: https://us2.make.com/${MAKE_TEAM_ID}/scenarios/${s.id}/edit` };
  }
  if (s.isActive === true) return { ok: true, already_active: true, scenario: s.name };
  await makeApi(`/scenarios/${encodeURIComponent(scenarioId)}/start`, { method: 'POST' });
  return { ok: true, scenario: s.name };
}

async function postMakeHealthAlert(text) {
  try {
    let channel = MAKE_ALERT_CHANNEL;
    try {
      const channels = await getCachedChannelList();
      const match = channels.find(c => c.name === String(MAKE_ALERT_CHANNEL).replace('#', ''));
      if (match) channel = match.id;
    } catch (e) {
      console.error('postMakeHealthAlert: channel lookup failed, posting by name:', e.message);
    }
    await slack.client.chat.postMessage({ channel, text });
  } catch (e) {
    console.error('postMakeHealthAlert: failed to post alert:', e.message);
  }
}

async function checkMakeScenarioHealth(correlationId) {
  if (!MAKE_API_TOKEN) return;

  // Paginate rather than assume one page holds the team's ~50 scenarios — Make does
  // not document the default pg[limit], and a silent truncation here would drop
  // scenarios from the watch list without any visible symptom.
  const all = [];
  const PAGE = 100;
  for (let offset = 0; offset < 1000; ) {
    const url = `${MAKE_API_BASE}/scenarios?teamId=${encodeURIComponent(MAKE_TEAM_ID)}`
              + `&pg%5Blimit%5D=${PAGE}&pg%5Boffset%5D=${offset}`;
    const res = await fetch(url, { headers: { Authorization: `Token ${MAKE_API_TOKEN}` } });
    if (!res.ok) {
      // A dead token is itself an outage of the watchdog — say so once per occurrence
      // rather than failing silently, which is the exact failure mode being fixed here.
      const body = await res.text().catch(() => '');
      console.error(`Make health: API ${res.status} — ${body.slice(0, 300)}`);
      if (!makeScenarioAlerted.has('__api__')) {
        makeScenarioAlerted.set('__api__', `api_${res.status}`);
        await postMakeHealthAlert(
          `⚠️ <@${RON_SLACK_ID}> Max cannot reach the Make API (HTTP ${res.status}) — the [PROD] scenario watchdog is blind until this is fixed. Check MAKE_API_TOKEN in Railway.`
        );
      }
      return;
    }
    const payload = await res.json();
    const page = Array.isArray(payload.scenarios) ? payload.scenarios : [];
    all.push(...page);
    if (!page.length) break;
    offset += page.length; // advance by what was actually returned, not what was asked for
  }
  makeScenarioAlerted.delete('__api__');

  const scenarios = all.filter(s =>
    String(s.name || '').startsWith('[PROD]') && !MAKE_WATCHDOG_IGNORE.has(String(s.id))
  );
  if (!scenarios.length) {
    console.log('Make health: no [PROD] scenarios returned — nothing to check.');
    return;
  }

  for (const s of scenarios) {
    // Three distinct failure modes, most severe first:
    //   inactive — Make stopped it (error or manual): nothing runs at all
    //   invalid  — the blueprint is broken; this is what PRECEDES a deactivation
    //   dlq      — still active, but runs are failing and piling up as incomplete
    //              executions. This is the class Make emails as "the scenario has
    //              NOT been paused" — isActive stays true through every failed run,
    //              so without this a scenario can fail 100% of the time and look green.
    const hardReason = s.isActive === false ? 'inactive'
                     : s.isinvalid === true ? 'invalid'
                     : null;

    // Streak bookkeeping for the noisy signal. A dip resets it, so a flapping
    // queue never reaches the threshold and never alerts.
    const dlqStreak = Number(s.dlqCount) > 0 ? (makeDlqStreak.get(s.id) || 0) + 1 : 0;
    if (dlqStreak) makeDlqStreak.set(s.id, dlqStreak); else makeDlqStreak.delete(s.id);

    const reason = hardReason || (dlqStreak >= DLQ_CONFIRM_POLLS ? 'dlq' : null);
    const previous = makeScenarioAlerted.get(s.id);
    if (reason) makeClearStreak.delete(s.id);

    if (reason && previous !== reason) {
      makeScenarioAlerted.set(s.id, reason);
      const detail = reason === 'inactive'
        ? 'is *DEACTIVATED* — incoming webhooks are queuing and no CAPI events or Slack alerts are firing from it.'
        : reason === 'invalid'
        ? 'has an *INVALID BLUEPRINT* — it will auto-deactivate on the next incoming event.'
        : `is active but has *${s.dlqCount} incomplete execution(s)* queued — runs are erroring even though Make has not paused it.`;
      const consequence = reason === 'dlq'
        ? 'Each incomplete execution is a booking that did not get its CAPI event or Slack alert.'
        : 'Nothing is lost while it is down — webhooks replay on reactivation — but fix it before the queue ages.';
      await postMakeHealthAlert(
        `🚨 <@${RON_SLACK_ID}> Make scenario *${s.name}* (${s.id}) ${detail}\n` +
        `${consequence}\n` +
        `https://us2.make.com/${MAKE_TEAM_ID}/scenarios/${s.id}/edit`
      );
      logActivity({
        event_type: 'alert', event_source: 'cron', action: 'make_scenario_down',
        status: reason, output: { scenario_id: s.id, name: s.name }, correlation_id: correlationId,
      });
      // Agentic triage. This branch already guarantees "reason CHANGED", so triage
      // cannot fire on every 10-minute sweep. Fire-and-forget with a hard catch —
      // triage must never break the alert loop it rides on.
      if (MAKE_TRIAGE_ENABLED) {
        proposeMakeRemediation(s, reason, correlationId)
          .catch(e => console.error(`Make triage failed for ${s.id}:`, e.message));
      }
    } else if (!reason && previous) {
      // Damp the recovery edge too. Posting "recovered" on the first clear poll of
      // a flapping queue is what turned one incident into three all-clears.
      // A genuine fix (deactivated → active, queue drained) holds across polls.
      const clear = (makeClearStreak.get(s.id) || 0) + 1;
      if (previous === 'dlq' && clear < DLQ_CONFIRM_POLLS) {
        makeClearStreak.set(s.id, clear);
        continue;
      }
      makeClearStreak.delete(s.id);
      makeScenarioAlerted.delete(s.id);
      await postMakeHealthAlert(`✅ Make scenario *${s.name}* (${s.id}) is active again — queued webhooks replay automatically.`);
    }
  }

  const down = scenarios.filter(s => makeScenarioAlerted.has(s.id)).length;
  console.log(`Make health: checked ${scenarios.length} [PROD] scenario(s), ${down} down.`);
}

// The Make watchdog's triage consumer. Called fire-and-forget from
// checkMakeScenarioHealth; must never throw into the alert loop.
async function proposeMakeRemediation(s, reason, correlationId) {
  // 'invalid' is deliberately NOT triaged: neither authorized action repairs a broken
  // blueprint, and reactivateMakeScenario refuses it anyway. That one stays human.
  if (reason !== 'dlq' && reason !== 'inactive') return;

  const alertKey = `make:${s.id}`;
  if (await hasOpenRemediationProposal(alertKey)) {
    console.log(`Make triage: proposal for ${alertKey} already open/recent, skipping.`);
    return;
  }

  const actions = reason === 'inactive'
    ? [{ id: 'reactivate_scenario', description: 'Restart the scenario in Make. Queued webhooks replay automatically. Only safe if the blueprint is valid.' }]
    : [{ id: 'retry_dlqs', description: 'Retry specific incomplete executions. You must supply dlq_ids. Max 10 per approval.' }];

  const proposal = await triageAlert({
    alertKey, source: 'make_watchdog', correlationId,
    title: `${s.name} (${s.id}) — ${reason}`,
    facts: {
      scenario_id: s.id, scenario_name: s.name, reason,
      isActive: s.isActive, isinvalid: s.isinvalid, dlqCount: s.dlqCount === undefined ? 0 : s.dlqCount,
      link: `https://us2.make.com/${MAKE_TEAM_ID}/scenarios/${s.id}/edit`,
    },
    onlyTools: ['make_list_dlqs', 'make_get_dlq'],
    guidance: [
      'Start with make_list_dlqs for this scenario_id, then make_get_dlq on the individual entries (at most 5) to see which MODULE failed and which contact or appointment the bundle names. Name the real bookings in your summary.',
      'RETRY SAFETY for [PROD] GHL Appt Booked (5148796): modules 2 and 8 are GHL contact lookups that run BEFORE the two Meta CAPI events (module 3 Schedule, module 4 Qualified). A failure at 2 or 8 means CAPI never fired, so a retry is a pure win. Modules 21 and 23 run AFTER CAPI, so a retry re-sends both events; they carry deterministic event_ids (ghl_schedule_<appointmentId>) so Meta dedups them, but only inside a 48h window. State which case you are in.',
      'Slack double-posting is already prevented by an atomic dedupe RPC at module 9 — do not raise it as a risk.',
      'For dlq, recommend retry_dlqs and list the specific dlq_ids. For inactive, recommend reactivate_scenario — unless isinvalid is true, in which case recommend escalate.',
    ].join('\n'),
  });

  if (!proposal) {
    console.error(`Make triage: no structured proposal returned for ${alertKey}`);
    return;
  }

  if (proposal.recommended_action === 'no_action' || proposal.recommended_action === 'escalate') {
    logActivity({
      event_type: 'remediation', event_source: 'cron', action: 'remediation_declined',
      status: proposal.recommended_action,
      output: { alert_key: alertKey, summary: proposal.summary, root_cause: proposal.root_cause },
      correlation_id: correlationId,
    });
    if (proposal.recommended_action === 'escalate') {
      await postMakeHealthAlert(`🧭 <@${RON_SLACK_ID}> Triage on *${s.name}* (${s.id}): ${proposal.summary}\nNo safe automated fix — a human is needed. ${proposal.root_cause}`);
    }
    return;
  }

  await postRemediationProposal({
    alertKey, correlationId, proposal,
    executor: proposal.recommended_action === 'reactivate_scenario' ? 'make_scenario_reactivate' : 'make_dlq_retry',
    title: `Make · ${s.name} (${s.id})`,
    payload: {
      scenario_id: String(s.id), scenario_name: String(s.name),
      dlq_ids: (proposal.dlq_ids || []).map(String).slice(0, MAKE_RETRY_CAP),
    },
  });
}

if (MAKE_API_TOKEN) {
  // Every 10 min, not hourly: the 2026-08-04 outage lasted ~4 minutes end to end,
  // and an hourly poll would have seen isActive:true on both sides of it. One API
  // call per tick.
  cron.schedule('*/10 * * * *', wrapCronJob('checkMakeScenarioHealth', async (c) => { await checkMakeScenarioHealth(c); }), { timezone: 'America/Costa_Rica' });
  console.log('Registered static cron: Make [PROD] scenario watchdog (*/10 * * * *)');
} else {
  console.warn('Make scenario watchdog NOT registered — MAKE_API_TOKEN is not set.');
}

// ─── BOOKING → ALERT DIVERGENCE ─────────────────────────────────────────────
// The scenario watchdog above only sees Make. Make can be perfectly green while
// the booking pipeline is broken upstream of it — if a GHL workflow stops firing
// the webhook, Make has nothing to error on and reports full health. That is not
// hypothetical: the GHL customData bug (tasks/todo.md, 2026-08-02) silently
// skipped 33 deliveries with no system anywhere reporting a fault.
//
// So this watches the OUTCOME instead of the plumbing: every booking that landed
// in revops_appointments should have a matching booked-alert row, written by the
// Make scenario's ng_register_booked_alert RPC. A gap means the chain broke
// somewhere — GHL, Make, or Supabase — and it does not care which.
const bookingDivergenceAlerted = new Set();
const DIVERGENCE_LOOKBACK_MS = 6 * 60 * 60 * 1000;
const DIVERGENCE_GRACE_MS    = 30 * 60 * 1000; // in-flight bookings are not gaps

async function checkBookingAlertDivergence(correlationId) {
  const now   = Date.now();
  const since = new Date(now - DIVERGENCE_LOOKBACK_MS).toISOString();
  const until = new Date(now - DIVERGENCE_GRACE_MS).toISOString();

  const { data: apptsRaw, error: apptErr } = await portalSupabase
    .from('revops_appointments')
    .select('ghl_appointment_id, iclosed_call_id, source, created_at')
    .eq('source', 'ghl')
    .gte('created_at', since)
    .lte('created_at', until)
    .limit(200);
  if (apptErr) { console.error('Booking divergence: appointments read failed:', apptErr.message); return; }

  const excludeIds = await getNonFlywheelCallIds();
  const appts = filterFlywheelAppts(apptsRaw, excludeIds)
    .filter(a => typeof a.ghl_appointment_id === 'string' && a.ghl_appointment_id);
  if (!appts.length) { console.log('Booking divergence: no bookings in window.'); return; }

  const ids = appts.map(a => a.ghl_appointment_id);
  const { data: alertRows, error: alertErr } = await portalSupabase
    .from('ng_appt_slack_alerts')
    .select('appointment_id')
    .eq('kind', 'booked')
    .in('appointment_id', ids);
  // FAIL CLOSED. An unreadable alerts table makes every booking look unalerted,
  // which would fire a full-window false alarm and teach everyone to ignore this
  // channel. Silence plus a loud log is the correct failure here.
  if (alertErr) { console.error('Booking divergence: alerts read failed, skipping run:', alertErr.message); return; }

  const alerted = new Set((alertRows || []).map(r => r.appointment_id));
  const missing = appts.filter(a => !alerted.has(a.ghl_appointment_id));

  // Only alert on gaps not already reported, so a standing gap does not re-post
  // every 30 min while it is being fixed.
  const fresh = missing.filter(a => !bookingDivergenceAlerted.has(a.ghl_appointment_id));
  for (const a of missing) bookingDivergenceAlerted.add(a.ghl_appointment_id);
  // Bound the memo so a long-running process cannot grow it without limit.
  if (bookingDivergenceAlerted.size > 500) {
    for (const id of [...bookingDivergenceAlerted].slice(0, bookingDivergenceAlerted.size - 500)) {
      bookingDivergenceAlerted.delete(id);
    }
  }

  console.log(`Booking divergence: ${appts.length} booking(s) in window, ${missing.length} without a booked alert (${fresh.length} new).`);
  if (!fresh.length) return;

  const lines = fresh.slice(0, 10).map(a =>
    `• \`${a.ghl_appointment_id}\` — booked ${new Date(a.created_at).toLocaleString('en-US', { timeZone: 'America/Costa_Rica' })} CR`
  );
  const more = fresh.length > 10 ? `\n…and ${fresh.length - 10} more.` : '';
  await postMakeHealthAlert(
    `🚨 <@${RON_SLACK_ID}> *${fresh.length} booking(s) landed with no booked-alert posted* in the last 6h.\n` +
    `The appointment reached Supabase but the Make scenario never registered an alert for it — so the chain broke somewhere between GHL, Make and Supabase. Setters did not see these in #ng-sales-goats, and the Meta CAPI Schedule/Qualified events are likely missing too.\n` +
    lines.join('\n') + more
  );
  logActivity({
    event_type: 'alert', event_source: 'cron', action: 'booking_alert_divergence',
    status: 'gap', output: { missing: fresh.map(a => a.ghl_appointment_id) }, correlation_id: correlationId,
  });
}

// Every 30 min — the 30-minute grace window means a tighter cadence cannot
// surface anything sooner, and this costs two Supabase reads per run.
cron.schedule('*/30 * * * *', wrapCronJob('checkBookingAlertDivergence', async (c) => { await checkBookingAlertDivergence(c); }), { timezone: 'America/Costa_Rica' });
console.log('Registered static cron: booking → alert divergence check (*/30 * * * *)');

// ─── GMAIL FAN-OUT ALERT QUALITY ────────────────────────────────────────────
// The customer lifecycle alerts (Make 4356754 + 5975679) had no criteria at all.
// See ~/automations/ops/recipes/customer-alert-fanout.md.
//
// Two failure modes, and they need two different checks:
//
//  1. The card is posted but WRONG. On 2026-08-13 a React Email <Preview> block
//     pushed the greeting past Gmail's ~200-char snippet; Make's split() returns
//     empty rather than erroring, so the scenario reported SUCCESS while posting
//     blank cards for four days. The 2026-08-17 rewrite moved routes 15/17/18 onto
//     the subject — but with a bare replace() and no ifempty(). replace() returns
//     its input unchanged when the pattern is absent, so a subject-prefix rename
//     now yields a card whose name IS the whole raw subject. That is worse than
//     blank: it looks right and survives any non-empty check. Hence the explicit
//     prefix-residue assertion below.
//
//  2. The card is never posted at all. On 2026-08-06 a real customer produced no
//     card and nothing noticed. No content check can see a message that does not
//     exist, so that needs a divergence check against client_dashboards.
const GMAIL_ALERT_CHANNELS = {
  C0A70J9638R: 'activity tracking / onboarding completed',
  C0A9NH9PZ7C: 'closer + setter required',
  C09TS6DUTU2: 'prosp campaign issues',
  C0A7X9G6S78: 'new flywheel ai customer',
};
// The ID, not the #name in NEW_CLIENT_CHANNEL above — conversations.history is
// keyed by ID and will not resolve a name.
const GMAIL_NEW_CLIENT_CHANNEL_ID = 'C0A7X9G6S78';

// Only cards this recipe produces. Matching on the header rather than on bot_id
// keeps the check working if the Make connection is ever re-authed under a new app.
// EMOJI-FREE ON PURPOSE. The Make blueprint sends a literal "⏱️ ACTIVITY TRACKING ⏱️",
// but conversations.history returns ":stopwatch: ACTIVITY TRACKING :stopwatch:" —
// Slack stores emoji as shortcodes. Matching on the literal emoji meant
// text.includes(header) was false for EVERY card from the moment this shipped, so
// the content contract examined nothing and reported "0 bad cards" for it, and
// seenCards stayed empty so the divergence check called every customer missing.
//
// The tests did not catch it because the fixtures were built from the blueprint —
// what Make SENDS — instead of from conversations.history — what this code READS.
// A fixture is only as good as the source it was copied from.
//
// Match the text between the emoji: it is unique per route and identical in both
// representations.
const GMAIL_ALERT_HEADERS = [
  'ACTIVITY TRACKING',
  'CLOSER REQUIRED',
  'ONBOARDING COMPLETED',
  'SETTER REQUIRED',
  'NEW FLYWHEEL AI CUSTOMER',
  'Issues with Prosp Campaign',
];

// The header must OPEN the first line, once the decoration is stripped. A card always
// leads with its header; "did the CLOSER REQUIRED alert ever fire?" is a teammate
// talking in the same channel, and a looser contains() would drag that into the
// content contract and report it as a malformed card.
function isAlertCard(text) {
  const head = String(text || '').split('\n')[0]
    .replace(/:[a-z0-9_+-]+:/gi, '')                      // :bell::bell:
    .replace(/[\p{Extended_Pictographic}\uFE0F]/gu, '')    // and the rendered glyph
    .replace(/^[\s*_~]+/, '');
  return GMAIL_ALERT_HEADERS.some(h => head.startsWith(h));
}

// Fragments the template strips. If one survives into the name, replace() did not
// fire — the subject was renamed and the card is carrying the raw subject.
const GMAIL_SUBJECT_RESIDUE = [
  'Closer Required - ', 'Closer requerido - ',
  'Setter Required - ', 'Setter requerido - ',
  ' - Flywheel AI Account', ' - Cuenta Flywheel AI',
  'Flywheel onboarding form completed', 'Formulario de incorporación Flywheel completado',
];
const GMAIL_JUNK_TOKENS = ['[object Object]', 'Collection', 'undefined', 'null', 'NaN', 'Invalid Date'];
// Legitimate ONLY on module 3 (activity tracking), the one route with a fallback.
const GMAIL_MODULE3_FALLBACK = '— not included in this email —';

// Pure. Exported shape: { ok, problems: [] }. Kept side-effect free so the test can
// drive it directly, same as evaluateStrikeMove.
function evaluateAlertCard(text) {
  const problems = [];
  const t = String(text || '');
  const lines = t.split('\n').map(l => l.trim()).filter(Boolean);

  for (const junk of GMAIL_JUNK_TOKENS) {
    if (t.includes(junk)) problems.push(`contains ${junk}`);
  }

  const nameMatch = t.match(/^\*?Customer\/Company Name:\*?[ \t]*(.*?)[ \t]*$/m);
  if (nameMatch) {
    const name = nameMatch[1];
    if (!name) {
      problems.push('empty customer name');
    } else if (!/[\p{L}\p{N}]/u.test(name)) {
      // The live 2026-08-12 CLOSER REQUIRED card rendered its name as "," — the
      // template's split found no anchor and emitted the separator alone. Non-empty,
      // so the blank check passes it, and it looks like a populated field.
      problems.push('customer name is punctuation only (template produced no name)');
    } else {
      for (const frag of GMAIL_SUBJECT_RESIDUE) {
        if (name.includes(frag)) { problems.push(`raw subject leaked into name (residue "${frag.trim()}")`); break; }
      }
      // Generic leak: the name is byte-identical to another line of the card. Routes
      // 15/17/18 print {{1.subject}} on its own line and derive the name from it by
      // stripping a prefix, so name === subject means nothing was stripped at all.
      //
      // LIMIT, stated rather than hidden: this catches a TOTAL strip failure. A
      // partial one — a renamed prefix where the suffix replace still fires — leaves
      // a name that is neither empty nor equal to the subject, and slips past both
      // this and the residue list above. Closing that needs the template to emit a
      // sentinel, which is the Make-side fix recommended in the recipe spec.
      if (lines.some(l => l === name)) {
        problems.push('name is identical to another line of the card (raw subject leak)');
      }
      if (name === GMAIL_MODULE3_FALLBACK && !t.includes('ACTIVITY TRACKING')) {
        problems.push('module-3 fallback text on a route that has no fallback');
      }
    }
  }

  const emailMatch = t.match(/^\*?Customer Email:\*?[ \t]*(.*?)[ \t]*$/m);
  if (emailMatch && !emailMatch[1].includes('@')) {
    problems.push(emailMatch[1] ? 'customer email is not an address' : 'empty customer email');
  }

  return { ok: problems.length === 0, problems };
}

// Does any posted new-customer card correspond to this client_dashboards row?
//
// Pure, so the test drives it directly. The first live run (2026-08-19 15:00 CR)
// flagged a customer whose card had been posted in the same minute, because the two
// systems order the name components differently:
//
//   client_dashboards : "Aura Bonilla - Cacao Legal"
//   posted card       : "Cacao Legal - Aura Bonilla"
//
// Neither string contains the other, so containment said "missing". A false alarm on
// a divergence check is worse than useless — it is how a channel gets muted, and
// then the real gap goes unseen too.
//
// Email is the stable key: both sides carry it and neither reorders it. The token
// fallback covers rows or cards with no email, and requires EVERY significant token
// to be present, so "Acme" does not match a card for "Acme Holdings International".
function cardExistsFor(client, cards) {
  const email = String(client.email || '').toLowerCase().trim();
  if (email && cards.some(card => card.includes(email))) return true;

  const tokens = String(client.client_name || '')
    .toLowerCase()
    .split(/[^\p{L}\p{N}]+/u)
    .filter(tok => tok.length >= 3);
  if (!tokens.length) return false;
  return cards.some(card => tokens.every(tok => card.includes(tok)));
}

// A skip is a standing condition, not an event.
//
// #ng-newclient-onboarding-tracking has been unreadable since this recipe shipped —
// Max is not a member of it, so the fix is a Slack invite, not a deploy. The hourly
// cron therefore had 24 chances a day to say so, and on 2026-08-19 it took every one
// of them: identical DMs on the hour carrying nothing to act on. That is precisely
// how an alerting channel gets muted, and a muted channel hides the real gap too —
// the failure this whole check exists to prevent.
//
// So a run that found NOTHING WRONG — no bad card, no uncovered customer, only skips
// — reports once a day, in the 09:00 CR run, and stays quiet the other 23 hours. The
// verdict is unchanged: a skip is still never green, and it is still attached to any
// run that DID find something. Only its cadence changed.
//
// Stateless on purpose. A "have I already said this today" memo lives in process
// memory and resets on every deploy — 2026-08-19 shipped six deploys inside forty
// minutes, and that memo would have sent six DMs. The clock reads the same after a
// restart as before one.
const SKIP_ONLY_REPORT_HOUR_CR = 9;

// Pure, so the test drives it directly rather than mocking a clock.
function shouldReportSkipOnly(nowMs) {
  const hour = Number(new Date(nowMs).toLocaleString('en-US', {
    timeZone: 'America/Costa_Rica', hour: '2-digit', hourCycle: 'h23',
  }));
  return hour === SKIP_ONLY_REPORT_HOUR_CR;
}

const gmailAlertAlerted = new Set();
const GMAIL_ALERT_LOOKBACK_MS = 24 * 60 * 60 * 1000;
// The dash webhook fires within seconds, but Make's Gmail trigger polls every 20
// min. A customer created 25 min ago is in flight, not missing.
const GMAIL_ALERT_GRACE_MS = 30 * 60 * 1000;

// nowMs is injectable so the test can drive both sides of the daily skip-only
// window without mocking the global clock. The cron passes only correlationId.
async function checkGmailAlertQuality(correlationId, nowMs = Date.now()) {
  const now = nowMs;
  const since = now - GMAIL_ALERT_LOOKBACK_MS;
  const problems = [];
  const skipped = [];
  const seenCards = [];
  let newClientChannelRead = false;

  // ── 1. Content contract on every card in the window ──
  for (const [channel, label] of Object.entries(GMAIL_ALERT_CHANNELS)) {
    let msgs;
    try {
      const hist = await slack.client.conversations.history({
        channel, limit: 200, oldest: String(Math.floor(since / 1000)),
      });
      msgs = hist.messages || [];
      if (channel === GMAIL_NEW_CLIENT_CHANNEL_ID) newClientChannelRead = true;
    } catch (err) {
      // FAIL CLOSED. An unreadable channel must not read as "no problems here".
      // Name the CHANNEL, not just the route content. The first live skip read
      // "activity tracking / onboarding completed (channel_not_found)", which says
      // what is unchecked but not where to go and fix it. <#ID> renders as the real
      // channel name in Slack and survives a rename. channel_not_found from a bot
      // that is not a member of a private channel is the common cause, so say so.
      const code = err.data?.error || err.message;
      const hint = code === 'channel_not_found' || code === 'not_in_channel'
        ? ' — invite Max to that channel'
        : '';
      skipped.push(`${label} <#${channel}> (${code})${hint}`);
      continue;
    }
    for (const m of msgs) {
      const text = m.text || '';
      if (!isAlertCard(text)) continue;
      // Only new-customer cards count as coverage for the divergence check. A
      // "closer required" card for the same person is a different event and must
      // not mask a missing new-customer alert.
      if (channel === GMAIL_NEW_CLIENT_CHANNEL_ID && text.includes('NEW FLYWHEEL AI CUSTOMER')) {
        // Keep the WHOLE card, not just the parsed name. The dash record and the
        // webhook payload do not agree on component order — client_dashboards held
        // "Aura Bonilla - Cacao Legal" while the posted card read "Cacao Legal -
        // Aura Bonilla" — so substring containment on the composite name reported a
        // card that was sitting right there as missing. Matching happens below,
        // against the full text.
        seenCards.push(text.toLowerCase());
      }
      const verdict = evaluateAlertCard(text);
      if (!verdict.ok) {
        const key = `${channel}:${m.ts}`;
        if (!gmailAlertAlerted.has(key)) {
          problems.push(`• \`${label}\` — ${verdict.problems.join('; ')}\n  ${text.split('\n')[0].slice(0, 80)}`);
        }
        gmailAlertAlerted.add(key);
      }
    }
  }

  // ── 2. Divergence: a customer exists but no card was ever posted ──
  // Only possible for the new-customer path; client_dashboards is the sole
  // queryable source of truth. The lifecycle routes originate as emails with no
  // table behind them — that hole is named in the recipe spec, not papered over.
  let missing = [];
  const { data: clients, error: clientErr } = await portalSupabase
    .from('client_dashboards')
    .select('client_name, email, created_at')
    .gte('created_at', new Date(since).toISOString())
    .lte('created_at', new Date(now - GMAIL_ALERT_GRACE_MS).toISOString())
    .limit(100);
  if (clientErr) {
    skipped.push(`client_dashboards divergence (${clientErr.message})`);
  } else if (!newClientChannelRead) {
    // Could not read the channel, so absence of a card proves nothing. Tracked as an
    // explicit flag rather than by prefix-matching the skip strings: that coupled the
    // suppression rule to a human-readable label, so renaming the label would have
    // silently turned a fail-closed path into a false-alarm generator.
    skipped.push(`client_dashboards divergence (<#${GMAIL_NEW_CLIENT_CHANNEL_ID}> unreadable)`);
  } else {
    missing = (clients || []).filter(c => !cardExistsFor(c, seenCards))
      .filter(c => !gmailAlertAlerted.has(`missing:${c.email}`));
    for (const c of missing) gmailAlertAlerted.add(`missing:${c.email}`);
  }

  // Bound the memo so a long-running process cannot grow it without limit.
  if (gmailAlertAlerted.size > 500) {
    for (const k of [...gmailAlertAlerted].slice(0, gmailAlertAlerted.size - 500)) gmailAlertAlerted.delete(k);
  }

  console.log(`Gmail alert quality: ${problems.length} bad card(s), ${missing.length} customer(s) with no alert, ${skipped.length} check(s) skipped.`);
  const nothingWrong = !problems.length && !missing.length;
  if (nothingWrong && !skipped.length) return;
  if (nothingWrong && !shouldReportSkipOnly(now)) {
    console.log(`Gmail alert quality: nothing wrong — holding ${skipped.length} skip(s) for the ${SKIP_ONLY_REPORT_HOUR_CR}:00 CR daily note.`);
    return;
  }

  // A skipped check can never read as green — it degrades the verdict to ⚠️.
  const parts = [`⚠️ <@${RON_SLACK_ID}> *Customer alert fan-out — quality check*`];
  if (problems.length) parts.push(`\n*${problems.length} card(s) posted with a bad field* in the last 24h:\n${problems.slice(0, 10).join('\n')}`);
  if (missing.length) {
    parts.push(`\n*${missing.length} new customer(s) with no alert card:*\n` +
      missing.slice(0, 10).map(c => `• ${c.client_name} <${c.email}> — created ${new Date(c.created_at).toLocaleString('en-US', { timeZone: 'America/Costa_Rica' })} CR`).join('\n') +
      `\nThe row is in \`client_dashboards\` but nothing reached <#${GMAIL_NEW_CLIENT_CHANNEL_ID}>. Do NOT re-post by hand before checking whether the dash webhook fired — a duplicate card is worse than a late one.`);
  }
  // Say so out loud on the daily note, or a DM whose only content is a skip line
  // reads like a truncated alert rather than a clean run with one blind spot.
  if (nothingWrong) parts.push(`\nNo bad cards and no uncovered customers in the last 24h. Daily note that one check still cannot run:`);
  if (skipped.length) parts.push(`\n_Skipped (not green, just unread): ${skipped.join(', ')}_`);

  await postMakeHealthAlert(parts.join('\n'));
  logActivity({
    event_type: 'alert', event_source: 'cron', action: 'gmail_alert_quality',
    status: problems.length || missing.length ? 'gap' : 'degraded',
    output: { bad_cards: problems.length, missing_alerts: missing.map(c => c.client_name), skipped },
    correlation_id: correlationId,
  });
}

// Hourly. The fan-out polls every 20 min, so this is 3 polls behind at worst —
// fast enough to catch a broken template the same morning, cheap enough that it
// costs four Slack reads and one Supabase read per run.
cron.schedule('0 * * * *', wrapCronJob('checkGmailAlertQuality', async (c) => { await checkGmailAlertQuality(c); }), { timezone: 'America/Costa_Rica' });
console.log('Registered static cron: gmail fan-out alert quality check (0 * * * *)');

// ─── CRON LIVENESS AUDIT ────────────────────────────────────────────────────
// Who watches the watchers. Nothing did, from 2026-07-29 — when the mcp
// scheduled-task audit fleet self-disabled via a hardcoded expiry date and took
// the "should we keep a lighter audit?" decision with it — until this shipped.
//
// It is a cron in Max rather than an agent task ON PURPOSE. The fleet that died
// was LLM-judged, untestable, and carried its own kill switch. This is
// deterministic SQL over agent_activity, has a test file, and has no expiry
// clause: it can report that a check looks retired, but only Ron can retire it.
//
// wrapCronJob already writes every run to agent_activity as a `started` row and a
// terminal `ok`/`error` row sharing a correlation_id. That record was complete and
// entirely unread — 4 vanished runs were sitting in it, including a Fulfillment
// EOD Pulse that started 2026-08-18 and never finished, last success 5 days prior.
const STATIC_CRON_SCHEDULES = {
  checkBookingAlertDivergence:  '*/30 * * * *',
  checkGmailAlertQuality:       '0 * * * *',
  checkMakeScenarioHealth:      '*/10 * * * *',
  runAppointmentStatusSync:     '0 15 * * *',
  runApptDeletionSweep:         '20 7-19/3 * * *',
  runAutoStrikeMover:           '0 7-21/2 * * *',
  // The audit declares itself, so a run where it dies shows up as stale on the
  // next one that survives. It cannot detect its own total death — nothing
  // in-process can — which is the standing limitation noted in the spec.
  runCronLivenessAudit:         '30 7 * * *',
  runEmailReplyPoller:          '0 8-20 * * 1-5',
  runFulfillmentStandup:        '0 9 * * 1-5',
  runMondayGapDetection:        '0 14 * * 1',
  runNightlyLearning:           '30 5 * * *',
  runReviCrossChecks:           '30 6 * * 2-6',
  runReviProspectNotesSync:     '0 14 * * *',
  runSalesCallPrep:             '0 * * * 1-5',
  runSalesStandup:              '0 9 * * 1-5',
  runStaleLeadDailySweep:       '0 18 * * *',
  runStaleLeadNagCheck:         '*/30 7-20 * * *',
  runStalledProspectFollowups:  '0 11 * * 1-5',
  runStrikeSalesDigest:         '30 21 * * *',
  runUnloggedOutcomeReminders:  '0 16 * * *',
  runWeeklyPortalTrends:        '30 22 * * 5',
  runWeeklySalesMarketingRecap: '0 17 * * 5',
  runWonHandoffNotes:           '0 9,17 * * *',
};

// How long may pass with no successful run before something is wrong?
//
// Deliberately LENIENT. A cron-liveness alarm that cries wolf gets muted, and a
// muted audit is worse than none — it looks like coverage. Every rule here rounds
// in favour of silence; the job is to catch a cron that STOPPED, not to measure
// punctuality. No cron-expression parser is used, and no dependency added: these
// five shapes cover everything Max actually registers.
function expectedMaxGapHours(expr) {
  const [min = '*', hour = '*', dom = '*', , dow = '*'] = String(expr || '').trim().split(/\s+/);

  // Intra-day tolerance first.
  let base;
  const stepMatch = /^\*\/(\d+)$/.exec(min);
  const hourIsWindowed = hour !== '*' && /[,\-/]/.test(hour);
  if (stepMatch && hour === '*') base = Math.max((Number(stepMatch[1]) * 3) / 60, 1.5);
  else if (hour === '*')         base = 3;      // hourly
  else if (hourIsWindowed)       base = 20;     // e.g. '0 9,17' has a real 16h overnight gap
  else                           base = 26;     // once a day

  // Then widen for anything that legitimately skips whole days.
  if (dom !== '*') return 32 * 24;                                 // monthly
  if (dow !== '*') {
    const gapDays = dowMaxGapDays(dow);
    if (gapDays === null) return Math.max(base, 8 * 24);           // unparseable → lenient
    return Math.max(base, gapDays * 24 + 4);
  }
  return base;
}

// The largest gap in days between consecutive scheduled weekdays, wrapping the week.
//
// A flat "weekday schedules get 76h" rule was wrong, and wrong in the direction that
// cries wolf. '0 18 * * 1,3' runs Mon and Wed, so its real Wed→Mon gap is FIVE days;
// 76h would have alarmed on it every single week. '0 7 * * 2,6' (Tue/Sat) has a
// four-day Tue→Sat gap. Both are healthy schedules a flat rule would have flagged.
// Contiguous ranges like 1-5 come out at 3 days, which is why the flat number looked
// right against the crons that happened to be checked first.
function dowMaxGapDays(dow) {
  const days = new Set();
  for (const part of String(dow).split(',')) {
    const m = /^(\d)(?:-(\d))?(?:\/(\d+))?$/.exec(part.trim());
    if (!m) return null;
    const from = Number(m[1]);
    const to   = m[2] !== undefined ? Number(m[2]) : from;
    const step = m[3] ? Number(m[3]) : 1;
    if (step < 1 || to < from) return null;
    for (let d = from; d <= to; d += step) days.add(d % 7);        // cron allows 7 for Sunday
  }
  const sorted = [...days].sort((a, b) => a - b);
  if (!sorted.length) return null;
  if (sorted.length === 1) return 7;
  let max = sorted[0] + 7 - sorted[sorted.length - 1];             // the wrap-around gap
  for (let i = 1; i < sorted.length; i++) max = Math.max(max, sorted[i] - sorted[i - 1]);
  return max;
}

const SELF_ACTION = 'runCronLivenessAudit';

// Pure. Returns the four ways a scheduled job can be wrong, which are genuinely
// different failures and want different reactions.
function evaluateCronLiveness({ expectations, lastOkByAction, recentRuns, now }) {
  const stale = [], vanished = [], drifted = [], silent = [];

  for (const [action, expr] of Object.entries(expectations)) {
    const maxGapHours = expectedMaxGapHours(expr);
    const lastOk = lastOkByAction[action];
    if (!lastOk) {
      // The audit has no successful run of its own until it finishes this one, so
      // on first deploy it would otherwise open by reporting itself as broken.
      // Skipping it here costs nothing: once it has run once it is subject to the
      // ordinary staleness rule like everything else.
      if (action !== SELF_ACTION) silent.push({ action, expr });
      continue;
    }
    const gapHours = (now - new Date(lastOk).getTime()) / 3600000;
    if (gapHours > maxGapHours) {
      stale.push({ action, expr, gapHours: Math.round(gapHours), maxGapHours: Math.round(maxGapHours), lastOk });
    }
  }

  // A run that started and never reached a terminal status. The process died
  // mid-job, or the job hung. Either way nobody was told.
  const byCorrelation = new Map();
  for (const r of recentRuns) {
    if (!r.correlation_id) continue;
    const e = byCorrelation.get(r.correlation_id) || { action: r.action, started: false, finished: false, at: r.created_at };
    if (r.status === 'started') { e.started = true; e.at = r.created_at; }
    if (r.status === 'ok' || r.status === 'error') e.finished = true;
    byCorrelation.set(r.correlation_id, e);
  }
  for (const e of byCorrelation.values()) {
    if (e.started && !e.finished) vanished.push({ action: e.action, at: e.at });
  }

  // A cron that logs but nobody declared — the registry drifted. This is how five
  // new crons appeared in a day without the inventory noticing.
  const declared = new Set(Object.keys(expectations));
  for (const a of new Set(recentRuns.map(r => r.action))) {
    if (!declared.has(a)) drifted.push(a);
  }

  return { stale, vanished, drifted: drifted.sort(), silent };
}

const VANISHED_LOOKBACK_MS = 48 * 60 * 60 * 1000;
// A run in flight right now is not a vanished run.
const VANISHED_GRACE_MS = 20 * 60 * 1000;

async function runCronLivenessAudit(correlationId) {
  const now = Date.now();

  // Dynamic tasks are declared in the DB, so read them rather than hardcoding —
  // a task added through Max is then watched automatically, with no code change.
  const expectations = { ...STATIC_CRON_SCHEDULES };
  let dynamicNote = '';
  const { data: dynTasks, error: dynErr } = await supabase
    .from('scheduled_tasks').select('name, cron_expression').eq('active', true);
  if (dynErr) dynamicNote = `could not read scheduled_tasks (${dynErr.message}) — dynamic tasks unchecked this run`;
  else for (const t of dynTasks || []) expectations[`dynamic_cron:${t.name}`] = t.cron_expression;

  // Last success per action, one bounded query each. Cheap at daily cadence, and
  // it handles a monthly cron correctly without pulling 40 days of rows.
  const lastOkByAction = {};
  for (const action of Object.keys(expectations)) {
    const { data } = await portalSupabase
      .from('agent_activity')
      .select('created_at')
      .eq('event_type', 'cron_run').eq('action', action).eq('status', 'ok')
      .order('created_at', { ascending: false }).limit(1);
    if (data && data.length) lastOkByAction[action] = data[0].created_at;
  }

  const { data: recentRuns, error: runsErr } = await portalSupabase
    .from('agent_activity')
    .select('action, status, created_at, correlation_id')
    .eq('event_type', 'cron_run')
    .gte('created_at', new Date(now - VANISHED_LOOKBACK_MS).toISOString())
    .lte('created_at', new Date(now - VANISHED_GRACE_MS).toISOString())
    .limit(2000);
  // FAIL CLOSED. Unreadable history means "unknown", never "healthy".
  if (runsErr) {
    await slack.client.chat.postMessage({
      channel: RON_SLACK_ID,
      text: `⚠️ *Cron liveness audit could not run* — agent_activity unreadable: ${runsErr.message}\nThis is not an all-clear. Nothing was checked.`,
    });
    logActivity({ event_type: 'alert', event_source: 'cron', action: 'cron_liveness_audit', status: 'degraded', correlation_id: correlationId });
    return;
  }

  const result = evaluateCronLiveness({ expectations, lastOkByAction, recentRuns: recentRuns || [], now });
  const { stale, vanished, drifted, silent } = result;
  const total = stale.length + vanished.length + drifted.length + silent.length;

  console.log(`Cron liveness: ${Object.keys(expectations).length} declared, ${stale.length} stale, ${vanished.length} vanished, ${drifted.length} undeclared, ${silent.length} never logged.`);
  logActivity({
    event_type: 'audit', event_source: 'cron', action: 'cron_liveness_audit',
    status: total ? 'gap' : 'ok',
    output: { declared: Object.keys(expectations).length, stale: stale.map(s => s.action), vanished: vanished.map(v => v.action), drifted, silent: silent.map(s => s.action) },
    correlation_id: correlationId,
  });

  // Quiet when healthy. An audit that posts every day teaches you to skim it.
  if (!total && !dynamicNote) return;

  const lines = [`⚠️ *Cron liveness audit* — ${Object.keys(expectations).length} scheduled jobs declared`];
  if (stale.length) lines.push(`\n*${stale.length} stale* — no successful run inside the expected window:\n` +
    stale.map(s => `• \`${s.action}\` (\`${s.expr}\`) — last ok ${s.gapHours}h ago, tolerance ${s.maxGapHours}h`).join('\n'));
  if (vanished.length) lines.push(`\n*${vanished.length} vanished* — started and never reached a terminal status, so the process died mid-run or the job hung:\n` +
    vanished.slice(0, 10).map(v => `• \`${v.action}\` — started ${new Date(v.at).toLocaleString('en-US', { timeZone: 'America/Costa_Rica' })} CR`).join('\n'));
  if (silent.length) lines.push(`\n*${silent.length} never logged* — declared but no successful run on record. Either it has never fired, or it is not wrapped in \`wrapCronJob\` and is invisible:\n` +
    silent.map(s => `• \`${s.action}\` (\`${s.expr}\`)`).join('\n'));
  if (drifted.length) lines.push(`\n*${drifted.length} undeclared* — logging but absent from \`STATIC_CRON_SCHEDULES\`, so the registry has drifted:\n` +
    drifted.map(a => `• \`${a}\``).join('\n'));
  if (dynamicNote) lines.push(`\n_${dynamicNote}_`);

  // Liveness, never an expiry. This audit reports that it is alive and when it
  // runs next; it has no mechanism to disable itself. If a check here becomes
  // permanently noisy, Ron retires it in chat — the previous fleet is gone
  // precisely because it could make that decision on its own.
  lines.push(`\n_Audit alive · next run tomorrow 07:30 CR. This job has no expiry and cannot disable itself._`);

  await slack.client.chat.postMessage({ channel: RON_SLACK_ID, text: lines.join('\n') });
}

// 07:30 CR daily — after the 05:30 nightly learning and before the 09:00
// standups, so a job that died overnight is known before the day depends on it.
// Daily, not weekly: a daily report that stops would otherwise wait up to seven
// days to be noticed, and this costs ~30 bounded queries.
cron.schedule('30 7 * * *', wrapCronJob('runCronLivenessAudit', async (c) => { await runCronLivenessAudit(c); }), { timezone: 'America/Costa_Rica' });
console.log('Registered static cron: cron liveness audit (30 7 * * *)');

// ─── START ────────────────────────────────────────────────────────────────────
(async () => {
  try {
    await slack.start();
  } catch (err) {
    // Socket Mode could not be established (revoked/rotated SLACK_APP_TOKEN,
    // Socket Mode toggled off on the app, or Slack-side failure). Running on
    // without the socket means inbound events are silently dead, so fail loud
    // and exit — Railway's ON_FAILURE policy restarts and retries.
    console.error('FATAL: slack.start() failed — Socket Mode could not connect:', err && err.message);
    await alertSocketHealth(`🚨 Max failed to start Socket Mode: ${err && err.message ? err.message : 'unknown error'}. Inbound Slack events are down. Restarting…`);
    process.exit(1);
  }
  console.log('NeuroGrowth PM Agent is running.');
  attachSocketWatchdog();
  await loadAndRegisterDynamicCrons();
  // Static infrastructure crons (not stored in DB)
  cron.schedule('0 7 15 * *', wrapCronJob('runPhase3Reconciliation', async (c) => { await runPhase3Reconciliation(c); }), { timezone: 'America/Costa_Rica' });
  console.log('Registered static cron: Phase 3 reconciliation (0 7 15 * *)');
})();

// ─── REACTION-DRIVEN LEAD CLAIM ───────────────────────────────────────────────
// Setter reacts on a #ng-sales-goats lead post → Max writes assignedTo in GHL,
// posts a ✅ + threaded confirmation. First reactor wins; idempotent on race.
const CAMPAIGN_APPROVE_EMOJIS = new Set(['white_check_mark', 'heavy_check_mark', 'check', 'ballot_box_with_check']);
const CAMPAIGN_SKIP_EMOJIS    = new Set(['x', 'no_entry', 'no_entry_sign', 'negative_squared_cross_mark']);

// ✅/❌ on an outcome-proposal DM. Ownership is structural — the DM channel is
// private to the closer it was sent to — but we still verify the reactor is
// that closer (or Ron) against the message metadata before writing anything.
// A typed reply in an outcome card's thread. The card's metadata carries the
// appointment, so a bare `no show` or `won 3500` is unambiguous — resolved
// deterministically, never via the LLM (which, with no thread context, was
// asking closers "which prospect?" right under the card that says which).
// Returns true when fully handled; a context STRING when the parent is a card
// but the text is conversational (the DM flow hands that to the LLM as a
// hint); false when the parent is not an outcome card at all.
async function handleOutcomeCardThreadReply(message, say) {
  let parent = null;
  try {
    const hist = await slack.client.conversations.history({
      channel: message.channel, latest: message.thread_ts, limit: 1, inclusive: true, include_all_metadata: true,
    });
    parent = hist.messages && hist.messages[0];
  } catch (err) {
    console.log('outcome-card thread lookup failed (falling back to LLM):', err.message);
    return false;
  }
  if (parent?.metadata?.event_type !== 'outcome_proposal' || !parent.metadata.event_payload) return false;
  const payload = parent.metadata.event_payload;

  const expectedSlackId = CLOSER_SLACK[payload.closer_email] || CLOSER_SLACK[(payload.closer_email || '').toLowerCase()];
  if (message.user !== RON_SLACK_ID && message.user !== expectedSlackId) {
    return `[CONTEXT: reply in the outcome-card thread for prospect ${payload.prospect_name}, but the sender is not the owning closer — do not log an outcome from this.]`;
  }

  const parsed = parseOutcomeReply(message.text);
  if (!parsed) {
    return `[CONTEXT: this message is a reply in the outcome-card thread for prospect ${payload.prospect_name} (appointment_id ${payload.appointment_id}). If the user is stating that call's outcome in their own words, log it with log_call_outcome for THIS prospect — never ask which prospect they mean.]`;
  }
  if (parsed.outcome === 'won' && !parsed.revenue) {
    await say({ text: 'Almost — logging won needs the real amount: `won 3500`.', thread_ts: message.thread_ts });
    return true;
  }

  const result = await logOutcomeToPortal({
    appointmentId: payload.appointment_id,
    outcome: parsed.outcome,
    source: 'closer',
    notes: `Typed on the outcome card by <@${message.user}> (prospect: ${payload.prospect_name})`,
    closedRevenue: parsed.revenue,
  });
  const stages = GHL_OUTCOME_STAGES[payload.pipeline_id] || GHL_OUTCOME_STAGES[GHL_PIPELINE.APPT_SETTING];
  if (result.ok) {
    await upsertKnowledge('process', `outcome-proposal:${payload.appointment_id}`, `confirmed|${new Date().toISOString().slice(0, 10)}`, 'outcome-proposal');
    await slack.client.reactions.add({ channel: message.channel, timestamp: message.thread_ts, name: 'white_check_mark' }).catch(() => {});
    const label = stages[parsed.outcome]?.label || parsed.outcome.replace('_', ' ');
    const statusNote = result.statusChange ? ` Prospect status: ${result.statusChange}.` : '';
    const move = result.stageMove;
    const moveNote = move?.ok
      ? (move.already ? ` GHL card was already in *${move.label}*.` : ` Moved the GHL card to *${move.label}*.`)
      : ` ⚠️ Logged in the portal, but I couldn't move the GHL card (${move?.message || 'unknown error'}) — please move it by hand.`;
    const att = result.attendance;
    const attNote = att?.ok && !att.already ? ` Attendance set to *${att.status}*.` : '';
    await say({ text: `Logged *${label}*${parsed.revenue ? ` ($${parsed.revenue})` : ''} for ${payload.prospect_name}.${statusNote}${moveNote}${attNote}`, thread_ts: message.thread_ts });
  } else if (result.reason === 'exists') {
    await say({ text: `Already logged as *${result.existing?.outcome || 'unknown'}* (source: ${result.existing?.source || '?'}) — I never overwrite, so no change.`, thread_ts: message.thread_ts });
  } else if (result.reason === 'won_needs_revenue') {
    await say({ text: 'Logging won needs the amount — `won 3500`.', thread_ts: message.thread_ts });
  } else {
    await say({ text: `Couldn't log it: ${result.message || result.reason}. Log it in GHL directly, or tell Ron the write path is down.`, thread_ts: message.thread_ts });
  }
  return true;
}

async function handleOutcomeProposalReaction(event, baseEmoji, dmMsg, payload) {
  const channel = event.item.channel;
  const ts = event.item.ts;
  const expectedSlackId = CLOSER_SLACK[payload.closer_email] || CLOSER_SLACK[(payload.closer_email || '').toLowerCase()];
  if (event.user !== RON_SLACK_ID && event.user !== expectedSlackId) {
    console.log(`outcome-proposal ${payload.appointment_id}: reaction from ${event.user} is not the owning closer, ignoring`);
    return;
  }
  // Already actioned? Max stamps ✅ / no_entry / warning after handling.
  const actioned = (dmMsg.reactions || []).find(r =>
    ['white_check_mark', 'no_entry', 'warning'].includes(r.name) && r.users?.includes(process.env.SLACK_BOT_USER_ID));
  if (actioned) {
    console.log(`outcome-proposal ${payload.appointment_id} already actioned, ignoring`);
    return;
  }

  if (CAMPAIGN_SKIP_EMOJIS.has(baseEmoji)) {
    await upsertKnowledge('process', `outcome-proposal:${payload.appointment_id}`, `dismissed|${new Date().toISOString().slice(0, 10)}`, 'outcome-proposal');
    await slack.client.reactions.add({ channel, timestamp: ts, name: 'no_entry' }).catch(() => {});
    await slack.client.chat.postMessage({ channel, thread_ts: ts, text: 'Dismissed — nothing logged. Log the real outcome in GHL, or reply here (`won <amount>` / `lost` / `dq` / `follow up` / `no show`) and I\'ll log it.' });
    return;
  }
  if (!CAMPAIGN_APPROVE_EMOJIS.has(baseEmoji)) return;

  const stages = GHL_OUTCOME_STAGES[payload.pipeline_id] || GHL_OUTCOME_STAGES[GHL_PIPELINE.APPT_SETTING];
  const altActions = outcomeActionsFor(payload.pipeline_id).map(a => `\`${a.say}\``).join(' / ');

  if (!payload.proposed_outcome) {
    // won-hint (or read-less) card: ✅ is not enough on purpose.
    await slack.client.chat.postMessage({ channel, thread_ts: ts, text: payload.won_hint
      ? `I never log *${stages.won.label}* from a reaction — reply \`won <amount>\` to confirm it (amount = what was actually closed).`
      : `No confident read to confirm here — reply \`won <amount>\` / ${altActions} and I'll log it.` });
    return;
  }
  // Confidence gate: only a fact-backed read is one-tap. Anything softer is
  // shown on the card but must be typed, so a rubber-stamped guess can't land
  // in the scorecard.
  if (payload.confidence && payload.confidence !== 'high') {
    await slack.client.chat.postMessage({ channel, thread_ts: ts, text: `That read isn't confident enough for one tap (${payload.confidence}). Reply \`won <amount>\` / ${altActions} and I'll log it.` });
    return;
  }

  const result = await logOutcomeToPortal({
    appointmentId: payload.appointment_id,
    outcome: payload.proposed_outcome,
    source: payload.rule && payload.rule.startsWith('ghl_') ? 'ghl_confirmed' : 'revi_confirmed',
    notes: `Max-proposed outcome (${payload.rule || 'revi'}) confirmed via Slack by <@${event.user}> (prospect: ${payload.prospect_name})`,
  });
  if (result.ok) {
    await upsertKnowledge('process', `outcome-proposal:${payload.appointment_id}`, `confirmed|${new Date().toISOString().slice(0, 10)}`, 'outcome-proposal');
    await slack.client.reactions.add({ channel, timestamp: ts, name: 'white_check_mark' }).catch(() => {});
    const statusNote = result.statusChange ? ` Prospect status: ${result.statusChange}.` : '';
    // The GHL move is best-effort and reported honestly — the portal row is
    // already written either way, so a CRM hiccup never silently loses it.
    const move = result.stageMove;
    const moveNote = move?.ok
      ? (move.already ? ` GHL card was already in *${move.label}*.` : ` Moved the GHL card to *${move.label}*.`)
      : ` ⚠️ Logged in the portal, but I couldn't move the GHL card (${move?.message || 'unknown error'}) — please move it by hand.`;
    const label = stages[payload.proposed_outcome]?.label || payload.proposed_outcome.replace('_', ' ');
    const att = result.attendance;
    const attNote = att?.ok && !att.already ? ` Attendance set to *${att.status}* — that's Paso 1 done too.` : '';
    await slack.client.chat.postMessage({ channel, thread_ts: ts, text: `Logged *${label}* for ${payload.prospect_name}.${statusNote}${moveNote}${attNote}` });
  } else if (result.reason === 'exists') {
    await slack.client.reactions.add({ channel, timestamp: ts, name: 'white_check_mark' }).catch(() => {});
    await slack.client.chat.postMessage({ channel, thread_ts: ts, text: `Already logged as *${result.existing?.outcome || 'unknown'}* (source: ${result.existing?.source || '?'}) — I never overwrite, so no change.` });
  } else {
    await slack.client.reactions.add({ channel, timestamp: ts, name: 'warning' }).catch(() => {});
    await slack.client.chat.postMessage({ channel, thread_ts: ts, text: `Couldn't log it: ${result.message || result.reason}. Log it in GHL directly, or tell Ron the outcome write path is down.` });
    if (result.reason === 'not_configured') {
      await slack.client.chat.postMessage({ channel: RON_SLACK_ID, text: '⚠️ Outcome proposal ✅ failed: PORTAL_WRITER_DATABASE_URL is not set on ng-pm-MAX, so one-tap outcome logging is dead. The proposal DMs are still going out.' }).catch(() => {});
    }
  }
}

slack.event('reaction_added', async ({ event }) => {
  try {
    if (!event || !event.item || event.item.type !== 'message') return;
    if (event.user === process.env.SLACK_BOT_USER_ID) return;

    // Strip skin-tone modifier (Slack delivers e.g. `hand::skin-tone-3`)
    const baseEmoji = String(event.reaction || '').split('::')[0];

    // Route 0: outcome-proposal DM — sits ahead of the campaign route because
    // closers are not in SLACK_TO_GHL_USER, so the campaign gate would swallow
    // their ✅ (its no-metadata fallthrough hard-returns for DM channels).
    // Only intercepts messages whose metadata says outcome_proposal; every
    // other DM reaction falls through to Route 1 untouched.
    if (String(event.item.channel || '').startsWith('D') &&
        (CAMPAIGN_APPROVE_EMOJIS.has(baseEmoji) || CAMPAIGN_SKIP_EMOJIS.has(baseEmoji))) {
      try {
        const hist = await slack.client.conversations.history({
          channel: event.item.channel, latest: event.item.ts, limit: 1, inclusive: true, include_all_metadata: true,
        });
        const msg = hist.messages && hist.messages[0];
        if (msg?.metadata?.event_type === 'outcome_proposal' && msg.metadata.event_payload) {
          await handleOutcomeProposalReaction(event, baseEmoji, msg, msg.metadata.event_payload);
          return;
        }
      } catch (opErr) {
        // channel_not_found = a DM this bot cannot read (not one of its own
        // card DMs) — benign, and 3 of these in a row once paged as errors.
        if (opErr.message && opErr.message.includes('channel_not_found')) {
          console.log('outcome-proposal pre-route: unreadable DM, falling through');
        } else {
          console.error('outcome-proposal reaction pre-route error:', opErr.message);
        }
        // fall through — campaign route may still own this reaction
      }
    }

    // Route 1: campaign-draft DM — Ron OR the assigned setter approves/skips a
    // generated re-engagement message. Stalled-cron drafts now land in the setter's
    // own DM (they own the conversation), so gating on Ron alone would leave those
    // reactions silently inert. Ownership is re-checked against the draft metadata
    // below; a DM channel is private to its member, so a setter can only ever react
    // to their own drafts.
    const isCampaignChannel = event.item.channel && event.item.channel.startsWith('D');
    const isCampaignEmoji   = CAMPAIGN_APPROVE_EMOJIS.has(baseEmoji) || CAMPAIGN_SKIP_EMOJIS.has(baseEmoji);
    const isKnownApprover   = event.user === RON_SLACK_ID || !!SLACK_TO_GHL_USER[event.user];
    if (isCampaignChannel && isCampaignEmoji && isKnownApprover) {
      try {
        const dmHistory = await slack.client.conversations.history({
          channel: event.item.channel, latest: event.item.ts, limit: 1, inclusive: true, include_all_metadata: true,
        });
        const dmMsg = dmHistory.messages && dmHistory.messages[0];
        const dmMeta = dmMsg?.metadata?.event_type === 'campaign_draft' ? dmMsg.metadata.event_payload : null;
        // Ownership: Ron may approve anything; a setter may approve only a draft
        // addressed to them. Drafts with no setter_slack_id (Ron's manual `campaign:`
        // flow) stay Ron-only.
        const ownsDraft = dmMeta && (
          event.user === RON_SLACK_ID ||
          (dmMeta.setter_slack_id && event.user === dmMeta.setter_slack_id)
        );
        if (dmMeta && !ownsDraft) {
          console.log(`campaign-draft ${dmMeta.contact_id}: reaction from ${event.user} is not the assigned approver, ignoring`);
          return;
        }
        if (dmMeta) {
          // Already actioned? Look for an existing Max-applied ✅ or ❌ on this message
          const existing = (dmMsg.reactions || []).find(r => (CAMPAIGN_APPROVE_EMOJIS.has(r.name) || CAMPAIGN_SKIP_EMOJIS.has(r.name)) && r.users?.includes(process.env.SLACK_BOT_USER_ID));
          if (existing) {
            console.log(`campaign-draft ${dmMeta.contact_id} already actioned, ignoring`);
            return;
          }

          if (CAMPAIGN_SKIP_EMOJIS.has(baseEmoji)) {
            // Skip — log only
            try {
              await supabase.from('campaign_sends').insert({
                contact_id: dmMeta.contact_id,
                contact_name: dmMeta.contact_name,
                channel: 'WhatsApp',
                draft_text: dmMeta.draft_text,
                approved_by_slack_id: event.user,
                status: 'skipped',
                correlation_id: dmMeta.correlation_id,
              });
            } catch (err) { console.error('campaign skip insert failed:', err.message); }
            await slack.client.reactions.add({ channel: event.item.channel, timestamp: event.item.ts, name: 'no_entry' }).catch(() => {});
            await slack.client.chat.postMessage({ channel: event.item.channel, thread_ts: event.item.ts, text: `Skipped — no message sent.` });
            return;
          }

          // Approve → send
          const result = await sendCampaignMessage({
            contactId: dmMeta.contact_id,
            contactName: dmMeta.contact_name,
            draftText: dmMeta.draft_text,
            approverSlackId: event.user,
            correlationId: dmMeta.correlation_id,
          });
          if (result.ok) {
            await slack.client.reactions.add({ channel: event.item.channel, timestamp: event.item.ts, name: 'white_check_mark' }).catch(() => {});
            await slack.client.chat.postMessage({ channel: event.item.channel, thread_ts: event.item.ts, text: `✉️ Sent via GHL. Message ID: \`${result.messageId}\`` });

            // Stalled-cron drafts: also write to prospect_followups so cooldown/
            // lifetime-cap queries see approval-batch sends. The setter who owns
            // the thread also gets a confirmation DM.
            if (dmMeta.source === 'stalled_cron') {
              try {
                await supabase.from('prospect_followups').insert({
                  contact_id: dmMeta.contact_id,
                  contact_name: dmMeta.contact_name,
                  conversation_id: result.conversationId || null,
                  channel: 'WhatsApp',
                  message: dmMeta.draft_text,
                  ghl_message_id: result.messageId || null,
                  setter_slack_id: dmMeta.setter_slack_id || event.user,
                  attempt_n: dmMeta.attempt_n || 1,
                  status: 'sent',
                  correlation_id: dmMeta.correlation_id,
                });
              } catch (err) { console.error(`stalled approval-batch followup audit failed: ${err.message}`); }
              if (dmMeta.setter_slack_id && dmMeta.setter_slack_id !== event.user) {
                try {
                  await slack.client.chat.postMessage({
                    channel: dmMeta.setter_slack_id,
                    text: `🤖 Auto-followup approved + sent to *${dmMeta.contact_name}*: "${String(dmMeta.draft_text).slice(0, 200)}"\n_DM \`pause ${dmMeta.contact_id}\` to stop future auto-followups for this contact._`,
                  });
                } catch (err) { console.error(`stalled approval-batch setter notify failed: ${err.message}`); }
              }
            }
          } else {
            await slack.client.reactions.add({ channel: event.item.channel, timestamp: event.item.ts, name: 'warning' }).catch(() => {});
            await slack.client.chat.postMessage({ channel: event.item.channel, thread_ts: event.item.ts, text: `❌ Send failed: ${result.error}` });
          }
          return;
        }
      } catch (err) {
        console.error('campaign-draft reaction handler error:', err.message);
        // Fall through — could still be a non-campaign DM reaction we don't care about
      }
      return; // DM reaction was campaign-shaped but no metadata → ignore
    }

    // Route 1b: remediation proposal in an ops channel — any roster member ✅/❌.
    // Sits ahead of lead-claim because both accept ✅; safe either way since Route 2
    // hard-returns on any channel that isn't LEAD_CHANNEL_ID. handleRemediationReaction
    // returns false when the message isn't a proposal, so we fall through cleanly.
    if (!String(event.item.channel || '').startsWith('D') &&
        (CAMPAIGN_APPROVE_EMOJIS.has(baseEmoji) || CAMPAIGN_SKIP_EMOJIS.has(baseEmoji))) {
      const handled = await handleRemediationReaction(event, baseEmoji).catch(err => {
        console.error('remediation reaction handler error:', err.message);
        return true;   // it WAS ours and it blew up — do not fall through to lead-claim
      });
      if (handled) return;
    }

    // Route 2: lead-claim in #ng-sales-goats (existing flow)
    if (event.item.channel !== LEAD_CHANNEL_ID) return;
    if (!LEAD_CLAIM_EMOJIS.has(baseEmoji)) return;

    const channel   = event.item.channel;
    const timestamp = event.item.ts;

    const history = await slack.client.conversations.history({
      channel, latest: timestamp, limit: 1, inclusive: true, include_all_metadata: true,
    });
    const msg = history.messages && history.messages[0];
    if (!msg || msg.ts !== timestamp) return;

    // Gate: must be a Max-posted lead message with metadata
    const meta = msg.metadata && msg.metadata.event_type === 'ghl_lead' ? msg.metadata.event_payload : null;
    if (!meta || !meta.contact_id) return;

    // Idempotency: if Max already added the claim emoji, this lead is already taken
    const reactions = msg.reactions || [];
    const claimReaction = reactions.find(r => r.name === LEAD_CLAIMED_EMOJI);
    if (claimReaction && claimReaction.users && claimReaction.users.includes(process.env.SLACK_BOT_USER_ID)) {
      console.log(`reaction_added: lead ${meta.contact_id} already claimed, ignoring`);
      return;
    }

    const ghlUserId = SLACK_TO_GHL_USER[event.user];
    if (!ghlUserId) {
      await slack.client.chat.postMessage({
        channel, thread_ts: timestamp,
        text: `<@${event.user}> you're not in the GHL setter map yet — ping Ron to add you before claiming leads.`,
      });
      return;
    }

    const claimCorr = newCorrelationId();
    const ghlAuth = { 'Authorization': `Bearer ${process.env.GHL_API_KEY}`, 'Version': '2021-07-28', 'Content-Type': 'application/json' };
    try {
      // 1. Reassign the contact
      const putRes = await ghlFetch(`https://services.leadconnectorhq.com/contacts/${meta.contact_id}`, {
        method: 'PUT', headers: ghlAuth, body: JSON.stringify({ assignedTo: ghlUserId }),
      }, { label: `PUT /contacts/${meta.contact_id} (claim)` });
      if (!putRes.ok) {
        const errBody = await putRes.text();
        throw new Error(`GHL PUT /contacts/${meta.contact_id} → ${putRes.status}: ${errBody.slice(0, 200)}`);
      }

      // 2. Reassign opportunities tied to this contact — but ONLY in setter pipelines.
      //    VSL self-bookings live in a separate pipeline and must NOT be reassigned.
      //    Allow-list comes from GHL_SETTER_PIPELINE_IDS env var (comma-separated).
      const setterPipelineIds = (process.env.GHL_SETTER_PIPELINE_IDS || 'KH1IQuaN8aNB1lfRpvP4')
        .split(',').map(s => s.trim()).filter(Boolean);
      const oppsRes = await ghlFetch(
        `https://services.leadconnectorhq.com/opportunities/search?location_id=${meta.location_id}&contact_id=${meta.contact_id}`,
        { headers: ghlAuth },
        { label: `GET /opportunities/search (claim ${meta.contact_id})` },
      );
      const oppsData = oppsRes.ok ? await oppsRes.json() : { opportunities: [] };
      const allOpps = oppsData.opportunities || [];
      const opps = allOpps.filter(o => setterPipelineIds.includes(o.pipelineId));
      const skippedNonSetterOpps = allOpps.length - opps.length;
      if (skippedNonSetterOpps > 0) {
        console.log(`Skipped ${skippedNonSetterOpps} non-setter-pipeline opp(s) for contact ${meta.contact_id}`);
      }
      const oppResults = await Promise.all(opps.map(async (opp) => {
        try {
          const r = await ghlFetch(`https://services.leadconnectorhq.com/opportunities/${opp.id}`, {
            method: 'PUT', headers: ghlAuth, body: JSON.stringify({ assignedTo: ghlUserId }),
          }, { label: `PUT /opportunities/${opp.id} (claim)` });
          if (!r.ok) {
            const eb = await r.text();
            console.warn(`opp PUT ${opp.id} → ${r.status}: ${eb.slice(0, 150)}`);
            return { id: opp.id, ok: false };
          }
          return { id: opp.id, ok: true };
        } catch (e) { console.warn(`opp PUT ${opp.id} threw:`, e.message); return { id: opp.id, ok: false }; }
      }));
      const oppsOk = oppResults.filter(r => r.ok).length;
      const oppsFail = oppResults.length - oppsOk;

      // Mark message as claimed (idempotency anchor for future reactions)
      try { await slack.client.reactions.add({ channel, timestamp, name: LEAD_CLAIMED_EMOJI }); }
      catch (reactErr) {
        if (!String(reactErr.data?.error || reactErr.message).includes('already_reacted')) {
          console.warn('reactions.add failed:', reactErr.message);
        }
      }

      const lines = [`✅ Claimed by <@${event.user}>.`];
      if (opps.length === 0) {
        lines.push(`• GHL contact reassigned to you.`);
        lines.push(`• No opportunity on the Appointment Setting Pipeline for this lead.`);
      } else if (oppsFail === 0) {
        const oppWord = oppsOk === 1 ? 'opportunity' : 'opportunities';
        lines.push(`• GHL contact reassigned to you.`);
        lines.push(`• ${oppsOk} setter ${oppWord} on the Appointment Setting Pipeline reassigned to you.`);
      } else {
        lines.push(`• GHL contact reassigned to you.`);
        lines.push(`• ${oppsOk} of ${opps.length} setter opportunities reassigned, ${oppsFail} failed — check logs.`);
      }
      if (skippedNonSetterOpps > 0) {
        const oppWord = skippedNonSetterOpps === 1 ? 'opportunity' : 'opportunities';
        lines.push(`• ${skippedNonSetterOpps} ${oppWord} on another pipeline (e.g. VSL self-booking) left as-is.`);
      }
      await slack.client.chat.postMessage({
        channel, thread_ts: timestamp,
        text: lines.join('\n'),
      });

      logActivity({
        event_type: 'ghl_lead_claimed', event_source: 'slack', action: 'lead_claim',
        actor_user_id: event.user, channel_id: channel,
        output: { contact_id: meta.contact_id, ghl_user_id: ghlUserId, full_name: meta.full_name, opps_in_setter_pipelines: opps.length, opps_reassigned: oppsOk, opps_failed: oppsFail, opps_skipped_non_setter: skippedNonSetterOpps },
        correlation_id: claimCorr,
      });

      // Audit trail — record the claim with seconds-to-claim for offline SLA mining
      try {
        const secondsToClaim = Math.max(0, Math.round(Date.now() / 1000 - parseFloat(timestamp)));
        await supabase.from('setter_claims').insert({
          ghl_contact_id: meta.contact_id,
          contact_name: meta.full_name,
          prospect_email: meta.email || null,
          slack_message_ts: timestamp,
          slack_channel_id: channel,
          claimed_by_slack_user_id: event.user,
          claimed_by_setter_name: GHL_USER_NAMES[ghlUserId] || GHL_USER_NAMES[ghlUserId.toLowerCase()] || null,
          ghl_user_id: ghlUserId,
          opps_reassigned: oppsOk,
          seconds_to_claim: secondsToClaim,
          claim_source: 'slack_reaction',
        });
      } catch (claimErr) {
        // Non-fatal — Slack post + GHL writes already succeeded
        console.error('setter_claims insert failed:', claimErr.message);
      }

      console.log(`Lead ${meta.contact_id} (${meta.full_name}) claimed by ${event.user} → GHL user ${ghlUserId}; opps ${oppsOk}/${opps.length}`);
    } catch (apiErr) {
      console.error('Lead claim GHL write failed:', apiErr.message);
      // A dropped claim used to leave nothing but a console line — no row to query
      // when asking "how often does this happen?".
      logActivity({
        event_type: 'ghl_lead_claim_failed', event_source: 'slack', action: 'lead_claim',
        actor_user_id: event.user, channel_id: channel, status: 'error',
        output: { contact_id: meta.contact_id, ghl_user_id: ghlUserId, full_name: meta.full_name, error: apiErr.message },
        correlation_id: claimCorr,
      });
      // Rate limiting / GHL hiccups survive the retries only when GHL is genuinely
      // busy. The claim emoji was never added, so re-reacting is a clean retry —
      // that's the one instruction worth giving, not "reach out to Ron".
      const transientStatus = (apiErr.message.match(/→ (429|5\d\d):/) || [])[1] || null;
      await slack.client.chat.postMessage({
        channel, thread_ts: timestamp,
        text: transientStatus
          ? `<@${event.user}> your claim didn't stick — GHL returned ${transientStatus} (rate limit / busy) and it still failed after ${GHL_RETRY_DELAYS_MS.length} retries. Remove your ✅ and add it again in a minute. Still failing? Reach out to Ron.`
          : `<@${event.user}> tried to claim, but GHL update failed: ${apiErr.message}. Reach out to Ron.`,
      });
    }
  } catch (err) {
    console.error('reaction_added handler error:', err.message);
  }
});

// ─── MEMBER JOINED CHANNEL ────────────────────────────────────────────────────
slack.event('member_joined_channel', async ({ event }) => {
  try {
    const channelInfo = await slack.client.conversations.info({ channel: event.channel });
    const channelName = channelInfo.channel?.name || '';
    if (!channelName.includes('ng-pm-agent')) return;
    if (event.user === process.env.SLACK_BOT_USER_ID) return;
    const member = getMemberContext(event.user);
    const roleIntros = {
      ceo:            `You are greeting Ron, the CEO and Founder of NeuroGrowth. This is your home base. Give him a sharp 2-line welcome that shows you're ready to work — mention you can pull emails, calendar, GHL, Slack channels, Drive, and Notion on demand.`,
      client_success: `You are greeting ${member.displayName}, the Client Success Operations Manager. Welcome them and let them know you can help with: client health checks, drafting client comms, checking fulfillment channel activity, contract reminders, and searching the knowledge base. Keep it to 3-4 lines max.`,
      tech_ops:       `You are greeting Josue, the Technical Operations Manager. Welcome him and let him know you can help with: client launch status, campaign blockers, fulfillment channel recaps, Notion SOPs, and his daily briefing every morning at 8:30 AM. Keep it to 3-4 lines max.`,
      tech_lead:      `You are greeting David, the Lead Technology and Automation specialist. Welcome him and let him know you can help with: systems channel activity, Make.com issue tracking, process documentation, and Notion. Keep it to 3-4 lines max.`,
      fulfillment:    `You are greeting ${member.displayName}, a Fulfillment Operations specialist. Welcome them and let them know you can help with: delivery doc status, client setup coordination, fulfillment channel recaps, and Notion. Keep it to 3-4 lines max.`,
      campaigns:      `You are greeting Felipe, the Technical Campaign Specialist. Welcome him and let him know you can help with: campaign status per client, Prosp config questions, fulfillment channel updates, and content pipeline tracking. Keep it to 3-4 lines max.`,
      setter:         `You are greeting ${member.displayName}, an Appointment Setter at NeuroGrowth. Welcome them and let them know you can help with: GHL prospect lookups, drafting follow-up messages in Spanish, sales channel activity, and EOD report prep. Keep it to 3-4 lines max.`,
      closer:         `You are greeting Jose, the High-Ticket Closer. Welcome him and let him know you can help with: GHL pipeline status, prospect follow-up drafts, sales channel activity, and EOD report prep. Keep it to 3-4 lines max.`,
    };
    const roleIntro = roleIntros[member.role] || `You are greeting a new NeuroGrowth team member named ${member.displayName}. Welcome them warmly and briefly explain what you can help with.`;
    const prompt    = `You are Max, the NeuroGrowth PM Agent. A new team member just joined the #ng-pm-agent channel.\n\n${roleIntro}\n\nAddress them by name: ${member.displayName}.\nSound like a sharp, friendly colleague — not a corporate bot. No markdown formatting. No bullet points. Conversational tone.`;
    const mjCid = newCorrelationId();
    const greeting  = await callClaude([{ role: 'user', content: prompt }], 3, event.user, mjCid);
    if (!greeting || !greeting.trim()) return;
    await slack.client.chat.postMessage({ channel: event.channel, text: greeting });
    logActivity({ event_type: 'slack_message', event_source: 'slack', action: 'outbound', channel_id: event.channel, output: { text: String(greeting).slice(0, 2000) }, correlation_id: mjCid });
    console.log(`Greeted ${member.displayName} (${member.role}) in #ng-pm-agent`);
  } catch (err) { console.error('member_joined_channel error:', err.message); }
});