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

const { Pool } = require('pg');
const portalPg = process.env.PORTAL_READONLY_DATABASE_URL
  ? new Pool({
      connectionString: process.env.PORTAL_READONLY_DATABASE_URL,
      ssl: { rejectUnauthorized: false },
      max: 3,
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

function wrapCronJob(actionName, jobFn) {
  return async () => {
    const correlation_id = newCorrelationId();
    const started = Date.now();
    let errored = null;
    logActivity({ event_type: 'cron_run', event_source: 'cron', action: actionName, status: 'started', correlation_id });
    try {
      await jobFn(correlation_id);
    } catch (err) {
      errored = err;
      throw err;
    } finally {
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

6. When Ron asks for a portal data field that the pre-built tools don't cover (anything beyond onboarding phase status, Phase 0 pipeline, alerts, or sales intelligence — e.g. client emails, LinkedIn handles, any ad-hoc lookup), do NOT guess table names and do NOT say you lack access. Call search_portal_schema with keywords from Ron's question, pick the best-matching table from the grouped result, then call query_portal_db with a SELECT statement. Only fall back to list_portal_tables if schema search returns nothing.

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
Tania (U07SMMDMSLQ) — Client Success Operations Manager. Client health, AR, contracts, case studies.
Josue Duran (U08ABBFNGUW) — Technical Operations Manager (full-time fulfillment). Activation calls, campaign ops, client launch sequencing.
David McKinney (U08ACUHUUP6) — Lead Technology & Automation. Portal, Make.com, Supabase infrastructure.
Valeria (U09Q3BXJ18B) — Fulfillment Operations. Delivery documents, Claude Projects.
Felipe (U09TNMVML3F) — Technical Campaign Specialist (part-time). Campaign launches, Prosp management.
Joseph Salazar (U0A9J00EMGD) — Appointment Setter. Books discovery calls.
Oscar M (U0B1S1UMH9P) — Appointment Setter. Books discovery calls.
William B (U0B16P6DQ2F) — Appointment Setter. Books discovery calls.
Jose Carranza (U0AMTEKDCPN) and Jonathan Madriz (U0APYAE0999) — High-Ticket Closers. They close deals after setting.

---

HOW YOU OPERATE

Task Execution: When Ron assigns a task, confirm you understood it, execute with available tools, and report completion. If blocked or needs Ron's decision, surface that clearly without over-explaining.

Communication Drafting: Draft all routine outgoing messages. Ron reviews client-facing or sales-critical content before it goes out. Write in professional, confident, direct tone for external; efficient and direct for internal.

Standup Accountability (Scheduled Jobs):
- Daily standup (weekdays 9:00 AM Costa Rica): Post in team Slack channel. Ask what each person is working on and if there are blockers. Tag Tania, Josue, Valeria, Felipe.
- EOD check (weekdays 5:00 PM Costa Rica): Cross-reference open items against reported completions. Flag anything unresolved.
- Weekly summary (Fridays 4:00 PM Costa Rica): Digest for Ron covering sales closures, delivery status, blockers, Monday priorities.
- Sales call prep (evening before any sales call on calendar): Alert Ron to review prospect brief. If no brief exists, alert Tania.

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
   RIGHT — Call every relevant tool, compile the result, return a single complete answer: "Andres Chavez — assigned to Joseph Salazar. Last message April 9, no outbound response sent."

   If ALL sources return nothing, say so immediately in one message: "Andres Chavez not found in GHL, sales intelligence, or knowledge base. He may not have been logged yet."

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
- EOD reports from Joseph, Oscar, William, and Jose (calls booked, pipeline updates, actions needed)
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

You have access to GHL, iClosed, Meta Ads, the portal, Supabase, Slack, Notion, and Google Drive/Docs/Sheets. You do NOT have access to Ron's Gmail or Google Calendar — if asked, explain that those are Ron-only and offer to help a different way.`;

const SYSTEM_PROMPT = SYSTEM_PROMPT_BASE + SYSTEM_PROMPT_RULES + SYSTEM_PROMPT_RON;

const AGENT_CHANNEL         = process.env.AGENT_CHANNEL         || '#ng-pm-agent';
const OPS_CHANNEL           = process.env.OPS_CHANNEL           || '#ng-fullfillment-ops';
const NEW_CLIENT_CHANNEL    = process.env.NEW_CLIENT_CHANNEL    || '#ng-new-client-alerts';
const SALES_CHANNEL         = process.env.SALES_CHANNEL         || '#ng-sales-goats';
const SYSTEMS_CHANNEL       = process.env.SYSTEMS_CHANNEL       || '#ng-app-and-systems-improvents';
const ANNOUNCEMENTS_CHANNEL = process.env.ANNOUNCEMENTS_CHANNEL || '#ng-internal-announcements';

const pendingApprovals = {};

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
      delete pendingApprovals[userId];
    }
  }
}, 30 * 60 * 1000);

const RON_SLACK_ID = 'U05HXGX18H3';

// ─── PILOT ACCESS ─────────────────────────────────────────────────────────────
// Only these Slack user IDs can invoke Max. Others get a polite bounce.
// Shrink to just Ron's ID to roll back to single-user mode.
const PILOT_USERS = new Set([
  'U05HXGX18H3', // Ron
  'U07SMMDMSLQ', // Tania
  'U08ABBFNGUW', // Josue
  'U08ACUHUUP6', // David
]);

// Tools restricted to Ron — Gmail and Calendar rely on Ron's personal OAuth.
// Drive/Docs/Sheets stay open to the pilot (read-only against Ron's token).
const RON_ONLY_TOOLS = new Set([
  'get_recent_emails',
  'send_email',
  'get_calendar_events',
  'create_calendar_event',
  'add_calendar_attendees',
]);

// ─── TEAM MEMBER REGISTRY ─────────────────────────────────────────────────────
const TEAM_MEMBERS = {
  'U05HXGX18H3': { name: 'Ron',      role: 'ceo',            displayName: 'Ron Duarte' },
  'U07SMMDMSLQ': { name: 'Tania',    role: 'client_success', displayName: 'Tania'      },
  'U08ABBFNGUW': { name: 'Josue',    role: 'tech_ops',       displayName: 'Josue'      },
  'U08ACUHUUP6': { name: 'David',    role: 'tech_lead',      displayName: 'David'      },
  'U09Q3BXJ18B': { name: 'Valeria',  role: 'fulfillment',    displayName: 'Valeria'    },
  'U09TNMVML3F': { name: 'Felipe',   role: 'campaigns',      displayName: 'Felipe'     },
  'U0A9J00EMGD': { name: 'Joseph',   role: 'setter',         displayName: 'Joseph'     },
  'U0B1S1UMH9P': { name: 'Oscar',    role: 'setter',         displayName: 'Oscar'      },
  'U0B16P6DQ2F': { name: 'William',  role: 'setter',         displayName: 'William'    },
  'U0AMTEKDCPN': { name: 'Jose',     role: 'closer',         displayName: 'Jose Carranza' },
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
    client_success: `You are speaking with Tania, the Client Success Operations Manager at NeuroGrowth. She is the operational backbone of the business — hybrid Chief of Staff and Client Success role reporting to Ron.

CROSS-ROLE CONTEXT: Fulfillment and sales are not siloed from Tania. When she asks about a client, surface both sides — delivery status AND sales history (what was promised on the close call, renewal timing, setter notes). When she asks about a prospect or renewal, pull delivery reputation (on-time launch, blockers, satisfaction signals) since that shapes retention and case-study potential.

PORTAL FOCUS: Lead with her book of business first, then expand. She can ask about any client, prospect, or deal — answer freely. Prioritize client health, onboarding phase, AR status, renewal signals, and any sale-to-delivery handoff gaps.

Her 3 pillars:
- Executive Ops (30%): Draft and manage all contracts and SLAs, maintain contract repo with renewal dates, prepare pre-meeting research packages for Ron, own OKR tracking and sprint completion monitoring, produce weekly 5-min ops summary for Ron.
- Client Success (50%): Primary contact for all non-strategic client comms — respond within 2 hours. Bi-weekly client check-in calls (Ron handles monthly strategic sessions). Track client health scores (target >80/100 average). Monitor early warning signals (reduced responsiveness, declining campaign metrics). Identify upsell and expansion opportunities. Execute case study and testimonial SOP. Coordinate quarterly business reviews with performance data.
- Project and Team Coordination (20%): Own project tracking, coordinate with David on infrastructure, facilitate comms between SDR team and technical team, track action items across team members.

Key KPIs: 100% client retention, >80 health score average, <2hr response time, 90%+ feedback actioned within 1 week, 1 case study per quarter, CEO operational time <20%.

When Tania asks about a client, give her full health context: engagement level, last interaction, open action items, any risk signals. Help her draft client comms, check-in messages, expansion proposals, escalation summaries, and case study outreach. She cannot access Ron's Gmail, calendar, or GHL.`,

    tech_ops: `You are speaking with Josue, the Technical Operations Manager at NeuroGrowth. He reports to Ron (CEO) and is the single point of accountability for technical campaign excellence across all clients.

CROSS-ROLE CONTEXT: Sales context matters to Josue. When a new client lands in delivery, the close-call promises, price tier, and setter-to-closer notes shape how he scopes the 14-day build. When a client is blocked or at-risk, sales needs to know before the next renewal or case-study ask. Surface both sides freely.

PORTAL FOCUS: Lead with fulfillment pipeline health — phase transitions, launch risk, clients hitting Day 7 or Day 14, SLA status. He can ask about sales, setters, or any client — answer freely, and flag cross-over risks (e.g. a stalled client whose renewal is near).

His role is split:
- 60% Build & Release: Own the complete 14-day launch cycle from client activation through technical deployment. Phase 1 (Days 1-3): client activation & onboarding. Phase 2 (Days 4-10): fulfillment coordination. Phase 3 (Days 11-13): technical QA. Phase 4 (Day 14): launch execution & handoff.
- 40% Full Service / Done-For-You: Monitor and optimize ongoing campaigns for full-service clients. Monday 9AM: 60-min campaign fix session. Fridays: portfolio performance deep dive (GREEN/YELLOW/RED status). Monthly audits every 30-45 days per client.

Key performance targets: 95%+ on-time launch rate within 14-day guarantee, 90%+ SLA compliance across DFY portfolio, keep CEO time on campaign ops under 5 hours/week.

After Day 14, Tania becomes primary client contact for satisfaction/admin — Josue remains owner of technical campaign performance.

When Josue asks about a client, pull from knowledge base and fulfillment channel to give him full context: current status, last action taken, what's blocking them, and what the next step is. Be direct and operational — tell him exactly what to do, not a summary. Help him draft channel updates, client comms, campaign fix plans, and escalation messages. He cannot access Ron's email, calendar, or GHL.`,

    tech_lead: `You are speaking with David, the Lead Technology and Automation specialist at NeuroGrowth. He builds and maintains Make.com scenarios, Supabase infrastructure, and the Neurogrowth Portal. Help him with technical questions, systems channel activity, and process documentation.

CROSS-ROLE CONTEXT: System issues rarely stay in the system channel. A Make.com failure can cascade into fulfillment delays, stuck onboarding phases, or missed sales handoffs. When David asks about infrastructure, also surface which active clients or sales workflows are affected downstream.

PORTAL FOCUS: Lead with system health, Make.com scenario activity, portal data integrity, and automation failure signals. He can ask about any client, team, or sales data — answer freely, and flag when a tech issue is touching live client or deal work.

He cannot access Ron's email or calendar.`,

    fulfillment: `You are speaking with Valeria, the Fulfillment Operations specialist at NeuroGrowth. Her primary role is creating client delivery documents — she runs the LinkedIn Flywheel Delivery System (Project 1 and Project 2 pipeline).

How the delivery system works:
- Project 1 (Profile Optimization and Client Intelligence): Takes onboarding form + activation call + LinkedIn PDF as inputs. Runs language gate and activation gate quality checks, then runs 14-step market analysis. Produces 3 docs: Doc 1 (Voice + Calendar) goes to client via WhatsApp, Doc 2 (LinkedIn guide) goes to fulfillment team, Doc 3 (Intelligence bundle) hands off to Project 2.
- Project 2 (Campaign Factory): Takes the intelligence bundle, runs bundle detection and pre-gen summary confirmation, builds 3 sequences (A, B, C — 5 messages each) + voice notes + Sales Navigator D1-D12 + Prosp.ai config. Produces File 1 (internal campaign bible for fulfillment, 7 sections D1-D12) and File 2 (founder-facing campaign overview, plain language).
- Delivery: Doc 1 and File 2 go to founder. Doc 2 and File 1 go to fulfillment team (Felipe).

Client onboarding checklist phases she owns or coordinates:
- Phase 1: Voice Profile and Content Calendar setup, Video General Overview, Voice Profile Prompt
- Phase 2: Campaign Validation (with Felipe), content calendar and profile steps

Help Valeria with delivery doc status, client setup coordination, and fulfillment channel activity. She cannot access Ron's Gmail, calendar, or GHL.`,

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

    closer: `You are speaking with a High-Ticket Closer at NeuroGrowth. The closers are Jose Carranza (U0AMTEKDCPN) and Jonathan Madriz (U0APYAE0999). They take booked calls from Joseph, Oscar, and William and close them into paying clients.

His daily responsibilities:
- Build and manage his own sales pipeline from booked calls
- Take discovery and closing calls with prospects set by Joseph
- Nurture pipeline: follow up with no-shows and maybes, handle objections, re-engage cold leads
- Collect payments from new clients
- Add new clients into the NeuroGrowth system (Neurogrowth portal and GHL)
- File an EOD report every day: calls taken, deals closed, pipeline updates, follow-ups needed

Key context:
- Prospects arrive pre-qualified by Joseph with niche, service, price point, and ICP already gathered
- He works the post-call nurture sequence for no-shows and undecided prospects
- Payment collection and client data entry into GHL is his responsibility on close
- Works with Tania on handoff once a client pays

When Jose asks about a prospect or pipeline, pull from GHL conversations and knowledge base. Help him draft follow-up messages, re-engagement scripts, and closing sequences. Help him prep his EOD report. He cannot access Ron's Gmail or calendar.`,
  };

  const baseContext = roleContext[member.role] || roleContext.fulfillment;
  const channelList = perms.canReadChannels.join(', ');
  return `${SYSTEM_PROMPT_BASE}${SYSTEM_PROMPT_RULES}${SYSTEM_PROMPT_TEAM_TIER}\n\n---\nCURRENT USER CONTEXT:\n${baseContext}\n\nThis user can access these channels: ${channelList}\nAddress this person by their first name: ${member.name}.\nKeep responses focused on their operational scope. Do not share sensitive business financials or information outside their role.`;
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

function registerDynamicCron(task) {
  try {
    if (activeDynamicCrons[task.id]) { activeDynamicCrons[task.id].stop(); }
    const job = cron.schedule(task.cron_expression, async () => {
      const correlation_id = newCorrelationId();
      const started = Date.now();
      const cronAction = `dynamic_cron:${task.name}`;
      let errored = null;
      let lastErr = null;
      logActivity({ event_type: 'cron_run', event_source: 'cron', action: cronAction, status: 'started', correlation_id, metadata: { task_id: task.id } });
      try {
      console.log(`Running dynamic cron: ${task.name}`);

      // Inject live email + calendar context into scheduled report prompts
      let liveContext = '';
      try {
        const todayEvents = await getCalendarEvents(0, 1);
        if (todayEvents && !todayEvents.includes('error') && !todayEvents.includes('No events')) {
          liveContext += `\n\nTODAY'S CALENDAR:\n${todayEvents}`;
        }
        const tomorrowEvents = await getCalendarEvents(1, 1);
        if (tomorrowEvents && !tomorrowEvents.includes('error') && !tomorrowEvents.includes('No events')) {
          liveContext += `\n\nTOMORROW'S CALENDAR:\n${tomorrowEvents}`;
        }
        const emails = await getRecentEmails();
        if (emails && !emails.includes('error')) {
          liveContext += `\n\nRECENT EMAILS (unread):\n${emails}`;
        }
      } catch (e) { console.error('Live context fetch error for scheduled task:', e.message); }

      // Inject any lessons learned from team feedback on previous reports for this channel
      const taskChannel = (task.channel || AGENT_CHANNEL).replace(/^#/, '');
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
      const enrichedPrompt = liveContext
        ? `${task.prompt}${lessonContext}${clientCtxBlock}\n\n---\nLIVE CONTEXT (use this to inform the report):\n${liveContext}`
        : `${task.prompt}${lessonContext}${clientCtxBlock}`;

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

      // Strip APPROVAL_NEEDED sentinel if Claude used draft_channel_post tool
      if (reply.startsWith('APPROVAL_NEEDED|')) {
        const parts = reply.split('|');
        reply = parts.slice(5).join('|').trim();
      }

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
        'Fulfillment EOD Pulse':      ['WINS TODAY', 'BLOCKERS', 'TOMORROW'],
        'Friday Delivery Wrap-Up':    ['WEEK IN REVIEW', 'CLIENT STATUS BOARD', 'TEAM WINS THIS WEEK', 'MISSES THIS WEEK', 'MONDAY PRIORITIES'],
        'Ron Weekly Ops Digest':      ['DELIVERY', 'SALES', 'WHAT NEEDS YOUR ATTENTION'],
        'Sales Call Prep Reminder':         [], // short task, skip header check
        'Blocked Client Report — MWF':      [], // short report, no fixed headers
      };

      function isValidFinalReport(text, taskName) {
        const upper = text.toUpperCase();
        const headers = TASK_HEADERS[taskName];
        if (headers === undefined) {
          // Unknown task — fall back to length check only
          return text.trim().length >= 300;
        }
        if (headers.length === 0) {
          // Short tasks like Sales Call Prep — just check it's not empty
          return text.trim().length > 20;
        }
        // Must contain at least half the expected headers to pass
        const found = headers.filter(h => upper.includes(h)).length;
        const threshold = Math.ceil(headers.length / 2);
        return found >= threshold && text.trim().length >= 300;
      }

      const isValidReport = isValidFinalReport(reply, task.name);

      if (!isValidReport) {
        console.log(`Cron "${task.name}": output failed structural validation (${reply.trim().length} chars, missing required headers). Re-prompting...`);
        try {
          const finalReply = await callClaude([
            { role: 'user', content: task.prompt },
            { role: 'assistant', content: reply },
            { role: 'user', content: 'Your previous response was rejected because it did not contain the required section headers. Do NOT narrate your process, explain what you are doing, or show your reasoning. Output ONLY the final compiled report with every section header and all data filled in, exactly as specified in the original instructions. Start directly with the first section header. Nothing before it.' }
          ], 3, null, correlation_id);
          if (finalReply && isValidFinalReport(finalReply, task.name)) {
            reply = finalReply;
            console.log(`Cron "${task.name}": re-prompt passed validation (${reply.trim().length} chars)`);
          } else {
            // Re-prompt also failed — DM Ron with an error instead of sending garbage
            const errMsg = `Scheduled task "${task.name}" failed to produce a valid structured report after 2 attempts. The output did not contain the required section headers. Please trigger this report manually or check Railway logs for details.`;
            await slack.client.chat.postMessage({ channel: RON_SLACK_ID, text: errMsg });
            console.error(`Cron "${task.name}": re-prompt also failed validation. Notified Ron.`);
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
        metadata: { task_id: task.id },
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

async function sendEmail(to, subject, body) {
  const auth  = getGoogleAuth();
  const gmail = google.gmail({ version: 'v1', auth });
  const message = [`To: ${to}`, `Subject: ${subject}`, '', body].join('\n');
  const encoded = Buffer.from(message).toString('base64').replace(/\+/g, '-').replace(/\//g, '_');
  await gmail.users.messages.send({ userId: 'me', requestBody: { raw: encoded } });
  return `Email sent to ${to}`;
}

// ─── GOOGLE CALENDAR ──────────────────────────────────────────────────────────
async function getCalendarEvents(daysFromNow = 0, daysRange = 1) {
  const auth     = getGoogleAuth();
  const calendar = google.calendar({ version: 'v3', auth });
  const startDate = new Date();
  startDate.setDate(startDate.getDate() + daysFromNow);
  startDate.setHours(6, 0, 0, 0);
  const endDate = new Date(startDate);
  endDate.setDate(endDate.getDate() + daysRange);
  endDate.setHours(29, 59, 59, 0);
  const res = await calendar.events.list({ calendarId: 'primary', timeMin: startDate.toISOString(), timeMax: endDate.toISOString(), singleEvents: true, orderBy: 'startTime' });
  const events = res.data.items || [];
  if (!events.length) return 'No events found in that range.';
  return events.map(e => {
    const when = e.start.dateTime || e.start.date;
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
    // CAC = spend / iClosed sales (purchase pixel fires when closer marks outcome = sale)
    const cac      = parseInt(purchases) > 0 ? (parseFloat(spend) / parseInt(purchases)).toFixed(2) : 'N/A';
    return [
      `Meta Ads — ${datePreset.replace(/_/g,' ')}:`,
      `Spend: $${spend} | Impressions: ${parseInt(d.impressions||0).toLocaleString()} | Reach: ${parseInt(d.reach||0).toLocaleString()}`,
      `Clicks: ${parseInt(d.clicks||0).toLocaleString()} | CTR: ${ctr}% | CPC: $${cpc} | CPM: $${cpm}`,
      leads !== '0'     ? `Form leads: ${leads} | Form CPL: $${formCpl}` : 'Form leads: 0 (no lead pixel fires — VSL funnel not counted)',
      purchases !== '0' ? `iClosed sales (purchase pixel): ${purchases} | CAC: $${cac}` : 'iClosed sales (purchase pixel): 0',
    ].join('\n');
  } catch (err) { return `Meta Ads summary error: ${err.message}`; }
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

      // Day 1 anchor logic — depends on phase:
      // Phase 3: use stabilization_started_at (the correct stabilization Day 1)
      // All others: use Activation Call completed_at (14-day SLA anchor)
      // Fall back to portal created_at if neither is available
      const activationCallAct = activities.find(a => {
        const title = (templateMap[a.template_id]?.title || '').toLowerCase();
        return title.includes('activation call') && a.completed_at;
      });
      let startDate, dayAnchor;
      if (dash.customer_status === 'phase_3' && dash.stabilization_started_at) {
        startDate  = new Date(dash.stabilization_started_at);
        dayAnchor  = 'since stabilization start';
      } else if (activationCallAct) {
        startDate  = new Date(activationCallAct.completed_at);
        dayAnchor  = 'since activation call';
      } else {
        startDate  = dash.created_at ? new Date(dash.created_at) : null;
        dayAnchor  = 'since portal creation';
      }
      const daysSince = startDate ? Math.floor((Date.now() - startDate.getTime()) / (1000 * 60 * 60 * 24)) : null;
      const lines = [
        `${statusEmoji} ${dash.client_name || dash.email} [${(dash.customer_type || '').replace('flywheel-ai','Flywheel').replace('full-service','Full Service')}]`,
        `${statusLabel} | Day ${daysSince ?? '?'} ${dayAnchor}`,
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
async function getPortalAlerts() {
  try {
    const { data: dashboards, error } = await portalSupabase
      .from('client_dashboards')
      .select('id, client_name, email, customer_status, customer_type, created_at')
      .eq('is_active', true)
      .in('customer_status', ['blocked', 'phase_1', 'phase_2'])
      .order('created_at', { ascending: true });
    if (error) throw error;
    if (!dashboards || !dashboards.length) return '✅ No blocked or at-risk clients. All clients on track.';

    // Fetch templates upfront so we can resolve activity titles and find activation call
    const { data: templates } = await portalSupabase
      .from('customer_activity_templates')
      .select('id, title');
    const templateMap = {};
    (templates || []).forEach(t => { templateMap[t.id] = t.title; });

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

      // ── Day anchor: activation call completed_at → fallback to created_at ──
      const activationAct = allActs.find(a => {
        const title = (templateMap[a.template_id] || '').toLowerCase();
        return title.includes('activation call') && a.completed_at;
      });
      const startDate = activationAct
        ? new Date(activationAct.completed_at)
        : (dash.created_at ? new Date(dash.created_at) : null);
      const daysSince = startDate ? Math.floor((now - startDate.getTime()) / (1000 * 60 * 60 * 24)) : 0;

      // ── Blocked activity details ──
      const blockedActs = allActs.filter(a => a.status === 'blocked');
      const blockedDetails = blockedActs.map(a => templateMap[a.template_id] || 'Unknown').join(', ');

      if (dash.customer_status === 'blocked') {
        alerts.push(`🔴 BLOCKED — ${dash.client_name || dash.email} (Day ${daysSince})${blockedDetails ? ` | Blocked on: ${blockedDetails}` : ''}`);
      } else if (daysSince >= 14) {
        alerts.push(`🔴 OVERDUE — ${dash.client_name || dash.email} | ${dash.customer_status} | Day ${daysSince} (past 14-day window)`);
      } else if (daysSince >= 7) {
        alerts.push(`🟡 AT RISK — ${dash.client_name || dash.email} | ${dash.customer_status} | Day ${daysSince}`);
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

// ─── SALES INTELLIGENCE (iClosed + RevOps) ───────────────────────────────────
// Queries revops_* tables in the portal Supabase project.
// Tables are populated by the iClosed webhook pipeline.
// setter_id / closer_id mapping — update when David confirms the ID format.
const SALES_TEAM_MAP = {
  // ── SETTERS — GHL user IDs ───────────────────────────────────────────────
  'cuttpcov7ztlvyjkhdx8': 'Joseph Salazar',   'cUTTPGov7ZTLvyjKHdX8': 'Joseph Salazar',
  'zcmdiz2eerapd80w2zop': 'Oscar M',          'ZcmdIz2EEraPd80W2zop': 'Oscar M',
  'n8mvtuhbbby7qppqnmr7': 'William B',        'N8mvtuHbbbY7QppqNMr7': 'William B',
  '5orsahkh2joujb5fczrp': 'Debbanny Romero',  '5OrSaHkh2joUjB5FCZrP': 'Debbanny Romero', // historical — no longer active

  // ── CLOSERS — iClosed identifies hosts by email address ─────────────────
  'ronny.duarte@neurogrowth.io':  'Ron Duarte',
  'jose.neurogrowth@gmail.com':   'Jose Carranza',
  'jonathan.madriz.neurogrowth@gmail.com': 'Jonathan Madriz',

  // ── SETTERS — iClosed EOD email IDs ─────────────────────────────────────
  'joseph.neurogrowth@gmail.com':   'Joseph Salazar',
  'Salazcamjos@gmail.com':          'Joseph Salazar',
  'oscar.neurogrowth@gmail.com':    'Oscar M',
  'william.neurogrowth@gmail.com':  'William B',
  'debbanny.neurogrowth@gmail.com': 'Debbanny Romero', // historical — no longer active

  // ── FALLBACK — GHL user IDs for closers if iClosed uses those instead ───
  'gqymykpddltdxvbkfl2c': 'Jonathan Madriz', 'gqYMYkpDDlTdxvBkfl2C': 'Jonathan Madriz',
  'izlta0jy5orkymsyltjv': 'Jose Carranza',       'izLTA0jy5OrKyMvyltjV': 'Jose Carranza',
};
function resolveSalesMember(id) {
  if (!id) return 'Unknown';
  return SALES_TEAM_MAP[id] || SALES_TEAM_MAP[id.toLowerCase()] || id;
}

async function getSalesIntelligence(query) {
  try {
    const q = (query || '').toLowerCase();
    const now = new Date();
    const todayStart = new Date(now); todayStart.setHours(0,0,0,0);
    const todayEnd   = new Date(now); todayEnd.setHours(23,59,59,999);
    const weekStart  = new Date(now); weekStart.setDate(now.getDate() - now.getDay()); weekStart.setHours(0,0,0,0);
    const monthStart = new Date(now.getFullYear(), now.getMonth(), 1);

    const toDateStr = (d) => d.toISOString().split('T')[0];
    const todayStr      = toDateStr(now);
    const weekStartStr  = toDateStr(weekStart);
    const monthStartStr = toDateStr(monthStart);

    // ── Query EOD tables ───────────────────────────────────────────────────
    const { data: setterEOD } = await portalSupabase
      .from('revops_setter_eod_daily')
      .select('report_date, setter_id, new_conversations, follow_ups, qualified_leads, not_qualified_leads, scheduled_calls, calls_show, calls_no_show, notes')
      .order('report_date', { ascending: false })
      .limit(100);

    const { data: closerEOD } = await portalSupabase
      .from('revops_closer_eod_daily')
      .select('report_date, closer_id, scheduled_calls, canceled_calls, no_shows, qualified_calls, bookings, installment_closes, full_closes, notes')
      .order('report_date', { ascending: false })
      .limit(100);

    // ── Query appointments for individual call lookups ─────────────────────
    const { data: appointments } = await portalSupabase
      .from('revops_appointments')
      .select(`
        id, setter_id, closer_id, booked_at, scheduled_start,
        attended, no_show_reason, reschedule_count, meeting_type, iclosed_call_id,
        prospect:prospect_id ( full_name, company, email, lead_source, setter_owner_id, closer_owner_id, status )
      `)
      .order('scheduled_start', { ascending: false })
      .limit(200);

    // ── Query outcomes ─────────────────────────────────────────────────────
    const { data: outcomes } = await portalSupabase
      .from('revops_sales_outcomes')
      .select('appointment_id, outcome, offer_pitched, proposed_value, closed_revenue, close_date, lost_reason, objection_category, notes')
      .order('created_at', { ascending: false })
      .limit(200);

    const outcomeMap = {};
    (outcomes || []).forEach(o => { outcomeMap[o.appointment_id] = o; });
    const appts      = appointments || [];
    const setterRows = setterEOD || [];
    const closerRows = closerEOD || [];

    if (!appts.length && !setterRows.length && !closerRows.length) {
      return 'No sales data found yet. iClosed API integration is being set up — data will appear here once it starts syncing.';
    }

    const filterByDate = (rows, dateField, fromStr) =>
      rows.filter(r => r[dateField] && r[dateField] >= fromStr);

    // ── TODAY'S CALLS ──────────────────────────────────────────────────────
    if (q.includes('today') || q.includes('hoy')) {
      const todayCalls = appts.filter(a => {
        if (!a.scheduled_start) return false;
        const d = new Date(a.scheduled_start);
        return d >= todayStart && d <= todayEnd;
      });
      const todayCloserRows = closerRows.filter(r => r.report_date === todayStr);
      const todaySetterRows = setterRows.filter(r => r.report_date === todayStr);

      const lines = [];

      if (todayCalls.length) {
        lines.push(`Scheduled calls today (${todayCalls.length}):`);
        todayCalls.forEach(a => {
          const name    = a.prospect?.full_name || 'Unknown prospect';
          const closer  = resolveSalesMember(a.closer_id);
          const time    = new Date(a.scheduled_start).toLocaleTimeString('en-US', { hour: '2-digit', minute: '2-digit', timeZone: 'America/Costa_Rica' });
          const outcome = outcomeMap[a.id];
          const status  = outcome ? `outcome: ${outcome.outcome}` : (a.attended === false ? 'no-show' : a.attended === true ? 'attended' : 'scheduled');
          lines.push(`  ${name} — ${time} CR — closer: ${closer} — ${status}`);
        });
      }

      if (todayCloserRows.length) {
        lines.push(`\nCloser activity today:`);
        todayCloserRows.forEach(r => {
          const name = resolveSalesMember(r.closer_id);
          lines.push(`  ${name}: ${r.scheduled_calls} scheduled, ${r.canceled_calls} canceled, ${r.no_shows} no-shows, ${r.qualified_calls} qualified, ${r.full_closes} closes`);
        });
      }

      if (todaySetterRows.length) {
        lines.push(`\nSetter activity today:`);
        todaySetterRows.forEach(r => {
          const name = resolveSalesMember(r.setter_id);
          lines.push(`  ${name}: ${r.new_conversations} new convos, ${r.qualified_leads} qualified, ${r.scheduled_calls} calls booked`);
        });
      }

      if (!lines.length) return 'No call or EOD data found for today.';
      return lines.join('\n');
    }

    // ── SETTER PERFORMANCE ─────────────────────────────────────────────────
    if (q.includes('setter') || q.includes('joseph') || q.includes('oscar') || q.includes('william') || q.includes('debbanny') || q.includes('booked') || q.includes('conversations') || q.includes('qualified leads')) {
      const fromStr = q.includes('month') ? monthStartStr : weekStartStr;
      const period  = q.includes('month') ? 'this month' : 'this week';
      const rows    = filterByDate(setterRows, 'report_date', fromStr);

      if (!rows.length) return `No setter EOD data found for ${period}. Note: setter data comes from GHL EOD reports submitted daily.`;

      const byPerson = {};
      rows.forEach(r => {
        const name = resolveSalesMember(r.setter_id);
        if (!byPerson[name]) byPerson[name] = { new_conversations: 0, qualified_leads: 0, not_qualified: 0, scheduled_calls: 0, calls_show: 0, calls_no_show: 0, days: 0 };
        byPerson[name].new_conversations += r.new_conversations || 0;
        byPerson[name].qualified_leads   += r.qualified_leads   || 0;
        byPerson[name].not_qualified     += r.not_qualified_leads || 0;
        byPerson[name].scheduled_calls   += r.scheduled_calls   || 0;
        byPerson[name].calls_show        += r.calls_show        || 0;
        byPerson[name].calls_no_show     += r.calls_no_show     || 0;
        byPerson[name].days++;
      });

      const lines = [`Setter performance ${period}:`];
      Object.entries(byPerson).forEach(([name, s]) => {
        lines.push(`\n${name} (${s.days} day(s) with EOD):`);
        lines.push(`  New conversations: ${s.new_conversations}`);
        lines.push(`  Qualified: ${s.qualified_leads} | Not qualified: ${s.not_qualified}`);
        lines.push(`  Calls booked: ${s.scheduled_calls} | Show: ${s.calls_show} | No-show: ${s.calls_no_show}`);
      });
      return lines.join('\n');
    }

    // ── CLOSER PERFORMANCE ─────────────────────────────────────────────────
    if (q.includes('closer') || q.includes('jonathan') || q.includes('jose') || q.includes('close rate') || q.includes('closed') || q.includes('cancel') || q.includes('no-show') || q.includes('qualified calls')) {
      const fromStr = q.includes('month') ? monthStartStr : weekStartStr;
      const period  = q.includes('month') ? 'this month' : 'this week';
      const rows    = filterByDate(closerRows, 'report_date', fromStr);

      if (!rows.length) return `No closer EOD data found for ${period}.`;

      const byPerson = {};
      rows.forEach(r => {
        const name = resolveSalesMember(r.closer_id);
        if (!byPerson[name]) byPerson[name] = { scheduled: 0, canceled: 0, no_shows: 0, qualified: 0, bookings: 0, installment: 0, full: 0, days: 0 };
        byPerson[name].scheduled   += r.scheduled_calls   || 0;
        byPerson[name].canceled    += r.canceled_calls    || 0;
        byPerson[name].no_shows    += r.no_shows          || 0;
        byPerson[name].qualified   += r.qualified_calls   || 0;
        byPerson[name].bookings    += r.bookings          || 0;
        byPerson[name].installment += r.installment_closes || 0;
        byPerson[name].full        += r.full_closes       || 0;
        byPerson[name].days++;
      });

      const lines = [`Closer performance ${period}:`];
      Object.entries(byPerson).forEach(([name, c]) => {
        const closeRate = c.qualified > 0 ? Math.round((c.full + c.installment) / c.qualified * 100) : 0;
        lines.push(`\n${name} (${c.days} day(s) with EOD):`);
        lines.push(`  Scheduled: ${c.scheduled} | Canceled: ${c.canceled} | No-shows: ${c.no_shows}`);
        lines.push(`  Qualified calls: ${c.qualified} | Bookings: ${c.bookings}`);
        lines.push(`  Closes: ${c.full} full + ${c.installment} installment | Close rate: ${closeRate}%`);
      });
      return lines.join('\n');
    }

    // ── PROSPECT LOOKUP ────────────────────────────────────────────────────
    const matchedProspect = appts.find(a => a.prospect?.full_name && q.includes(a.prospect.full_name.toLowerCase().split(' ')[0]));
    if (matchedProspect) {
      const closer  = resolveSalesMember(matchedProspect.closer_id);
      const time    = matchedProspect.scheduled_start ? new Date(matchedProspect.scheduled_start).toLocaleString('en-US', { timeZone: 'America/Costa_Rica', month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit' }) : 'not scheduled';
      const outcome = outcomeMap[matchedProspect.id];
      const lines   = [
        `Prospect: ${matchedProspect.prospect?.full_name || 'Unknown'}`,
        `Company: ${matchedProspect.prospect?.company || 'N/A'}`,
        `Closer: ${closer}`,
        `Setter: not available from iClosed — check GHL for setter assignment`,
        `Scheduled: ${time} CR`,
        `Attended: ${matchedProspect.attended === true ? 'Yes' : matchedProspect.attended === false ? 'No — ' + (matchedProspect.no_show_reason || 'no reason given') : 'Not recorded'}`,
        matchedProspect.reschedule_count > 0 ? `Rescheduled: ${matchedProspect.reschedule_count}x` : '',
        outcome ? `Outcome: ${outcome.outcome}` : 'Outcome: not recorded yet',
        outcome?.closed_revenue ? `Revenue closed: $${outcome.closed_revenue}` : '',
        outcome?.lost_reason ? `Lost reason: ${outcome.lost_reason}` : '',
      ].filter(Boolean);
      return lines.join('\n');
    }

    // ── DEFAULT: PIPELINE SUMMARY ──────────────────────────────────────────
    const upcoming       = appts.filter(a => a.scheduled_start && new Date(a.scheduled_start) >= now).length;
    const thisWeekAppts  = appts.filter(a => a.scheduled_start && new Date(a.scheduled_start) >= weekStart).length;
    const totalCloses    = closerRows.reduce((sum, r) => sum + (r.full_closes || 0) + (r.installment_closes || 0), 0);
    const thisWeekCloses = filterByDate(closerRows, 'report_date', weekStartStr).reduce((sum, r) => sum + (r.full_closes || 0) + (r.installment_closes || 0), 0);
    const thisWeekConvos = filterByDate(setterRows, 'report_date', weekStartStr).reduce((sum, r) => sum + (r.new_conversations || 0), 0);

    return [
      `Sales pipeline summary:`,
      `Appointments on record: ${appts.length} | Upcoming: ${upcoming} | This week: ${thisWeekAppts}`,
      `New conversations this week (setters): ${thisWeekConvos}`,
      `Closes this week: ${thisWeekCloses} | All time: ${totalCloses}`,
      Object.keys(outcomeMap).length > 0 ? `Outcomes recorded: ${Object.keys(outcomeMap).length}` : 'No outcomes recorded yet',
    ].join('\n');

  } catch (err) {
    return `Sales intelligence error: ${err.message}`;
  }
}

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
      '5orsahkh2joujb5fczrp': 'Debbanny Romero', '5OrSaHkh2joUjB5FCZrP': 'Debbanny Romero',
      'gqymykpddltdxvbkfl2c': 'Jonathan Madriz', 'gqYMYkpDDlTdxvBkfl2C': 'Jonathan Madriz',
      'izlta0jy5orkymsyltjv': 'Jose Carranza',   'izLTA0jy5OrKyMvyltjV': 'Jose Carranza',
    };
    const lines = convos.map(c => {
      const lastDate  = new Date(c.lastMessageDate).toLocaleString('en-US', { timeZone: 'America/Costa_Rica', month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit' });
      const age       = Math.floor((now - c.lastMessageDate) / oneDayMs);
      const unread    = c.unreadCount > 0 ? ` [UNREAD: ${c.unreadCount}]` : '';
      const direction = c.lastMessageDirection === 'inbound' ? '<-- inbound' : '--> outbound';
      const channel   = c.lastMessageType?.replace('TYPE_', '') || 'unknown';
      const stale     = age >= 3 ? ` [${age}d ago - needs follow-up]` : '';
      // Resolve assigned setter from GHL user ID
      const assignedId   = c.assignedTo || c.userId || '';
      const assignedName = GHL_USERS[assignedId] || GHL_USERS[assignedId.toLowerCase()] || (assignedId ? `user:${assignedId}` : 'unassigned');
      return `${c.contactName || c.fullName || 'Unknown'} | setter: ${assignedName} | ${channel} | ${direction}${unread}${stale}\nLast: "${(c.lastMessageBody || '').substring(0, 120)}" (${lastDate})`;
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
    const { data: dashboards } = await portalSupabase.from('client_dashboards').select('id, client_name, customer_status, customer_type, created_at').eq('is_active', true);
    const { data: templates }  = await portalSupabase.from('customer_activity_templates').select('id, title, order_index');
    const tMap = {};
    (templates || []).forEach(t => { tMap[t.id] = t.title; });
    const { data: allActs } = await portalSupabase.from('customer_activities').select('customer_id, template_id, status, assigned_to, completed_at');
    if (!dashboards || !allActs) return;

    const phaseCounts = dashboards.reduce((acc, d) => { acc[d.customer_status] = (acc[d.customer_status]||0)+1; return acc; }, {});
    const blockedByActivity = {};
    allActs.filter(a => a.status === 'blocked').forEach(a => { const t = tMap[a.template_id]||'Unknown'; blockedByActivity[t] = (blockedByActivity[t]||0)+1; });
    const topBlocked = Object.entries(blockedByActivity).sort((a,b) => b[1]-a[1]).slice(0,5).map(([t,c]) => `${t} (${c}x)`).join(', ');
    const pendingByAssignee = {};
    allActs.filter(a => a.status === 'phase_1' || a.status === 'phase_2').forEach(a => { const e = (a.assigned_to||'unassigned').split('@')[0]; pendingByAssignee[e] = (pendingByAssignee[e]||0)+1; });
    const workload = Object.entries(pendingByAssignee).sort((a,b) => b[1]-a[1]).map(([e,c]) => `${e}: ${c} pending`).join(' | ');
    const liveClients   = dashboards.filter(d => d.customer_status === 'live' && d.created_at);
    const avgDaysToLive = liveClients.length > 0 ? Math.round(liveClients.reduce((sum,d) => sum + Math.floor((Date.now()-new Date(d.created_at).getTime())/(1000*60*60*24)),0)/liveClients.length) : null;
    const fwCount = dashboards.filter(d => d.customer_type === 'flywheel-ai').length;
    const fsCount = dashboards.filter(d => d.customer_type === 'full-service').length;
    const today = new Date().toLocaleDateString('en-US', { weekday:'long', month:'long', day:'numeric', timeZone:'America/Costa_Rica' });
    const trendReport = [`Week ending ${today}:`, `Phase distribution: ${Object.entries(phaseCounts).map(([k,v])=>`${k}:${v}`).join(', ')}`, `Client mix: ${fwCount} Flywheel AI, ${fsCount} Full Service`, topBlocked ? `Top blocked activities: ${topBlocked}` : 'No blocked activities this week.', workload ? `Team workload: ${workload}` : '', avgDaysToLive ? `Avg days since onboarding for live clients: ${avgDaysToLive} days` : ''].filter(Boolean).join(' | ');
    await upsertKnowledge('intel', `weekly-trends-${new Date().toISOString().slice(0,10)}`, trendReport, 'weekly-cron');
    if (topBlocked) await upsertKnowledge('process', 'recurring-blocked-activities', `As of ${today}: Most blocked activities are: ${topBlocked}. Review with Josue and Felipe.`, 'weekly-cron');
    if (workload)   await upsertKnowledge('team', 'current-workload', `As of ${today}: ${workload}`, 'weekly-cron');
    console.log('Weekly trend analysis complete.');
    await postToSlack(AGENT_CHANNEL, `📊 *Weekly trend analysis saved* — check knowledge base for latest intel and process insights.`);
  } catch (err) { console.error('Weekly trend error:', err.message); }
}

// ─── PORTAL: MONDAY GAP DETECTION ────────────────────────────────────────────
async function runMondayGapDetection(_correlationId) {
  console.log('Running Monday gap detection...');
  try {
    const { data: dashboards } = await portalSupabase.from('client_dashboards').select('id, client_name, email, customer_status, customer_type, created_at').eq('is_active', true).in('customer_status', ['phase_1','phase_2','blocked']);
    if (!dashboards || !dashboards.length) { console.log('No at-risk clients detected.'); return; }
    const { data: templates } = await portalSupabase.from('customer_activity_templates').select('id, title, order_index');
    const tMap = {};
    (templates || []).forEach(t => { tMap[t.id] = t.title; });
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

      // ── Day anchor: activation call completed_at → fallback to created_at ──
      // Fetch all activities (not just in-progress) to find completed activation call
      const { data: allActsForAnchor } = await portalSupabase.from('customer_activities').select('template_id, status, completed_at').eq('customer_id', dash.id);
      const activationAct = (allActsForAnchor || []).find(a => {
        const title = (tMap[a.template_id] || '').toLowerCase();
        return title.includes('activation call') && a.completed_at;
      });
      const startDate = activationAct ? new Date(activationAct.completed_at) : (dash.created_at ? new Date(dash.created_at) : null);
      const daysSince = startDate ? Math.floor((now - startDate.getTime()) / (1000*60*60*24)) : 0;

      const staleActs = acts.filter(a => { const u = a.updated_at ? new Date(a.updated_at).getTime() : 0; return (now - u) > (72*60*60*1000); });
      let gapLine = '';
      if (dash.customer_status === 'blocked') {
        const blockedTitles = acts.filter(a=>a.status==='blocked').map(a=>tMap[a.template_id]||'Unknown').join(', ');
        gapLine = `🔴 BLOCKED — ${dash.client_name} (Day ${daysSince}): ${blockedTitles}`;
      } else if (daysSince >= 14) {
        gapLine = `🔴 OVERDUE — ${dash.client_name} still in ${dash.customer_status} at Day ${daysSince} (past 14-day window)`;
      } else if (daysSince >= 7 && staleActs.length > 0) {
        const assignees = [...new Set(staleActs.map(a=>(a.assigned_to||'').split('@')[0]))].join(', ');
        gapLine = `🟡 STALE — ${dash.client_name} (Day ${daysSince}): ${staleActs.length} activities with no update in 72hrs. Assigned to: ${assignees}`;
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
      gaps.push(`${label} — ${name}${co} | ${stepLabels[r.phase0_step] || r.phase0_step} | Day ${r.days_in_phase0} (Tania to unblock)`);
    }

    // ── Sales gap detection ────────────────────────────────────────────────────
    try {
      const nowTs = Date.now();
      const salesGapLines = [];

      // a. No-shows with no reschedule in last 7 days
      const sevenDaysAgo = new Date(nowTs - 7 * 24 * 60 * 60 * 1000).toISOString();
      const { data: noShows } = await portalSupabase
        .from('revops_appointments')
        .select('id, prospect_id, closer_id, scheduled_start, prospect:prospect_id(full_name)')
        .eq('attended', false)
        .gte('scheduled_start', sevenDaysAgo);

      if (noShows && noShows.length) {
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
            const dStr  = new Date(appt.scheduled_start).toLocaleDateString('en-US', { month: 'short', day: 'numeric', timeZone: 'America/Costa_Rica' });
            const closerName = resolveSalesMember(appt.closer_id);
            noShowFlags.push(`• ${pName} — ${dStr}, closer: ${closerName}`);
          }
        }
        if (noShowFlags.length) {
          salesGapLines.push(`No-shows, no reschedule (last 7d):\n${noShowFlags.join('\n')}`);
        }
      }

      // b. Outcomes not logged (attended ≥48h ago, no outcome record)
      const fortyEightHAgo = new Date(nowTs - 48 * 60 * 60 * 1000).toISOString();
      const fourteenDaysAgo = new Date(nowTs - 14 * 24 * 60 * 60 * 1000).toISOString();
      const { data: attended } = await portalSupabase
        .from('revops_appointments')
        .select('id, prospect_id, closer_id, scheduled_start, prospect:prospect_id(full_name)')
        .eq('attended', true)
        .lte('scheduled_start', fortyEightHAgo)
        .gte('scheduled_start', fourteenDaysAgo);

      if (attended && attended.length) {
        const attendedIds = attended.map(a => a.id);
        const { data: outcomes } = await portalSupabase
          .from('revops_sales_outcomes')
          .select('appointment_id')
          .in('appointment_id', attendedIds);
        const loggedSet = new Set((outcomes || []).map(o => o.appointment_id));
        const unlogged = attended.filter(a => !loggedSet.has(a.id));
        if (unlogged.length) {
          const unloggedLines = unlogged.map(a => {
            const pName = a.prospect?.full_name || 'Unknown';
            const dStr  = new Date(a.scheduled_start).toLocaleString('en-US', { month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit', timeZone: 'America/Costa_Rica' });
            const closerName = resolveSalesMember(a.closer_id);
            return `• ${pName} — ${dStr} — please log in iClosed`;
          });
          salesGapLines.push(`Outcomes not logged (held >48h, no iClosed entry):\n${unloggedLines.join('\n')}`);
        }
      }

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
        digest += `\n\n=== CALENDAR (tomorrow) ===\n${tomorrowEvents}`;
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
    if (!digest) return;
    const todayStr = new Date().toLocaleDateString('en-US', { weekday:'long', month:'long', day:'numeric' });
    const learningPrompt = `You are the NeuroGrowth PM agent. Today is ${todayStr}. The current year is 2026.\n\nBelow is today's activity from key Slack channels and the portal. Extract and summarize operational intelligence.\n\nFormat EVERY insight as exactly: CATEGORY | KEY | VALUE\n\nRules:\n- CATEGORY must be exactly one of these words with no other characters: client, team, process, decision, alert, intel\n- Do NOT use markdown in CATEGORY. No asterisks, no backticks, no bold, no formatting. Just the plain word.\n- KEY should be a short descriptive identifier (client name, issue name, topic)\n- VALUE should be a single clear sentence or short paragraph, max 150 words\n- Only extract meaningful operational intelligence — skip small talk, greetings, and noise\n\nWhat to capture:\n1. Client status changes — who moved forward, who is blocked, who launched, who needs attention\n2. Wins and completions — what the team shipped or finished today\n3. Open action items that were raised but not resolved\n4. Team decisions made today\n5. Recurring patterns or blockers appearing across multiple clients\n6. Anything that should be flagged as an alert for tomorrow\n7. Email threads — any client or prospect communication that signals urgency, dissatisfaction, or opportunity\n8. Calendar events tomorrow — any sales calls, client check-ins, or deadlines Max should be aware of for morning briefing\n\n${digest}`;
    const tNightly = Date.now();
    const response = await anthropic.messages.create({ model: 'claude-sonnet-4-6', max_tokens: 1024, messages: [{ role: 'user', content: learningPrompt }] });
    logLlmFromAnthropicResponse(response, Date.now() - tNightly, correlationId);
    const text  = response.content.filter(b => b.type === 'text').map(b => b.text).join('\n');
    const lines = text.split('\n').filter(l => l.includes('|'));
    let saved = 0;
    for (const line of lines) {
      const parts = line.split('|').map(p => p.trim());
      if (parts.length >= 3) {
        const [rawCategory, key, ...valueParts] = parts;
        // Strip any markdown characters and normalize to valid category
        const category = rawCategory.toLowerCase().replace(/[^a-z]/g, '').trim();
        const VALID_CATEGORIES = new Set(['client','team','process','decision','alert','intel']);
        const value = valueParts.join('|').trim();
        if (category && VALID_CATEGORIES.has(category) && key && value) {
          const normalizedKey = category === 'client'
            ? `client:${key.toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/(^-|-$)/g, '')}:${new Date().toISOString().slice(0, 10)}`
            : key;
          await upsertKnowledge(category, normalizedKey, value, 'nightly-learning');
          saved++;
        }
      }
    }
    console.log(`Nightly learning complete. ${saved} knowledge entries saved.`);
    await postToSlack(AGENT_CHANNEL, `🧠 *Nightly learning complete* — ${new Date().toLocaleDateString('en-US', {weekday:'long', month:'long', day:'numeric', timeZone:'America/Costa_Rica'})}\nSources scanned: 5 Slack channels + Gmail + Calendar | Knowledge entries saved: ${saved}`);
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

    const now = Date.now();
    const tomorrow = new Date();
    tomorrow.setDate(tomorrow.getDate() + 1);
    const tomorrowStr = tomorrow.toLocaleDateString('en-US', { weekday:'long', month:'long', day:'numeric', timeZone:'America/Costa_Rica' });

    const hitting14Tomorrow   = []; // Day 13 today → Day 14 tomorrow (launch deadline)
    const hitting7Tomorrow    = []; // Day 6 today → Day 7 tomorrow (at-risk threshold)
    const blocked             = []; // Currently blocked
    const stalledPhase1       = []; // Stuck in phase_1 for 4+ days
    const stalledPhase2       = []; // Stuck in phase_2 for 4+ days

    for (const dash of dashboards) {
      if (!dash.created_at) continue;
      const daysSince = Math.floor((now - new Date(dash.created_at).getTime()) / (1000 * 60 * 60 * 24));

      if (daysSince === 13) hitting14Tomorrow.push(dash);
      else if (daysSince === 6) hitting7Tomorrow.push(dash);
      if (dash.customer_status === 'blocked') blocked.push(dash);
      if (dash.customer_status === 'phase_1' && daysSince >= 4) stalledPhase1.push(dash);
      if (dash.customer_status === 'phase_2' && daysSince >= 4) stalledPhase2.push(dash);
    }

    // ── DM Josue: clients hitting Day 14 today ──
    if (hitting14Tomorrow.length > 0) {
      const names = hitting14Tomorrow.map(d => `${d.client_name} (Day 13)`).join(', ');
      const msg = `Heads up — ${hitting14Tomorrow.length === 1 ? 'this client hits' : 'these clients hit'} their 14-day launch deadline today: ${names}. If campaigns are not live by end of day, we miss the SLA. What needs to happen right now to make sure they launch on time?`;
      for (const id of (slackIdsByRole('tech_ops').length ? slackIdsByRole('tech_ops') : ['U08ABBFNGUW'])) {
        await slack.client.chat.postMessage({ channel: id, text: msg });
      }
      console.log(`Proactive DM sent to Josue: ${hitting14Tomorrow.length} client(s) hitting Day 14 today`);
    }

    // ── DM Josue: clients hitting at-risk threshold today ──
    if (hitting7Tomorrow.length > 0) {
      const names = hitting7Tomorrow.map(d => `${d.client_name} (Day 6)`).join(', ');
      const msg = `Quick flag — ${hitting7Tomorrow.length === 1 ? 'this client hits' : 'these clients hit'} Day 7 today, which is the at-risk threshold: ${names}. Worth checking their progress now so we're not scrambling next week.`;
      for (const id of (slackIdsByRole('tech_ops').length ? slackIdsByRole('tech_ops') : ['U08ABBFNGUW'])) {
        await slack.client.chat.postMessage({ channel: id, text: msg });
      }
      console.log(`Proactive DM sent to Josue: ${hitting7Tomorrow.length} client(s) hitting Day 7 today`);
    }

    // ── DM Valeria: clients stalled in phase_1 ──
    if (stalledPhase1.length > 0) {
      const names = stalledPhase1.map(d => `${d.client_name} (Day ${Math.floor((now - new Date(d.created_at).getTime()) / (1000*60*60*24))})`).join(', ');
      const msg = `These clients are still in Phase 1 and have been for a while: ${names}. If any delivery documents are pending on your end, this is the priority. Let Josue know if you're blocked on anything.`;
      for (const id of (slackIdsByRole('fulfillment').length ? slackIdsByRole('fulfillment') : ['U09Q3BXJ18B'])) {
        await slack.client.chat.postMessage({ channel: id, text: msg });
      }
      console.log(`Proactive DM sent to fulfillment role: ${stalledPhase1.length} client(s) stalled in Phase 1`);
    }

    // ── DM Felipe: clients stalled in phase_2 ──
    if (stalledPhase2.length > 0) {
      const names = stalledPhase2.map(d => `${d.client_name} (Day ${Math.floor((now - new Date(d.created_at).getTime()) / (1000*60*60*24))})`).join(', ');
      const msg = `These clients are still in Phase 2 and haven't moved in a few days: ${names}. If campaign config or Prosp setup is pending on your end, these need to be the first thing tomorrow. Flag Josue if anything is blocked.`;
      for (const id of (slackIdsByRole('campaigns').length ? slackIdsByRole('campaigns') : ['U09TNMVML3F'])) {
        await slack.client.chat.postMessage({ channel: id, text: msg });
      }
      console.log(`Proactive DM sent to campaigns role: ${stalledPhase2.length} client(s) stalled in Phase 2`);
    }

    // ── DM Tania: blocked clients ──
    if (blocked.length > 0) {
      const names = blocked.map(d => `${d.client_name}`).join(', ');
      const msg = `These clients are currently blocked: ${names}. If the block is on the client side — missing onboarding form, unresponsive, contract issue — this needs a proactive outreach before it becomes a bigger problem. Can you check what's needed and follow up?`;
      for (const id of (slackIdsByRole('client_success').length ? slackIdsByRole('client_success') : ['U07SMMDMSLQ'])) {
        await slack.client.chat.postMessage({ channel: id, text: msg });
      }
      console.log(`Proactive DM sent to Tania: ${blocked.length} blocked client(s)`);
    }

    // ── DM Tania: Phase 0 clients stuck ≥7 days in same step ──────────────────
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
      for (const id of (slackIdsByRole('client_success').length ? slackIdsByRole('client_success') : ['U07SMMDMSLQ'])) {
        await slack.client.chat.postMessage({ channel: id, text: msg });
      }
      console.log(`Proactive DM sent to Tania: ${stuckPhase0.length} Phase 0 client(s) stuck ≥7 days`);
    }

    // ── DM Tania: clients hitting Day 20 in Phase 3 (stabilization) ──
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
      for (const id of (slackIdsByRole('client_success').length ? slackIdsByRole('client_success') : ['U07SMMDMSLQ'])) {
        await slack.client.chat.postMessage({ channel: id, text: msg });
      }
      console.log(`Proactive DM sent to Tania: ${hitting20InStabilization.length} client(s) at Day 20 stabilization`);
    }

    if (approaching20InStabilization.length > 0) {
      const names = approaching20InStabilization.map(d => d.client_name).join(', ');
      const msg = `Heads up — these clients hit Day 20 in stabilization in 2 days: ${names}. Start preparing the 1:1 progress check outreach so it's ready to go on Day 20.`;
      for (const id of (slackIdsByRole('client_success').length ? slackIdsByRole('client_success') : ['U07SMMDMSLQ'])) {
        await slack.client.chat.postMessage({ channel: id, text: msg });
      }
      console.log(`Proactive DM sent to Tania: ${approaching20InStabilization.length} client(s) approaching Day 20 stabilization`);
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

// CAC (iClosed sales, attributed via Meta `purchase` pixel fired on sale outcome)
async function _scrapeMetaCacToday() {
  const row = await _metaAccountInsights('today');
  if (!row) return null;
  const spend    = parseFloat(row.spend || 0);
  const sales    = parseInt((row.actions || []).find(a => a.action_type === 'purchase')?.value || '0', 10);
  if (sales <= 0) return null;
  return +(spend / sales).toFixed(2);
}

/// Cost-per-booking: Meta total spend today / iClosed calls booked today
async function _scrapeMetaCostPerBookingToday() {
  const row = await _metaAccountInsights('today');
  if (!row) return null;
  const spend = parseFloat(row.spend || 0);
  if (spend <= 0) return null;
  const todayStr = new Date().toISOString().slice(0, 10);
  const { count, error } = await portalSupabase
    .from('revops_appointments')
    .select('id', { count: 'exact', head: true })
    .gte('booked_at', `${todayStr}T00:00:00`)
    .lt('booked_at',  `${todayStr}T23:59:59`);
  if (error) throw new Error(error.message);
  if (!count || count <= 0) return null;
  return +(spend / count).toFixed(2);
}

// GHL new contacts today (both funnels — universal lead-volume signal)
async function _scrapeGhlNewContactsToday() {
  const locationId = process.env.GHL_LOCATION_ID;
  const apiKey     = process.env.GHL_API_KEY;
  if (!locationId || !apiKey) return null;
  const headers  = { 'Authorization': `Bearer ${apiKey}`, 'Version': '2021-07-28' };
  const todayStr = new Date().toISOString().slice(0, 10);
  const res  = await fetch(
    `https://services.leadconnectorhq.com/contacts/?locationId=${locationId}&startDate=${todayStr}&endDate=${todayStr}&limit=100`,
    { headers }
  );
  const data = await res.json();
  if (data.meta?.total !== undefined) return data.meta.total;
  return (data.contacts || []).length;
}

// iClosed calls booked yesterday (both funnels via revops_appointments)
async function _scrapeIclosedCallsBookedYesterday() {
  const yesterday = new Date(Date.now() - 24 * 60 * 60 * 1000).toISOString().slice(0, 10);
  const { data, error } = await portalSupabase
    .from('revops_appointments')
    .select('id')
    .gte('booked_at', `${yesterday}T00:00:00`)
    .lt('booked_at',  `${yesterday}T23:59:59`);
  if (error) throw new Error(error.message);
  return (data || []).length;
}

// iClosed calls held yesterday (attended = true)
async function _scrapeIclosedCallsHeldYesterday() {
  const yesterday = new Date(Date.now() - 24 * 60 * 60 * 1000).toISOString().slice(0, 10);
  const { data, error } = await portalSupabase
    .from('revops_appointments')
    .select('id')
    .eq('attended', true)
    .gte('scheduled_start', `${yesterday}T00:00:00`)
    .lt('scheduled_start',  `${yesterday}T23:59:59`);
  if (error) throw new Error(error.message);
  return (data || []).length;
}

// iClosed sales yesterday (full_closes from closer EOD table — same source as close_rate)
async function _scrapeIclosedSalesYesterday() {
  const yesterday = new Date(Date.now() - 24 * 60 * 60 * 1000).toISOString().slice(0, 10);
  const { data, error } = await portalSupabase
    .from('revops_closer_eod_daily')
    .select('full_closes')
    .eq('report_date', yesterday);
  if (error) throw new Error(error.message);
  if (!data || !data.length) return null;
  return data.reduce((a, r) => a + (r.full_closes || 0), 0);
}

async function _scrapeCloseRateYesterday() {
  const yesterday = new Date(Date.now() - 24 * 60 * 60 * 1000).toISOString().slice(0, 10);
  const { data, error } = await portalSupabase
    .from('revops_closer_eod_daily')
    .select('full_closes, qualified_calls')
    .eq('report_date', yesterday);
  if (error) throw new Error(error.message);
  if (!data || !data.length) return null;
  const closes = data.reduce((a, r) => a + (r.full_closes || 0), 0);
  const qualified = data.reduce((a, r) => a + (r.qualified_calls || 0), 0);
  if (qualified <= 0) return null;
  return +(closes / qualified).toFixed(4);
}

async function _scrapeSetterCallsBookedYesterday() {
  const yesterday = new Date(Date.now() - 24 * 60 * 60 * 1000).toISOString().slice(0, 10);
  const { data, error } = await portalSupabase
    .from('revops_setter_eod_daily')
    .select('scheduled_calls')
    .eq('report_date', yesterday);
  if (error) throw new Error(error.message);
  if (!data || !data.length) return null;
  return data.reduce((a, r) => a + (r.scheduled_calls || 0), 0);
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
    .select('id, created_at, customer_status')
    .eq('is_active', true)
    .eq('customer_status', targetStatus);
  if (error) throw new Error(error.message);
  if (!dashboards || !dashboards.length) return null;

  const { data: templates } = await portalSupabase
    .from('customer_activity_templates')
    .select('id, title');
  const tmap = {};
  (templates || []).forEach(t => { tmap[t.id] = (t.title || '').toLowerCase(); });

  const days = [];
  for (const d of dashboards) {
    const { data: acts } = await portalSupabase
      .from('customer_activities')
      .select('template_id, completed_at')
      .eq('customer_id', d.id);
    const activation = (acts || []).find(a => (tmap[a.template_id] || '').includes('activation call') && a.completed_at);
    const start = activation ? new Date(activation.completed_at) : (d.created_at ? new Date(d.created_at) : null);
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
    .select('id, created_at, customer_status')
    .eq('is_active', true)
    .in('customer_status', ['phase_1', 'phase_2']);
  if (error) throw new Error(error.message);
  if (!dashboards) return 0;

  const { data: templates } = await portalSupabase
    .from('customer_activity_templates')
    .select('id, title');
  const tmap = {};
  (templates || []).forEach(t => { tmap[t.id] = (t.title || '').toLowerCase(); });

  let count = 0;
  for (const d of dashboards) {
    const { data: acts } = await portalSupabase
      .from('customer_activities')
      .select('template_id, completed_at')
      .eq('customer_id', d.id);
    const activation = (acts || []).find(a => (tmap[a.template_id] || '').includes('activation call') && a.completed_at);
    const start = activation ? new Date(activation.completed_at) : (d.created_at ? new Date(d.created_at) : null);
    if (!start) continue;
    const days = Math.floor((Date.now() - start.getTime()) / (24 * 60 * 60 * 1000));
    if (days >= 7) count++;
  }
  return count;
}

// ── Metric registry — single source of truth for what gets scraped ──────────
const METRIC_REGISTRY = [
  // Marketing — per-funnel CPL signals (blended CPL is meaningless across two funnels)
  { name: 'meta_form_cpl_today',          domain: 'marketing',   scrape: _scrapeMetaFormCplToday,          label: 'Meta Form CPL (today)' },
  { name: 'meta_cac_today',               domain: 'marketing',   scrape: _scrapeMetaCacToday,               label: 'Meta CAC via iClosed purchase pixel (today)' },
  { name: 'meta_cost_per_booking_today',  domain: 'marketing',   scrape: _scrapeMetaCostPerBookingToday,    label: 'Meta cost-per-booking (today)' },
  // Lead volume — GHL is the universal truth-source (covers both VSL + Form funnels)
  { name: 'ghl_new_contacts_today',       domain: 'sales',       scrape: _scrapeGhlNewContactsToday,        label: 'GHL new contacts (today)' },
  // Call pipeline — iClosed revops_appointments is the truth-source for booked/held
  { name: 'iclosed_calls_booked_yest',    domain: 'sales',       scrape: _scrapeIclosedCallsBookedYesterday, label: 'iClosed calls booked (yesterday)' },
  { name: 'iclosed_calls_held_yest',      domain: 'sales',       scrape: _scrapeIclosedCallsHeldYesterday,   label: 'iClosed calls held (yesterday)' },
  { name: 'iclosed_sales_yest',           domain: 'sales',       scrape: _scrapeIclosedSalesYesterday,       label: 'iClosed sales / full closes (yesterday)' },
  { name: 'close_rate_yesterday',         domain: 'sales',       scrape: _scrapeCloseRateYesterday,          label: 'Close rate (yesterday)' },
  { name: 'setter_calls_booked_yest',     domain: 'sales',       scrape: _scrapeSetterCallsBookedYesterday,  label: 'Setter calls booked EOD (yesterday)' },
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
async function callClaude(messages, retries = 3, userId = null, correlationId = null) {
  const correlation_id = correlationId != null && correlationId !== undefined ? correlationId : newCorrelationId();
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
      const fullSystemPrompt = (userId ? buildRoleSystemPrompt(userId) : SYSTEM_PROMPT) + timeContext;

      const TOOLS = [
          { name: 'search_notion',       description: 'Search NeuroGrowth Notion workspace for pages, tasks, client info, and SOPs',           input_schema: { type: 'object', properties: { query: { type: 'string' } }, required: ['query'] } },
          { name: 'get_notion_page',      description: 'Get the content of a specific Notion page by its ID',                                   input_schema: { type: 'object', properties: { page_id: { type: 'string' } }, required: ['page_id'] } },
          { name: 'get_recent_emails',    description: "Get recent unread emails from Ron's Gmail inbox including full email body content",      input_schema: { type: 'object', properties: {} } },
          { name: 'send_email',           description: "Send an email on Ron's behalf. Always confirm before sending.",                          input_schema: { type: 'object', properties: { to: { type: 'string' }, subject: { type: 'string' }, body: { type: 'string' } }, required: ['to','subject','body'] } },
          { name: 'get_calendar_events',  description: 'Get calendar events. daysFromNow: 0=today, 1=tomorrow, -1=yesterday. daysRange: 1=day, 7=week, 14=two weeks.', input_schema: { type: 'object', properties: { daysFromNow: { type: 'number' }, daysRange: { type: 'number' } } } },
          { name: 'search_drive',         description: "Search Ron's Google Drive for files and documents",                                      input_schema: { type: 'object', properties: { query: { type: 'string' } }, required: ['query'] } },
          { name: 'read_google_sheet',    description: 'Read the actual cell data from a Google Sheet. Accepts a Google Sheets URL or file ID. Optionally specify a range.',                                                                                                                                        input_schema: { type: 'object', properties: { spreadsheetId: { type: 'string', description: 'Google Sheets URL or spreadsheet ID' }, range: { type: 'string', description: 'Optional range e.g. Sheet1!A1:Z100' } }, required: ['spreadsheetId'] } },
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
          { name: 'get_sales_intelligence', description: 'Query iClosed and RevOps sales data from Supabase. Use for: closer performance (Jonathan, Jose — scheduled calls, cancellations, no-shows, qualified calls, closes, close rate from revops_closer_eod_daily), setter performance (Joseph, Oscar, William — new conversations, qualified leads, calls booked from revops_setter_eod_daily — NOTE: setter data comes from GHL EOD reports not iClosed; Debbanny is no longer active but historical rows resolve to her name), today\'s calls, prospect lookup by name, pipeline summary. Setter assignment on individual calls is not available from iClosed — direct setter questions to GHL conversations.', input_schema: { type: 'object', properties: { query: { type: 'string', description: 'Natural language query e.g. who booked the Andres Chavez call, how many calls today, close rate this month, Joseph bookings this week' } }, required: ['query'] } },
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
          { name: 'query_metric_history', description: 'Return the time series for a tracked metric so the user can see trend, baseline, and recent observations. Use when someone asks "show me CPL over the last 30 days" or "how has close rate trended?". Available metrics: meta_cpl_today, close_rate_yesterday, setter_calls_booked_yest, phase0_to_phase1_conv_7d, phase1_cycle_days_p50, phase2_cycle_days_p50, day7_at_risk_count, ghl_response_time_p50_min.', input_schema: { type: 'object', properties: { metric: { type: 'string', description: 'Exact metric name from the registry.' }, days: { type: 'number', description: 'Window of history to return, default 30, max 90.' } }, required: ['metric'] } },
      ];

      // ── Tool dispatcher — shared across initial and all follow-up rounds ──────
      async function dispatchTool(toolUse) {
        // Gate: Ron-only tools refuse for non-Ron users
        if (userId && userId !== RON_SLACK_ID && RON_ONLY_TOOLS.has(toolUse.name)) {
          return {
            type: 'tool_result',
            tool_use_id: toolUse.id,
            content: JSON.stringify(`BLOCKED: ${toolUse.name} is currently Ron-only (Gmail and Calendar use his personal OAuth). Ask Ron to run this, or let me help a different way.`),
          };
        }
        let result;
        if      (toolUse.name === 'search_notion')          result = await searchNotion(toolUse.input.query);
        else if (toolUse.name === 'get_notion_page')        result = await getNotionPage(toolUse.input.page_id);
        else if (toolUse.name === 'get_recent_emails')      result = await getRecentEmails();
        else if (toolUse.name === 'send_email')             result = await sendEmail(toolUse.input.to, toolUse.input.subject, toolUse.input.body);
        else if (toolUse.name === 'get_calendar_events')    result = await getCalendarEvents(toolUse.input.daysFromNow || 0, toolUse.input.daysRange || 1);
        else if (toolUse.name === 'search_drive')           { const r = await searchDrive(toolUse.input.query); result = r.length > 4000 ? r.substring(0, 4000) + '...[trimmed]' : r; }
        else if (toolUse.name === 'read_google_sheet')      result = await readGoogleSheet(extractGoogleFileId(toolUse.input.spreadsheetId), toolUse.input.range || null);
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
        else if (toolUse.name === 'query_metric_history')   result = await queryMetricHistory(toolUse.input.metric, Math.min(toolUse.input.days || 30, 90));
        return { type: 'tool_result', tool_use_id: toolUse.id, content: JSON.stringify(result) };
      }

      // ── Initial call ─────────────────────────────────────────────────────────
      const tInitial = Date.now();
      let response = await anthropic.messages.create({
        model: 'claude-sonnet-4-6',
        max_tokens: 2048,
        system: fullSystemPrompt,
        messages,
        tools: TOOLS,
      });
      logLlmFromAnthropicResponse(response, Date.now() - tInitial, correlation_id);

      // ── Multi-round tool loop (max 5 rounds to prevent infinite chains) ──────
      const MAX_TOOL_ROUNDS = 5;
      let currentMessages = [...messages];

      for (let round = 0; round < MAX_TOOL_ROUNDS; round++) {
        if (response.stop_reason !== 'tool_use') break;

        const toolUses = response.content.filter(b => b.type === 'tool_use');
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
        const draftResult = toolResults.find(r => { try { return JSON.parse(r.content).startsWith('APPROVAL_NEEDED|'); } catch { return false; } });
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
              max_tokens: 1024,
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

async function executeChannelPost(channelName, message, say, correlationId) {
  try {
    const channels = await getCachedChannelList();
    const channel  = channels.find(c => c.name === channelName.replace('#', ''));
    if (!channel) { await say(`Could not find channel ${channelName}.`); }
    else {
      await slack.client.chat.postMessage({ channel: channel.id, text: message });
      if (correlationId) {
        logActivity({ event_type: 'slack_message', event_source: 'slack', action: 'outbound', channel_id: channel.id, output: { text: String(message).slice(0, 2000) }, correlation_id: correlationId });
      }
      await say(`Posted to ${channelName}.`);
      if (correlationId) {
        logActivity({ event_type: 'slack_message', event_source: 'slack', action: 'outbound', output: { text: `Posted to ${channelName}.`.slice(0, 2000) }, correlation_id: correlationId });
      }
    }
  } catch (err) { await say(`Something went wrong posting: ${err.message}`); }
}

async function checkApproval(message, say, userId) {
  const pending = pendingApprovals[userId];
  if (!pending) return false;
  const approvalCid = newCorrelationId();
  const text = (typeof message === 'string' ? message : message.text || '').toLowerCase().trim();
  if (['yes','send it','approved','go ahead','👍'].includes(text)) {
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
  if (['no','cancel','stop'].includes(text)) {
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

// DM handler
slack.message(async ({ message, say }) => {
  if (message.bot_id) return;
  if (message.channel_type !== 'im') return;
  const isApproval = await checkApproval(message, say, message.user);
  if (isApproval) return;
  const userId = message.user;
  if (!PILOT_USERS.has(userId)) { await say("Max isn't enabled for you yet — Ron is rolling this out in phases. Ping him if you need access."); return; }
  if (message.subtype === 'file_share' && message.files?.length > 0) { await handleFileMessage(message, say, userId, false); return; }
  if (message.subtype) return;
  if (isRateLimited(userId)) { await say('Slow down a bit — you are sending messages too fast. Give me a moment.'); return; }
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
  history.push({ role: 'user', content: message.text });
  try {
    let reply = await callClaude(history, 3, userId, correlation_id);
    if (!reply || !reply.trim()) { console.error('Empty reply, retrying for user:', userId); reply = await callClaude(history, 2, userId, correlation_id); }
    if (!reply || !reply.trim()) return;
    if (handleDraftReply(reply, userId, say, correlation_id)) return;
    await saveMessage(userId, 'user', message.text);
    await saveMessage(userId, 'assistant', reply);
    await say(reply);
    logActivity({ event_type: 'slack_message', event_source: 'slack', action: 'outbound', channel_id: message.channel, thread_ts: message.thread_ts, output: { text: String(reply).slice(0, 2000) }, correlation_id });
  } catch (err) { console.error('Claude API error (DM):', err); await say('Got turned around for a second — go ahead and ask again.'); }
});

// @mention handler
slack.event('app_mention', async ({ event, say }) => {
  if (event.bot_id) return;
  const cleanText = event.text.replace(/<@[A-Z0-9]+>/g, '').trim();
  if (!cleanText) return;
  const userId = event.user;
  if (!PILOT_USERS.has(userId)) { await say({ text: "Max isn't enabled for you yet — Ron is rolling this out in phases.", thread_ts: event.thread_ts || event.ts }); return; }

  // If this mention is inside a thread, fetch the full thread context first
  let threadContext = '';
  if (event.thread_ts && event.thread_ts !== event.ts) {
    try {
      const threadResult = await slack.client.conversations.replies({
        channel: event.channel,
        ts: event.thread_ts,
        limit: 50,
      });
      const threadMessages = (threadResult.messages || []).filter(m => m.ts !== event.ts);
      if (threadMessages.length > 0) {
        const channelInfo = await slack.client.conversations.info({ channel: event.channel }).catch(() => null);
        const channelName = channelInfo?.channel?.name || 'unknown channel';
        threadContext = `\n\nTHREAD CONTEXT from #${channelName} (read this before responding — this is what was discussed before you were tagged):\n` +
          threadMessages.map(m => {
            const time = new Date(parseFloat(m.ts) * 1000).toLocaleString('en-US', { timeZone: 'America/Costa_Rica', month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit' });
            const sender = m.bot_id ? 'Max' : (m.user === event.user ? 'Ron' : `user:${m.user}`);
            return `[${time}] ${sender}: ${(m.text || '').substring(0, 300)}`;
          }).join('\n');

        // Save this thread to knowledge base so Max remembers it
        const threadSummaryKey = `thread:${event.channel}:${event.thread_ts}`;
        await upsertKnowledge('process', threadSummaryKey,
          `Thread tagged for Max on ${new Date().toLocaleDateString('en-US', { timeZone: 'America/Costa_Rica' })} in #${channelInfo?.channel?.name || 'unknown'}. Last action: "${cleanText.substring(0, 200)}"`,
          'thread-mention'
        );

        // If the thread root was posted by Max (a report), extract the lesson from
        // the user's feedback before responding — so Max can acknowledge it.
        const rootMessage = threadMessages[0];
        const isMaxBotPost = rootMessage && !rootMessage.user && rootMessage.bot_id;
        if (isMaxBotPost) {
          try {
            const lesson = await extractAndSaveReportLesson(
              rootMessage.text || '',
              cleanText,
              channelName,
              userId,
              newCorrelationId()
            );
            if (lesson) {
              threadContext += `\n\nIMPORTANT: This person is giving feedback on a report Max posted. A lesson has been extracted and saved: "${lesson}". Acknowledge this in your reply — confirm what you learned and that you will apply it to future reports for this channel. Keep it to 1-2 sentences, plain text.`;
            }
          } catch (lessonErr) {
            console.error('Lesson extraction error:', lessonErr.message);
          }
        }

        // Extract and store client-specific context from any thread (not just Max bot posts)
        try {
          const clientCtx = await extractClientContext(threadMessages, cleanText, channelName, userId);
          if (clientCtx) {
            const key = `client:${clientCtx.client.toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/(^-|-$)/g, '')}:${new Date().toISOString().slice(0, 10)}`;
            await upsertKnowledge('client', key, clientCtx.context, 'thread-context', userId, 'shared');
            console.log(`Client context saved for "${clientCtx.client}" from #${channelName}`);
          }
        } catch (ctxErr) {
          console.error('Client context extraction error:', ctxErr.message);
        }
      }
    } catch (threadErr) {
      console.error('Thread context fetch error:', threadErr.message);
    }
  }

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

// #ng-pm-agent channel handler
slack.message(async ({ message, say }) => {
  if (message.subtype || message.bot_id) return;
  if (message.channel_type === 'im') return;
  let channelInfo;
  try { channelInfo = await slack.client.conversations.info({ channel: message.channel }); } catch { return; }
  const channelName = channelInfo.channel?.name || '';
  if (!channelName.includes('ng-pm-agent')) return;
  const isApproval = await checkApproval(message, say, message.user);
  if (isApproval) return;
  const userId = message.user;
  if (!PILOT_USERS.has(userId)) { await say({ text: "Max isn't enabled for you yet — Ron is rolling this out in phases.", thread_ts: message.thread_ts || message.ts }); return; }
  if (message.subtype === 'file_share' && message.files?.length > 0) { await handleFileMessage(message, say, userId, true); return; }
  if (message.subtype) return;
  if (isRateLimited(userId)) { await say({ text: 'Slow down a bit — too many messages at once. Give me a moment.', thread_ts: message.thread_ts || message.ts }); return; }
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
  history.push({ role: 'user', content: message.text });
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
// closer_id in revops_appointments is stored as email (iClosed API format)
const CLOSER_SLACK = {
  'jonathan.madriz.neurogrowth@gmail.com': 'U0APYAE0999', // Jonathan
  'jose.neurogrowth@gmail.com':            'U0AMTEKDCPN', // Jose
  'ronny.duarte@neurogrowth.io':           'U05HXGX18H3', // Ron (when he's the closer)
  // GHL user ID fallbacks (in case format changes)
  'gqymykpddltdxvbkfl2c': 'U0APYAE0999', 'gqYMYkpDDlTdxvBkfl2C': 'U0APYAE0999',
  'izlta0jy5orkymsyltjv': 'U0AMTEKDCPN', 'izLTA0jy5OrKyMvyltjV': 'U0AMTEKDCPN',
};

async function fetchGHLConvoForContact(contactId) {
  const locationId = process.env.GHL_LOCATION_ID;
  const apiKey     = process.env.GHL_API_KEY;
  const headers    = { 'Authorization': `Bearer ${apiKey}`, 'Version': '2021-07-28', 'Content-Type': 'application/json' };
  // Get conversations for this contact
  const convoRes  = await fetch(`https://services.leadconnectorhq.com/conversations/search?locationId=${locationId}&contactId=${contactId}&limit=1`, { headers });
  const convoData = await convoRes.json();
  const convoId   = convoData.conversations?.[0]?.id;
  if (!convoId) return null;
  // Get recent messages
  const msgRes  = await fetch(`https://services.leadconnectorhq.com/conversations/${convoId}/messages?limit=12`, { headers });
  const msgData = await msgRes.json();
  return msgData.messages || [];
}

async function searchGHLContact(email, name) {
  const locationId = process.env.GHL_LOCATION_ID;
  const apiKey     = process.env.GHL_API_KEY;
  const headers    = { 'Authorization': `Bearer ${apiKey}`, 'Version': '2021-07-28', 'Content-Type': 'application/json' };
  // Try by email first, fall back to name
  const query = email || name;
  const res  = await fetch(`https://services.leadconnectorhq.com/contacts/?locationId=${locationId}&query=${encodeURIComponent(query)}&limit=1`, { headers });
  const data = await res.json();
  return data.contacts?.[0] || null;
}

async function runSalesCallPrep(_correlationId) {
  console.log('Running sales call prep...');
  try {
    const now       = Date.now();
    const windowMin = now + (3.5 * 60 * 60 * 1000); // 3.5h from now
    const windowMax = now + (5   * 60 * 60 * 1000); // 5h from now

    // Query upcoming appointments in the window
    const { data: upcoming, error } = await portalSupabase
      .from('revops_appointments')
      .select(`
        id, closer_id, setter_id, scheduled_start, meeting_type, booked_at,
        prospect:prospect_id ( full_name, company, email, lead_source )
      `)
      .gte('scheduled_start', new Date(windowMin).toISOString())
      .lte('scheduled_start', new Date(windowMax).toISOString())
      .is('attended', null); // only upcoming (not yet attended/no-showed)

    if (error) throw error;
    if (!upcoming || !upcoming.length) {
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
      const prospectName = prospect.full_name || 'Unknown prospect';
      const company     = prospect.company   || '';
      const email       = prospect.email     || '';
      const leadSource  = prospect.lead_source || '';
      const closerName  = resolveSalesMember(appt.closer_id);
      const closerSlack = CLOSER_SLACK[appt.closer_id] || CLOSER_SLACK[(appt.closer_id || '').toLowerCase()];
      const callTime    = new Date(appt.scheduled_start).toLocaleString('en-US', { timeZone: 'America/Costa_Rica', weekday: 'short', month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit' });
      const hoursOut    = Math.round((new Date(appt.scheduled_start).getTime() - now) / (1000 * 60 * 60) * 10) / 10;

      // GHL conversation lookup + setter resolution
      let convoSection = 'GHL conversation not found for this prospect.';
      let setterName   = 'VSL self-booking'; // default — no setter unless GHL says otherwise
      try {
        const ghlContact = await searchGHLContact(email, prospectName);
        if (ghlContact) {
          // Resolve setter from GHL contact.assignedTo (Appointment Setter pipeline only)
          if (ghlContact.assignedTo) {
            const resolved = resolveSalesMember(ghlContact.assignedTo);
            if (resolved !== ghlContact.assignedTo) setterName = resolved; // mapped = real setter name
          }
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

      if (setterName === 'VSL self-booking' && convoSection === 'GHL conversation not found for this prospect.') {
        convoSection = 'VSL direct booking — prospect self-scheduled via the sales page. No setter contact, no GHL conversation.';
      }

      // Build the brief
      const brief = [
        `📞 *CALL PREP — ${prospectName}* | in ${hoursOut}h (${callTime} CR)`,
        company     ? `🏢 Company: ${company}` : '',
        email       ? `📧 Email: ${email}` : '',
        leadSource  ? `🔗 Lead source: ${leadSource}` : '',
        setterName === 'VSL self-booking' ? `📲 Source: VSL self-booking (no setter)` : `👤 Booked by: ${setterName}`,
        ``,
        `*GHL CONVERSATION HISTORY:*`,
        convoSection,
        ``,
        `Good luck on the call ${closerName.split(' ')[0]}. Let me know if you need anything before you jump on.`,
      ].filter(l => l !== null && l !== undefined).join('\n');

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

    // Helper: day count anchored to activation call or created_at
    function getDayCount(dash) {
      return dash.created_at ? Math.floor((now - new Date(dash.created_at).getTime()) / (1000 * 60 * 60 * 24)) : null;
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
      return `${name} — Day ${d ? getDayCount(d) : '?'}${acts ? ` | blocked on: ${acts}` : ''}`;
    }));
    josueLines.push(...renderDelta('Day 14 TODAY', dH14.new, dH14.resolved, dH14.unchanged, name => {
      const d = hitting14Today.find(x => x.client_name === name);
      return `${name} (${d ? phaseLabel[d.customer_status] || d.customer_status : ''}) — must launch today`;
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

    // ── DM Valeria — delivery docs, Phase 1 ───────────────────────────────────
    const valeriaSnap = await getYesterdayStandupSnapshot('valeria');
    const valeriaLines = [`Good morning Valeria! Here's your ${today} delivery brief:\n`];

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
      await slack.client.chat.postMessage({ channel: id, text: valeriaLines.join('\n') });
    }
    await saveStandupSnapshot('valeria', { phase1Clients: phase1.map(d => ({ name: d.client_name, day: getDayCount(d) })), stalledGe4: stalledP1, blocked: vBlockedNames });
    console.log('Standup DM sent to Valeria');

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

    // ── DM Tania — Phase 0 owner, SLA enforcer, Phase 3 client success ─────────
    const taniaSnap = await getYesterdayStandupSnapshot('tania');
    const taniaLines = [`Good morning Tania! Here's your ${today} client success brief:\n`];

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
        if (over) return `${name} — Day ${getDayCount(over)} | ${phaseLabel[over.customer_status] || over.customer_status} | past 14-day SLA`;
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

    for (const id of (slackIdsByRole('client_success').length ? slackIdsByRole('client_success') : ['U07SMMDMSLQ'])) {
      await slack.client.chat.postMessage({ channel: id, text: taniaLines.join('\n') });
    }
    await saveStandupSnapshot('tania', { phase0Clients: p0Names, slaWatch: slaNames, phase3StabClients: p3StabNames, blocked: blockedNames });
    console.log('Standup DM sent to Tania');

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
    const yesterday = new Date(now - 24 * 60 * 60 * 1000).toISOString().slice(0, 10);

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
      { slackId: 'U0A9J00EMGD', name: 'Joseph' },
      { slackId: 'U0B1S1UMH9P', name: 'Oscar' },
      { slackId: 'U0B16P6DQ2F', name: 'William' },
    ];

    // Fetch yesterday's setter EOD aggregate (team total)
    const { data: setterEOD } = await portalSupabase
      .from('revops_setter_eod_daily')
      .select('scheduled_calls, new_conversations, qualified_leads')
      .eq('report_date', yesterday);

    const totalScheduled  = (setterEOD || []).reduce((s, r) => s + (r.scheduled_calls      || 0), 0);
    const totalConvos     = (setterEOD || []).reduce((s, r) => s + (r.new_conversations    || 0), 0);
    const totalQualified  = (setterEOD || []).reduce((s, r) => s + (r.qualified_leads      || 0), 0);

    // Weekly total: last 7 rows for scheduled_calls (team aggregate)
    const { data: weeklyEOD } = await portalSupabase
      .from('revops_setter_eod_daily')
      .select('scheduled_calls')
      .order('report_date', { ascending: false })
      .limit(7);
    const weeklyScheduled = (weeklyEOD || []).reduce((s, r) => s + (r.scheduled_calls || 0), 0);

    const setterLessons = await getReportLessons('sales-standup-setter');
    const setterLessonNote = setterLessons.length
      ? `[Corrections applied from feedback]\n${setterLessons.map(l => `• ${l.value}`).join('\n')}\n\n`
      : '';

    for (const setter of setters) {
      try {
        const lines = [`${setterLessonNote}Good morning ${setter.name}! Here's your setter brief for ${today}:\n`];

        // Yesterday stats
        lines.push(`📊 Yesterday: ${totalScheduled} calls booked | ${totalConvos} new convos | ${totalQualified} leads qualified`);
        lines.push(`📈 This week: ${weeklyScheduled} calls booked total`);
        lines.push('');

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

    // Today's appointments
    const { data: todayCalls } = await portalSupabase
      .from('revops_appointments')
      .select('id, closer_id, scheduled_start, prospect:prospect_id(full_name)')
      .gte('scheduled_start', todayStartISO)
      .lte('scheduled_start', todayEndISO)
      .order('scheduled_start', { ascending: true });

    // Yesterday's attended with no outcome logged
    const ystStart = new Date(now - 24 * 60 * 60 * 1000); ystStart.setHours(0, 0, 0, 0);
    const ystEnd   = new Date(now - 24 * 60 * 60 * 1000); ystEnd.setHours(23, 59, 59, 999);
    const { data: ystAttended } = await portalSupabase
      .from('revops_appointments')
      .select('id, closer_id, scheduled_start, prospect:prospect_id(full_name)')
      .eq('attended', true)
      .gte('scheduled_start', ystStart.toISOString())
      .lte('scheduled_start', ystEnd.toISOString());

    let unloggedByCloser = {};
    if (ystAttended && ystAttended.length) {
      const ystIds = ystAttended.map(a => a.id);
      const { data: ystOutcomes } = await portalSupabase
        .from('revops_sales_outcomes')
        .select('appointment_id')
        .in('appointment_id', ystIds);
      const loggedSet = new Set((ystOutcomes || []).map(o => o.appointment_id));
      ystAttended.filter(a => !loggedSet.has(a.id)).forEach(a => {
        (unloggedByCloser[a.closer_id] = unloggedByCloser[a.closer_id] || []).push(a);
      });
    }

    // Yesterday's closer EOD stats
    const { data: closerEOD } = await portalSupabase
      .from('revops_closer_eod_daily')
      .select('closer_id, full_closes, qualified_calls, no_shows')
      .eq('report_date', yesterday);
    const closerEODMap = {};
    (closerEOD || []).forEach(r => { closerEODMap[r.closer_id] = r; });

    const closers = Object.entries(CLOSER_SLACK)
      .filter(([email, slackId]) => slackId && email.includes('@') && slackId !== RON_SLACK_ID)
      .map(([email, slackId]) => ({ email, slackId, name: resolveSalesMember(email) }));

    const closerLessons = await getReportLessons('sales-standup-closer');
    const closerLessonNote = closerLessons.length
      ? `[Corrections applied from feedback]\n${closerLessons.map(l => `• ${l.value}`).join('\n')}\n\n`
      : '';

    for (const closer of closers) {
      try {
        const eod           = closerEODMap[closer.email] || {};
        const heldCalls     = eod.qualified_calls || 0;
        const closes        = eod.full_closes     || 0;
        const noShows       = eod.no_shows        || 0;
        const closeRatePct  = heldCalls > 0 ? Math.round((closes / heldCalls) * 100) : 0;

        const myTodayCalls = (todayCalls || []).filter(a => a.closer_id === closer.email);
        const myUnlogged   = unloggedByCloser[closer.email] || [];

        const lines = [`${closerLessonNote}Good morning ${closer.name.split(' ')[0]}! Here's your closer brief for ${today}:\n`];

        // Yesterday stats
        lines.push(`📊 Yesterday: ${heldCalls} calls held | ${closes} closes | ${closeRatePct}% close rate | ${noShows} no-shows`);
        lines.push('');

        // Today on deck
        if (myTodayCalls.length) {
          lines.push(`📞 Today on deck (${myTodayCalls.length} calls):`);
          myTodayCalls.forEach(a => {
            const pName  = a.prospect?.full_name || 'Unknown';
            const timeStr = new Date(a.scheduled_start).toLocaleTimeString('en-US', { hour: '2-digit', minute: '2-digit', timeZone: 'America/Costa_Rica' });
            lines.push(`• ${pName} — ${timeStr} CR time`);
          });
          lines.push('');
        }

        // Outcomes not logged
        if (myUnlogged.length) {
          lines.push(`⚠️ Outcome not logged (${myUnlogged.length} calls):`);
          myUnlogged.forEach(a => {
            const pName  = a.prospect?.full_name || 'Unknown';
            const dStr   = new Date(a.scheduled_start).toLocaleString('en-US', { month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit', timeZone: 'America/Costa_Rica' });
            lines.push(`• ${pName} — ${dStr} — please log in iClosed`);
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

// ─── WEEKLY SALES & MARKETING RECAP ──────────────────────────────────────────
// Fires Friday 5 PM CR. DMs Ron only with 7-day sales + marketing summary.
async function runWeeklySalesMarketingRecap(_correlationId) {
  console.log('Running weekly sales & marketing recap...');
  try {
    // Date range labels for the week (Mon–Fri)
    const now     = new Date();
    const dayOfWk = now.getDay(); // 0=Sun, 5=Fri
    const monday  = new Date(now); monday.setDate(now.getDate() - (dayOfWk === 0 ? 6 : dayOfWk - 1)); monday.setHours(0,0,0,0);
    const monLabel = monday.toLocaleDateString('en-US', { month: 'long', day: 'numeric', timeZone: 'America/Costa_Rica' });
    const friLabel = now.toLocaleDateString('en-US',    { month: 'long', day: 'numeric', timeZone: 'America/Costa_Rica' });

    // ── Meta Ads summary ──────────────────────────────────────────────────
    const metaSummary = await getMetaAdsSummary('last_7d');

    // ── metric_observations from primary supabase ─────────────────────────
    async function fetchMetric(metricName) {
      const { data } = await supabase
        .from('metric_observations')
        .select('metric, value, observed_at')
        .eq('metric', metricName)
        .order('observed_at', { ascending: false })
        .limit(7);
      return data || [];
    }

    const [newContacts, callsBooked, callsHeld, salesRows, closeRates] = await Promise.all([
      fetchMetric('ghl_new_contacts_today'),
      fetchMetric('iclosed_calls_booked_yest'),
      fetchMetric('iclosed_calls_held_yest'),
      fetchMetric('iclosed_sales_yest'),
      fetchMetric('close_rate_yesterday'),
    ]);

    const sumMetric  = rows => rows.reduce((s, r) => s + (parseFloat(r.value) || 0), 0);
    const avgMetric  = rows => rows.length ? (sumMetric(rows) / rows.length) : 0;

    const totalContacts  = Math.round(sumMetric(newContacts));
    const avgContactsDay = newContacts.length ? (totalContacts / newContacts.length).toFixed(1) : 'N/A';
    const totalBooked    = Math.round(sumMetric(callsBooked));
    const totalHeld      = Math.round(sumMetric(callsHeld));
    const showRatePct    = totalBooked > 0 ? Math.round((totalHeld / totalBooked) * 100) : 0;
    const totalSales     = Math.round(sumMetric(salesRows));
    const avgCloseRate   = avgMetric(closeRates).toFixed(1);

    // ── Anomalies from agent_knowledge ────────────────────────────────────
    const sevenDaysAgoISO = new Date(Date.now() - 7 * 24 * 60 * 60 * 1000).toISOString();
    const { data: alerts } = await supabase
      .from('agent_knowledge')
      .select('key, value')
      .eq('category', 'alert')
      .gte('updated_at', sevenDaysAgoISO)
      .order('updated_at', { ascending: false })
      .limit(20);

    // ── Build message ─────────────────────────────────────────────────────
    const parts = [
      `📈 Weekly Sales & Marketing Recap — Week of ${monLabel} to ${friLabel}`,
      '',
      `💰 META SPEND`,
      metaSummary,
      '',
      `📥 LEAD VOLUME (GHL — both funnels)`,
      `New contacts this week: ${totalContacts} total (avg ${avgContactsDay}/day)`,
      '',
      `📞 SALES PIPELINE`,
      `Calls booked: ${totalBooked} | Calls held: ${totalHeld} | Show rate: ${showRatePct}%`,
      `Closes: ${totalSales} | Avg close rate: ${avgCloseRate}%`,
    ];

    if (alerts && alerts.length) {
      parts.push('');
      parts.push(`🚨 ANOMALIES THIS WEEK (${alerts.length})`);
      alerts.forEach(a => {
        parts.push(`• ${a.key} — ${(a.value || '').substring(0, 120)}`);
      });
    }

    // Pull client-level updates from team threads and nightly learning this week
    const { data: clientUpdates } = await supabase
      .from('agent_knowledge')
      .select('key, value')
      .eq('category', 'client')
      .gte('updated_at', sevenDaysAgoISO)
      .order('updated_at', { ascending: false })
      .limit(10);
    if (clientUpdates && clientUpdates.length) {
      parts.push('');
      parts.push(`👥 CLIENT UPDATES FROM TEAM (this week)`);
      clientUpdates.forEach(r => {
        const clientName = (r.key.split(':')[1] || r.key).replace(/-/g, ' ');
        parts.push(`• ${clientName}: ${(r.value || '').substring(0, 150)}`);
      });
    }

    const recapLessons = await getReportLessons('weekly-sales-marketing-recap');
    if (recapLessons.length) {
      parts.unshift(`[Corrections applied from feedback]\n${recapLessons.map(l => `• ${l.value}`).join('\n')}\n`);
    }
    parts.push('');
    parts.push('See something off? Reply to this message tagging @Max with the correction.');

    const msg = parts.join('\n');
    await slack.client.chat.postMessage({ channel: RON_SLACK_ID, text: msg });
    console.log('Weekly sales & marketing recap sent to Ron.');
  } catch (err) {
    console.error('Weekly sales & marketing recap error:', err.message);
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

// Proactive alerts — 9:00 AM and 2:00 PM CR (infrastructure — posts stale alerts to agent channel)
cron.schedule('0 15 * * *',  wrapCronJob('runProactiveAlerts', runProactiveAlerts),     { timezone: 'America/Costa_Rica' });
cron.schedule('0 20 * * *',  wrapCronJob('runProactiveAlerts', runProactiveAlerts),     { timezone: 'America/Costa_Rica' });

// Proactive team DMs — 8:00 AM CR Mon–Fri (infrastructure — DMs Josue, Valeria, Felipe, Tania based on client status)
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

// Fulfillment morning standup — 9:00 AM CR Mon–Fri (DMs Josue, Valeria, Felipe with daily priorities)
cron.schedule('0 9 * * 1-5', wrapCronJob('runFulfillmentStandup', async (c) => { await runFulfillmentStandup(c); }),  { timezone: 'America/Costa_Rica' });

// Sales morning standup — 9:00 AM CR Mon–Fri (DMs setters and closers with role-specific briefs)
cron.schedule('0 9 * * 1-5', wrapCronJob('runSalesStandup', async (c) => { await runSalesStandup(c); }),              { timezone: 'America/Costa_Rica' });

// Sales call prep — every hour Mon–Fri (DMs closer 4h before any strategy call)
cron.schedule('0 * * * 1-5',  wrapCronJob('runSalesCallPrep', async (c) => { await runSalesCallPrep(c); }),       { timezone: 'America/Costa_Rica' });

// Stalled prospect follow-ups — 11 AM CR Mon–Fri. Dry-run DMs setters their stalled list;
// live auto-send activates only when STALLED_FOLLOWUPS_LIVE='true' (deferred until dry-run validated).
cron.schedule('0 11 * * 1-5', wrapCronJob('runStalledProspectFollowups', async (c) => { await runStalledProspectFollowups(c); }), { timezone: 'America/Costa_Rica' });

// Setter leaderboard — Wed + Sat 6 PM CR. Posts MTD per-setter performance to #ng-sales-goats.
cron.schedule('0 18 * * 3,6', wrapCronJob('runSetterLeaderboard', async (c) => { await runSetterLeaderboard(c); }), { timezone: 'America/Costa_Rica' });

// ─── GHL LEAD WEBHOOK ─────────────────────────────────────────────────────────
const GHL_USER_NAMES = {
  'cuttpcov7ztlvyjkhdx8': 'Joseph Salazar', 'cUTTPGov7ZTLvyjKHdX8': 'Joseph Salazar',
  'zcmdiz2eerapd80w2zop': 'Oscar M',         'ZcmdIz2EEraPd80W2zop': 'Oscar M',
  'n8mvtuhbbby7qppqnmr7': 'William B',       'N8mvtuHbbbY7QppqNMr7': 'William B',
  '5orsahkh2joujb5fczrp': 'Debbanny',        '5OrSaHkh2joUjB5FCZrP': 'Debbanny', // historical — no longer active
  'gqymykpddltdxvbkfl2c': 'Jonathan Madriz', 'gqYMYkpDDlTdxvBkfl2C': 'Jonathan Madriz',
  'izlta0jy5orkymsyltjv': 'Jose Carranza',   'izLTA0jy5OrKyMvyltjV': 'Jose Carranza',
  'zogw530idnpofqqnfssc': 'Ron Duarte',      'zoGW530iDnPOFqQNfssc': 'Ron Duarte',
};

const GHL_TO_SLACK = {
  'joseph': 'U0A9J00EMGD', 'joseph salazar': 'U0A9J00EMGD',
  'oscar': 'U0B1S1UMH9P', 'oscar m': 'U0B1S1UMH9P', 'oscar neurogrowth': 'U0B1S1UMH9P',
  'william': 'U0B16P6DQ2F', 'william b': 'U0B16P6DQ2F', 'william neurogrowth': 'U0B16P6DQ2F',
  'debbanny': 'U0AR16QVDB3', 'debanny': 'U0AR16QVDB3', 'debbanny neurogrowth': 'U0AR16QVDB3', 'debbanny romero': 'U0AR16QVDB3', // historical
  'jonnathan': 'U0APYAE0999', 'jonathan': 'U0APYAE0999', 'jonathan madriz': 'U0APYAE0999',
  'jose': 'U0AMTEKDCPN', 'jose carranza': 'U0AMTEKDCPN',
  'cuttpcov7ztlvyjkhdx8': 'U0A9J00EMGD', '5orsahkh2joujb5fczrp': 'U0AR16QVDB3',
  'zcmdiz2eerapd80w2zop': 'U0B1S1UMH9P', 'n8mvtuhbbby7qppqnmr7': 'U0B16P6DQ2F',
  'gqymykpddltdxvbkfl2c': 'U0APYAE0999', 'izlta0jy5orkymsyltjv': 'U0AMTEKDCPN',
};

// Reverse map for lead-claim flow: Slack user → GHL user ID (used by reaction_added handler)
// Active setters only — Debbanny was removed when she rolled off 2026-05-03.
const SLACK_TO_GHL_USER = {
  'U0A9J00EMGD': 'cUTTPGov7ZTLvyjKHdX8', // Joseph Salazar
  'U0B1S1UMH9P': 'ZcmdIz2EEraPd80W2zop', // Oscar M
  'U0B16P6DQ2F': 'N8mvtuHbbbY7QppqNMr7', // William B
  'U0APYAE0999': 'gqYMYkpDDlTdxvBkfl2C', // Jonathan Madriz
  'U0AMTEKDCPN': 'izLTA0jy5OrKyMvyltjV', // Jose Carranza
  'U05HXGX18H3': 'zoGW530iDnPOFqQNfssc', // Ron Duarte (testing)
};

const LEAD_CHANNEL_ID = 'C0AJANQBYUE'; // #ng-sales-goats
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
        if (!resolvedAssignedTo && contactId) {
          try {
            const contactRes  = await fetch(`https://services.leadconnectorhq.com/contacts/${contactId}`, { headers: { 'Authorization': `Bearer ${process.env.GHL_API_KEY}`, 'Version': '2021-07-28' } });
            const contactData = await contactRes.json();
            const assignedUser = contactData.contact?.assignedTo || contactData.assignedTo || '';
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
          } catch (apiErr) { console.error('GHL contact lookup error:', apiErr.message); }
        }

        console.log('GHL parsed:', { fullName, email, phone, source, assignedTo: resolvedAssignedTo, contactId });

        const ghlLink      = contactId ? `https://app.gohighlevel.com/v2/location/${locationId}/contacts/detail/${contactId}` : 'https://app.gohighlevel.com';
        const setterSlackId = resolveSetterSlackId(resolvedAssignedTo);
        const contextLine = leadContext ? `\n- Context: ${leadContext}` : '';
        const actionGuidance = leadContext
          ? `Their first action should reflect the Context above (e.g. if it mentions booking friction, the setter should call/DM the lead now to unblock the booking, not just "reach out").`
          : `Their first action (reach out now, check GHL).`;
        const prompt = `You are Max, the NeuroGrowth PM Agent. A new lead just came in and was assigned to a setter.\n\nLead details:\n- Name: ${fullName}\n- Email: ${email || 'not provided'}\n- Phone: ${phone || 'not provided'}\n- Source: ${source}\n- Assigned to: ${resolvedAssignedTo || 'unassigned'}${contextLine}\n- GHL link: ${ghlLink}\n\nWrite a short, direct Slack DM to the setter (2-3 sentences max) telling them: 1. A new lead came in and was assigned to them. 2. Key lead details. 3. ${actionGuidance} Sound like a colleague, not a bot. No markdown. Include the GHL link.`;
        const ghlCorr = newCorrelationId();
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
          phone             ? `📱 ${phone}`   : null,
          source && source !== 'Unknown channel' ? `📌 Source: ${source}` : null,
          resolvedAssignedTo ? `👤 Assigned to: ${resolvedAssignedTo}` : null,
          leadContext        ? `📝 ${leadContext}` : null,
          contactId          ? `🔗 ${ghlLink}` : null,
        ].filter(Boolean).join('\n') + claimHint;
        await slack.client.chat.postMessage({
          channel: LEAD_CHANNEL_ID,
          text: channelNote,
          metadata: contactId ? {
            event_type: 'ghl_lead',
            event_payload: { contact_id: contactId, location_id: locationId, full_name: fullName, correlation_id: ghlCorr },
          } : undefined,
        });
        logActivity({ event_type: 'slack_message', event_source: 'slack', action: 'outbound', channel_id: LEAD_CHANNEL_ID, output: { text: channelNote.slice(0, 2000) }, correlation_id: ghlCorr });
      } catch (parseErr) { console.error('GHL webhook parse error:', parseErr.message); }
    });
  } catch (err) { console.error('GHL webhook handler error:', err.message); res.writeHead(500); res.end('error'); }
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

async function ghlGetConversationMessages(conversationId) {
  const res = await fetch(`https://services.leadconnectorhq.com/conversations/${conversationId}/messages?limit=20`, {
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
  const res = await fetch(`https://services.leadconnectorhq.com/contacts/${contactId}`, {
    headers: { 'Authorization': `Bearer ${process.env.GHL_API_KEY}`, 'Version': '2021-07-28' },
  });
  if (!res.ok) return null;
  const data = await res.json();
  return data.contact || data || null;
}

async function hasFutureBooking(contact) {
  // iClosed booking lookup via revops_iclosed_bookings — match on email or phone
  try {
    const email = (contact.email || '').toLowerCase();
    const phone = (contact.phone || '').replace(/\D/g, '');
    const orFilters = [];
    if (email) orFilters.push(`email.eq.${email}`);
    if (phone) orFilters.push(`phone.like.%${phone.slice(-9)}%`);
    if (!orFilters.length) return null;
    const { data, error } = await supabase
      .from('revops_iclosed_bookings')
      .select('event_at, event_status')
      .or(orFilters.join(','))
      .gte('event_at', new Date().toISOString())
      .order('event_at', { ascending: true })
      .limit(1);
    if (error || !data || !data.length) return null;
    return data[0];
  } catch (_) { return null; }
}

async function hasAttendedCall(contact) {
  try {
    const email = (contact.email || '').toLowerCase();
    const phone = (contact.phone || '').replace(/\D/g, '');
    const orFilters = [];
    if (email) orFilters.push(`email.eq.${email}`);
    if (phone) orFilters.push(`phone.like.%${phone.slice(-9)}%`);
    if (!orFilters.length) return null;
    const { data, error } = await supabase
      .from('revops_iclosed_bookings')
      .select('event_at, event_status')
      .or(orFilters.join(','))
      .in('event_status', ['Showed', 'Qualified', 'Disqualified'])
      .order('event_at', { ascending: false })
      .limit(1);
    if (error || !data || !data.length) return null;
    return data[0];
  } catch (_) { return null; }
}

async function evaluateStalledCandidate(convo, ghlUserNames) {
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

  const futureBooking = await hasFutureBooking(contact);
  if (futureBooking) return { skip: `already_booked:${futureBooking.event_at}`, setterSlackId };

  const attended = await hasAttendedCall(contact);
  if (attended) return { skip: `already_attended:${attended.event_status}`, setterSlackId };

  return {
    skip: null,
    setterSlackId,
    setterName,
    contactName: convo.contactName || convo.fullName || contact.firstName || 'Unknown',
    lastBody: lastBody.slice(0, 140),
    ageDays: Math.floor((Date.now() - convo.lastMessageDate) / (24 * 60 * 60 * 1000)),
    contactId: convo.contactId,
    conversationId: convo.id,
  };
}

async function runStalledProspectFollowups(correlationId) {
  const isLive = process.env.STALLED_FOLLOWUPS_LIVE === 'true';
  console.log(`runStalledProspectFollowups starting (mode=${isLive ? 'LIVE' : 'DRY_RUN'})`);

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

  const ghlUserNames = {
    'cuttpcov7ztlvyjkhdx8': 'Joseph Salazar', 'cUTTPGov7ZTLvyjKHdX8': 'Joseph Salazar',
    'zcmdiz2eerapd80w2zop': 'Oscar M',         'ZcmdIz2EEraPd80W2zop': 'Oscar M',
    'n8mvtuhbbby7qppqnmr7': 'William B',       'N8mvtuHbbbY7QppqNMr7': 'William B',
    'gqymykpddltdxvbkfl2c': 'Jonathan Madriz', 'gqYMYkpDDlTdxvBkfl2C': 'Jonathan Madriz',
    'izlta0jy5orkymsyltjv': 'Jose Carranza',   'izLTA0jy5OrKyMvyltjV': 'Jose Carranza',
  };

  const skipCounts = {};
  const claimable = [];
  for (const c of candidates) {
    const result = await evaluateStalledCandidate(c, ghlUserNames);
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

  // DM each setter their stalled list (dry-run) — or send (live, future)
  for (const [slackId, list] of Object.entries(bySetter)) {
    if (!isLive) {
      const lines = list.map(r => `• ${r.contactName} (${r.ageDays}d, WhatsApp) — last: "${r.lastBody}" → https://app.gohighlevel.com/v2/location/${locationId}/contacts/detail/${r.contactId}`);
      const text = `👀 *Stalled prospects* — dry-run (no auto-followup sent yet):\n${lines.join('\n')}\n\n_Reply to nudge them yourself, or sit tight — Max will start auto-following up after dry-run watch period._`;
      try {
        await slack.client.chat.postMessage({ channel: slackId, text });
        logActivity({ event_type: 'slack_message', event_source: 'slack', action: 'outbound', channel_id: slackId, output: { text: text.slice(0, 2000) }, correlation_id: correlationId });
      } catch (err) { console.error(`stalled DM to ${slackId} failed:`, err.message); }
    } else {
      // LIVE path is intentionally not wired in this commit — see rollout step 3.
      console.warn(`STALLED_FOLLOWUPS_LIVE=true but live send path not yet implemented. Falling back to dry-run DM.`);
    }
  }

  // Summary DM to Ron
  const totalCandidates = candidates.length;
  const totalSendable   = claimable.length;
  const totalSkipped    = totalCandidates - totalSendable;
  const skipBreakdown   = Object.entries(skipCounts).map(([k, v]) => `${k}: ${v}`).join(', ') || 'none';
  const summary = [
    `📊 *Stalled prospect dry-run summary* (${isLive ? 'LIVE' : 'DRY-RUN'})`,
    `Candidates: ${totalCandidates} | Would send: ${totalSendable} | Skipped: ${totalSkipped}`,
    `Skip breakdown: ${skipBreakdown}`,
    totalSendable > 0 ? `Setter DMs sent: ${Object.keys(bySetter).length}` : '_(no setter DMs sent — no eligible prospects today)_',
  ].join('\n');
  try {
    await slack.client.chat.postMessage({ channel: RON_SLACK_ID, text: summary });
    logActivity({ event_type: 'slack_message', event_source: 'slack', action: 'outbound', channel_id: RON_SLACK_ID, output: { text: summary }, correlation_id: correlationId });
  } catch (err) { console.error('stalled summary DM to Ron failed:', err.message); }

  console.log(`runStalledProspectFollowups done — ${totalSendable}/${totalCandidates} eligible, skipped ${totalSkipped}`);
}

// ─── SETTER LEADERBOARD (Wed + Sat 6 PM CR — MTD post to #ng-sales-goats) ────
// Pulls MTD setter performance from portal Supabase (revops_setter_eod_daily) +
// leads-claimed from primary Supabase (setter_claims). Posts a ranked leaderboard
// to #ng-sales-goats. No LLM call — pure SQL/JS aggregation, deterministic.

async function runSetterLeaderboard(correlationId) {
  const now = new Date();
  const monthStart = new Date(now.getFullYear(), now.getMonth(), 1);
  const monthStartDate = monthStart.toISOString().slice(0, 10);
  const monthStartIso  = monthStart.toISOString();
  const monthLabel     = monthStart.toLocaleString('en-US', { timeZone: 'America/Costa_Rica', month: 'long' });
  const todayLabel     = now.toLocaleString('en-US', { timeZone: 'America/Costa_Rica', weekday: 'short', month: 'short', day: 'numeric' });

  // 1. Portal Supabase — MTD EOD aggregation per setter
  let bySetter = {};
  try {
    const { data: eodRows, error: eodErr } = await portalSupabase
      .from('revops_setter_eod_daily')
      .select('setter_id, new_conversations, follow_ups, qualified_leads, scheduled_calls, calls_show, calls_no_show')
      .gte('report_date', monthStartDate);
    if (eodErr) throw eodErr;
    for (const r of (eodRows || [])) {
      const k = (r.setter_id || '').toLowerCase();
      if (!k) continue;
      const slot = bySetter[k] ||= { calls_placed: 0, qualified_leads: 0, shows: 0, no_shows: 0, engagement: 0 };
      slot.calls_placed    += r.scheduled_calls    || 0;
      slot.qualified_leads += r.qualified_leads    || 0;
      slot.shows           += r.calls_show         || 0;
      slot.no_shows        += r.calls_no_show      || 0;
      slot.engagement      += (r.new_conversations || 0) + (r.follow_ups || 0);
    }
  } catch (err) {
    console.error('runSetterLeaderboard EOD fetch failed:', err.message);
    bySetter = {}; // continue with claims-only data
  }

  // 2. Primary Supabase — leads claimed MTD from setter_claims
  let claimsBySlack = {};
  try {
    const { data: claimRows, error: claimErr } = await supabase
      .from('setter_claims')
      .select('claimed_by_slack_user_id')
      .gte('claimed_at', monthStartIso);
    if (claimErr) throw claimErr;
    for (const r of (claimRows || [])) {
      const k = r.claimed_by_slack_user_id;
      if (!k) continue;
      claimsBySlack[k] = (claimsBySlack[k] || 0) + 1;
    }
  } catch (err) {
    console.error('runSetterLeaderboard setter_claims fetch failed:', err.message);
    claimsBySlack = {};
  }

  // 3. Reconcile: convert Slack-keyed claims to GHL-keyed via SLACK_TO_GHL_USER
  const claimsByGhl = {};
  for (const [slackId, n] of Object.entries(claimsBySlack)) {
    const ghlId = (SLACK_TO_GHL_USER[slackId] || '').toLowerCase();
    if (ghlId) claimsByGhl[ghlId] = (claimsByGhl[ghlId] || 0) + n;
  }

  // 4. Build per-setter rows for the active setters (skip Ron — testing only)
  // Closers (Jonathan, Jose) excluded — they don't submit setter EOD reports.
  // Debbanny excluded — rolled off 2026-05-03; her historical rows still resolve via GHL_USER_NAMES
  // for ad-hoc lookups but she's not on the active leaderboard.
  const SETTER_GHL_IDS = [
    'cuttpcov7ztlvyjkhdx8', // Joseph Salazar
    'zcmdiz2eerapd80w2zop', // Oscar M
    'n8mvtuhbbby7qppqnmr7', // William B
  ];
  const rows = SETTER_GHL_IDS.map(ghlId => {
    const eod = bySetter[ghlId] || { calls_placed: 0, qualified_leads: 0, shows: 0, no_shows: 0, engagement: 0 };
    const callsAttempted = eod.shows + eod.no_shows;
    const showRate = callsAttempted > 0 ? Math.round((eod.shows / callsAttempted) * 100) : null;
    return {
      name: GHL_USER_NAMES[ghlId] || ghlId,
      leads_claimed: claimsByGhl[ghlId] || 0,
      calls_placed: eod.calls_placed,
      engagement: eod.engagement,
      qualified_leads: eod.qualified_leads,
      shows: eod.shows,
      no_shows: eod.no_shows,
      show_rate: showRate,
    };
  });

  // 5. Rank by qualified_leads desc, tiebreak on calls_placed
  rows.sort((a, b) => (b.qualified_leads - a.qualified_leads) || (b.calls_placed - a.calls_placed));
  const medals = ['🥇', '🥈', '🥉', '🏅'];

  // 6. Format Slack post
  const lines = [
    `📊 *SETTER LEADERBOARD* — ${todayLabel}`,
    `_Month-to-date for ${monthLabel}_`,
    '',
  ];
  for (let i = 0; i < rows.length; i += 1) {
    const r = rows[i];
    const showRateStr = r.show_rate === null
      ? '—'
      : `${r.show_rate}% (${r.shows} shows / ${r.shows + r.no_shows} booked)`;
    lines.push(`${medals[i] || '•'} *${r.name}*`);
    lines.push(`• Leads claimed: ${r.leads_claimed}`);
    lines.push(`• Calls placed: ${r.calls_placed}`);
    lines.push(`• Engagement actions: ${r.engagement}`);
    lines.push(`• Qualified leads: ${r.qualified_leads}`);
    lines.push(`• Show rate: ${showRateStr}`);
    lines.push('');
  }
  const leader = rows[0];
  if (leader && leader.qualified_leads > 0) {
    lines.push(`🏆 Month leader: *${leader.name}* (${leader.qualified_leads} qualified leads)`);
  } else {
    lines.push('_No qualified leads recorded yet this month._');
  }

  const text = lines.join('\n');
  await slack.client.chat.postMessage({ channel: LEAD_CHANNEL_ID, text });
  logActivity({
    event_type: 'slack_message', event_source: 'cron', action: 'setter_leaderboard',
    channel_id: LEAD_CHANNEL_ID,
    output: { text: text.slice(0, 2000), rows: rows.length, month: monthLabel },
    correlation_id: correlationId,
  });
  console.log(`runSetterLeaderboard posted to ${LEAD_CHANNEL_ID} — ${rows.length} setters, leader=${leader?.name}`);
}

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

// ─── HEALTH CHECK SERVER ──────────────────────────────────────────────────────
const healthServer = http.createServer((req, res) => {
  if (req.url === '/health' && req.method === 'GET') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ status: 'ok', agent: 'NeuroGrowth PM Agent (Max)', uptime: Math.floor(process.uptime()), timestamp: new Date().toISOString() }));
  } else if (req.url === '/webhook/ghl-lead' && req.method === 'POST') {
    handleGHLWebhook(req, res);
  } else if (req.url === '/webhook/supabase' && req.method === 'POST') {
    handleSupabaseWebhook(req, res);
  } else {
    res.writeHead(404); res.end('Not found');
  }
});
healthServer.listen(process.env.PORT || 3000, () => {
  console.log(`Health check server listening on port ${process.env.PORT || 3000}`);
});

// ─── START ────────────────────────────────────────────────────────────────────
(async () => {
  await slack.start();
  console.log('NeuroGrowth PM Agent is running.');
  await loadAndRegisterDynamicCrons();
  // Static infrastructure crons (not stored in DB)
  cron.schedule('0 7 15 * *', wrapCronJob('runPhase3Reconciliation', async (c) => { await runPhase3Reconciliation(c); }), { timezone: 'America/Costa_Rica' });
  console.log('Registered static cron: Phase 3 reconciliation (0 7 15 * *)');
})();

// ─── REACTION-DRIVEN LEAD CLAIM ───────────────────────────────────────────────
// Setter reacts on a #ng-sales-goats lead post → Max writes assignedTo in GHL,
// posts a ✅ + threaded confirmation. First reactor wins; idempotent on race.
slack.event('reaction_added', async ({ event }) => {
  try {
    if (!event || !event.item || event.item.type !== 'message') return;
    if (event.item.channel !== LEAD_CHANNEL_ID) return;
    // Strip skin-tone modifier (Slack delivers e.g. `hand::skin-tone-3`)
    const baseEmoji = String(event.reaction || '').split('::')[0];
    if (!LEAD_CLAIM_EMOJIS.has(baseEmoji)) return;
    if (event.user === process.env.SLACK_BOT_USER_ID) return;

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
      const putRes = await fetch(`https://services.leadconnectorhq.com/contacts/${meta.contact_id}`, {
        method: 'PUT', headers: ghlAuth, body: JSON.stringify({ assignedTo: ghlUserId }),
      });
      if (!putRes.ok) {
        const errBody = await putRes.text();
        throw new Error(`GHL PUT /contacts/${meta.contact_id} → ${putRes.status}: ${errBody.slice(0, 200)}`);
      }

      // 2. Reassign opportunities tied to this contact — but ONLY in setter pipelines.
      //    VSL self-bookings live in a separate pipeline and must NOT be reassigned.
      //    Allow-list comes from GHL_SETTER_PIPELINE_IDS env var (comma-separated).
      const setterPipelineIds = (process.env.GHL_SETTER_PIPELINE_IDS || 'KH1IQuaN8aNB1lfRpvP4')
        .split(',').map(s => s.trim()).filter(Boolean);
      const oppsRes = await fetch(
        `https://services.leadconnectorhq.com/opportunities/search?location_id=${meta.location_id}&contact_id=${meta.contact_id}`,
        { headers: ghlAuth },
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
          const r = await fetch(`https://services.leadconnectorhq.com/opportunities/${opp.id}`, {
            method: 'PUT', headers: ghlAuth, body: JSON.stringify({ assignedTo: ghlUserId }),
          });
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
        user_id: event.user, channel_id: channel,
        output: { contact_id: meta.contact_id, ghl_user_id: ghlUserId, full_name: meta.full_name, opps_in_setter_pipelines: opps.length, opps_reassigned: oppsOk, opps_failed: oppsFail, opps_skipped_non_setter: skippedNonSetterOpps },
        correlation_id: claimCorr,
      });

      // Audit trail — record the claim with seconds-to-claim for offline SLA mining
      try {
        const secondsToClaim = Math.max(0, Math.round(Date.now() / 1000 - parseFloat(timestamp)));
        await supabase.from('setter_claims').insert({
          ghl_contact_id: meta.contact_id,
          contact_name: meta.full_name,
          slack_message_ts: timestamp,
          slack_channel_id: channel,
          claimed_by_slack_user_id: event.user,
          claimed_by_setter_name: GHL_USER_NAMES[ghlUserId] || GHL_USER_NAMES[ghlUserId.toLowerCase()] || null,
          ghl_user_id: ghlUserId,
          opps_reassigned: oppsOk,
          seconds_to_claim: secondsToClaim,
        });
      } catch (claimErr) {
        // Non-fatal — Slack post + GHL writes already succeeded
        console.error('setter_claims insert failed:', claimErr.message);
      }

      console.log(`Lead ${meta.contact_id} (${meta.full_name}) claimed by ${event.user} → GHL user ${ghlUserId}; opps ${oppsOk}/${opps.length}`);
    } catch (apiErr) {
      console.error('Lead claim GHL write failed:', apiErr.message);
      await slack.client.chat.postMessage({
        channel, thread_ts: timestamp,
        text: `<@${event.user}> tried to claim, but GHL update failed: ${apiErr.message}. Reach out to Ron.`,
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
      client_success: `You are greeting Tania, the Client Success Operations Manager. Welcome her and let her know you can help with: client health checks, drafting client comms, checking fulfillment channel activity, contract reminders, and searching the knowledge base. Keep it to 3-4 lines max.`,
      tech_ops:       `You are greeting Josue, the Technical Operations Manager. Welcome him and let him know you can help with: client launch status, campaign blockers, fulfillment channel recaps, Notion SOPs, and his daily briefing every morning at 8:30 AM. Keep it to 3-4 lines max.`,
      tech_lead:      `You are greeting David, the Lead Technology and Automation specialist. Welcome him and let him know you can help with: systems channel activity, Make.com issue tracking, process documentation, and Notion. Keep it to 3-4 lines max.`,
      fulfillment:    `You are greeting Valeria, the Fulfillment Operations specialist. Welcome her and let her know you can help with: delivery doc status, client setup coordination, fulfillment channel recaps, and Notion. Keep it to 3-4 lines max.`,
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