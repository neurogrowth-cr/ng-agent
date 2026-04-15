require('dotenv').config();
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

// ─── SYSTEM PROMPT ────────────────────────────────────────────────────────────
const SYSTEM_PROMPT_BASE = `CRITICAL OPERATING RULES — NEVER VIOLATE THESE:

1. When asked about any Slack channel's activity, content, or discussions, you MUST call read_slack_channel immediately. No exceptions. Do not summarize from memory, do not say you lack access, do not explain — just call the tool and report what it returns. If the channel name is provided as a Slack link like <#C09TS6DUTU2|ng-fullfillment-ops>, extract the name after the pipe symbol and use that.

2. When asked about a client, team member, or ongoing situation, call search_knowledge first before answering. Do not rely on conversation history alone for operational context.

3. When Ron shares important context about a client, decision, or process, call save_knowledge to store it permanently using the correct category: client, team, process, decision, or alert.

4. Never use markdown formatting in Slack messages. No ##, no **, no ---, no tables, no bullet points unless the information genuinely requires a list. Write like a colleague sending a message, not a report.

5. When asked to reply or post in a Slack channel, always use draft_channel_post to prepare the message and show Ron the draft for approval. Never post directly to a channel unless triggered from within that channel.

---

IDENTITY & ROLE

You are Max, the Project Manager and Personal Assistant for NeuroGrowth Consulting LLC, a boutique LinkedIn growth consultancy run by CEO and Founder Ron Duarte. You operate as an embedded team member, not a generic assistant. You understand the business deeply, know every person on the team by name and function, and are responsible for keeping operations moving, Ron's attention focused on revenue-generating activities, and the team accountable to its commitments.

You communicate naturally. You are not a chatbot. You think, you have context, you have opinions. You respond the way a highly capable chief of staff would — someone who knows everything about the business and speaks plainly. Ron and you have a working relationship.

Your primary directive is to reduce Ron's operational involvement from ~40-60% of his time on execution to 20% or less.

PRIMARY POC: Ron Duarte is your only point of contact for all decisions — technical, operational, or strategic. You do not escalate to David or any other team member unless Ron explicitly asks you to.

---

THE BUSINESS

NeuroGrowth delivers Build & Release: a complete LinkedIn prospecting system (the "LinkedIn Flywheel") built in 14 days and handed off to the client as a fully owned asset. This is not a retainer model. Post-delivery retention tiers:
- OMEGA: 3-month community and support tier
- ROLEX: 6-month Done-With-You coaching tier
- PATEK: 6-month Done-For-You VIP tier

Core promise: 10-30 qualified LinkedIn calls per month with decision-makers. ICP: B2B and B2C coaches, consultants, and premium service providers. Markets: US, Costa Rica, Mexico. Full-service SDR management is no longer offered — legacy accounts are winding down.

---

THE TEAM

Ron Duarte (U05HXGX18H3) — CEO and Founder. Primary POC. Final decision-maker on clients, pricing, offers, hiring.
Tania (U07SMMDMSLQ) — Client Success Operations Manager. Client health, AR, contracts, case studies.
Josue Duran (U08ABBFNGUW) — Technical Operations Manager (full-time fulfillment). Activation calls, campaign ops, client launch sequencing.
David McKinney (U08ACUHUUP6) — Lead Technology & Automation. Portal, Make.com, Supabase. NOT a POC — do not involve unless Ron says so.
Valeria (U09Q3BXJ18B) — Fulfillment Operations. Delivery documents, Claude Projects.
Felipe (U09TNMVML3F) — Technical Campaign Specialist (part-time). Campaign launches, Prosp management.
Joseph Salazar (U0A9J00EMGD) — Appointment Setter. Books discovery calls.
Debbanny Romero (U0AR16QVDB3) — Appointment Setter. Books discovery calls.
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

DECISION ESCALATION

Escalate to Ron when:
- A client is expressing dissatisfaction or threatening to cancel
- A sales prospect requires custom pricing outside the standard structure
- A team member raises compensation, contract, or role concerns
- A technical failure affects active client campaigns unresolved in 24 hours
- Any new vendor, platform, or financial commitment above $25

Do NOT escalate for: follow-up timing, calendar scheduling, first-draft copy, routine status checks.

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

5. RON IS PRIMARY POC — All decisions, escalations, and technical questions go to Ron. Never suggest involving David unless Ron explicitly asks.

6. NO MID-CHAIN NARRATION — When answering a question requires multiple tool calls (e.g. check GHL, then check knowledge, then check Slack), do ALL of them silently and return ONE final answer. Never narrate between steps. Never say "let me check X" and then go silent. Never say "let me open it" and then stop responding.

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
- EOD reports from Joseph, Debbanny, and Jose (calls booked, pipeline updates, actions needed)
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

const SYSTEM_PROMPT = SYSTEM_PROMPT_BASE + SYSTEM_PROMPT_RULES;

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

// ─── TEAM MEMBER REGISTRY ─────────────────────────────────────────────────────
const TEAM_MEMBERS = {
  'U05HXGX18H3': { name: 'Ron',      role: 'ceo',            displayName: 'Ron Duarte' },
  'U07SMMDMSLQ': { name: 'Tania',    role: 'client_success', displayName: 'Tania'      },
  'U08ABBFNGUW': { name: 'Josue',    role: 'tech_ops',       displayName: 'Josue'      },
  'U08ACUHUUP6': { name: 'David',    role: 'tech_lead',      displayName: 'David'      },
  'U09Q3BXJ18B': { name: 'Valeria',  role: 'fulfillment',    displayName: 'Valeria'    },
  'U09TNMVML3F': { name: 'Felipe',   role: 'campaigns',      displayName: 'Felipe'     },
  'U0A9J00EMGD': { name: 'Joseph',   role: 'setter',         displayName: 'Joseph'     },
  'U0AR16QVDB3': { name: 'Debbanny', role: 'setter',         displayName: 'Debbanny'   },
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
    canReadChannels: ['ng-fullfillment-ops','ng-new-client-alerts','ng-ops-management'],
    canPostChannels: ['ng-fullfillment-ops','ng-new-client-alerts'],
    canUseEmail: false, canUseCalendar: false, canUseGHL: false,
    canUseDrive: true, canUseNotion: true, canSaveKnowledge: true, fullAccess: false,
  },
  tech_ops: {
    canReadChannels: ['ng-fullfillment-ops','ng-app-and-systems-improvents'],
    canPostChannels: ['ng-fullfillment-ops','ng-app-and-systems-improvents'],
    canUseEmail: false, canUseCalendar: false, canUseGHL: false,
    canUseDrive: true, canUseNotion: true, canSaveKnowledge: true, fullAccess: false,
  },
  tech_lead: {
    canReadChannels: ['ng-fullfillment-ops','ng-app-and-systems-improvents','ng-ops-management'],
    canPostChannels: ['ng-fullfillment-ops','ng-app-and-systems-improvents'],
    canUseEmail: false, canUseCalendar: false, canUseGHL: false,
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

function buildRoleSystemPrompt(userId) {
  const member = getMemberContext(userId);
  const perms  = getMemberPermissions(userId);
  if (perms.fullAccess) return SYSTEM_PROMPT;

  const roleContext = {
    client_success: `You are speaking with Tania, the Client Success Operations Manager at NeuroGrowth. She is the operational backbone of the business — hybrid Chief of Staff and Client Success role reporting to Ron.

Her 3 pillars:
- Executive Ops (30%): Draft and manage all contracts and SLAs, maintain contract repo with renewal dates, prepare pre-meeting research packages for Ron, own OKR tracking and sprint completion monitoring, produce weekly 5-min ops summary for Ron.
- Client Success (50%): Primary contact for all non-strategic client comms — respond within 2 hours. Bi-weekly client check-in calls (Ron handles monthly strategic sessions). Track client health scores (target >80/100 average). Monitor early warning signals (reduced responsiveness, declining campaign metrics). Identify upsell and expansion opportunities. Execute case study and testimonial SOP. Coordinate quarterly business reviews with performance data.
- Project and Team Coordination (20%): Own project tracking, coordinate with David on infrastructure, facilitate comms between SDR team and technical team, track action items across team members.

Key KPIs: 100% client retention, >80 health score average, <2hr response time, 90%+ feedback actioned within 1 week, 1 case study per quarter, CEO operational time <20%.

When Tania asks about a client, give her full health context: engagement level, last interaction, open action items, any risk signals. Help her draft client comms, check-in messages, expansion proposals, escalation summaries, and case study outreach. She cannot access Ron's Gmail, calendar, or GHL.`,

    tech_ops: `You are speaking with Josue, the Technical Operations Manager at NeuroGrowth. He reports to Ron (CEO) and is the single point of accountability for technical campaign excellence across all clients.

His role is split:
- 60% Build & Release: Own the complete 14-day launch cycle from client activation through technical deployment. Phase 1 (Days 1-3): client activation & onboarding. Phase 2 (Days 4-10): fulfillment coordination. Phase 3 (Days 11-13): technical QA. Phase 4 (Day 14): launch execution & handoff.
- 40% Full Service / Done-For-You: Monitor and optimize ongoing campaigns for full-service clients. Monday 9AM: 60-min campaign fix session. Fridays: portfolio performance deep dive (GREEN/YELLOW/RED status). Monthly audits every 30-45 days per client.

Key performance targets: 95%+ on-time launch rate within 14-day guarantee, 90%+ SLA compliance across DFY portfolio, keep CEO time on campaign ops under 5 hours/week.

After Day 14, Tania becomes primary client contact for satisfaction/admin — Josue remains owner of technical campaign performance.

When Josue asks about a client, pull from knowledge base and fulfillment channel to give him full context: current status, last action taken, what's blocking them, and what the next step is. Be direct and operational — tell him exactly what to do, not a summary. Help him draft channel updates, client comms, campaign fix plans, and escalation messages. He cannot access Ron's email, calendar, or GHL.`,

    tech_lead: `You are speaking with David, the Lead Technology and Automation specialist at NeuroGrowth. He builds and maintains Make.com scenarios, Supabase infrastructure, and the Neurogrowth Portal. Help him with technical questions, systems channel activity, and process documentation. He cannot access Ron's email, calendar, or GHL.`,

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

    closer: `You are speaking with a High-Ticket Closer at NeuroGrowth. The closers are Jose Carranza (U0AMTEKDCPN) and Jonathan Madriz (U0APYAE0999). They take booked calls from Joseph and Debbanny and close them into paying clients.

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
  return `${SYSTEM_PROMPT}\n\n---\nCURRENT USER CONTEXT:\n${baseContext}\n\nThis user can access these channels: ${channelList}\nAlways address this person by name: ${member.displayName}.\nKeep responses focused on their operational scope. Do not share sensitive business financials or information outside their role.`;
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
async function searchKnowledge(query, category = null) {
  try {
    const safeQuery = (query || '').replace(/[%_\\]/g, '\\$&').substring(0, 200);
    let q = supabase
      .from('agent_knowledge')
      .select('category, key, value, updated_at')
      .ilike('value', `%${safeQuery}%`)
      .order('updated_at', { ascending: false })
      .limit(8);
    if (category) q = q.eq('category', category);
    const { data, error } = await q;
    if (error) throw error;
    if (!data || !data.length) return `No knowledge found for: ${query}`;
    return data.map(r =>
      `[${r.category}] ${r.key}: ${r.value} (updated ${new Date(r.updated_at).toLocaleDateString()})`
    ).join('\n');
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

async function upsertKnowledge(category, key, value, source = 'agent') {
  try {
    if (containsSensitiveData(value) || containsSensitiveData(key)) {
      console.warn(`Knowledge save blocked — sensitive data detected in [${category}] ${key}`);
      return `Knowledge save skipped — sensitive data detected. This information was not stored.`;
    }
    const { error } = await supabase
      .from('agent_knowledge')
      .upsert(
        { category, key, value: value.substring(0, 2000), source, updated_at: new Date().toISOString() },
        { onConflict: 'category,key' }
      );
    if (error) throw error;
    return `Knowledge saved: [${category}] ${key}`;
  } catch (err) {
    return `Knowledge save error: ${err.message}`;
  }
}

async function getAllKnowledgeByCategory(category) {
  try {
    const { data, error } = await supabase
      .from('agent_knowledge')
      .select('key, value, updated_at')
      .eq('category', category)
      .order('updated_at', { ascending: false })
      .limit(20);
    if (error) throw error;
    if (!data || !data.length) return `No knowledge in category: ${category}`;
    return data.map(r => `${r.key}: ${r.value}`).join('\n');
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

const RON_SLACK_ID = 'U05HXGX18H3';

function requiresApproval(channel) {
  const ch = (channel || '').toLowerCase().replace('#', '');
  return APPROVAL_REQUIRED_CHANNELS.some(c => c.replace('#', '') === ch);
}

function registerDynamicCron(task) {
  try {
    if (activeDynamicCrons[task.id]) { activeDynamicCrons[task.id].stop(); }
    const job = cron.schedule(task.cron_expression, async () => {
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

      const enrichedPrompt = liveContext
        ? `${task.prompt}\n\n---\nLIVE CONTEXT (use this to inform the report):\n${liveContext}`
        : task.prompt;

      // Retry logic — up to 3 attempts with backoff for 529/503 overload errors
      let reply = null;
      let lastErr = null;
      const maxAttempts = 3;
      const retryDelays = [15000, 30000, 60000]; // 15s, 30s, 60s

      for (let attempt = 0; attempt < maxAttempts; attempt++) {
        try {
          reply = await callClaude([{ role: 'user', content: enrichedPrompt }]);
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
        'Fulfillment EOD Pulse':      ['WINS TODAY', 'DELIVERY STATUS', 'BLOCKERS', 'SLA WATCH', 'TOMORROW'],
        'Friday Delivery Wrap-Up':    ['WEEK IN REVIEW', 'CLIENT STATUS BOARD', 'TEAM WINS THIS WEEK', 'MISSES THIS WEEK', 'MONDAY PRIORITIES'],
        'Ron Weekly Ops Digest':      ['DELIVERY', 'SALES', 'WHAT NEEDS YOUR ATTENTION'],
        'Sales Call Prep Reminder':   [], // short task, skip header check
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
          ]);
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

      if (requiresApproval(targetChannel)) {
        pendingApprovals[RON_SLACK_ID] = {
          channelName: targetChannel,
          message: reply,
          createdAt: Date.now(),
        };
        await slack.client.chat.postMessage({
          channel: RON_SLACK_ID,
          text: `Draft ready for ${targetChannel} — task: ${task.name}\n\n${reply}\n\nReply "send it" to post or "cancel" to discard.`,
        });
        console.log(`Cron draft DMed to Ron for approval: "${task.name}" → ${targetChannel}`);
      } else {
        await postToSlack(targetChannel, reply);
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
    const cronResponse = await anthropic.messages.create({
      model: 'claude-sonnet-4-6',
      max_tokens: 50,
      messages: [{ role: 'user', content: cronPrompt }]
    });
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
  return events.map(e => `${e.start.dateTime || e.start.date} — ${e.summary}`).join('\n');
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
    const d     = insightData.data?.[0] || {};
    const leads = (d.actions || []).find(a => a.action_type === 'lead')?.value || '0';
    const spend = parseFloat(d.spend || 0).toFixed(2);
    const ctr   = parseFloat(d.ctr   || 0).toFixed(2);
    const cpc   = parseFloat(d.cpc   || 0).toFixed(2);
    const cpm   = parseFloat(d.cpm   || 0).toFixed(2);
    const cpl   = leads > 0 ? (parseFloat(spend) / parseInt(leads)).toFixed(2) : 'N/A';
    return [
      `Meta Ads — ${datePreset.replace(/_/g,' ')}:`,
      `Spend: $${spend} | Impressions: ${parseInt(d.impressions||0).toLocaleString()} | Reach: ${parseInt(d.reach||0).toLocaleString()}`,
      `Clicks: ${parseInt(d.clicks||0).toLocaleString()} | CTR: ${ctr}% | CPC: $${cpc} | CPM: $${cpm}`,
      leads !== '0' ? `Leads: ${leads} | CPL: $${cpl}` : 'No lead conversions tracked in this period.',
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
    if (!alerts.length) return '✅ No critical alerts. Some clients still in onboarding phases but within timeline.';
    return `Launch & block alerts (${alerts.length} clients):\n\n` + alerts.join('\n');
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
  '5orsahkh2joujb5fczrp': 'Debbanny Romero',  '5OrSaHkh2joUjB5FCZrP': 'Debbanny Romero',

  // ── CLOSERS — iClosed identifies hosts by email address ─────────────────
  'ronny.duarte@neurogrowth.io':  'Ron Duarte',
  'jose.neurogrowth@gmail.com':   'Jose Carranza',
  'jonathan.madriz.neurogrowth@gmail.com': 'Jonathan Madriz',

  // ── SETTERS — iClosed EOD email IDs ─────────────────────────────────────
  'debbanny.neurogrowth@gmail.com': 'Debbanny Romero',
  'joseph.neurogrowth@gmail.com':   'Joseph Salazar',
  'Salazcamjos@gmail.com':          'Joseph Salazar',

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
    if (q.includes('setter') || q.includes('joseph') || q.includes('debbanny') || q.includes('booked') || q.includes('conversations') || q.includes('qualified leads')) {
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
      '5orsahkh2joujb5fczrp': 'Debbanny Romero', '5OrSaHkh2joUjB5FCZrP': 'Debbanny Romero',
      'gqymykpddltdxvbkfl2c': 'Jonathan Madriz', 'gqYMYkpDDlTdxvBkfl2C': 'Jonathan Madriz',
      'izlta0jy5orkymsyltjv': 'Jose Carranza', 'izLTA0jy5OrKyMvyltjV': 'Jose Carranza',
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

// ─── PORTAL: WEEKLY TREND ANALYSIS ───────────────────────────────────────────
async function runWeeklyPortalTrends() {
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
async function runMondayGapDetection() {
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
      if (dash.customer_status === 'blocked') {
        const blockedTitles = acts.filter(a=>a.status==='blocked').map(a=>tMap[a.template_id]||'Unknown').join(', ');
        gaps.push(`🔴 BLOCKED — ${dash.client_name} (Day ${daysSince}): ${blockedTitles}`);
      } else if (daysSince >= 14) {
        gaps.push(`🔴 OVERDUE — ${dash.client_name} still in ${dash.customer_status} at Day ${daysSince} (past 14-day window)`);
      } else if (daysSince >= 7 && staleActs.length > 0) {
        const assignees = [...new Set(staleActs.map(a=>(a.assigned_to||'').split('@')[0]))].join(', ');
        gaps.push(`🟡 STALE — ${dash.client_name} (Day ${daysSince}): ${staleActs.length} activities with no update in 72hrs. Assigned to: ${assignees}`);
      }
    }
    if (!gaps.length) { console.log('Gap detection: no critical gaps found.'); return; }
    const today   = new Date().toLocaleDateString('en-US', { weekday:'long', month:'long', day:'numeric', timeZone:'America/Costa_Rica' });
    const message = `Good morning team. Here's your Monday delivery gap report for ${today}:\n\n${gaps.join('\n')}\n\nTag the responsible team member and confirm resolution by EOD.`;
    // Route through Ron for approval before posting to team channel
    pendingApprovals[RON_SLACK_ID] = {
      channelName: OPS_CHANNEL,
      message,
      createdAt: Date.now(),
    };
    await slack.client.chat.postMessage({
      channel: RON_SLACK_ID,
      text: `Draft ready for ${OPS_CHANNEL} — Monday Gap Detection\n\n${message}\n\nReply "send it" to post or "cancel" to discard.`,
    });
    console.log(`Gap detection: ${gaps.length} gap(s) DMed to Ron for approval.`);
  } catch (err) { console.error('Gap detection error:', err.message); }
}

// ─── NIGHTLY LEARNING ─────────────────────────────────────────────────────────
async function runNightlyLearning() {
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
    const response = await anthropic.messages.create({ model: 'claude-sonnet-4-6', max_tokens: 1024, messages: [{ role: 'user', content: learningPrompt }] });
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
        if (category && VALID_CATEGORIES.has(category) && key && value) { await upsertKnowledge(category, key, value, 'nightly-learning'); saved++; }
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
async function runProactiveAlerts() {
  console.log('Running proactive alert check...');
  try {
    const { data, error } = await supabase.from('agent_knowledge').select('key, value, updated_at').eq('category', 'alert').order('updated_at', { ascending: true });
    if (error || !data || !data.length) return;
    const now      = Date.now();
    const oneDayMs = 24 * 60 * 60 * 1000;
    const staleAlerts = data.filter(a => (now - new Date(a.updated_at).getTime()) > oneDayMs);
    if (!staleAlerts.length) return;
    const alertText = staleAlerts.map(a => `${a.key}: ${a.value}`).join('\n\n');
    const prompt    = `You are the NeuroGrowth PM agent checking on unresolved alerts.\n\nThese items have been flagged as alerts and have not been updated in over 24 hours:\n\n${alertText}\n\nWrite a brief, direct message to Ron (2-4 sentences) summarizing what is still unresolved and what needs his attention today. No markdown formatting. Sound like a colleague, not a report.`;
    const response  = await anthropic.messages.create({ model: 'claude-sonnet-4-6', max_tokens: 256, messages: [{ role: 'user', content: prompt }] });
    const message   = response.content.filter(b => b.type === 'text').map(b => b.text).join('\n');
    await postToSlack(AGENT_CHANNEL, message);
    console.log(`Proactive alert posted. ${staleAlerts.length} unresolved items flagged.`);
  } catch (err) { console.error('Proactive alert error:', err.message); }
}

// ─── PROACTIVE TEAM DMs ───────────────────────────────────────────────────────
// Runs nightly. Checks portal for clients hitting critical milestones tomorrow
// and DMs the responsible team member before they even have to ask.
async function runProactiveDMs() {
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

    // ── DM Josue: clients hitting Day 14 tomorrow ──
    if (hitting14Tomorrow.length > 0) {
      const names = hitting14Tomorrow.map(d => `${d.client_name} (Day 13)`).join(', ');
      const msg = `Heads up — ${hitting14Tomorrow.length === 1 ? 'this client hits' : 'these clients hit'} their 14-day launch deadline tomorrow: ${names}. If campaigns are not live by end of day tomorrow, we miss the SLA. What needs to happen tonight or first thing tomorrow to make sure they launch on time?`;
      await slack.client.chat.postMessage({ channel: 'U08ABBFNGUW', text: msg });
      console.log(`Proactive DM sent to Josue: ${hitting14Tomorrow.length} client(s) hitting Day 14 tomorrow`);
    }

    // ── DM Josue: clients hitting at-risk threshold tomorrow ──
    if (hitting7Tomorrow.length > 0) {
      const names = hitting7Tomorrow.map(d => `${d.client_name} (Day 6)`).join(', ');
      const msg = `Quick flag — ${hitting7Tomorrow.length === 1 ? 'this client hits' : 'these clients hit'} Day 7 tomorrow, which is the at-risk threshold: ${names}. Worth checking their progress today so we're not scrambling next week.`;
      await slack.client.chat.postMessage({ channel: 'U08ABBFNGUW', text: msg });
      console.log(`Proactive DM sent to Josue: ${hitting7Tomorrow.length} client(s) hitting Day 7 tomorrow`);
    }

    // ── DM Valeria: clients stalled in phase_1 ──
    if (stalledPhase1.length > 0) {
      const names = stalledPhase1.map(d => `${d.client_name} (Day ${Math.floor((now - new Date(d.created_at).getTime()) / (1000*60*60*24))})`).join(', ');
      const msg = `These clients are still in Phase 1 and have been for a while: ${names}. If any delivery documents are pending on your end, this is the priority. Let Josue know if you're blocked on anything.`;
      await slack.client.chat.postMessage({ channel: 'U09Q3BXJ18B', text: msg });
      console.log(`Proactive DM sent to Valeria: ${stalledPhase1.length} client(s) stalled in Phase 1`);
    }

    // ── DM Felipe: clients stalled in phase_2 ──
    if (stalledPhase2.length > 0) {
      const names = stalledPhase2.map(d => `${d.client_name} (Day ${Math.floor((now - new Date(d.created_at).getTime()) / (1000*60*60*24))})`).join(', ');
      const msg = `These clients are still in Phase 2 and haven't moved in a few days: ${names}. If campaign config or Prosp setup is pending on your end, these need to be the first thing tomorrow. Flag Josue if anything is blocked.`;
      await slack.client.chat.postMessage({ channel: 'U09TNMVML3F', text: msg });
      console.log(`Proactive DM sent to Felipe: ${stalledPhase2.length} client(s) stalled in Phase 2`);
    }

    // ── DM Tania: blocked clients ──
    if (blocked.length > 0) {
      const names = blocked.map(d => `${d.client_name}`).join(', ');
      const msg = `These clients are currently blocked: ${names}. If the block is on the client side — missing onboarding form, unresponsive, contract issue — this needs a proactive outreach before it becomes a bigger problem. Can you check what's needed and follow up?`;
      await slack.client.chat.postMessage({ channel: 'U07SMMDMSLQ', text: msg });
      console.log(`Proactive DM sent to Tania: ${blocked.length} blocked client(s)`);
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
      await slack.client.chat.postMessage({ channel: 'U07SMMDMSLQ', text: msg });
      console.log(`Proactive DM sent to Tania: ${hitting20InStabilization.length} client(s) at Day 20 stabilization`);
    }

    if (approaching20InStabilization.length > 0) {
      const names = approaching20InStabilization.map(d => d.client_name).join(', ');
      const msg = `Heads up — these clients hit Day 20 in stabilization in 2 days: ${names}. Start preparing the 1:1 progress check outreach so it's ready to go on Day 20.`;
      await slack.client.chat.postMessage({ channel: 'U07SMMDMSLQ', text: msg });
      console.log(`Proactive DM sent to Tania: ${approaching20InStabilization.length} client(s) approaching Day 20 stabilization`);
    }

    console.log('Proactive team DMs complete.');
  } catch (err) {
    console.error('Proactive DM error:', err.message);
  }
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

async function processFileWithClaude(fileBuffer, mimeType, userInstruction, systemPrompt) {
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
  const response = await anthropic.messages.create({
    model: 'claude-sonnet-4-6', max_tokens: 1024, system: systemPrompt,
    messages: [{ role: 'user', content: [contentBlock, { type: 'text', text: userInstruction || 'Analyze this file and provide a useful summary. Extract any action items, key information, or insights relevant to NeuroGrowth operations.' }] }],
  });
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
async function callClaude(messages, retries = 3, userId = null) {
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
          { name: 'draft_channel_post',   description: "Prepare a Slack channel post for Ron's approval before sending.",                        input_schema: { type: 'object', properties: { channelName: { type: 'string' }, message: { type: 'string' } }, required: ['channelName','message'] } },
          { name: 'get_ghl_conversations',description: 'Get recent GHL conversations — prospects and contacts across all channels. Each conversation includes the assigned setter name (or "unassigned" if no owner is set — that is a valid complete answer, not an error). Use this to answer questions about which setter is working a prospect, or whether a prospect is unassigned.',                                                                                                                        input_schema: { type: 'object', properties: { limit: { type: 'number', description: 'Number of conversations to pull, default 20' }, unreadOnly: { type: 'boolean', description: 'Set true to only show unread conversations' } } } },
          { name: 'search_knowledge',     description: "Search the agent's long-term knowledge base for accumulated intelligence about clients, team, processes, and decisions.", input_schema: { type: 'object', properties: { query: { type: 'string' }, category: { type: 'string', description: 'Optional: client, team, process, decision, alert, intel' } }, required: ['query'] } },
          { name: 'save_knowledge',       description: 'Save an important insight to long-term memory. Use when Ron shares important context or when a pattern emerges.',  input_schema: { type: 'object', properties: { category: { type: 'string', description: 'client, team, process, decision, alert, or intel' }, key: { type: 'string', description: 'Short identifier e.g. Max Valverde or onboarding bottleneck' }, value: { type: 'string', description: 'The knowledge to store' } }, required: ['category','key','value'] } },
          { name: 'get_knowledge_category',description: 'Get all knowledge entries for a specific category.',                                    input_schema: { type: 'object', properties: { category: { type: 'string', description: 'client, team, process, decision, alert, or intel' } }, required: ['category'] } },
          { name: 'get_client_status',    description: 'ALWAYS use this tool (NOT Notion) when asked about client onboarding status, client phases, portal status, where a client is in their onboarding, what activities are pending, or what clients are in the system. Queries live Supabase portal database directly.',           input_schema: { type: 'object', properties: { clientName: { type: 'string', description: 'Optional client name to search for. Leave empty to get all clients.' } } } },
          { name: 'get_portal_alerts',    description: 'ALWAYS use this tool (NOT Notion) when asked about launch risks, clients behind on their 14-day window, overdue clients, or who needs attention in fulfillment. Queries live Supabase portal data.',                                                                            input_schema: { type: 'object', properties: {} } },
          { name: 'get_sales_intelligence', description: 'Query iClosed and RevOps sales data from Supabase. Use for: closer performance (Jonathan, Jose — scheduled calls, cancellations, no-shows, qualified calls, closes, close rate from revops_closer_eod_daily), setter performance (Joseph, Debbanny — new conversations, qualified leads, calls booked from revops_setter_eod_daily — NOTE: setter data comes from GHL EOD reports not iClosed), today\'s calls, prospect lookup by name, pipeline summary. Setter assignment on individual calls is not available from iClosed — direct setter questions to GHL conversations.', input_schema: { type: 'object', properties: { query: { type: 'string', description: 'Natural language query e.g. who booked the Andres Chavez call, how many calls today, close rate this month, Joseph bookings this week' } }, required: ['query'] } },
          { name: 'create_notion_task',   description: 'Create a task in NeuroGrowth Notion. Operational/recurring tasks go to Operations Tracking. Project/strategic tasks go to Project Sprint Tracking.',                                                                                                                               input_schema: { type: 'object', properties: { title: { type: 'string' }, taskType: { type: 'string', description: 'operational (default) or project' }, priority: { type: 'string', description: 'P0 - Critical Customer Impact | P1 - High Business Impact | P2 - Growth & Scalability (default) | P3 - Strategic Initiatives' }, dueDate: { type: 'string', description: 'YYYY-MM-DD format (optional)' }, notes: { type: 'string', description: 'Additional context (optional)' }, customer: { type: 'string', description: 'Customer name (optional)' } }, required: ['title'] } },
          { name: 'create_scheduled_task',description: 'Create a new recurring scheduled task that Max will run automatically.',                  input_schema: { type: 'object', properties: { name: { type: 'string', description: 'Short name for the task' }, schedule: { type: 'string', description: 'Natural language schedule e.g. every Monday at 9am' }, prompt: { type: 'string', description: 'The instruction Max will execute at each scheduled run' }, channel: { type: 'string', description: 'Slack channel to post results to' } }, required: ['name','schedule','prompt'] } },
          { name: 'list_scheduled_tasks', description: 'List all scheduled tasks Max is currently running.',                                     input_schema: { type: 'object', properties: {} } },
          { name: 'clean_duplicate_tasks',description: 'Find and hard-delete duplicate scheduled tasks. Queries ALL rows including inactive. Keeps oldest clean-named version of each task.',                                                                                                                                               input_schema: { type: 'object', properties: {} } },
          { name: 'delete_scheduled_task',description: 'Deactivate and stop a scheduled task by its ID.',                                        input_schema: { type: 'object', properties: { taskId: { type: 'string', description: 'The task ID from list_scheduled_tasks' } }, required: ['taskId'] } },
          { name: 'get_meta_ads_summary', description: 'Get NeuroGrowth Meta Ads account-level performance summary — spend, impressions, reach, clicks, CTR, CPC, CPM, leads, and CPL.', input_schema: { type: 'object', properties: { datePreset: { type: 'string', description: 'Date range: today, yesterday, last_7d (default), last_14d, last_30d, last_month, this_month, this_quarter' } } } },
          { name: 'get_meta_campaigns',   description: 'Get Meta Ads campaign-level breakdown.',                                                  input_schema: { type: 'object', properties: { datePreset: { type: 'string', description: 'last_7d (default), last_14d, last_30d, this_month' }, limit: { type: 'number', description: 'Number of campaigns, default 10' } } } },
          { name: 'get_meta_adsets',      description: 'Get Meta Ads ad set level breakdown.',                                                    input_schema: { type: 'object', properties: { campaignId: { type: 'string', description: 'Optional campaign ID filter' }, datePreset: { type: 'string', description: 'last_7d (default), last_14d, last_30d, this_month' } } } },
          { name: 'get_meta_ads',         description: 'Get individual ad-level performance.',                                                    input_schema: { type: 'object', properties: { adSetId: { type: 'string', description: 'Optional ad set ID filter' }, datePreset: { type: 'string', description: 'last_7d (default), last_14d, last_30d, this_month' } } } },
      ];

      // ── Tool dispatcher — shared across initial and all follow-up rounds ──────
      async function dispatchTool(toolUse) {
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
        else if (toolUse.name === 'draft_channel_post')     result = `APPROVAL_NEEDED|${toolUse.input.channelName}|${toolUse.input.message}`;
        else if (toolUse.name === 'get_ghl_conversations')  result = await getGHLConversations(toolUse.input.limit || 20, toolUse.input.unreadOnly || false);
        else if (toolUse.name === 'search_knowledge')       result = await searchKnowledge(toolUse.input.query, toolUse.input.category);
        else if (toolUse.name === 'save_knowledge')         result = await upsertKnowledge(toolUse.input.category, toolUse.input.key, toolUse.input.value, 'conversation');
        else if (toolUse.name === 'get_knowledge_category') result = await getAllKnowledgeByCategory(toolUse.input.category);
        else if (toolUse.name === 'get_client_status')      result = await getClientStatus(toolUse.input.clientName || null);
        else if (toolUse.name === 'get_portal_alerts')      result = await getPortalAlerts();
        else if (toolUse.name === 'get_sales_intelligence') result = await getSalesIntelligence(toolUse.input.query);
        else if (toolUse.name === 'create_notion_task')     result = await createNotionTask(toolUse.input.title, toolUse.input.taskType || 'operational', toolUse.input.priority || 'P2 - Growth & Scalability', toolUse.input.dueDate, toolUse.input.notes, toolUse.input.customer);
        else if (toolUse.name === 'create_scheduled_task')  result = await createScheduledTask(toolUse.input.name, toolUse.input.schedule, toolUse.input.prompt, toolUse.input.channel, userId);
        else if (toolUse.name === 'list_scheduled_tasks')   result = await listScheduledTasks();
        else if (toolUse.name === 'clean_duplicate_tasks')  result = await cleanDuplicateTasks();
        else if (toolUse.name === 'delete_scheduled_task')  result = await deleteScheduledTask(toolUse.input.taskId);
        else if (toolUse.name === 'get_meta_ads_summary')   result = await getMetaAdsSummary(toolUse.input.datePreset || 'last_7d');
        else if (toolUse.name === 'get_meta_campaigns')     result = await getMetaCampaigns(toolUse.input.datePreset || 'last_7d', toolUse.input.limit || 10);
        else if (toolUse.name === 'get_meta_adsets')        result = await getMetaAdSets(toolUse.input.campaignId || null, toolUse.input.datePreset || 'last_7d');
        else if (toolUse.name === 'get_meta_ads')           result = await getMetaAds(toolUse.input.adSetId || null, toolUse.input.datePreset || 'last_7d');
        return { type: 'tool_result', tool_use_id: toolUse.id, content: JSON.stringify(result) };
      }

      // ── Initial call ─────────────────────────────────────────────────────────
      let response = await anthropic.messages.create({
        model: 'claude-sonnet-4-6',
        max_tokens: 2048,
        system: fullSystemPrompt,
        messages,
        tools: TOOLS,
      });

      // ── Multi-round tool loop (max 5 rounds to prevent infinite chains) ──────
      const MAX_TOOL_ROUNDS = 5;
      let currentMessages = [...messages];

      for (let round = 0; round < MAX_TOOL_ROUNDS; round++) {
        if (response.stop_reason !== 'tool_use') break;

        const toolUses = response.content.filter(b => b.type === 'tool_use');
        const toolResults = await Promise.all(toolUses.map(async (toolUse) => {
          try { return await dispatchTool(toolUse); }
          catch (err) { return { type: 'tool_result', tool_use_id: toolUse.id, content: JSON.stringify(`Error running tool ${toolUse.name}: ${err.message}`) }; }
        }));

        // Check for approval draft before continuing
        const draftResult = toolResults.find(r => { try { return JSON.parse(r.content).startsWith('APPROVAL_NEEDED|'); } catch { return false; } });
        if (draftResult) {
          const parts = JSON.parse(draftResult.content).split('|');
          return `APPROVAL_NEEDED|${parts[1]}|${parts.slice(2).join('|')}`;
        }

        // Advance message chain and call Claude again with same fullSystemPrompt (preserves time context)
        currentMessages = [...currentMessages, { role: 'assistant', content: response.content }, { role: 'user', content: toolResults }];

        let nextResponse = null;
        for (let fuAttempt = 0; fuAttempt < 3; fuAttempt++) {
          try {
            nextResponse = await anthropic.messages.create({
              model: 'claude-sonnet-4-6',
              max_tokens: 1024,
              system: fullSystemPrompt,
              messages: currentMessages,
              tools: TOOLS,
            });
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

async function executeChannelPost(channelName, message, say) {
  try {
    const channels = await getCachedChannelList();
    const channel  = channels.find(c => c.name === channelName.replace('#', ''));
    if (!channel) { await say(`Could not find channel ${channelName}.`); }
    else { await slack.client.chat.postMessage({ channel: channel.id, text: message }); await say(`Posted to ${channelName}.`); }
  } catch (err) { await say(`Something went wrong posting: ${err.message}`); }
}

async function checkApproval(message, say, userId) {
  const pending = pendingApprovals[userId];
  if (!pending) return false;
  const text = (typeof message === 'string' ? message : message.text || '').toLowerCase().trim();
  if (['yes','send it','approved','go ahead','👍'].includes(text)) {
    await executeChannelPost(pending.channelName, pending.message, say);
    delete pendingApprovals[userId];
    return true;
  }
  if (['no','cancel','stop'].includes(text)) {
    await say('Cancelled. Nothing was posted.');
    delete pendingApprovals[userId];
    return true;
  }
  return false;
}

function handleDraftReply(reply, userId, say) {
  if (!reply.startsWith('APPROVAL_NEEDED|')) return false;
  const parts       = reply.split('|');
  const channelName = parts[1];
  const draftMessage = parts.slice(2).join('|');
  pendingApprovals[userId] = { channelName, message: draftMessage, createdAt: Date.now() };
  say(`Here is what I would post to *${channelName}*:\n\n"${draftMessage}"\n\nReply *yes* to send it or *no* to cancel.`);
  return true;
}

// ─── SHARED FILE HANDLER ──────────────────────────────────────────────────────
async function handleFileMessage(message, say, userId, threadReply = false) {
  const file        = message.files[0];
  const instruction = message.text || null;
  const mimeType    = getFileMimeType(file.name, file.mimetype);

  if (isAudioFile(mimeType, file.name)) {
    const ack = threadReply ? { text: '🎙️ Got the voice note. Transcribing...', thread_ts: message.thread_ts || message.ts } : '🎙️ Got the voice note. Transcribing...';
    await say(ack);
    try {
      const fileBuffer = await downloadSlackFile(file.url_private);
      const transcript = await transcribeAudio(fileBuffer, file.name);
      if (!transcript || !transcript.trim()) {
        const errMsg = "Couldn't make out anything in that audio. Try again?";
        await say(threadReply ? { text: errMsg, thread_ts: message.thread_ts || message.ts } : errMsg);
        return;
      }
      console.log(`Audio transcribed (${file.name}): ${transcript.substring(0, 100)}...`);
      const transcriptNotice = `_Transcript:_ "${transcript.substring(0, 200)}${transcript.length > 200 ? '...' : ''}"`;
      await say(threadReply ? { text: transcriptNotice, thread_ts: message.thread_ts || message.ts } : transcriptNotice);
      const history = await loadHistory(userId);
      history.push({ role: 'user', content: `[Voice note transcript]: ${transcript}` });
      let reply = await callClaude(history, 3, userId);
      if (!reply || !reply.trim()) reply = await callClaude(history, 2, userId);
      if (!reply || !reply.trim()) return;
      if (handleDraftReply(reply, userId, say)) return;
      await saveMessage(userId, 'user', `[Voice note]: ${transcript}`);
      await saveMessage(userId, 'assistant', reply);
      await say(threadReply ? { text: reply, thread_ts: message.thread_ts || message.ts } : reply);
    } catch (err) {
      console.error('Audio processing error:', err);
      const errMsg = `Had trouble with that audio — ${err.message}`;
      await say(threadReply ? { text: errMsg, thread_ts: message.thread_ts || message.ts } : errMsg);
    }
    return;
  }

  const supported = ['application/pdf','image/png','image/jpeg','image/gif','image/webp'];
  if (!supported.includes(mimeType)) {
    const errMsg = `I can process images (PNG, JPG, GIF, WEBP), PDFs, and audio files. This file type (${mimeType}) isn't supported yet.`;
    await say(threadReply ? { text: errMsg, thread_ts: message.thread_ts || message.ts } : errMsg);
    return;
  }
  const ackMsg = `Got the ${mimeType.includes('pdf') ? 'PDF' : 'image'}. Give me a moment to analyze it...`;
  await say(threadReply ? { text: ackMsg, thread_ts: message.thread_ts || message.ts } : ackMsg);
  try {
    const fileBuffer = await downloadSlackFile(file.url_private);
    const result     = await processFileWithClaude(fileBuffer, mimeType, instruction, buildRoleSystemPrompt(userId));
    await saveMessage(userId, 'user', `[File: ${file.name}] ${instruction || 'analyze this'}`);
    await saveMessage(userId, 'assistant', result);
    await say(threadReply ? { text: result, thread_ts: message.thread_ts || message.ts } : result);
  } catch (err) {
    console.error('File processing error:', err);
    const errMsg = `Had trouble processing that file — ${err.message}`;
    await say(threadReply ? { text: errMsg, thread_ts: message.thread_ts || message.ts } : errMsg);
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
  if (message.subtype === 'file_share' && message.files?.length > 0) { await handleFileMessage(message, say, userId, false); return; }
  if (message.subtype) return;
  if (isRateLimited(userId)) { await say('Slow down a bit — you are sending messages too fast. Give me a moment.'); return; }
  const history = await loadHistory(userId);
  history.push({ role: 'user', content: message.text });
  try {
    let reply = await callClaude(history, 3, userId);
    if (!reply || !reply.trim()) { console.error('Empty reply, retrying for user:', userId); reply = await callClaude(history, 2, userId); }
    if (!reply || !reply.trim()) return;
    if (handleDraftReply(reply, userId, say)) return;
    await saveMessage(userId, 'user', message.text);
    await saveMessage(userId, 'assistant', reply);
    await say(reply);
  } catch (err) { console.error('Claude API error (DM):', err); await say('Got turned around for a second — go ahead and ask again.'); }
});

// @mention handler
slack.event('app_mention', async ({ event, say }) => {
  if (event.bot_id) return;
  const cleanText = event.text.replace(/<@[A-Z0-9]+>/g, '').trim();
  if (!cleanText) return;
  const userId = event.user;

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
      }
    } catch (threadErr) {
      console.error('Thread context fetch error:', threadErr.message);
    }
  }

  const history = await loadHistory(userId);
  const fullMessage = threadContext ? `${threadContext}\n\nMY TASK (what I was just tagged to do): ${cleanText}` : cleanText;
  history.push({ role: 'user', content: fullMessage });
  try {
    let reply = await callClaude(history, 3, userId);
    if (!reply || !reply.trim()) { console.error('Empty reply on mention, retrying for user:', userId); reply = await callClaude(history, 2, userId); }
    if (!reply || !reply.trim()) return;
    if (handleDraftReply(reply, userId, say)) return;
    await saveMessage(userId, 'user', cleanText);
    await saveMessage(userId, 'assistant', reply);
    await say({ text: reply, thread_ts: event.thread_ts || event.ts });
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
  if (message.subtype === 'file_share' && message.files?.length > 0) { await handleFileMessage(message, say, userId, true); return; }
  if (message.subtype) return;
  if (isRateLimited(userId)) { await say({ text: 'Slow down a bit — too many messages at once. Give me a moment.', thread_ts: message.thread_ts || message.ts }); return; }
  const history = await loadHistory(userId);
  history.push({ role: 'user', content: message.text });
  try {
    let reply = await callClaude(history, 3, userId);
    if (!reply || !reply.trim()) { console.error('Empty reply on channel, retrying for user:', userId); reply = await callClaude(history, 2, userId); }
    if (!reply || !reply.trim()) return;
    if (handleDraftReply(reply, userId, say)) return;
    await saveMessage(userId, 'user', message.text);
    await saveMessage(userId, 'assistant', reply);
    await say({ text: reply, thread_ts: message.thread_ts || message.ts });
  } catch (err) { console.error('Claude API error (channel):', err); }
});

// ─── CRON JOBS ────────────────────────────────────────────────────────────────
// All scheduled jobs run as dynamic tasks loaded from Supabase scheduled_tasks table.
// Internal system functions (nightly learning, portal trends, gap detection, proactive alerts)
// are still wired to their schedules below — these are infrastructure-level and not
// configurable via Slack, so they stay hardcoded.

// Nightly learning — 11:30 PM CR (infrastructure — reads all channels, saves knowledge)
cron.schedule('30 5 * * *',  async () => { await runNightlyLearning(); },     { timezone: 'America/Costa_Rica' });

// Weekly portal trend analysis — Friday 4:30 PM CR (infrastructure — saves intel to knowledge base)
cron.schedule('30 22 * * 5', async () => { await runWeeklyPortalTrends(); },  { timezone: 'America/Costa_Rica' });

// Monday gap detection — 8:00 AM CR (infrastructure — posts to ops channel)
cron.schedule('0 14 * * 1',  async () => { await runMondayGapDetection(); },  { timezone: 'America/Costa_Rica' });

// Proactive alerts — 9:00 AM and 2:00 PM CR (infrastructure — posts stale alerts to agent channel)
cron.schedule('0 15 * * *',  async () => { await runProactiveAlerts(); },     { timezone: 'America/Costa_Rica' });
cron.schedule('0 20 * * *',  async () => { await runProactiveAlerts(); },     { timezone: 'America/Costa_Rica' });

// Proactive team DMs — 8:00 PM CR weekdays (infrastructure — DMs Josue, Valeria, Felipe, Tania based on client status)
cron.schedule('0 2 * * 2-6', async () => { await runProactiveDMs(); },        { timezone: 'America/Costa_Rica' });

// ─── GHL LEAD WEBHOOK ─────────────────────────────────────────────────────────
const GHL_USER_NAMES = {
  'cuttpcov7ztlvyjkhdx8': 'Joseph Salazar', 'cUTTPGov7ZTLvyjKHdX8': 'Joseph Salazar',
  '5orsahkh2joujb5fczrp': 'Debbanny',       '5OrSaHkh2joUjB5FCZrP': 'Debbanny',
  'gqymykpddltdxvbkfl2c': 'Jonathan Madriz', 'gqYMYkpDDlTdxvBkfl2C': 'Jonathan Madriz',
  'izlta0jy5orkymsyltjv': 'Jose Carranza',  'izLTA0jy5OrKyMvyltjV': 'Jose Carranza',
};

const GHL_TO_SLACK = {
  'joseph': 'U0A9J00EMGD', 'joseph salazar': 'U0A9J00EMGD',
  'debbanny': 'U0AR16QVDB3', 'debanny': 'U0AR16QVDB3', 'debbanny neurogrowth': 'U0AR16QVDB3', 'debbanny romero': 'U0AR16QVDB3',
  'jonnathan': 'U0APYAE0999', 'jonathan': 'U0APYAE0999', 'jonathan madriz': 'U0APYAE0999',
  'jose': 'U0AMTEKDCPN', 'jose carranza': 'U0AMTEKDCPN',
  'cuttpcov7ztlvyjkhdx8': 'U0A9J00EMGD', '5orsahkh2joujb5fczrp': 'U0AR16QVDB3',
  'gqymykpddltdxvbkfl2c': 'U0APYAE0999', 'izlta0jy5orkymsyltjv': 'U0AMTEKDCPN',
};

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
        const prompt = `You are Max, the NeuroGrowth PM Agent. A new lead just came in and was assigned to a setter.\n\nLead details:\n- Name: ${fullName}\n- Email: ${email || 'not provided'}\n- Phone: ${phone || 'not provided'}\n- Source: ${source}\n- Assigned to: ${resolvedAssignedTo || 'unassigned'}\n- GHL link: ${ghlLink}\n\nWrite a short, direct Slack DM to the setter (2-3 sentences max) telling them: 1. A new lead came in and was assigned to them. 2. Key lead details. 3. Their first action (reach out now, check GHL). Sound like a colleague, not a bot. No markdown. Include the GHL link.`;
        const briefingResponse = await anthropic.messages.create({ model: 'claude-sonnet-4-6', max_tokens: 300, messages: [{ role: 'user', content: prompt }] });
        const briefing = briefingResponse.content.filter(b => b.type === 'text').map(b => b.text).join('');
        if (!briefing || !briefing.trim()) { console.error('GHL webhook: empty briefing from Claude'); return; }

        if (setterSlackId) {
          await slack.client.chat.postMessage({ channel: setterSlackId, text: briefing });
          console.log(`GHL lead briefing sent to setter ${assignedTo} (${setterSlackId})`);
        } else {
          console.log(`GHL lead received but setter not resolved. assignedTo: "${assignedTo}". Add to GHL_TO_SLACK map if needed.`);
        }

        const channelNote = [
          `🆕 *New Lead* — ${fullName}`,
          email             ? `📧 ${email}`   : null,
          phone             ? `📱 ${phone}`   : null,
          source && source !== 'Unknown channel' ? `📌 Source: ${source}` : null,
          resolvedAssignedTo ? `👤 Assigned to: ${resolvedAssignedTo}` : null,
          contactId          ? `🔗 ${ghlLink}` : null,
        ].filter(Boolean).join('\n');
        await slack.client.chat.postMessage({ channel: 'C0AJANQBYUE', text: channelNote });
      } catch (parseErr) { console.error('GHL webhook parse error:', parseErr.message); }
    });
  } catch (err) { console.error('GHL webhook handler error:', err.message); res.writeHead(500); res.end('error'); }
}

// ─── HEALTH CHECK SERVER ──────────────────────────────────────────────────────
const healthServer = http.createServer((req, res) => {
  if (req.url === '/health' && req.method === 'GET') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ status: 'ok', agent: 'NeuroGrowth PM Agent (Max)', uptime: Math.floor(process.uptime()), timestamp: new Date().toISOString() }));
  } else if (req.url === '/webhook/ghl-lead' && req.method === 'POST') {
    handleGHLWebhook(req, res);
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
})();

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
    const greeting  = await callClaude([{ role: 'user', content: prompt }]);
    if (!greeting || !greeting.trim()) return;
    await slack.client.chat.postMessage({ channel: event.channel, text: greeting });
    console.log(`Greeted ${member.displayName} (${member.role}) in #ng-pm-agent`);
  } catch (err) { console.error('member_joined_channel error:', err.message); }
});