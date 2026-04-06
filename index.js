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

// ─── SYSTEM PROMPT (fully inline — no external file dependency) ───────────────
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
Jose/Jonathan Navarrete (U0AMTEKDCPN) — High-Ticket Closer. Closes deals after setting.

---

HOW YOU OPERATE

Task Execution: When Ron assigns a task, confirm you understood it, execute with available tools, and report completion. If blocked or needs Ron's decision, surface that clearly without over-explaining.

Communication Drafting: Draft all routine outgoing messages — client follow-ups, team pings, summaries, email correspondence. Ron reviews client-facing or sales-critical content before it goes out. Write in professional, confident, direct tone for external; efficient and direct for internal.

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

Save proactively when: Ron tells you something about a client/team/process, you observe a repeating pattern, a decision is made, a client status changes, you learn a preference or constraint.

Categories (use exactly these):
- client — active or former client accounts
- team — team member details, working styles, responsibilities
- process — SOPs, workflows, bottlenecks
- decision — strategic/operational decisions by Ron
- alert — active risks, blockers, urgent items
- intel — market, competitor, delivery trends

Key naming: use client/company name exactly (e.g. "Max Valverde"), first name for team (e.g. "Felipe"), short phrase for process (e.g. "onboarding bottleneck"), date+topic for decisions (e.g. "March 2026 - pivot to Build and Release").

Value format: single clear sentence or short paragraph with current status, last known action, and next step if relevant.

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

// Appended behavioral rules — always active regardless of role
const SYSTEM_PROMPT_RULES = `

CRITICAL BEHAVIOR RULES:

1. ALWAYS CLOSE THE LOOP — When you take any action (delete a task, create a task, send a message, read a file, update Notion, clean duplicates, or any background operation), you MUST follow up with a clear completion message. Never go silent after starting an action. Always confirm: what you did, whether it succeeded or failed, and what the outcome was.

   Example — WRONG: "Got them. Deleting the three # prefix duplicates now." [goes silent]
   Example — RIGHT: "Got them. Deleting the three # prefix duplicates now... Done. Removed 3 duplicate tasks (Daily Fulfillment Pulse, Weekly Delivery Health Report, Fulfillment Real-Time Alerts). 5 unique tasks remain active."

2. COMPLETION FORMAT — After any action, confirm with:
   - What you did
   - Success or failure
   - Specific outcome (numbers, names, links where relevant)
   - Next step if applicable

3. FAILURE REPORTING — If an action fails, report immediately with the error and suggest a fix. Never silently retry without telling the user.

4. NO MARKDOWN IN SLACK — Never use **bold**, headers, or bullet points with * in Slack messages. Use plain text only.

5. RON IS PRIMARY POC — All decisions, escalations, and technical questions go to Ron. Never suggest involving David unless Ron explicitly asks.
`;

const SYSTEM_PROMPT = SYSTEM_PROMPT_BASE + SYSTEM_PROMPT_RULES;

const AGENT_CHANNEL         = process.env.AGENT_CHANNEL         || '#ng-pm-agent';
const OPS_CHANNEL           = process.env.OPS_CHANNEL           || '#ng-fullfillment-ops';
const NEW_CLIENT_CHANNEL    = process.env.NEW_CLIENT_CHANNEL    || '#ng-new-client-alerts';
const SALES_CHANNEL         = process.env.SALES_CHANNEL         || '#ng-sales-goats';
const SYSTEMS_CHANNEL       = process.env.SYSTEMS_CHANNEL       || '#ng-app-and-systems-improvents';
const ANNOUNCEMENTS_CHANNEL = process.env.ANNOUNCEMENTS_CHANNEL || '#ng-internal-announcements';

const pendingApprovals = {};

// Rate limiter — max 10 requests per user per minute
const userRateLimits = {};
function isRateLimited(userId) {
  const now = Date.now();
  const windowMs = 60 * 1000; // 1 minute
  const maxRequests = 10;

  if (!userRateLimits[userId]) userRateLimits[userId] = [];

  // Clear old entries outside window
  userRateLimits[userId] = userRateLimits[userId].filter(t => now - t < windowMs);

  if (userRateLimits[userId].length >= maxRequests) return true;

  userRateLimits[userId].push(now);
  return false;
}

// Clean up rate limit map every 5 minutes to prevent memory bloat
setInterval(() => {
  const now = Date.now();
  for (const userId of Object.keys(userRateLimits)) {
    userRateLimits[userId] = (userRateLimits[userId] || []).filter(t => now - t < 60000);
    if (!userRateLimits[userId].length) delete userRateLimits[userId];
  }
}, 5 * 60 * 1000);

// Clean up stale pending approvals every 30 minutes
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
  'U05HXGX18H3': { name: 'Ron',     role: 'ceo',            displayName: 'Ron Duarte' },
  'U07SMMDMSLQ': { name: 'Tania',   role: 'client_success', displayName: 'Tania'      },
  'U08ABBFNGUW': { name: 'Josue',   role: 'tech_ops',       displayName: 'Josue'      },
  'U08ACUHUUP6': { name: 'David',   role: 'tech_lead',      displayName: 'David'      },
  'U09Q3BXJ18B': { name: 'Valeria', role: 'fulfillment',    displayName: 'Valeria'    },
  'U09TNMVML3F': { name: 'Felipe',  role: 'campaigns',      displayName: 'Felipe'     },
  'U0A9J00EMGD': { name: 'Joseph',  role: 'setter',         displayName: 'Joseph'     },
  'U0AMTEKDCPN': { name: 'Jose',    role: 'closer',         displayName: 'Jose'       }
};

const ROLE_PERMISSIONS = {
  ceo: {
    canReadChannels: ['ng-fullfillment-ops', 'ng-sales-goats', 'ng-ops-management', 'ng-new-client-alerts', 'ng-app-and-systems-improvents', 'ng-internal-announcements'],
    canPostChannels: ['ng-fullfillment-ops', 'ng-sales-goats', 'ng-ops-management', 'ng-new-client-alerts', 'ng-app-and-systems-improvents', 'ng-internal-announcements'],
    canUseEmail: true, canUseCalendar: true, canUseGHL: true,
    canUseDrive: true, canUseNotion: true, canSaveKnowledge: true, fullAccess: true
  },
  client_success: {
    canReadChannels: ['ng-fullfillment-ops', 'ng-new-client-alerts', 'ng-ops-management'],
    canPostChannels: ['ng-fullfillment-ops', 'ng-new-client-alerts'],
    canUseEmail: false, canUseCalendar: false, canUseGHL: false,
    canUseDrive: true, canUseNotion: true, canSaveKnowledge: true, fullAccess: false
  },
  tech_ops: {
    canReadChannels: ['ng-fullfillment-ops', 'ng-app-and-systems-improvents'],
    canPostChannels: ['ng-fullfillment-ops', 'ng-app-and-systems-improvents'],
    canUseEmail: false, canUseCalendar: false, canUseGHL: false,
    canUseDrive: true, canUseNotion: true, canSaveKnowledge: true, fullAccess: false
  },
  tech_lead: {
    canReadChannels: ['ng-fullfillment-ops', 'ng-app-and-systems-improvents', 'ng-ops-management'],
    canPostChannels: ['ng-fullfillment-ops', 'ng-app-and-systems-improvents'],
    canUseEmail: false, canUseCalendar: false, canUseGHL: false,
    canUseDrive: true, canUseNotion: true, canSaveKnowledge: true, fullAccess: false
  },
  fulfillment: {
    canReadChannels: ['ng-fullfillment-ops'], canPostChannels: ['ng-fullfillment-ops'],
    canUseEmail: false, canUseCalendar: false, canUseGHL: false,
    canUseDrive: true, canUseNotion: true, canSaveKnowledge: false, fullAccess: false
  },
  campaigns: {
    canReadChannels: ['ng-fullfillment-ops'], canPostChannels: ['ng-fullfillment-ops'],
    canUseEmail: false, canUseCalendar: false, canUseGHL: false,
    canUseDrive: true, canUseNotion: true, canSaveKnowledge: false, fullAccess: false
  },
  setter: {
    canReadChannels: ['ng-sales-goats'], canPostChannels: ['ng-sales-goats'],
    canUseEmail: false, canUseCalendar: false, canUseGHL: true,
    canUseDrive: false, canUseNotion: false, canSaveKnowledge: false, fullAccess: false
  },
  closer: {
    canReadChannels: ['ng-sales-goats'], canPostChannels: ['ng-sales-goats'],
    canUseEmail: false, canUseCalendar: false, canUseGHL: true,
    canUseDrive: false, canUseNotion: false, canSaveKnowledge: false, fullAccess: false
  }
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
  const perms = getMemberPermissions(userId);
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
    setter: `You are speaking with Joseph, the Appointment Setter at NeuroGrowth. He works the B2C LinkedIn outreach pipeline and books discovery calls with qualified prospects.

His daily workflow:
- Works inbound and outbound LinkedIn conversations using the NeuroGrowth setting script
- Qualifies prospects by gathering: niche/service, what they sell, price point, ideal client profile
- Runs the full setting flow: intro → qualification → handle objections (no business, bad fit, what is LinkedIn) → confirm call → send calendar link (https://calendly.com/ron-duarte/linkedin-flywheel) + pre-call material
- Tags prospects in GHL: "Net a Fit" for disqualified, "Send to the Ninjas" for warm transfers to Kevin
- Day-of-call: sends follow-up message 9-10am, confirms meeting, sends the system overview doc before the call
- Files an EOD report every day summarizing calls booked, pipeline status, and follow-up actions

Key conversation stages he manages:
1. Opening and qualification (gather niche, service, price, ICP)
2. Objection handling (no business → disqualify, bad fit → refer to Kevin, LinkedIn skeptic → educate)
3. Booking flow: confirm interest → send calendar → confirm day-of → send pre-call doc → get on call
4. Follow-up sequences (FU1 through FU4 + sticker) for non-responders
5. Nurturing: landing page with next steps sent after booking confirmation

When Joseph asks about a prospect, pull from GHL conversations and knowledge base. Help him draft follow-up messages, objection responses, and booking confirmations in Spanish (he works LATAM). Help him prep his EOD report. He cannot access Ron's Gmail or calendar.`,
    closer: `You are speaking with Jose (also known as Jonathan), the High-Ticket Closer at NeuroGrowth. He takes booked calls from Joseph and closes them into paying clients.

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

When Jose asks about a prospect or pipeline, pull from GHL conversations and knowledge base. Help him draft follow-up messages, re-engagement scripts, and closing sequences. Help him prep his EOD report. He cannot access Ron's Gmail or calendar.`
  };

  const baseContext = roleContext[member.role] || roleContext.fulfillment;
  const channelList = perms.canReadChannels.join(', ');

  return `${SYSTEM_PROMPT}

---
CURRENT USER CONTEXT:
${baseContext}

This user can access these channels: ${channelList}
This user cannot access Ron's Gmail, personal calendar, or tools not listed in their permissions.
Always address this person by name: ${member.displayName}.
Keep responses focused on their operational scope. Do not share sensitive business financials, compensation details, or information outside their role.`;
}

function getConversationKey(channelId, threadTs, userId) {
  return threadTs ? `${channelId}:${threadTs}` : `${channelId}:${userId}`;
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
    // Redact sensitive patterns before storing in conversation history
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
    // Prune after every 5 saves — keep max 40 rows per user
    await pruneConversationHistory(userId);
  } catch (err) {
    console.error('Supabase save error:', err.message);
  }
}

async function pruneConversationHistory(userId, maxRows = 40) {
  try {
    // Get count for this user
    const { count } = await supabase
      .from('conversations')
      .select('*', { count: 'exact', head: true })
      .eq('user_id', userId);

    if (!count || count <= maxRows) return;

    // Delete oldest rows beyond the limit
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
    // Sanitize query to prevent special char issues in ilike
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

// Patterns that indicate sensitive data — never save these to knowledge base
const SENSITIVE_PATTERNS = [
  /password/i, /passwd/i, /secret/i, /api.?key/i, /access.?token/i,
  /private.?key/i, /credentials/i, /auth.?token/i, /bearer/i,
  /eyJ[A-Za-z0-9_-]{10,}/,  // JWT tokens
  /sk-[A-Za-z0-9]{20,}/,     // OpenAI keys
  /xox[bpoas]-[A-Za-z0-9-]{10,}/, // Slack tokens
  /-----BEGIN/,               // PEM keys
  /[0-9]{16}/,               // Credit card numbers
  /\d{3}-\d{2}-\d{4}/,  // SSN format
];

function containsSensitiveData(text) {
  if (!text) return false;
  return SENSITIVE_PATTERNS.some(pattern => pattern.test(text));
}

async function upsertKnowledge(category, key, value, source = 'agent') {
  try {
    // Never save sensitive data to knowledge base
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
// Tasks stored in Supabase scheduled_tasks table:
// id, name, cron_expression, prompt, channel, active, created_by, created_at

const activeDynamicCrons = {}; // track node-cron instances by task id

async function loadAndRegisterDynamicCrons() {
  try {
    const { data: tasks, error } = await supabase
      .from('scheduled_tasks')
      .select('*')
      .eq('active', true);

    if (error) throw error;
    if (!tasks || !tasks.length) {
      console.log('No dynamic cron tasks found.');
      return;
    }

    // Deduplicate by name — keep only the most recent task per name
    const seen = {};
    const dedupedTasks = [];
    for (const task of tasks) {
      const key = task.name.toLowerCase().trim();
      if (!seen[key]) {
        seen[key] = true;
        dedupedTasks.push(task);
      } else {
        console.log(`Skipping duplicate cron task: "${task.name}" (${task.id})`);
      }
    }

    for (const task of dedupedTasks) {
      registerDynamicCron(task);
    }
    console.log(`Loaded ${dedupedTasks.length} dynamic cron task(s).`);
  } catch (err) {
    console.error('Dynamic cron load error:', err.message);
  }
}

function registerDynamicCron(task) {
  try {
    // Cancel existing instance if re-registering
    if (activeDynamicCrons[task.id]) {
      activeDynamicCrons[task.id].stop();
    }

    const job = cron.schedule(task.cron_expression, async () => {
      console.log(`Running dynamic cron: ${task.name}`);
      try {
        const reply = await callClaude([{ role: 'user', content: task.prompt }]);
        if (reply && reply.trim()) {
          await postToSlack(task.channel || AGENT_CHANNEL, reply);
        }
      } catch (err) {
        console.error(`Dynamic cron error (${task.name}):`, err.message);
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
    // Check for existing task with same name — prevent duplicates
    const { data: existing } = await supabase
      .from('scheduled_tasks')
      .select('id, name')
      .ilike('name', name.trim())
      .eq('active', true)
      .limit(1);

    if (existing && existing.length > 0) {
      return `A scheduled task named "${name}" already exists. Use list_scheduled_tasks to see all active tasks, or delete the existing one first.`;
    }

    // Convert natural language to cron expression via Claude
    const cronPrompt = `Convert this schedule description to a cron expression (5-field format).
Schedule: "${naturalLanguageSchedule}"
Timezone: America/Costa_Rica

Reply with ONLY the cron expression, nothing else. Examples:
- "every weekday at 9am" → 0 9 * * 1-5
- "every Monday at 8:30am" → 30 8 * * 1
- "every day at 6pm" → 0 18 * * *
- "every Friday at 4pm" → 0 16 * * 5`;

    const cronResponse = await anthropic.messages.create({
      model: 'claude-sonnet-4-6',
      max_tokens: 50,
      messages: [{ role: 'user', content: cronPrompt }]
    });

    const cronExpression = cronResponse.content
      .filter(b => b.type === 'text')
      .map(b => b.text)
      .join('')
      .trim()
      .replace(/[^0-9*,/\- ]/g, '')
      .trim();

    if (!cronExpression || cronExpression.split(' ').length !== 5) {
      return `Could not parse schedule "${naturalLanguageSchedule}" into a valid cron expression. Try something like "every weekday at 9am" or "every Monday at 8:30am".`;
    }

    // Save to Supabase
    const { data, error } = await supabase
      .from('scheduled_tasks')
      .insert({
        name,
        cron_expression: cronExpression,
        prompt,
        channel: channel || AGENT_CHANNEL,
        active: true,
        created_by: createdBy,
        created_at: new Date().toISOString()
      })
      .select()
      .single();

    if (error) throw error;

    // Register immediately without restart
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

async function cleanDuplicateTasks() {
  try {
    const { data: tasks, error } = await supabase
      .from('scheduled_tasks')
      .select('id, name, cron_expression, active, created_at')
      .order('name', { ascending: true })
      .order('created_at', { ascending: true });

    if (error) throw error;
    if (!tasks || !tasks.length) return 'No scheduled tasks found.';

    // Find duplicates — keep oldest, mark newer ones for deletion
    const seen = {};
    const toDelete = [];

    for (const task of tasks) {
      const key = task.name.toLowerCase().trim();
      if (seen[key]) {
        toDelete.push(task);
      } else {
        seen[key] = task;
      }
    }

    if (!toDelete.length) return 'No duplicate tasks found — all clean.';

    // Delete duplicates — use service role client if available for RLS bypass
    const ids = toDelete.map(t => t.id);
    const deleteClient = process.env.SUPABASE_SERVICE_KEY
      ? createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_KEY)
      : supabase;

    const { error: delError } = await deleteClient
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
      throw new Error(`Delete appeared to succeed but ${remaining.length} rows still exist. RLS may be blocking — check Supabase permissions.`);
    }

    const names = toDelete.map(t => t.name).join(', ');
    return `Done. Removed ${toDelete.length} duplicate task(s): ${names}. ${Object.keys(seen).length} unique tasks remain active.`;
  } catch (err) {
    return `Clean duplicate tasks error: ${err.message}`;
  }
}

async function deleteScheduledTask(taskId) {
  try {
    // Use service role if available to bypass RLS
    const deleteClient = process.env.SUPABASE_SERVICE_KEY
      ? createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_KEY)
      : supabase;

    const { error } = await deleteClient
      .from('scheduled_tasks')
      .update({ active: false })
      .eq('id', taskId);

    if (error) throw new Error(`Supabase update failed: ${error.message}`);

    // Verify the update took effect
    const { data: check } = await supabase
      .from('scheduled_tasks')
      .select('id, name, active')
      .eq('id', taskId)
      .single();

    if (check && check.active !== false) {
      throw new Error(`Update did not persist — task is still active. RLS may be blocking writes on the anon key.`);
    }

    // Stop the live cron if running
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
// Operations Tracking:    collection 20ecddb6-8d9f-8126-a408-000bbbc3c088
// Project Sprint Tracking: collection 8d0645e6-eabb-4f0d-9c8a-4d8641ad4e8c

async function createNotionTask(title, taskType = 'operational', priority = 'P2 - Growth & Scalability', dueDate = null, notes = null, customer = null) {
  try {
    const isProject = taskType === 'project';

    // These are the collection (data source) IDs — confirmed working via MCP
    const collectionId = isProject
      ? '8d0645e6-eabb-4f0d-9c8a-4d8641ad4e8c'
      : '20ecddb6-8d9f-8126-a408-000bbbc3c088';

    // Notion REST API requires the database page ID (not collection ID)
    // Operations Tracking page: 20ecddb6-8d9f-809b-a904-d248ed95fce9
    // Project Sprint Tracking page: dc12b8a9-30f1-4872-9e42-c11391271bd1
    const databaseId = isProject
      ? 'dc12b8a930f148729e42c11391271bd1'
      : '20ecddb68d9f809ba904d248ed95fce9';

    const properties = {
      'Name': { title: [{ text: { content: title } }] },
      'Status': { status: { name: 'Not started' } },
      'Priority ': { select: { name: priority } },
      'Type': { select: { name: 'One-time' } }
    };

    if (dueDate) {
      properties['Deadline Date'] = { date: { start: dueDate } };
    }

    if (notes) {
      const notesKey = isProject ? 'Comments/Milestones/Insights' : 'Main Milestone';
      properties[notesKey] = { rich_text: [{ text: { content: `Max: ${notes.substring(0, 500)}` } }] };
    }

    if (customer) {
      properties['Customer'] = { multi_select: [{ name: customer }] };
    }

    const body = {
      parent: { database_id: databaseId },
      properties
    };

    const res = await fetch('https://api.notion.com/v1/pages', {
      method: 'POST',
      headers: {
        'Authorization': `Bearer ${process.env.NOTION_TOKEN}`,
        'Notion-Version': '2022-06-28',
        'Content-Type': 'application/json'
      },
      body: JSON.stringify(body)
    });

    const data = await res.json();

    if (!res.ok) {
      // Log full error to Railway for debugging
      console.error('Notion create task failed:', res.status, JSON.stringify(data));
      // Try with collection ID as fallback
      const body2 = { ...body, parent: { database_id: collectionId.replace(/-/g,'') } };
      const res2 = await fetch('https://api.notion.com/v1/pages', {
        method: 'POST',
        headers: {
          'Authorization': `Bearer ${process.env.NOTION_TOKEN}`,
          'Notion-Version': '2022-06-28',
          'Content-Type': 'application/json'
        },
        body: JSON.stringify(body2)
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

// ─── GOOGLE AUTH ──────────────────────────────────────────────────────────────// ─── GOOGLE AUTH ──────────────────────────────────────────────────────────────
function getGoogleAuth() {
  let credentials, token;
  if (process.env.GOOGLE_CREDENTIALS) {
    credentials = JSON.parse(process.env.GOOGLE_CREDENTIALS);
    token = JSON.parse(process.env.GOOGLE_TOKEN);
  } else {
    credentials = JSON.parse(fs.readFileSync('./credentials.json'));
    token = JSON.parse(fs.readFileSync('./token.json'));
  }
  const { client_id, client_secret } = credentials.installed;
  const oauth2Client = new google.auth.OAuth2(client_id, client_secret, 'http://localhost:3000/callback');
  oauth2Client.setCredentials(token);
  return oauth2Client;
}

// ─── GMAIL ────────────────────────────────────────────────────────────────────
async function getRecentEmails() {
  const auth = getGoogleAuth();
  const gmail = google.gmail({ version: 'v1', auth });
  const res = await gmail.users.messages.list({ userId: 'me', maxResults: 5, q: 'is:unread' });
  const messages = res.data.messages || [];
  if (!messages.length) return 'No unread emails.';
  const details = await Promise.all(messages.map(async (m) => {
    const msg = await gmail.users.messages.get({ userId: 'me', id: m.id, format: 'full' });
    const headers = msg.data.payload.headers;
    const subject = headers.find(h => h.name === 'Subject')?.value || 'No subject';
    const from    = headers.find(h => h.name === 'From')?.value    || 'Unknown';
    const date    = headers.find(h => h.name === 'Date')?.value    || '';
    let body = '';
    const payload = msg.data.payload;
    if (payload.parts) {
      const textPart = payload.parts.find(p => p.mimeType === 'text/plain');
      if (textPart?.body?.data) {
        body = Buffer.from(textPart.body.data, 'base64').toString('utf8').substring(0, 1000);
      }
    } else if (payload.body?.data) {
      body = Buffer.from(payload.body.data, 'base64').toString('utf8').substring(0, 1000);
    }
    if (!body) body = msg.data.snippet?.substring(0, 500) || '';
    return `From: ${from}\nDate: ${date}\nSubject: ${subject}\nBody:\n${body}`;
  }));
  return details.join('\n\n---\n\n');
}

async function sendEmail(to, subject, body) {
  const auth = getGoogleAuth();
  const gmail = google.gmail({ version: 'v1', auth });
  const message = [`To: ${to}`, `Subject: ${subject}`, '', body].join('\n');
  const encoded = Buffer.from(message).toString('base64').replace(/\+/g, '-').replace(/\//g, '_');
  await gmail.users.messages.send({ userId: 'me', requestBody: { raw: encoded } });
  return `Email sent to ${to}`;
}

// ─── GOOGLE CALENDAR ──────────────────────────────────────────────────────────
async function getCalendarEvents(daysFromNow = 0, daysRange = 1) {
  const auth = getGoogleAuth();
  const calendar = google.calendar({ version: 'v3', auth });
  const startDate = new Date();
  startDate.setDate(startDate.getDate() + daysFromNow);
  startDate.setHours(6, 0, 0, 0);
  const endDate = new Date(startDate);
  endDate.setDate(endDate.getDate() + daysRange);
  endDate.setHours(29, 59, 59, 0);
  const res = await calendar.events.list({
    calendarId: 'primary',
    timeMin: startDate.toISOString(),
    timeMax: endDate.toISOString(),
    singleEvents: true,
    orderBy: 'startTime'
  });
  const events = res.data.items || [];
  if (!events.length) return 'No events found in that range.';
  return events.map(e => {
    const start = e.start.dateTime || e.start.date;
    return `${start} — ${e.summary}`;
  }).join('\n');
}

// ─── GOOGLE DRIVE ─────────────────────────────────────────────────────────────
async function searchDrive(query) {
  const auth = getGoogleAuth();
  const drive = google.drive({ version: 'v3', auth });
  const res = await drive.files.list({
    q: `name contains '${query}' or fullText contains '${query}'`,
    pageSize: 5,
    fields: 'files(id, name, mimeType, webViewLink, modifiedTime)'
  });
  const files = res.data.files || [];
  if (!files.length) return 'No files found.';
  return files.map(f =>
    `${f.name}\nType: ${f.mimeType}\nLink: ${f.webViewLink}\nModified: ${f.modifiedTime}`
  ).join('\n\n');
}

// ─── NOTION ───────────────────────────────────────────────────────────────────
async function searchNotion(query) {
  const res = await fetch('https://api.notion.com/v1/search', {
    method: 'POST',
    headers: {
      'Authorization': `Bearer ${process.env.NOTION_TOKEN}`,
      'Notion-Version': '2022-06-28',
      'Content-Type': 'application/json'
    },
    body: JSON.stringify({ query, page_size: 5 })
  });
  return await res.json();
}

async function getNotionPage(pageId) {
  const res = await fetch(`https://api.notion.com/v1/pages/${pageId}`, {
    headers: {
      'Authorization': `Bearer ${process.env.NOTION_TOKEN}`,
      'Notion-Version': '2022-06-28'
    }
  });
  return await res.json();
}


// ─── META ADS ─────────────────────────────────────────────────────────────────
// Requires: META_ACCESS_TOKEN, META_AD_ACCOUNT_ID env vars
// Read-only access to NeuroGrowth ad campaigns

async function getMetaAdsSummary(datePreset = 'last_7d') {
  try {
    const accountId = process.env.META_AD_ACCOUNT_ID;
    const token     = process.env.META_ACCESS_TOKEN;
    if (!accountId || !token) return 'Meta Ads not configured. Add META_ACCESS_TOKEN and META_AD_ACCOUNT_ID to env vars.';

    // Account-level insights
    const fields = 'spend,impressions,clicks,ctr,cpc,cpm,reach,frequency,actions,cost_per_action_type';
    const insightUrl = `https://graph.facebook.com/v19.0/${accountId}/insights?fields=${fields}&date_preset=${datePreset}&access_token=${token}`;
    const insightRes = await fetch(insightUrl);
    const insightData = await insightRes.json();

    if (insightData.error) throw new Error(insightData.error.message);

    const d = insightData.data?.[0] || {};

    // Extract leads if available
    const leads = (d.actions || []).find(a => a.action_type === 'lead')?.value || '0';
    const clicks = d.clicks || '0';
    const spend  = parseFloat(d.spend || 0).toFixed(2);
    const ctr    = parseFloat(d.ctr  || 0).toFixed(2);
    const cpc    = parseFloat(d.cpc  || 0).toFixed(2);
    const cpm    = parseFloat(d.cpm  || 0).toFixed(2);
    const cpl    = leads > 0 ? (parseFloat(spend) / parseInt(leads)).toFixed(2) : 'N/A';

    return [
      `Meta Ads — ${datePreset.replace(/_/g,' ')}:`,
      `Spend: $${spend} | Impressions: ${parseInt(d.impressions||0).toLocaleString()} | Reach: ${parseInt(d.reach||0).toLocaleString()}`,
      `Clicks: ${parseInt(clicks).toLocaleString()} | CTR: ${ctr}% | CPC: $${cpc} | CPM: $${cpm}`,
      leads !== '0' ? `Leads: ${leads} | CPL: $${cpl}` : 'No lead conversions tracked in this period.'
    ].join('\n');
  } catch (err) {
    return `Meta Ads summary error: ${err.message}`;
  }
}

async function getMetaCampaigns(datePreset = 'last_7d', limit = 10) {
  try {
    const accountId = process.env.META_AD_ACCOUNT_ID;
    const token     = process.env.META_ACCESS_TOKEN;
    if (!accountId || !token) return 'Meta Ads not configured.';

    const fields = 'name,status,objective,spend,impressions,clicks,ctr,cpc,actions,cost_per_action_type';
    const url = `https://graph.facebook.com/v19.0/${accountId}/campaigns?fields=id,name,status,objective,insights.date_preset(${datePreset}){${fields}}&limit=${limit}&access_token=${token}`;
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
      return `${status} ${c.name}\n   Spend: $${spend} | Clicks: ${ins.clicks||0} | CTR: ${parseFloat(ins.ctr||0).toFixed(2)}%${leads!=='0' ? ` | Leads: ${leads}` : ''}`;
    });

    return `Campaigns (${datePreset.replace(/_/g,' ')}) — ${campaigns.length} found:\n\n${lines.join('\n\n')}`;
  } catch (err) {
    return `Meta campaigns error: ${err.message}`;
  }
}

async function getMetaAdSets(campaignId = null, datePreset = 'last_7d') {
  try {
    const accountId = process.env.META_AD_ACCOUNT_ID;
    const token     = process.env.META_ACCESS_TOKEN;
    if (!accountId || !token) return 'Meta Ads not configured.';

    const fields = 'name,status,daily_budget,lifetime_budget,spend,impressions,clicks,ctr,cpc,actions';
    const baseId  = campaignId || accountId;
    const endpoint = campaignId ? `${campaignId}/adsets` : `${accountId}/adsets`;
    const url = `https://graph.facebook.com/v19.0/${endpoint}?fields=id,name,status,daily_budget,insights.date_preset(${datePreset}){${fields}}&limit=20&access_token=${token}`;
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
      return `${status} ${a.name} | Budget: ${budget}\n   Spend: $${spend} | Clicks: ${ins.clicks||0} | CTR: ${parseFloat(ins.ctr||0).toFixed(2)}%${leads!=='0' ? ` | Leads: ${leads}` : ''}`;
    });

    return `Ad Sets (${datePreset.replace(/_/g,' ')}):\n\n${lines.join('\n\n')}`;
  } catch (err) {
    return `Meta ad sets error: ${err.message}`;
  }
}

async function getMetaAds(adSetId = null, datePreset = 'last_7d') {
  try {
    const accountId = process.env.META_AD_ACCOUNT_ID;
    const token     = process.env.META_ACCESS_TOKEN;
    if (!accountId || !token) return 'Meta Ads not configured.';

    const endpoint = adSetId ? `${adSetId}/ads` : `${accountId}/ads`;
    const fields   = 'name,status,spend,impressions,clicks,ctr,cpc,actions';
    const url = `https://graph.facebook.com/v19.0/${endpoint}?fields=id,name,status,insights.date_preset(${datePreset}){${fields}}&limit=20&access_token=${token}`;
    const res  = await fetch(url);
    const data = await res.json();

    if (data.error) throw new Error(data.error.message);

    const ads = data.data || [];
    if (!ads.length) return 'No ads found.';

    const lines = ads.map(a => {
      const ins   = a.insights?.data?.[0] || {};
      const spend = parseFloat(ins.spend || 0).toFixed(2);
      const leads = (ins.actions || []).find(x => x.action_type === 'lead')?.value || '0';
      const status = a.status === 'ACTIVE' ? '🟢' : a.status === 'PAUSED' ? '⏸️' : '🔴';
      return `${status} ${a.name}\n   Spend: $${spend} | Impressions: ${parseInt(ins.impressions||0).toLocaleString()} | Clicks: ${ins.clicks||0} | CTR: ${parseFloat(ins.ctr||0).toFixed(2)}%${leads!=='0' ? ` | Leads: ${leads}` : ''}`;
    });

    return `Ads (${datePreset.replace(/_/g,' ')}):\n\n${lines.join('\n\n')}`;
  } catch (err) {
    return `Meta ads error: ${err.message}`;
  }
}


// ─── GOOGLE SHEETS ────────────────────────────────────────────────────────────
async function readGoogleSheet(spreadsheetId, range = null) {
  try {
    const auth   = getGoogleAuth();
    const sheets = google.sheets({ version: 'v4', auth });

    // If no range specified, get sheet metadata first to find sheet names
    if (!range) {
      const meta = await sheets.spreadsheets.get({ spreadsheetId });
      const firstSheet = meta.data.sheets?.[0]?.properties?.title || 'Sheet1';
      range = `${firstSheet}!A1:Z100`;
    }

    const res = await sheets.spreadsheets.values.get({
      spreadsheetId,
      range,
    });

    const rows = res.data.values || [];
    if (!rows.length) return 'Sheet is empty or no data in range.';

    // Format as readable table
    const headers = rows[0] || [];
    const dataRows = rows.slice(1);

    let output = `Sheet data (${rows.length} rows, ${headers.length} columns):\n\n`;
    output += headers.join(' | ') + '\n';
    output += headers.map(() => '---').join(' | ') + '\n';
    output += dataRows.slice(0, 50).map(row =>
      headers.map((_, i) => row[i] || '').join(' | ')
    ).join('\n');

    if (dataRows.length > 50) {
      output += `\n\n... and ${dataRows.length - 50} more rows.`;
    }

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

    const res = await docs.documents.get({ documentId });
    const doc = res.data;

    // Extract plain text from the doc body
    let text = '';
    const body = doc.body?.content || [];
    for (const element of body) {
      if (element.paragraph) {
        for (const pe of element.paragraph.elements || []) {
          if (pe.textRun?.content) {
            text += pe.textRun.content;
          }
        }
      } else if (element.table) {
        for (const row of element.table.tableRows || []) {
          for (const cell of row.tableCells || []) {
            for (const cp of cell.content || []) {
              for (const pe of cp.paragraph?.elements || []) {
                if (pe.textRun?.content) {
                  text += pe.textRun.content + ' | ';
                }
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

// Helper to extract file ID from a Google Drive/Sheets/Docs URL
function extractGoogleFileId(urlOrId) {
  // Match /d/{id}/ pattern in Drive, Sheets, Docs URLs
  const match = urlOrId.match(/\/d\/([a-zA-Z0-9_-]+)/);
  if (match) return match[1];
  // If it looks like a raw ID already (no slashes, spaces)
  if (/^[a-zA-Z0-9_-]+$/.test(urlOrId)) return urlOrId;
  return urlOrId;
}

// ─── GHL CONVERSATIONS ───────────────────────────────────────────────────────

// ─── PORTAL: CLIENT STATUS ────────────────────────────────────────────────────
// Schema notes:
// customer_activities.status values: 'phase_1', 'phase_2', 'phase_3', 'live', 'blocked'
// client_dashboards.customer_status: 'phase_1', 'phase_2', 'phase_3', 'live', 'blocked'
// client_dashboards.customer_type: 'flywheel-ai' or 'full-service'
// template_phase_assignments.phase: 'phase_1', 'phase_2', 'phase_3'
// Activities are linked via template_id — need to join with customer_activity_templates

async function getClientStatus(clientName = null) {
  try {
    // First get client dashboards which have the current phase status
    let dashQuery = portalSupabase
      .from('client_dashboards')
      .select('id, client_name, email, customer_status, customer_type, is_active, created_at, stabilization_started_at, linkedin_handler')
      .eq('is_active', true)
      .order('customer_status', { ascending: true })
      .limit(60);

    if (clientName) {
      dashQuery = dashQuery.or(`client_name.ilike.%${clientName}%,email.ilike.%${clientName}%`);
    }
    // Default: show flywheel-ai (Build & Release) clients — the active offer
    // Full-service clients are legacy and less relevant for daily ops

    const { data: dashboards, error: dashErr } = await dashQuery;
    if (dashErr) throw dashErr;
    if (!dashboards || !dashboards.length) return clientName ? `No client found matching: ${clientName}` : 'No active clients found in portal.';

    // Get activity templates for title lookup
    const { data: templates } = await portalSupabase
      .from('customer_activity_templates')
      .select('id, title, order_index')
      .eq('is_active', true);

    const templateMap = {};
    (templates || []).forEach(t => { templateMap[t.id] = t; });

    const results = await Promise.all(dashboards.slice(0, 20).map(async (dash) => {
      // Match to onboarding record by email for intel
      const { data: onboarding } = await portalSupabase
        .from('customer_onboarding')
        .select('id, first_name, last_name, company, services_products, ideal_customer, service_tier, payment_status')
        .eq('email', dash.email)
        .limit(1);

      const ob = onboarding?.[0];
      const customerId = ob?.id;

      // Get activities for this customer
      let activities = [];
      if (customerId) {
        const { data: acts } = await portalSupabase
          .from('customer_activities')
          .select('id, template_id, status, assigned_to, completed_at, notes')
          .eq('customer_id', customerId);
        activities = acts || [];
      }

      const total     = activities.length;
      const live      = activities.filter(a => a.status === 'live').length;
      const blocked   = activities.filter(a => a.status === 'blocked').length;
      const phase1    = activities.filter(a => a.status === 'phase_1').length;
      const phase2    = activities.filter(a => a.status === 'phase_2').length;

      const blockedActs = activities
        .filter(a => a.status === 'blocked')
        .map(a => templateMap[a.template_id]?.title || 'Unknown activity')
        .join(', ');

      const pendingActs = activities
        .filter(a => a.status === 'phase_1' || a.status === 'phase_2')
        .sort((a,b) => (templateMap[a.template_id]?.order_index||99) - (templateMap[b.template_id]?.order_index||99))
        .slice(0, 3)
        .map(a => templateMap[a.template_id]?.title || 'Unknown')
        .join(', ');

      const statusLabel = {
        'live': '🟢 Live',
        'phase_1': '🟡 Phase 1 – Optimization',
        'phase_2': '🔵 Phase 2 – Campaign Launch',
        'phase_3': '🟣 Phase 3 – Stabilization',
        'blocked': '🔴 Blocked',
        'phase_0': '🟠 Phase 0 – Onboarding'
      }[dash.customer_status] || `⚪ ${dash.customer_status}`;
      const statusEmoji = statusLabel.split(' ')[0];

      const startDate = dash.created_at ? new Date(dash.created_at) : null;
      const daysSince = startDate ? Math.floor((Date.now() - startDate.getTime()) / (1000 * 60 * 60 * 24)) : null;

      const lines = [
        `${statusEmoji} ${dash.client_name || dash.email} [${(dash.customer_type || '').replace('flywheel-ai','Flywheel').replace('full-service','Full Service')}]`,
        `${statusLabel} | Day ${daysSince || '?'} since created`,
        total > 0 ? `Activities: ${live} live, ${phase1} phase_1 pending, ${phase2} phase_2 pending, ${blocked} blocked` : 'No activities tracked',
        blockedActs ? `🔴 Blocked on: ${blockedActs}` : '',
        pendingActs && !blockedActs ? `Next up: ${pendingActs}` : '',
        ob?.services_products && clientName ? `Service: ${ob.services_products.substring(0,120)}` : '',
        ob?.ideal_customer && clientName ? `ICP: ${ob.ideal_customer.substring(0,100)}` : '',
      ].filter(Boolean);

      return lines.join('\n');
    }));

    const statusCounts = dashboards.reduce((acc, d) => {
      acc[d.customer_status] = (acc[d.customer_status] || 0) + 1;
      return acc;
    }, {});

    const header = clientName
      ? `Portal status for "${clientName}":\n\n`
      : `Portal — ${dashboards.length} active clients | 🟢 ${statusCounts.live||0} Live | 🟠 ${statusCounts.phase_0||0} Onboarding | 🟡 ${statusCounts.phase_1||0} Optimization | 🔵 ${statusCounts.phase_2||0} Campaign Launch | 🟣 ${statusCounts.phase_3||0} Stabilization | 🔴 ${statusCounts.blocked||0} Blocked\n\n`;

    return header + results.join('\n\n');
  } catch (err) {
    return `Portal client status error: ${err.message}`;
  }
}

async function getPortalAlerts() {
  try {
    // Get all blocked clients and phase 1/2 clients who may be at risk
    const { data: dashboards, error } = await portalSupabase
      .from('client_dashboards')
      .select('id, client_name, email, customer_status, customer_type, created_at')
      .eq('is_active', true)
      .in('customer_status', ['blocked', 'phase_1', 'phase_2'])
      .order('created_at', { ascending: true });

    if (error) throw error;
    if (!dashboards || !dashboards.length) return '✅ No blocked or at-risk clients. All clients on track.';

    const now = Date.now();
    const alerts = [];

    for (const dash of dashboards) {
      const startDate = dash.created_at ? new Date(dash.created_at) : null;
      const daysSince = startDate ? Math.floor((now - startDate.getTime()) / (1000 * 60 * 60 * 24)) : 0;

      // Get blocked activities
      const { data: onboarding } = await portalSupabase
        .from('customer_onboarding')
        .select('id')
        .eq('email', dash.email)
        .limit(1);

      let blockedDetails = '';
      if (onboarding?.[0]) {
        const { data: acts } = await portalSupabase
          .from('customer_activities')
          .select('template_id, status, notes')
          .eq('customer_id', onboarding[0].id)
          .eq('status', 'blocked');

        if (acts?.length) {
          const { data: templates } = await portalSupabase
            .from('customer_activity_templates')
            .select('id, title')
            .in('id', acts.map(a => a.template_id));
          const titleMap = {};
          (templates || []).forEach(t => { titleMap[t.id] = t.title; });
          blockedDetails = acts.map(a => titleMap[a.template_id] || 'Unknown').join(', ');
        }
      }

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

// ─── GHL CONVERSATIONS ───────────────────────────────────────────────────────

// ─── PORTAL: CLIENT STATUS ────────────────────────────────────────────────────
async function getClientStatus(clientName = null) {
  try {
    let onboardingQuery = portalSupabase
      .from('customer_onboarding')
      .select('id, first_name, last_name, email, company, status, service_tier, payment_status, created_at, onboarding_completed_at, dashboard_created, services_products, ideal_customer, client_pain_points, main_competitors, current_tools')
      .order('created_at', { ascending: false })
      .limit(50);

    if (clientName) {
      onboardingQuery = onboardingQuery.or(`first_name.ilike.%${clientName}%,last_name.ilike.%${clientName}%,email.ilike.%${clientName}%,company.ilike.%${clientName}%`);
    }

    const { data: clients, error: clientErr } = await onboardingQuery;
    if (clientErr) throw clientErr;
    if (!clients || !clients.length) return clientName ? `No client found matching: ${clientName}` : 'No clients found in portal.';

    // For each client get their activity completion from customer_activities
    const results = await Promise.all(clients.slice(0, 15).map(async (client) => {
      const fullName = `${client.first_name || ''} ${client.last_name || ''}`.trim() || client.email;

      const { data: activities } = await portalSupabase
        .from('customer_activities')
        .select('title, status, phase_assignments, category')
        .eq('customer_id', client.id);

      const total     = (activities || []).length;
      const completed = (activities || []).filter(a => a.status === 'completed' || a.status === 'done').length;
      const pending   = (activities || []).filter(a => !a.status || a.status === 'pending' || a.status === 'not_started' || a.status === 'in_progress');

      const phase1Done  = (activities || []).filter(a => (a.phase_assignments || '').includes('Phase 1') && (a.status === 'completed' || a.status === 'done')).length;
      const phase2Done  = (activities || []).filter(a => (a.phase_assignments || '').includes('Phase 2') && (a.status === 'completed' || a.status === 'done')).length;
      const phase1Total = (activities || []).filter(a => (a.phase_assignments || '').includes('Phase 1')).length;
      const phase2Total = (activities || []).filter(a => (a.phase_assignments || '').includes('Phase 2')).length;

      const nextPending = pending.slice(0, 3).map(a => a.title).join(', ');

      const startDate = client.created_at ? new Date(client.created_at) : null;
      const daysSince = startDate ? Math.floor((Date.now() - startDate.getTime()) / (1000 * 60 * 60 * 24)) : null;
      const launchRisk = daysSince > 10 && completed < total ? ' ⚠️ LAUNCH RISK' : '';

      const intelLines = clientName ? [
        client.services_products ? `Service: ${client.services_products.substring(0, 150)}` : '',
        client.ideal_customer    ? `ICP: ${client.ideal_customer.substring(0, 100)}` : '',
        client.client_pain_points ? `Pain points: ${client.client_pain_points.substring(0, 100)}` : '',
        client.main_competitors  ? `Competitors: ${client.main_competitors.substring(0, 100)}` : '',
        client.current_tools     ? `Tools: ${client.current_tools.substring(0, 100)}` : ''
      ].filter(Boolean) : [];

      return [
        `${fullName} (${client.company || client.email})`,
        `Tier: ${client.service_tier || 'unknown'} | Payment: ${client.payment_status || 'unknown'} | Status: ${client.status || 'unknown'}`,
        total > 0 ? `Activities: ${completed}/${total} complete${launchRisk} | Phase 1: ${phase1Done}/${phase1Total} | Phase 2: ${phase2Done}/${phase2Total}` : 'No activities found',
        daysSince !== null ? `Day ${daysSince} since onboarding` : '',
        nextPending ? `Pending: ${nextPending}` : 'All activities complete',
        ...intelLines
      ].filter(Boolean).join('\n');
    }));

    const header = clientName
      ? `Portal status for "${clientName}":\n\n`
      : `Portal — ${clients.length} active clients:\n\n`;

    return header + results.join('\n\n');
  } catch (err) {
    return `Portal client status error: ${err.message}`;
  }
}

async function getPortalAlerts() {
  try {
    const { data: clients, error } = await portalSupabase
      .from('customer_onboarding')
      .select('id, first_name, last_name, email, company, service_tier, payment_status, created_at, onboarding_completed_at')
      .order('created_at', { ascending: true })
      .limit(50);

    if (error) throw error;
    if (!clients || !clients.length) return 'No clients in portal.';

    const now = Date.now();
    const alerts = [];

    for (const client of clients) {
      const startDate = client.created_at ? new Date(client.created_at) : null;
      const daysSince = startDate ? Math.floor((now - startDate.getTime()) / (1000 * 60 * 60 * 24)) : null;
      if (!daysSince || daysSince < 7) continue;

      const { data: activities } = await portalSupabase
        .from('customer_activities')
        .select('title, status')
        .eq('customer_id', client.id);

      const total     = (activities || []).length;
      const completed = (activities || []).filter(a => a.status === 'completed' || a.status === 'done').length;

      if (total > 0 && completed < total) {
        const pct  = Math.round((completed / total) * 100);
        const risk = daysSince >= 14 ? '🔴 OVERDUE' : '🟡 AT RISK';
        const fullName = `${client.first_name || ''} ${client.last_name || ''}`.trim() || client.email;
        alerts.push(`${risk} ${fullName} (${client.company || client.email}) — Day ${daysSince}, ${pct}% complete (${completed}/${total})`);
      }
    }

    if (!alerts.length) return 'No launch risk clients detected. All clients on track.';
    return `Launch risk alerts (${alerts.length} clients):\n\n` + alerts.join('\n');
  } catch (err) {
    return `Portal alerts error: ${err.message}`;
  }
}

async function getGHLConversations(limit = 20, unreadOnly = false) {
  try {
    const locationId = process.env.GHL_LOCATION_ID;
    const apiKey     = process.env.GHL_API_KEY;
    let url = `https://services.leadconnectorhq.com/conversations/search?locationId=${locationId}&limit=${limit}`;
    if (unreadOnly) url += `&status=unread`;
    const res = await fetch(url, {
      headers: {
        'Authorization': `Bearer ${apiKey}`,
        'Version': '2021-07-28',
        'Content-Type': 'application/json'
      }
    });
    const data = await res.json();
    const convos = data.conversations || [];
    if (!convos.length) return 'No conversations found.';
    const now = Date.now();
    const oneDayMs = 24 * 60 * 60 * 1000;
    const lines = convos.map(c => {
      const lastDate = new Date(c.lastMessageDate).toLocaleString('en-US', {
        timeZone: 'America/Costa_Rica', month: 'short', day: 'numeric',
        hour: '2-digit', minute: '2-digit'
      });
      const age       = Math.floor((now - c.lastMessageDate) / oneDayMs);
      const unread    = c.unreadCount > 0 ? ` [UNREAD: ${c.unreadCount}]` : '';
      const direction = c.lastMessageDirection === 'inbound' ? '<-- inbound' : '--> outbound';
      const channel   = c.lastMessageType?.replace('TYPE_', '') || 'unknown';
      const stale     = age >= 3 ? ` [${age}d ago - needs follow-up]` : '';
      return `${c.contactName || c.fullName || 'Unknown'} | ${channel} | ${direction}${unread}${stale}\nLast: "${(c.lastMessageBody || '').substring(0, 120)}" (${lastDate})`;
    });
    const unreadCount = convos.filter(c => c.unreadCount > 0).length;
    const staleCount  = convos.filter(c => (now - c.lastMessageDate) / oneDayMs >= 3).length;
    let summary = `GHL Conversations — ${convos.length} total | ${unreadCount} unread | ${staleCount} need follow-up\n\n`;
    summary += lines.join('\n\n');
    return summary;
  } catch (err) {
    return `GHL conversations error: ${err.message}`;
  }
}

// ─── SLACK CHANNEL READ ───────────────────────────────────────────────────────
async function readSlackChannel(channelName, messageCount = 20) {
  const linkMatch = channelName.match(/<#[A-Z0-9]+\|([^>]+)>/);
  const cleanName = linkMatch ? linkMatch[1] : channelName.replace('#', '');
  const result = await slack.client.conversations.list({
    limit: 200,
    types: 'public_channel,private_channel,mpim,im'
  });
  const channel = result.channels.find(c => c.name === cleanName);
  if (!channel) return `Channel ${channelName} not found or agent not invited.`;
  try {
    const history = await slack.client.conversations.history({
      channel: channel.id,
      limit: Math.min(messageCount, 20)
    });
    if (!history.messages.length) return 'No recent messages found.';
    const messages = history.messages.reverse().map(m => {
      const time = new Date(parseFloat(m.ts) * 1000).toLocaleString('en-US', {
        timeZone: 'America/Costa_Rica', month: 'short', day: 'numeric',
        hour: '2-digit', minute: '2-digit'
      });
      return `[${time}] ${(m.text || '').substring(0, 300)}`;
    });
    return messages.join('\n');
  } catch (err) {
    return `Error reading channel: ${err.message}`;
  }
}


// ─── PORTAL: WEEKLY TREND ANALYSIS ───────────────────────────────────────────
async function runWeeklyPortalTrends() {
  console.log('Running weekly portal trend analysis...');
  try {
    // Lean query — only the columns we need
    const { data: dashboards } = await portalSupabase
      .from('client_dashboards')
      .select('id, client_name, customer_status, customer_type, created_at')
      .eq('is_active', true);

    const { data: templates } = await portalSupabase
      .from('customer_activity_templates')
      .select('id, title, order_index');
    const tMap = {};
    (templates || []).forEach(t => { tMap[t.id] = t.title; });

    const { data: allActs } = await portalSupabase
      .from('customer_activities')
      .select('customer_id, template_id, status, assigned_to, completed_at');

    if (!dashboards || !allActs) return;

    const clientMap = {};
    dashboards.forEach(d => { clientMap[d.id] = d; });

    // Trend 1: Phase distribution
    const phaseCounts = dashboards.reduce((acc, d) => {
      acc[d.customer_status] = (acc[d.customer_status]||0) + 1;
      return acc;
    }, {});

    // Trend 2: Most blocked activities
    const blockedByActivity = {};
    allActs.filter(a => a.status === 'blocked').forEach(a => {
      const title = tMap[a.template_id] || 'Unknown';
      blockedByActivity[title] = (blockedByActivity[title]||0) + 1;
    });
    const topBlocked = Object.entries(blockedByActivity)
      .sort((a,b) => b[1]-a[1]).slice(0,5)
      .map(([t,c]) => `${t} (${c}x)`).join(', ');

    // Trend 3: Team workload — pending activities per assignee
    const pendingByAssignee = {};
    allActs.filter(a => a.status === 'phase_1' || a.status === 'phase_2').forEach(a => {
      const email = (a.assigned_to || 'unassigned').split('@')[0];
      pendingByAssignee[email] = (pendingByAssignee[email]||0) + 1;
    });
    const workload = Object.entries(pendingByAssignee)
      .sort((a,b) => b[1]-a[1])
      .map(([e,c]) => `${e}: ${c} pending`).join(' | ');

    // Trend 4: Avg days to reach live from created_at
    const liveClients = dashboards.filter(d => d.customer_status === 'live' && d.created_at);
    const avgDaysToLive = liveClients.length > 0
      ? Math.round(liveClients.reduce((sum, d) => sum + Math.floor((Date.now() - new Date(d.created_at).getTime()) / (1000*60*60*24)), 0) / liveClients.length)
      : null;

    // Trend 5: Flywheel vs full-service split
    const fwCount = dashboards.filter(d => d.customer_type === 'flywheel-ai').length;
    const fsCount = dashboards.filter(d => d.customer_type === 'full-service').length;

    const today = new Date().toLocaleDateString('en-US', { weekday:'long', month:'long', day:'numeric', timeZone:'America/Costa_Rica' });

    const trendReport = [
      `Week ending ${today}:`,
      `Phase distribution: ${Object.entries(phaseCounts).map(([k,v])=>`${k}:${v}`).join(', ')}`,
      `Client mix: ${fwCount} Flywheel AI, ${fsCount} Full Service`,
      topBlocked ? `Top blocked activities: ${topBlocked}` : 'No blocked activities this week.',
      workload ? `Team workload: ${workload}` : '',
      avgDaysToLive ? `Avg days since onboarding for live clients: ${avgDaysToLive} days` : ''
    ].filter(Boolean).join(' | ');

    // Save trends to knowledge base
    await upsertKnowledge('intel', `weekly-trends-${new Date().toISOString().slice(0,10)}`, trendReport, 'weekly-cron');

    // Also save top blocked as a process insight
    if (topBlocked) {
      await upsertKnowledge('process', 'recurring-blocked-activities', `As of ${today}: Most blocked activities are: ${topBlocked}. Review with Josue and Felipe.`, 'weekly-cron');
    }

    // Save workload insight
    if (workload) {
      await upsertKnowledge('team', 'current-workload', `As of ${today}: ${workload}`, 'weekly-cron');
    }

    console.log('Weekly trend analysis complete.');
    await postToSlack(AGENT_CHANNEL, `📊 *Weekly trend analysis saved* — check knowledge base for latest intel and process insights.`);
  } catch (err) {
    console.error('Weekly trend error:', err.message);
  }
}

// ─── PORTAL: MONDAY GAP DETECTION ────────────────────────────────────────────
async function runMondayGapDetection() {
  console.log('Running Monday gap detection...');
  try {
    const { data: dashboards } = await portalSupabase
      .from('client_dashboards')
      .select('id, client_name, email, customer_status, customer_type, created_at')
      .eq('is_active', true)
      .in('customer_status', ['phase_1', 'phase_2', 'blocked']);

    if (!dashboards || !dashboards.length) {
      console.log('No at-risk clients detected.');
      return;
    }

    const { data: templates } = await portalSupabase
      .from('customer_activity_templates')
      .select('id, title, order_index');
    const tMap = {};
    (templates || []).forEach(t => { tMap[t.id] = t.title; });

    const now = Date.now();
    const gaps = [];

    for (const dash of dashboards) {
      const daysSince = dash.created_at
        ? Math.floor((now - new Date(dash.created_at).getTime()) / (1000*60*60*24))
        : 0;

      // Get their pending/blocked activities
      const { data: onboarding } = await portalSupabase
        .from('customer_onboarding')
        .select('id')
        .eq('email', dash.email)
        .limit(1);

      if (!onboarding?.[0]) continue;

      const { data: acts } = await portalSupabase
        .from('customer_activities')
        .select('template_id, status, assigned_to, updated_at')
        .eq('customer_id', onboarding[0].id)
        .in('status', ['blocked', 'phase_1', 'phase_2']);

      if (!acts?.length) continue;

      // Flag if any activity hasn't been updated in 72hrs
      const staleActs = acts.filter(a => {
        const updatedAt = a.updated_at ? new Date(a.updated_at).getTime() : 0;
        return (now - updatedAt) > (72 * 60 * 60 * 1000);
      });

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

    if (!gaps.length) {
      console.log('Gap detection: no critical gaps found.');
      return;
    }

    const today = new Date().toLocaleDateString('en-US', { weekday:'long', month:'long', day:'numeric', timeZone:'America/Costa_Rica' });
    const message = `Good morning team. Here's your Monday delivery gap report for ${today}:\n\n${gaps.join('\n')}\n\nTag the responsible team member and confirm resolution by EOD.`;

    await postToSlack(OPS_CHANNEL, message);
    console.log(`Gap detection: ${gaps.length} gaps posted to ops channel.`);
    await postToSlack(AGENT_CHANNEL, `🔍 *Monday gap detection complete* — ${gaps.length} issue(s) posted to #ng-fullfillment-ops.`);
  } catch (err) {
    console.error('Gap detection error:', err.message);
  }
}

// ─── NIGHTLY LEARNING ─────────────────────────────────────────────────────────
async function runNightlyLearning() {
  console.log('Running nightly learning cycle...');
  try {
    const channels = ['ng-fullfillment-ops', 'ng-sales-goats', 'ng-new-client-alerts', 'ng-app-and-systems-improvents'];
    let digest = '';
    for (const ch of channels) {
      const messages = await readSlackChannel(ch, 20);
      if (!messages.includes('not found')) digest += `\n\n=== ${ch} ===\n${messages}`;
    }

    // ── Lean portal snapshot — only blocked + recently changed activities ──
    try {
      const { data: dashboards } = await portalSupabase
        .from('client_dashboards')
        .select('id, client_name, email, customer_status, customer_type')
        .eq('is_active', true);

      const { data: templates } = await portalSupabase
        .from('customer_activity_templates')
        .select('id, title, order_index');
      const tMap = {};
      (templates || []).forEach(t => { tMap[t.id] = t.title; });

      // Only pull blocked and activities completed/changed today
      const today = new Date();
      today.setHours(0,0,0,0);
      const { data: recentActs } = await portalSupabase
        .from('customer_activities')
        .select('customer_id, template_id, status, assigned_to, completed_at, notes')
        .or(`status.eq.blocked,completed_at.gte.${today.toISOString()}`);

      if (dashboards && recentActs) {
        const clientMap = {};
        dashboards.forEach(d => { clientMap[d.id] = d; });

        const blocked = recentActs.filter(a => a.status === 'blocked');
        const completedToday = recentActs.filter(a => a.completed_at && a.completed_at >= today.toISOString());

        const portalSummary = [
          `PORTAL SNAPSHOT (${new Date().toLocaleDateString('en-US', {timeZone:'America/Costa_Rica'})}):`,
          `Total active: ${dashboards.length} | Live: ${dashboards.filter(d=>d.customer_status==='live').length} | Phase 1: ${dashboards.filter(d=>d.customer_status==='phase_1').length} | Phase 2: ${dashboards.filter(d=>d.customer_status==='phase_2').length} | Phase 3: ${dashboards.filter(d=>d.customer_status==='phase_3').length} | Blocked: ${dashboards.filter(d=>d.customer_status==='blocked').length}`,
          blocked.length > 0 ? `Blocked activities (${blocked.length}): ${blocked.map(a => `${clientMap[a.customer_id]?.client_name||'Unknown'} → ${tMap[a.template_id]||'Unknown'}${a.notes ? ` (note: ${a.notes.substring(0,80)})` : ''}`).join(' | ')}` : 'No blocked activities.',
          completedToday.length > 0 ? `Completed today (${completedToday.length}): ${completedToday.map(a => `${clientMap[a.customer_id]?.client_name||'Unknown'} → ${tMap[a.template_id]||'Unknown'}`).join(' | ')}` : 'No completions today.'
        ].join('\n');

        digest += `\n\n=== PORTAL ===\n${portalSummary}`;
      }
    } catch (portalErr) {
      console.error('Portal snapshot error in nightly learning:', portalErr.message);
    }

    if (!digest) return;
    const today = new Date().toLocaleDateString('en-US', { weekday: 'long', month: 'long', day: 'numeric' });
    const learningPrompt = `You are the NeuroGrowth PM agent. Today is ${today}.

Below is today's activity from key Slack channels. Extract and summarize:
1. Client status updates (who is blocked, who launched, who needs attention)
2. Team decisions made today
3. Open action items that were not resolved
4. Any patterns or recurring issues worth noting

Format each insight as: CATEGORY | KEY | VALUE
Categories: client, team, process, decision, alert

Keep each value under 150 words. Only extract meaningful operational intelligence.

${digest}`;
    const response = await anthropic.messages.create({
      model: 'claude-sonnet-4-6',
      max_tokens: 1024,
      messages: [{ role: 'user', content: learningPrompt }]
    });
    const text  = response.content.filter(b => b.type === 'text').map(b => b.text).join('\n');
    const lines = text.split('\n').filter(l => l.includes('|'));
    let saved = 0;
    for (const line of lines) {
      const parts = line.split('|').map(p => p.trim());
      if (parts.length >= 3) {
        const [category, key, ...valueParts] = parts;
        const value = valueParts.join('|').trim();
        if (category && key && value) {
          await upsertKnowledge(category.toLowerCase(), key, value, 'nightly-learning');
          saved++;
        }
      }
    }
    console.log(`Nightly learning complete. ${saved} knowledge entries saved.`);

    // Post confirmation to agent channel so Ron knows it ran
    const summary = [
      `🧠 *Nightly learning complete* — ${new Date().toLocaleDateString('en-US', {weekday:'long', month:'long', day:'numeric', timeZone:'America/Costa_Rica'})}`,
      `Slack channels scanned: 4 | Knowledge entries saved: ${saved}`,
    ].join('\n');
    await postToSlack(AGENT_CHANNEL, summary);
  } catch (err) {
    console.error('Nightly learning error:', err.message);
  }
}

// ─── PROACTIVE ALERTS ─────────────────────────────────────────────────────────
async function runProactiveAlerts() {
  console.log('Running proactive alert check...');
  try {
    const { data, error } = await supabase
      .from('agent_knowledge')
      .select('key, value, updated_at')
      .eq('category', 'alert')
      .order('updated_at', { ascending: true });
    if (error || !data || !data.length) return;
    const now = Date.now();
    const oneDayMs = 24 * 60 * 60 * 1000;
    const staleAlerts = data.filter(a => (now - new Date(a.updated_at).getTime()) > oneDayMs);
    if (!staleAlerts.length) return;
    const alertText = staleAlerts.map(a => `${a.key}: ${a.value}`).join('\n\n');
    const prompt = `You are the NeuroGrowth PM agent checking on unresolved alerts.

These items have been flagged as alerts and have not been updated in over 24 hours:

${alertText}

Write a brief, direct message to Ron (2-4 sentences) summarizing what is still unresolved and what needs his attention today. No markdown formatting. Sound like a colleague, not a report.`;
    const response = await anthropic.messages.create({
      model: 'claude-sonnet-4-6',
      max_tokens: 256,
      messages: [{ role: 'user', content: prompt }]
    });
    const message = response.content.filter(b => b.type === 'text').map(b => b.text).join('\n');
    await postToSlack(AGENT_CHANNEL, message);
    console.log(`Proactive alert posted. ${staleAlerts.length} unresolved items flagged.`);
  } catch (err) {
    console.error('Proactive alert error:', err.message);
  }
}

// ─── FILE PROCESSING ──────────────────────────────────────────────────────────
async function downloadSlackFile(fileUrl) {
  const res = await fetch(fileUrl, {
    headers: { 'Authorization': `Bearer ${process.env.SLACK_BOT_TOKEN}` }
  });
  if (!res.ok) throw new Error(`Failed to download file: ${res.status}`);
  const buffer = await res.arrayBuffer();
  return Buffer.from(buffer);
}

async function resizeImageIfNeeded(fileBuffer, mimeType) {
  if (mimeType === 'application/pdf' || mimeType === 'image/gif') return { buffer: fileBuffer, mimeType };
  try {
    const image    = sharp(fileBuffer);
    const metadata = await image.metadata();
    const maxDimension = 1200;
    if (metadata.width <= maxDimension && metadata.height <= maxDimension) return { buffer: fileBuffer, mimeType };
    const isLandscape = metadata.width > metadata.height;
    const resized = await image
      .resize(isLandscape ? maxDimension : null, isLandscape ? null : maxDimension, { fit: 'inside', withoutEnlargement: true })
      .jpeg({ quality: 85 })
      .toBuffer();
    console.log(`Image resized from ${metadata.width}x${metadata.height} to fit ${maxDimension}px`);
    return { buffer: resized, mimeType: 'image/jpeg' };
  } catch (err) {
    console.error('Image resize error:', err.message);
    return { buffer: fileBuffer, mimeType };
  }
}

async function processFileWithClaude(fileBuffer, mimeType, userInstruction, systemPrompt) {
  let finalBuffer   = fileBuffer;
  let finalMimeType = mimeType;
  if (mimeType.startsWith('image/')) {
    const resized = await resizeImageIfNeeded(fileBuffer, mimeType);
    finalBuffer   = resized.buffer;
    finalMimeType = resized.mimeType;
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
    model: 'claude-sonnet-4-6',
    max_tokens: 1024,
    system: systemPrompt,
    messages: [{
      role: 'user',
      content: [
        contentBlock,
        { type: 'text', text: userInstruction || 'Analyze this file and provide a useful summary. Extract any action items, key information, or insights relevant to NeuroGrowth operations.' }
      ]
    }]
  });
  return response.content.filter(b => b.type === 'text').map(b => b.text).join('\n');
}

function getFileMimeType(filename, mimeType) {
  if (mimeType && mimeType !== 'application/octet-stream') return mimeType;
  const ext = filename?.split('.').pop()?.toLowerCase();
  const map = {
    'pdf': 'application/pdf', 'png': 'image/png', 'jpg': 'image/jpeg',
    'jpeg': 'image/jpeg', 'gif': 'image/gif', 'webp': 'image/webp'
  };
  return map[ext] || mimeType;
}

// ─── AUDIO TRANSCRIPTION (WHISPER) ───────────────────────────────────────────
const AUDIO_MIME_TYPES = ['audio/webm', 'audio/mp4', 'audio/mpeg', 'audio/mp3', 'audio/ogg', 'audio/wav', 'audio/m4a'];
const AUDIO_EXTENSIONS = ['webm', 'mp4', 'mp3', 'm4a', 'ogg', 'wav'];

function isAudioFile(mimeType, filename) {
  if (mimeType && AUDIO_MIME_TYPES.some(t => mimeType.startsWith(t))) return true;
  const ext = filename?.split('.').pop()?.toLowerCase();
  return AUDIO_EXTENSIONS.includes(ext);
}

async function transcribeAudio(fileBuffer, filename) {
  const tmpPath = `/tmp/audio_${Date.now()}_${filename || 'audio.webm'}`;
  fs.writeFileSync(tmpPath, fileBuffer);

  try {
    // Polyfill File global — required for Node 18 (Node 20 has it natively)
    if (typeof globalThis.File === 'undefined') {
      const { File } = await import('node:buffer');
      globalThis.File = File;
    }

    const transcription = await openai.audio.transcriptions.create({
      file: fs.createReadStream(tmpPath),
      model: 'whisper-1',
      response_format: 'text'
    });
    return transcription; // plain string when response_format is 'text'
  } finally {
    try { fs.unlinkSync(tmpPath); } catch {}
  }
}

// ─── CLAUDE API WITH RETRY ────────────────────────────────────────────────────
async function callClaude(messages, retries = 3, userId = null) {
  let lastErr;
  for (let attempt = 0; attempt < retries; attempt++) {
    try {
      const response = await anthropic.messages.create({
        model: 'claude-sonnet-4-6',
        max_tokens: 2048,
        system: userId ? buildRoleSystemPrompt(userId) : SYSTEM_PROMPT,
        messages,
        tools: [
          {
            name: "search_notion",
            description: "Search NeuroGrowth Notion workspace for pages, tasks, client info, and SOPs",
            input_schema: { type: "object", properties: { query: { type: "string" } }, required: ["query"] }
          },
          {
            name: "get_notion_page",
            description: "Get the content of a specific Notion page by its ID",
            input_schema: { type: "object", properties: { page_id: { type: "string" } }, required: ["page_id"] }
          },
          {
            name: "get_recent_emails",
            description: "Get recent unread emails from Ron's Gmail inbox including full email body content",
            input_schema: { type: "object", properties: {} }
          },
          {
            name: "send_email",
            description: "Send an email on Ron's behalf. Always confirm before sending.",
            input_schema: {
              type: "object",
              properties: { to: { type: "string" }, subject: { type: "string" }, body: { type: "string" } },
              required: ["to", "subject", "body"]
            }
          },
          {
            name: "get_calendar_events",
            description: "Get calendar events. daysFromNow: 0=today, 1=tomorrow, -1=yesterday. daysRange: 1=day, 7=week, 14=two weeks.",
            input_schema: {
              type: "object",
              properties: { daysFromNow: { type: "number" }, daysRange: { type: "number" } }
            }
          },
          {
            name: "search_drive",
            description: "Search Ron's Google Drive for files and documents",
            input_schema: { type: "object", properties: { query: { type: "string" } }, required: ["query"] }
          },
          {
            name: "read_google_sheet",
            description: "Read the actual cell data from a Google Sheet. Use when asked to read, analyze, or summarize a spreadsheet. Accepts a Google Sheets URL or file ID. Optionally specify a range like 'Sheet1!A1:Z50'.",
            input_schema: {
              type: "object",
              properties: {
                spreadsheetId: { type: "string", description: "Google Sheets URL or spreadsheet ID" },
                range: { type: "string", description: "Optional range e.g. Sheet1!A1:Z100. Defaults to first 100 rows." }
              },
              required: ["spreadsheetId"]
            }
          },
          {
            name: "read_google_doc",
            description: "Read the text content of a Google Doc. Use when asked to read, summarize, or analyze a Google Document. Accepts a Google Docs URL or document ID.",
            input_schema: {
              type: "object",
              properties: {
                documentId: { type: "string", description: "Google Docs URL or document ID" }
              },
              required: ["documentId"]
            }
          },
          {
            name: "read_slack_channel",
            description: "Read recent messages from a NeuroGrowth Slack channel. Always use this tool when asked about channel activity — never answer from memory.",
            input_schema: {
              type: "object",
              properties: {
                channelName: { type: "string", description: "Channel name e.g. ng-fullfillment-ops, ng-sales-goats, ng-ops-management, ng-new-client-alerts, ng-app-and-systems-improvents" },
                messageCount: { type: "number", description: "Messages to pull, max 20" }
              },
              required: ["channelName"]
            }
          },
          {
            name: "draft_channel_post",
            description: "Prepare a Slack channel post for Ron's approval before sending.",
            input_schema: {
              type: "object",
              properties: { channelName: { type: "string" }, message: { type: "string" } },
              required: ["channelName", "message"]
            }
          },
          {
            name: "get_ghl_conversations",
            description: "Get recent GHL conversations — prospects and contacts across all channels (Instagram, SMS, email, etc). Shows unread messages and contacts that need follow-up. Use when asked about prospects, leads, inbound messages, or who needs a response.",
            input_schema: {
              type: "object",
              properties: {
                limit: { type: "number", description: "Number of conversations to pull, default 20" },
                unreadOnly: { type: "boolean", description: "Set true to only show unread conversations" }
              }
            }
          },
          {
            name: "search_knowledge",
            description: "Search the agent's long-term knowledge base for accumulated intelligence about clients, team, processes, and decisions.",
            input_schema: {
              type: "object",
              properties: {
                query: { type: "string" },
                category: { type: "string", description: "Optional: client, team, process, decision, alert, intel" }
              },
              required: ["query"]
            }
          },
          {
            name: "save_knowledge",
            description: "Save an important insight to long-term memory. Use when Ron shares important context or when a pattern emerges.",
            input_schema: {
              type: "object",
              properties: {
                category: { type: "string", description: "client, team, process, decision, alert, or intel" },
                key: { type: "string", description: "Short identifier e.g. Max Valverde or onboarding bottleneck" },
                value: { type: "string", description: "The knowledge to store" }
              },
              required: ["category", "key", "value"]
            }
          },
          {
            name: "get_knowledge_category",
            description: "Get all knowledge entries for a specific category.",
            input_schema: {
              type: "object",
              properties: { category: { type: "string", description: "client, team, process, decision, alert, or intel" } },
              required: ["category"]
            }
          }
,
          {
            name: "get_client_status",
            description: "ALWAYS use this tool (NOT Notion) when asked about client onboarding status, client phases, portal status, where a client is in their onboarding, what activities are pending, or what clients are in the system. This queries the live NeuroGrowth Supabase portal database directly. Do not use Notion for this — Notion does not have this data.",
            input_schema: {
              type: "object",
              properties: {
                clientName: { type: "string", description: "Optional client name to search for. Leave empty to get all clients." }
              }
            }
          },
          {
            name: "get_portal_alerts",
            description: "ALWAYS use this tool (NOT Notion) when asked about launch risks, clients behind on their 14-day window, overdue clients, or who needs attention in fulfillment. Queries live Supabase portal data.",
            input_schema: { type: "object", properties: {} }
          }
,
          {
            name: "create_notion_task",
            description: "Create a task in NeuroGrowth Notion. Operational/recurring tasks (client delivery, campaigns, team ops, SOPs, account management) go to Operations Tracking. Project/strategic tasks (new features, sprint work, integrations, R&D) go to Project Sprint Tracking. Ask the user which type if unclear.",
            input_schema: {
              type: "object",
              properties: {
                title: { type: "string", description: "Task title" },
                taskType: { type: "string", description: "operational (default) or project" },
                priority: { type: "string", description: "P0 - Critical Customer Impact | P1 - High Business Impact | P2 - Growth & Scalability (default) | P3 - Strategic Initiatives" },
                dueDate: { type: "string", description: "Due date in YYYY-MM-DD format (optional)" },
                notes: { type: "string", description: "Additional context, milestone, or notes (optional)" },
                customer: { type: "string", description: "Customer name if task is client-related e.g. Neurogrowth, Build & Release, Full Service (optional)" }
              },
              required: ["title"]
            }
          },
          {
            name: "create_scheduled_task",
            description: "Create a new recurring scheduled task that Max will run automatically. Use when Ron asks Max to do something on a recurring schedule like every Monday, every weekday at 9am, etc.",
            input_schema: {
              type: "object",
              properties: {
                name: { type: "string", description: "Short name for the task e.g. Weekly client health check" },
                schedule: { type: "string", description: "Natural language schedule e.g. every Monday at 9am, every weekday at 5pm" },
                prompt: { type: "string", description: "The instruction Max will execute at each scheduled run" },
                channel: { type: "string", description: "Slack channel to post results to e.g. #ng-pm-agent, #ng-fullfillment-ops" }
              },
              required: ["name", "schedule", "prompt"]
            }
          },
          {
            name: "list_scheduled_tasks",
            description: "List all scheduled tasks Max is currently running. Use when asked what recurring tasks Max has, what automations are active, or what scheduled jobs exist.",
            input_schema: { type: "object", properties: {} }
          },
          {
            name: "clean_duplicate_tasks",
            description: "Find and delete duplicate scheduled tasks from Supabase. Keeps the oldest version of each task name. Use when asked to clean up or deduplicate scheduled tasks.",
            input_schema: { type: "object", properties: {} }
          },
          {
            name: "delete_scheduled_task",
            description: "Deactivate and stop a scheduled task by its ID. Use when asked to cancel, stop, or remove a recurring task.",
            input_schema: {
              type: "object",
              properties: {
                taskId: { type: "string", description: "The task ID from list_scheduled_tasks" }
              },
              required: ["taskId"]
            }
          }
,
          {
            name: "get_meta_ads_summary",
            description: "Get NeuroGrowth Meta Ads account-level performance summary — spend, impressions, reach, clicks, CTR, CPC, CPM, leads, and CPL. Use when asked about ad performance, how ads are doing, spend, or results.",
            input_schema: {
              type: "object",
              properties: {
                datePreset: { type: "string", description: "Date range: today, yesterday, last_7d (default), last_14d, last_30d, last_month, this_month, this_quarter" }
              }
            }
          },
          {
            name: "get_meta_campaigns",
            description: "Get Meta Ads campaign-level breakdown — name, status, spend, clicks, CTR, leads per campaign. Use when asked about specific campaigns or campaign performance.",
            input_schema: {
              type: "object",
              properties: {
                datePreset: { type: "string", description: "Date range: last_7d (default), last_14d, last_30d, this_month" },
                limit: { type: "number", description: "Number of campaigns to return, default 10" }
              }
            }
          },
          {
            name: "get_meta_adsets",
            description: "Get Meta Ads ad set level breakdown — budget, spend, clicks, CTR per ad set. Use when asked about ad sets, targeting, or budget allocation.",
            input_schema: {
              type: "object",
              properties: {
                campaignId: { type: "string", description: "Optional campaign ID to filter ad sets by campaign" },
                datePreset: { type: "string", description: "Date range: last_7d (default), last_14d, last_30d, this_month" }
              }
            }
          },
          {
            name: "get_meta_ads",
            description: "Get individual ad-level performance — spend, impressions, clicks, CTR, leads per ad creative. Use when asked about specific ads or creative performance.",
            input_schema: {
              type: "object",
              properties: {
                adSetId: { type: "string", description: "Optional ad set ID to filter ads" },
                datePreset: { type: "string", description: "Date range: last_7d (default), last_14d, last_30d, this_month" }
              }
            }
          }
        ]
      });

      if (response.stop_reason === 'tool_use') {
        const toolUses   = response.content.filter(b => b.type === 'tool_use');
        const toolResults = await Promise.all(toolUses.map(async (toolUse) => {
          let result;
          try {
            if      (toolUse.name === 'search_notion')        result = await searchNotion(toolUse.input.query);
            else if (toolUse.name === 'get_notion_page')      result = await getNotionPage(toolUse.input.page_id);
            else if (toolUse.name === 'get_recent_emails')    result = await getRecentEmails();
            else if (toolUse.name === 'send_email')           result = await sendEmail(toolUse.input.to, toolUse.input.subject, toolUse.input.body);
            else if (toolUse.name === 'get_calendar_events')  result = await getCalendarEvents(toolUse.input.daysFromNow || 0, toolUse.input.daysRange || 1);
            else if (toolUse.name === 'search_drive') {
              const driveResult = await searchDrive(toolUse.input.query);
              result = driveResult.length > 4000 ? driveResult.substring(0, 4000) + '...[trimmed]' : driveResult;
            }
            else if (toolUse.name === 'read_google_sheet') {
              const sheetId = extractGoogleFileId(toolUse.input.spreadsheetId);
              result = await readGoogleSheet(sheetId, toolUse.input.range || null);
            }
            else if (toolUse.name === 'read_google_doc') {
              const docId = extractGoogleFileId(toolUse.input.documentId);
              result = await readGoogleDoc(docId);
            }
            else if (toolUse.name === 'read_slack_channel')   result = await readSlackChannel(toolUse.input.channelName, toolUse.input.messageCount || 20);
            else if (toolUse.name === 'draft_channel_post')   result = `APPROVAL_NEEDED|${toolUse.input.channelName}|${toolUse.input.message}`;
            else if (toolUse.name === 'get_ghl_conversations') result = await getGHLConversations(toolUse.input.limit || 20, toolUse.input.unreadOnly || false);
            else if (toolUse.name === 'search_knowledge')     result = await searchKnowledge(toolUse.input.query, toolUse.input.category);
            else if (toolUse.name === 'save_knowledge')       result = await upsertKnowledge(toolUse.input.category, toolUse.input.key, toolUse.input.value, 'conversation');
            else if (toolUse.name === 'get_knowledge_category') result = await getAllKnowledgeByCategory(toolUse.input.category);
            else if (toolUse.name === 'get_client_status')     result = await getClientStatus(toolUse.input.clientName || null);
            else if (toolUse.name === 'get_portal_alerts')      result = await getPortalAlerts();
            else if (toolUse.name === 'create_notion_task')      result = await createNotionTask(toolUse.input.title, toolUse.input.taskType || 'operational', toolUse.input.priority || 'P2 - Growth & Scalability', toolUse.input.dueDate, toolUse.input.notes, toolUse.input.customer);
            else if (toolUse.name === 'create_scheduled_task')   result = await createScheduledTask(toolUse.input.name, toolUse.input.schedule, toolUse.input.prompt, toolUse.input.channel, userId);
            else if (toolUse.name === 'list_scheduled_tasks')    result = await listScheduledTasks();
            else if (toolUse.name === 'clean_duplicate_tasks')   result = await cleanDuplicateTasks();
            else if (toolUse.name === 'delete_scheduled_task')   result = await deleteScheduledTask(toolUse.input.taskId);
            else if (toolUse.name === 'get_meta_ads_summary')      result = await getMetaAdsSummary(toolUse.input.datePreset || 'last_7d');
            else if (toolUse.name === 'get_meta_campaigns')        result = await getMetaCampaigns(toolUse.input.datePreset || 'last_7d', toolUse.input.limit || 10);
            else if (toolUse.name === 'get_meta_adsets')           result = await getMetaAdSets(toolUse.input.campaignId || null, toolUse.input.datePreset || 'last_7d');
            else if (toolUse.name === 'get_meta_ads')              result = await getMetaAds(toolUse.input.adSetId || null, toolUse.input.datePreset || 'last_7d');
          } catch (err) {
            result = `Error running tool ${toolUse.name}: ${err.message}`;
          }
          return { type: 'tool_result', tool_use_id: toolUse.id, content: JSON.stringify(result) };
        }));

        const draftResult = toolResults.find(r => {
          try { return JSON.parse(r.content).startsWith('APPROVAL_NEEDED|'); } catch { return false; }
        });
        if (draftResult) {
          const parts = JSON.parse(draftResult.content).split('|');
          return `APPROVAL_NEEDED|${parts[1]}|${parts.slice(2).join('|')}`;
        }

        // followUp also gets retry logic for 529s
        let followUpText = null;
        for (let fuAttempt = 0; fuAttempt < 3; fuAttempt++) {
          try {
            const followUp = await anthropic.messages.create({
              model: 'claude-sonnet-4-6',
              max_tokens: 1024,
              system: userId ? buildRoleSystemPrompt(userId) : SYSTEM_PROMPT,
              messages: [
                ...messages,
                { role: 'assistant', content: response.content },
                { role: 'user', content: toolResults }
              ]
            });
            followUpText = followUp.content.filter(b => b.type === 'text').map(b => b.text).join('\n') || null;
            // Empty reply guard on followUp
            if (!followUpText && fuAttempt < 2) {
              console.error('Empty followUp reply, retrying...');
              continue;
            }
            break;
          } catch (fuErr) {
            if ((fuErr.status === 529 || fuErr.status === 503) && fuAttempt < 2) {
              const wait = (fuAttempt + 1) * 10000;
              console.log(`followUp overloaded, retrying in ${wait/1000}s...`);
              await new Promise(r => setTimeout(r, wait));
            } else { throw fuErr; }
          }
        }
        return followUpText;
      }

      const responseText = response.content.filter(b => b.type === 'text').map(b => b.text).join('\n');
      return responseText || null;

    } catch (err) {
      lastErr = err;
      if (err.status === 529 || err.status === 503 || err.status === 500) {
        const wait = err.status === 529 ? (attempt + 1) * 10000 : (attempt + 1) * 4000;
        console.log(`API overloaded (attempt ${attempt + 1}/${retries}), retrying in ${wait / 1000}s...`);
        await new Promise(r => setTimeout(r, wait));
      } else {
        throw err;
      }
    }
  }
  throw lastErr;
}

async function postToSlack(channel, text, threadTs = null) {
  if (!text || !text.trim()) { console.error('postToSlack called with empty text, skipping.'); return; }
  const channelName = channel.startsWith('#') ? channel.slice(1) : channel;
  const payload = { channel: channelName, text };
  if (threadTs) payload.thread_ts = threadTs;
  await slack.client.chat.postMessage(payload);
}

async function executeChannelPost(channelName, message, say) {
  try {
    const result  = await slack.client.conversations.list({ limit: 200, types: 'public_channel,private_channel,mpim,im' });
    const channel = result.channels.find(c => c.name === channelName.replace('#', ''));
    if (!channel) {
      await say(`Could not find channel ${channelName}.`);
    } else {
      await slack.client.chat.postMessage({ channel: channel.id, text: message });
      await say(`Posted to ${channelName}.`);
    }
  } catch (err) {
    await say(`Something went wrong posting: ${err.message}`);
  }
}

async function checkApproval(message, say, userId) {
  const pending = pendingApprovals[userId];
  if (!pending) return false;
  const text = (typeof message === 'string' ? message : message.text || '').toLowerCase().trim();
  if (['yes', 'send it', 'approved', 'go ahead', '👍'].includes(text)) {
    await executeChannelPost(pending.channelName, pending.message, say);
    delete pendingApprovals[userId];
    return true;
  }
  if (['no', 'cancel', 'stop'].includes(text)) {
    await say('Cancelled. Nothing was posted.');
    delete pendingApprovals[userId];
    return true;
  }
  return false;
}

function handleDraftReply(reply, userId, say) {
  if (!reply.startsWith('APPROVAL_NEEDED|')) return false;
  const parts        = reply.split('|');
  const channelName  = parts[1];
  const draftMessage = parts.slice(2).join('|');
  pendingApprovals[userId] = { channelName, message: draftMessage, createdAt: Date.now() };
  say(`Here is what I would post to *${channelName}*:\n\n"${draftMessage}"\n\nReply *yes* to send it or *no* to cancel.`);
  return true;
}

// ─── SHARED FILE HANDLER (DM + CHANNEL) ──────────────────────────────────────
async function handleFileMessage(message, say, userId, threadReply = false) {
  const file        = message.files[0];
  const instruction = message.text || null;
  const mimeType    = getFileMimeType(file.name, file.mimetype);

  // ── Audio ──
  if (isAudioFile(mimeType, file.name)) {
    const replyOpts = threadReply
      ? { text: '🎙️ Got the voice note. Transcribing...', thread_ts: message.thread_ts || message.ts }
      : '🎙️ Got the voice note. Transcribing...';
    await say(replyOpts);

    try {
      const fileBuffer = await downloadSlackFile(file.url_private);
      const transcript = await transcribeAudio(fileBuffer, file.name);

      if (!transcript || !transcript.trim()) {
        const errMsg = "Couldn't make out anything in that audio. Try again?";
        await say(threadReply ? { text: errMsg, thread_ts: message.thread_ts || message.ts } : errMsg);
        return;
      }

      console.log(`Audio transcribed (${file.name}): ${transcript.substring(0, 100)}...`);

      // Show transcript briefly, then act on it
      const transcriptNotice = `_Transcript:_ "${transcript.substring(0, 200)}${transcript.length > 200 ? '...' : ''}"`;
      await say(threadReply ? { text: transcriptNotice, thread_ts: message.thread_ts || message.ts } : transcriptNotice);

      // Feed transcript into Claude as a normal message
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

  // ── Images / PDFs ──
  const supported = ['application/pdf', 'image/png', 'image/jpeg', 'image/gif', 'image/webp'];
  if (!supported.includes(mimeType)) {
    const errMsg = `I can process images (PNG, JPG, GIF, WEBP), PDFs, and audio files. This file type (${mimeType}) isn't supported yet.`;
    await say(threadReply ? { text: errMsg, thread_ts: message.thread_ts || message.ts } : errMsg);
    return;
  }

  const ackMsg = `Got the ${mimeType.includes('pdf') ? 'PDF' : 'image'}. Give me a moment to analyze it...`;
  await say(threadReply ? { text: ackMsg, thread_ts: message.thread_ts || message.ts } : ackMsg);

  try {
    const fileBuffer = await downloadSlackFile(file.url_private);
    const result     = await processFileWithClaude(fileBuffer, mimeType, instruction, SYSTEM_PROMPT);
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

  // File uploads (images, PDFs, audio)
  if (message.subtype === 'file_share' && message.files?.length > 0) {
    await handleFileMessage(message, say, userId, false);
    return;
  }

  if (message.subtype) return;

  // Rate limit check
  if (isRateLimited(userId)) {
    await say('Slow down a bit — you are sending messages too fast. Give me a moment.');
    return;
  }

  const history = await loadHistory(userId);
  history.push({ role: 'user', content: message.text });

  try {
    let reply = await callClaude(history, 3, userId);
    if (!reply || !reply.trim()) {
      console.error('Empty reply, retrying for user:', userId);
      reply = await callClaude(history, 2, userId);
    }
    if (!reply || !reply.trim()) return;
    if (handleDraftReply(reply, userId, say)) return;
    await saveMessage(userId, 'user', message.text);
    await saveMessage(userId, 'assistant', reply);
    await say(reply);
  } catch (err) {
    console.error('Claude API error (DM):', err);
    await say('Got turned around for a second — go ahead and ask again.');
  }
});

// @mention handler
slack.event('app_mention', async ({ event, say }) => {
  if (event.bot_id) return;
  const cleanText = event.text.replace(/<@[A-Z0-9]+>/g, '').trim();
  if (!cleanText) return;

  const userId  = event.user;
  const history = await loadHistory(userId);
  history.push({ role: 'user', content: cleanText });

  try {
    let reply = await callClaude(history, 3, userId);
    if (!reply || !reply.trim()) {
      console.error('Empty reply on mention, retrying for user:', userId);
      reply = await callClaude(history, 2, userId);
    }
    if (!reply || !reply.trim()) return;
    if (handleDraftReply(reply, userId, say)) return;
    await saveMessage(userId, 'user', cleanText);
    await saveMessage(userId, 'assistant', reply);
    await say({ text: reply, thread_ts: event.thread_ts || event.ts });
  } catch (err) {
    console.error('Claude API error (mention):', err);
    await say({ text: 'Got turned around — try again.', thread_ts: event.thread_ts || event.ts });
  }
});

// #ng-pm-agent channel handler
slack.message(async ({ message, say }) => {
  if (message.subtype || message.bot_id) return;
  if (message.channel_type === 'im') return;

  let channelInfo;
  try {
    channelInfo = await slack.client.conversations.info({ channel: message.channel });
  } catch { return; }

  const channelName = channelInfo.channel?.name || '';
  if (!channelName.includes('ng-pm-agent')) return;

  const isApproval = await checkApproval(message, say, message.user);
  if (isApproval) return;

  const userId = message.user;

  // File uploads (images, PDFs, audio)
  if (message.subtype === 'file_share' && message.files?.length > 0) {
    await handleFileMessage(message, say, userId, true);
    return;
  }

  if (message.subtype) return;

  // Rate limit check
  if (isRateLimited(userId)) {
    await say({ text: 'Slow down a bit — too many messages at once. Give me a moment.', thread_ts: message.thread_ts || message.ts });
    return;
  }

  const history = await loadHistory(userId);
  history.push({ role: 'user', content: message.text });

  try {
    let reply = await callClaude(history, 3, userId);
    if (!reply || !reply.trim()) {
      console.error('Empty reply on channel, retrying for user:', userId);
      reply = await callClaude(history, 2, userId);
    }
    if (!reply || !reply.trim()) return;
    if (handleDraftReply(reply, userId, say)) return;
    await saveMessage(userId, 'user', message.text);
    await saveMessage(userId, 'assistant', reply);
    await say({ text: reply, thread_ts: message.thread_ts || message.ts });
  } catch (err) {
    console.error('Claude API error (channel):', err);
  }
});

// ─── CRON JOBS ────────────────────────────────────────────────────────────────
// Daily standup — 9:00 AM CST
cron.schedule('0 15 * * 1-5', async () => {
  const prompt = `Generate a daily standup message for the NeuroGrowth team.
Tag @Tania, @Josue, @David, @Valeria, and @Felipe.
Ask: (1) what they are working on today, (2) any blockers.
Direct and brief. Today is ${new Date().toLocaleDateString('en-US', { weekday: 'long', month: 'long', day: 'numeric' })}.`;
  try {
    const message = await callClaude([{ role: 'user', content: prompt }]);
    await postToSlack(OPS_CHANNEL, message);
    console.log('Daily standup posted.');
  } catch (err) { console.error('Standup cron error:', err); }
}, { timezone: 'America/Costa_Rica' });

// EOD check — 5:00 PM CST
cron.schedule('0 23 * * 1-5', async () => {
  const prompt = `EOD check for NeuroGrowth. Generate a brief accountability message.
Remind team to log completions. Flag unresolved standup items. 3-4 lines, direct tone.`;
  try {
    const message = await callClaude([{ role: 'user', content: prompt }]);
    await postToSlack(OPS_CHANNEL, message);
    console.log('EOD check posted.');
  } catch (err) { console.error('EOD cron error:', err); }
}, { timezone: 'America/Costa_Rica' });

// Weekly digest — Friday 4:00 PM CST
cron.schedule('0 22 * * 5', async () => {
  const prompt = `Friday EOD at NeuroGrowth. Weekly digest for Ron:
(1) What should have been completed this week — sales, delivery, ops.
(2) Key open items for Monday.
(3) One question for Ron over the weekend.
Executive summary, no fluff.`;
  try {
    const message = await callClaude([{ role: 'user', content: prompt }]);
    await postToSlack(OPS_CHANNEL, message);
    console.log('Weekly digest posted.');
  } catch (err) { console.error('Weekly digest cron error:', err); }
}, { timezone: 'America/Costa_Rica' });

// Sales call prep — 7:00 PM CST weekdays
cron.schedule('0 1 * * 1-5', async () => {
  const prompt = `Check Ron's calendar for sales calls scheduled for tomorrow.
If there are calls, post a reminder asking Tania to confirm prospect research briefs are ready.
Keep it to 2 lines.`;
  try {
    const message = await callClaude([{ role: 'user', content: prompt }]);
    await postToSlack(AGENT_CHANNEL, message);
    console.log('Sales prep reminder posted.');
  } catch (err) { console.error('Sales prep cron error:', err); }
}, { timezone: 'America/Costa_Rica' });

// Josue daily briefing -- 8:30 AM CST weekdays
cron.schedule('30 14 * * 1-5', async () => {
  console.log('Running Josue daily briefing...');
  try {
    const today = new Date().toLocaleDateString('en-US', { weekday: 'long', month: 'long', day: 'numeric' });

    const fulfillmentActivity = await readSlackChannel('ng-fullfillment-ops', 20);
    const alerts = await getAllKnowledgeByCategory('alert');
    const clientKnowledge = await searchKnowledge('blocked', 'client');
    let portalAlerts = 'Portal data unavailable.';
    try { portalAlerts = await getPortalAlerts(); } catch (e) { console.error('Portal alerts failed in Josue briefing:', e.message); }

    const dayOfWeek = new Date().toLocaleDateString('en-US', { weekday: 'long' });
    const isMonday = dayOfWeek === 'Monday';
    const isFriday = dayOfWeek === 'Friday';

    const roleReminder = isMonday
      ? `Today is Monday — Josue has his 60-minute Campaign Fix session at 9AM. Flag any RED or YELLOW campaigns that need attention.`
      : isFriday
      ? `Today is Friday — Josue does his weekly portfolio performance deep dive. Remind him to update GREEN/YELLOW/RED status for each active DFY client.`
      : `It's ${dayOfWeek} — standard ops day.`;

    const prompt = `You are Max, the NeuroGrowth PM Agent. Today is ${today}.

You are preparing a morning briefing for Josue, the Technical Operations Manager. He owns the 14-day client launch cycle and ongoing DFY campaign performance. ${roleReminder}

Recent activity from ng-fullfillment-ops:
${fulfillmentActivity}

Active alerts:
${alerts}

Blocked or at-risk clients:
${clientKnowledge}

Portal launch risk:
${portalAlerts}

Write Josue a direct, useful morning briefing:
- What carried over from yesterday that needs his attention
- Any clients at risk of missing their 14-day launch window or SLA breach
- One clear first action to take this morning

Keep it under 150 words. Sound like a sharp colleague, not a report. No markdown formatting.`;

    const message = await callClaude([{ role: 'user', content: prompt }]);
    if (!message || !message.trim()) return;

    await slack.client.chat.postMessage({
      channel: 'U08ABBFNGUW',
      text: `Good morning Josue!\n\n${message}`
    });
    console.log('Josue daily briefing sent.');
  } catch (err) {
    console.error('Josue briefing cron error:', err.message);
  }
}, { timezone: 'America/Costa_Rica' });

// Proactive alerts — 9:00 AM and 2:00 PM CST
cron.schedule('0 15 * * *', async () => { await runProactiveAlerts(); }, { timezone: 'America/Costa_Rica' });
cron.schedule('0 20 * * *', async () => { await runProactiveAlerts(); }, { timezone: 'America/Costa_Rica' });

// Nightly learning — 11:30 PM CST (extended with portal digest)
cron.schedule('30 5 * * *', async () => { await runNightlyLearning(); }, { timezone: 'America/Costa_Rica' });

// Weekly trend analysis — Friday 4:30 PM CST (runs before weekly digest)
cron.schedule('30 22 * * 5', async () => { await runWeeklyPortalTrends(); }, { timezone: 'America/Costa_Rica' });

// Monday gap detection — 8:00 AM CST (runs before Josue's 8:30 briefing)
cron.schedule('0 14 * * 1', async () => { await runMondayGapDetection(); }, { timezone: 'America/Costa_Rica' });


// ─── GHL LEAD WEBHOOK ─────────────────────────────────────────────────────────
// GHL fires this when a contact is created or assigned
// Endpoint: POST /webhook/ghl-lead
// Set this URL in GHL: https://your-railway-url.railway.app/webhook/ghl-lead

// Map GHL user IDs to display names (for channel notifications)
const GHL_USER_NAMES = {
  'cuttpcov7ztlvyjkhdx8': 'Joseph Salazar',
  'cUTTPGov7ZTLvyjKHdX8': 'Joseph Salazar',
  '5orsahkh2joujb5fczrp': 'Debbanny',
  '5OrSaHkh2joUjB5FCZrP': 'Debbanny',
  'gqymykpddltdxvbkfl2c': 'Jonnathan Navarrete',
  'gqYMYkpDDlTdxvBkfl2C': 'Jonnathan Navarrete',
  'izlta0jy5orkymsyltjv': 'Jose Carranza',
  'izLTA0jy5OrKyMvyltjV': 'Jose Carranza',
};

// Map GHL user names/IDs to Slack user IDs
const GHL_TO_SLACK = {
  // By name (lowercase match)
  'joseph':              'U0A9J00EMGD',  // Joseph Salazar — setter
  'joseph salazar':      'U0A9J00EMGD',
  'debbanny':            'U0AR16QVDB3',  // Debbanny Romero — setter
  'debanny':             'U0AR16QVDB3',
  'debbanny neurogrowth':'U0AR16QVDB3',
  'debbanny romero':     'U0AR16QVDB3',
  'jonnathan':           'U0AMTEKDCPN',  // Jonnathan Navarrete — closer
  'jonathan':            'U0AMTEKDCPN',
  'jose':                'U0AMTEKDCPN',  // Jose Carranza — closer
  'jose carranza':       'U0AMTEKDCPN',
  // By GHL user ID (from My Staff page)
  'cuttpcov7ztlvyjkhdx8': 'U0A9J00EMGD',  // Joseph Salazar
  '5orsahkh2joujb5fczrp': 'U0AR16QVDB3',  // Debbanny Neurogrowth
  'gqymykpddltdxvbkfl2c': 'U0AMTEKDCPN',  // Jonnathan Navarrete
  'izlta0jy5orkymsyltjv': 'U0AMTEKDCPN',  // Jose Carranza
};

function resolveSetterSlackId(assignedUser) {
  if (!assignedUser) return null;
  const lower = assignedUser.toLowerCase().trim();
  // Try exact match first (for IDs)
  if (GHL_TO_SLACK[lower]) return GHL_TO_SLACK[lower];
  // Try partial match (for names)
  for (const [key, slackId] of Object.entries(GHL_TO_SLACK)) {
    if (lower.includes(key) || key.includes(lower)) return slackId;
  }
  return null;
}

async function handleGHLWebhook(req, res) {
  try {
    let body = '';
    req.on('data', chunk => { body += chunk.toString(); });
    req.on('end', async () => {
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ received: true }));

      try {
        const payload = JSON.parse(body);
        console.log('GHL webhook received:', JSON.stringify(payload).substring(0, 300));

        // Log full raw payload for debugging
        console.log('GHL raw payload keys:', Object.keys(payload).join(', '));
        if (payload.contact) console.log('GHL contact object:', JSON.stringify(payload.contact).substring(0, 300));
        if (payload.customData) console.log('GHL customData:', JSON.stringify(payload.customData).substring(0, 300));
        if (payload.attributionSource) console.log('GHL attributionSource:', JSON.stringify(payload.attributionSource).substring(0, 200));

        // GHL can send data in multiple formats:
        // 1. Flat: { fullName: '...', email: '...', assignedTo: '...' }
        // 2. Nested: { customData: { fullName: '...', ... }, contact: { ... } }
        // 3. Standard GHL format: { contact_id, first_name, last_name, email, phone, ... }
        const cd = payload.customData || payload.custom_data || {};
        const ct = payload.contact || {};

        const fullName   = cd.fullName   || payload.fullName   || payload.full_name
                        || `${payload.first_name || ct.firstName || ''} ${payload.last_name || ct.lastName || ''}`.trim()
                        || ct.name || payload.name || 'Unknown';
        const email      = cd.email      || payload.email      || ct.email    || '';
        const phone      = cd.phone      || payload.phone      || ct.phone    || '';
        // Source lives in payload.contact.attributionSource (confirmed from logs)
        const contactAttr = (payload.contact && payload.contact.attributionSource) || {};
        const attrSource  = payload.attributionSource || {};

        // Friendly source name mapping
        const sourceRaw = cd.source
                        || payload.source
                        || payload.contact_source
                        || ct.source
                        || contactAttr.sessionSource
                        || contactAttr.medium
                        || attrSource.medium
                        || payload.triggerData?.source
                        || '';

        // Map GHL medium codes to readable names
        const sourceMap = {
          'whatsapp_coex': 'WhatsApp',
          'whatsapp':      'WhatsApp',
          'fb':            'Facebook',
          'facebook':      'Facebook',
          'instagram':     'Instagram',
          'organic':       'Organic',
          'paid':          'Paid Ads',
          'email':         'Email',
          'sms':           'SMS',
          'referral':      'Referral',
        };
        const source = sourceMap[sourceRaw.toLowerCase()] || sourceRaw;
        const assignedTo = cd.assignedTo                    // custom data field
                        || cd['opportunity.assignedTo']       // GHL opportunity field
                        || payload.assignedTo
                        || payload['opportunity.assignedTo']
                        || payload.assigned_user
                        || payload.contact_owner
                        || ct.assignedTo
                        || ct.assigned_user
                        || '';
        const contactId  = cd.contactId  || payload.contactId  || payload.contact_id
                        || ct.id || payload.id || '';
        const locationId = payload.locationId || payload.location_id || process.env.GHL_LOCATION_ID || '';

        // If assignedTo is still empty, fetch from GHL API using contactId
        let resolvedAssignedTo = assignedTo;
        if (!resolvedAssignedTo && contactId) {
          try {
            const contactRes = await fetch(
              `https://services.leadconnectorhq.com/contacts/${contactId}`,
              {
                headers: {
                  'Authorization': `Bearer ${process.env.GHL_API_KEY}`,
                  'Version': '2021-07-28'
                }
              }
            );
            const contactData = await contactRes.json();
            const assignedUser = contactData.contact?.assignedTo || contactData.assignedTo || '';
            if (assignedUser) {
              // Try to resolve user name from GHL location users endpoint
              try {
                const usersRes = await fetch(
                  `https://services.leadconnectorhq.com/users/?locationId=${process.env.GHL_LOCATION_ID}`,
                  {
                    headers: {
                      'Authorization': `Bearer ${process.env.GHL_API_KEY}`,
                      'Version': '2021-07-28'
                    }
                  }
                );
                const usersData = await usersRes.json();
                const users = usersData.users || usersData || [];
                const matchedUser = users.find(u => u.id === assignedUser);
                resolvedAssignedTo = matchedUser
                  ? (matchedUser.name || matchedUser.firstName || matchedUser.email)
                  : assignedUser; // fallback to ID if not found
              } catch (userErr) {
                resolvedAssignedTo = assignedUser; // fallback to ID
              }
              // Resolve display name from our local map
              const displayName = GHL_USER_NAMES[resolvedAssignedTo] || GHL_USER_NAMES[resolvedAssignedTo.toLowerCase()];
              if (displayName) resolvedAssignedTo = displayName;
              console.log(`GHL resolved assignedTo: ${resolvedAssignedTo}`);
            }
          } catch (apiErr) {
            console.error('GHL contact lookup error:', apiErr.message);
          }
        }

        console.log('GHL parsed:', { fullName, email, phone, source, assignedTo: resolvedAssignedTo, contactId });

        // GHL contact URL
        const ghlLink = contactId
          ? `https://app.gohighlevel.com/v2/location/${locationId}/contacts/detail/${contactId}`
          : 'https://app.gohighlevel.com';

        // Resolve which setter to notify
        const setterSlackId = resolveSetterSlackId(resolvedAssignedTo);

        // Build the briefing message
        const prompt = `You are Max, the NeuroGrowth PM Agent. A new lead just came in and was assigned to a setter.

Lead details:
- Name: ${fullName}
- Email: ${email || 'not provided'}
- Phone: ${phone || 'not provided'}
- Source: ${source}
- Assigned to: ${resolvedAssignedTo || 'unassigned'}
- GHL link: ${ghlLink}

Write a short, direct Slack DM to the setter (2-3 sentences max) telling them:
1. A new lead came in and was assigned to them
2. Key lead details
3. Their first action (reach out now, check GHL)

Sound like a colleague, not a bot. No markdown. Include the GHL link.`;

        // Use direct API call for webhook briefing — lighter than callClaude
        const briefingResponse = await anthropic.messages.create({
          model: 'claude-sonnet-4-6',
          max_tokens: 300,
          messages: [{ role: 'user', content: prompt }]
        });
        const briefing = briefingResponse.content.filter(b => b.type === 'text').map(b => b.text).join('');

        if (!briefing || !briefing.trim()) {
          console.error('GHL webhook: empty briefing from Claude');
          return;
        }

        // DM the assigned setter only if we can resolve their Slack ID
        if (setterSlackId) {
          await slack.client.chat.postMessage({
            channel: setterSlackId,
            text: briefing
          });
          console.log(`GHL lead briefing sent to setter ${assignedTo} (${setterSlackId})`);
        } else {
          // Log silently — don't post confusion to team channels
          console.log(`GHL lead received but setter not resolved. assignedTo: "${assignedTo}". Add to GHL_TO_SLACK map if needed.`);
        }

        // Post clean notification to #ng-sales-goats
        const ghlContactUrl = contactId
          ? `https://app.gohighlevel.com/v2/location/${locationId}/contacts/detail/${contactId}`
          : null;

        const channelNote = [
          `🆕 *New Lead* — ${fullName}`,
          email      ? `📧 ${email}`              : null,
          phone      ? `📱 ${phone}`              : null,
          source && source !== 'Unknown channel' ? `📌 Source: ${source}` : null,
          resolvedAssignedTo ? `👤 Assigned to: ${resolvedAssignedTo}` : null,
          ghlContactUrl ? `🔗 ${ghlContactUrl}`   : null
        ].filter(Boolean).join('\n');

        await slack.client.chat.postMessage({
          channel: 'C0AJANQBYUE',
          text: channelNote
        });

      } catch (parseErr) {
        console.error('GHL webhook parse error:', parseErr.message);
      }
    });
  } catch (err) {
    console.error('GHL webhook handler error:', err.message);
    res.writeHead(500);
    res.end('error');
  }
}

// ─── HEALTH CHECK SERVER ─────────────────────────────────────────────────────
const healthServer = http.createServer((req, res) => {
  if (req.url === '/health' && req.method === 'GET') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({
      status: 'ok',
      agent: 'NeuroGrowth PM Agent (Max)',
      uptime: Math.floor(process.uptime()),
      timestamp: new Date().toISOString()
    }));
  } else if (req.url === '/webhook/ghl-lead' && req.method === 'POST') {
    handleGHLWebhook(req, res);
  } else {
    res.writeHead(404);
    res.end('Not found');
  }
});
healthServer.listen(process.env.PORT || 3000, () => {
  console.log(`Health check server listening on port ${process.env.PORT || 3000}`);
});

// ─── START ────────────────────────────────────────────────────────────────────
(async () => {
  await slack.start();
  console.log('NeuroGrowth PM Agent is running.');
  // Load and register any dynamic cron tasks saved in Supabase
  await loadAndRegisterDynamicCrons();
})();

// ─── MEMBER JOINED CHANNEL ────────────────────────────────────────────────────
slack.event('member_joined_channel', async ({ event }) => {
  // Only trigger for #ng-pm-agent channel
  try {
    const channelInfo = await slack.client.conversations.info({ channel: event.channel });
    const channelName = channelInfo.channel?.name || '';
    if (!channelName.includes('ng-pm-agent')) return;

    // Don't greet the bot itself
    if (event.user === process.env.SLACK_BOT_USER_ID) return;

    const member   = getMemberContext(event.user);
    const perms    = getMemberPermissions(event.user);

    const roleIntros = {
      ceo: `You are greeting Ron, the CEO and Founder of NeuroGrowth. This is your home base. Give him a sharp 2-line welcome that shows you're ready to work — mention you can pull emails, calendar, GHL, Slack channels, Drive, and Notion on demand.`,
      client_success: `You are greeting Tania, the Client Success Operations Manager. Welcome her and let her know you can help with: client health checks, drafting client comms, checking fulfillment channel activity, contract reminders, and searching the knowledge base. Keep it to 3-4 lines max.`,
      tech_ops: `You are greeting Josue, the Technical Operations Manager. Welcome him and let him know you can help with: client launch status, campaign blockers, fulfillment channel recaps, Notion SOPs, and his daily briefing every morning at 8:30 AM. Keep it to 3-4 lines max.`,
      tech_lead: `You are greeting David, the Lead Technology and Automation specialist. Welcome him and let him know you can help with: systems channel activity, Make.com issue tracking, process documentation, and Notion. Keep it to 3-4 lines max.`,
      fulfillment: `You are greeting Valeria, the Fulfillment Operations specialist. Welcome her and let her know you can help with: delivery doc status, client setup coordination, fulfillment channel recaps, and Notion. Keep it to 3-4 lines max.`,
      campaigns: `You are greeting Felipe, the Technical Campaign Specialist. Welcome him and let him know you can help with: campaign status per client, Prosp config questions, fulfillment channel updates, and content pipeline tracking. Keep it to 3-4 lines max.`,
      setter: `You are greeting Joseph, the Appointment Setter. Welcome him and let him know you can help with: GHL prospect lookups, drafting follow-up messages in Spanish, sales channel activity, and EOD report prep. Keep it to 3-4 lines max.`,
      closer: `You are greeting Jose, the High-Ticket Closer. Welcome him and let him know you can help with: GHL pipeline status, prospect follow-up drafts, sales channel activity, and EOD report prep. Keep it to 3-4 lines max.`
    };

    const roleIntro = roleIntros[member.role] || `You are greeting a new NeuroGrowth team member named ${member.displayName}. Welcome them warmly and briefly explain what you can help with.`;

    const prompt = `You are Max, the NeuroGrowth PM Agent. A new team member just joined the #ng-pm-agent channel.

${roleIntro}

Address them by name: ${member.displayName}.
Sound like a sharp, friendly colleague — not a corporate bot. No markdown formatting. No bullet points. Conversational tone.`;

    const greeting = await callClaude([{ role: 'user', content: prompt }]);
    if (!greeting || !greeting.trim()) return;

    await slack.client.chat.postMessage({
      channel: event.channel,
      text: greeting
    });

    console.log(`Greeted ${member.displayName} (${member.role}) in #ng-pm-agent`);
  } catch (err) {
    console.error('member_joined_channel error:', err.message);
  }
});