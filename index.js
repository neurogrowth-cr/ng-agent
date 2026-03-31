require('dotenv').config();
const { App } = require('@slack/bolt');
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
const SYSTEM_PROMPT = fs.readFileSync('./system_prompt.txt', 'utf8');

const AGENT_CHANNEL         = process.env.AGENT_CHANNEL         || '#ng-pm-agent';
const OPS_CHANNEL           = process.env.OPS_CHANNEL           || '#ng-fullfillment-ops';
const NEW_CLIENT_CHANNEL    = process.env.NEW_CLIENT_CHANNEL    || '#ng-new-client-alerts';
const SALES_CHANNEL         = process.env.SALES_CHANNEL         || '#ng-sales-goats';
const SYSTEMS_CHANNEL       = process.env.SYSTEMS_CHANNEL       || '#ng-app-and-systems-improvents';
const ANNOUNCEMENTS_CHANNEL = process.env.ANNOUNCEMENTS_CHANNEL || '#ng-internal-announcements';

const pendingApprovals = {};

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
    const { error } = await supabase
      .from('conversations')
      .insert({ user_id: userId, role, content: content.substring(0, 8000) });
    if (error) throw error;
  } catch (err) {
    console.error('Supabase save error:', err.message);
  }
}

// ─── SUPABASE: KNOWLEDGE STORE ────────────────────────────────────────────────
async function searchKnowledge(query, category = null) {
  try {
    let q = supabase
      .from('agent_knowledge')
      .select('category, key, value, updated_at')
      .ilike('value', `%${query}%`)
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

async function upsertKnowledge(category, key, value, source = 'agent') {
  try {
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

// ─── GOOGLE AUTH ──────────────────────────────────────────────────────────────
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

// ─── GHL CONVERSATIONS ───────────────────────────────────────────────────────
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
        max_tokens: 1024,
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
            else if (toolUse.name === 'search_drive')         result = await searchDrive(toolUse.input.query);
            else if (toolUse.name === 'read_slack_channel')   result = await readSlackChannel(toolUse.input.channelName, toolUse.input.messageCount || 20);
            else if (toolUse.name === 'draft_channel_post')   result = `APPROVAL_NEEDED|${toolUse.input.channelName}|${toolUse.input.message}`;
            else if (toolUse.name === 'get_ghl_conversations') result = await getGHLConversations(toolUse.input.limit || 20, toolUse.input.unreadOnly || false);
            else if (toolUse.name === 'search_knowledge')     result = await searchKnowledge(toolUse.input.query, toolUse.input.category);
            else if (toolUse.name === 'save_knowledge')       result = await upsertKnowledge(toolUse.input.category, toolUse.input.key, toolUse.input.value, 'conversation');
            else if (toolUse.name === 'get_knowledge_category') result = await getAllKnowledgeByCategory(toolUse.input.category);
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
        const followUpText = followUp.content.filter(b => b.type === 'text').map(b => b.text).join('\n');
        return followUpText || null;
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
  pendingApprovals[userId] = { channelName, message: draftMessage };
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

// Nightly learning — 11:30 PM CST
cron.schedule('30 5 * * *', async () => { await runNightlyLearning(); }, { timezone: 'America/Costa_Rica' });

// ─── START ────────────────────────────────────────────────────────────────────
(async () => {
  await slack.start();
  console.log('NeuroGrowth PM Agent is running.');
})();