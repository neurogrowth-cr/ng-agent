require('dotenv').config();
const { App } = require('@slack/bolt');
const Anthropic = require('@anthropic-ai/sdk');
const { createClient } = require('@supabase/supabase-js');
const fs = require('fs');
const cron = require('node-cron');
const { google } = require('googleapis');
const sharp = require('sharp');

const slack = new App({
  token: process.env.SLACK_BOT_TOKEN,
  appToken: process.env.SLACK_APP_TOKEN,
  socketMode: true,
});

const anthropic = new Anthropic({ apiKey: process.env.ANTHROPIC_API_KEY });
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
  'U05HXGX18H3': {
    name: 'Ron',
    role: 'ceo',
    displayName: 'Ron Duarte'
  },
  'U07SMMDMSLQ': {
    name: 'Tania',
    role: 'client_success',
    displayName: 'Tania'
  },
  'U08ABBFNGUW': {
    name: 'Josue',
    role: 'tech_ops',
    displayName: 'Josue'
  },
  'U08ACUHUUP6': {
    name: 'David',
    role: 'tech_lead',
    displayName: 'David'
  },
  'U09Q3BXJ18B': {
    name: 'Valeria',
    role: 'fulfillment',
    displayName: 'Valeria'
  },
  'U09TNMVML3F': {
    name: 'Felipe',
    role: 'campaigns',
    displayName: 'Felipe'
  },
  'U0A9J00EMGD': {
    name: 'Joseph',
    role: 'setter',
    displayName: 'Joseph'
  },
  'U0AMTEKDCPN': {
    name: 'Jose',
    role: 'closer',
    displayName: 'Jose'
  }
};

const ROLE_PERMISSIONS = {
  ceo: {
    canReadChannels: ['ng-fullfillment-ops', 'ng-sales-goats', 'ng-ops-management', 'ng-new-client-alerts', 'ng-app-and-systems-improvents', 'ng-internal-announcements'],
    canPostChannels: ['ng-fullfillment-ops', 'ng-sales-goats', 'ng-ops-management', 'ng-new-client-alerts', 'ng-app-and-systems-improvents', 'ng-internal-announcements'],
    canUseEmail: true,
    canUseCalendar: true,
    canUseGHL: true,
    canUseDrive: true,
    canUseNotion: true,
    canSaveKnowledge: true,
    fullAccess: true
  },
  client_success: {
    canReadChannels: ['ng-fullfillment-ops', 'ng-new-client-alerts', 'ng-ops-management'],
    canPostChannels: ['ng-fullfillment-ops', 'ng-new-client-alerts'],
    canUseEmail: false,
    canUseCalendar: false,
    canUseGHL: false,
    canUseDrive: true,
    canUseNotion: true,
    canSaveKnowledge: true,
    fullAccess: false
  },
  tech_ops: {
    canReadChannels: ['ng-fullfillment-ops', 'ng-app-and-systems-improvents'],
    canPostChannels: ['ng-fullfillment-ops', 'ng-app-and-systems-improvents'],
    canUseEmail: false,
    canUseCalendar: false,
    canUseGHL: false,
    canUseDrive: true,
    canUseNotion: true,
    canSaveKnowledge: true,
    fullAccess: false
  },
  tech_lead: {
    canReadChannels: ['ng-fullfillment-ops', 'ng-app-and-systems-improvents', 'ng-ops-management'],
    canPostChannels: ['ng-fullfillment-ops', 'ng-app-and-systems-improvents'],
    canUseEmail: false,
    canUseCalendar: false,
    canUseGHL: false,
    canUseDrive: true,
    canUseNotion: true,
    canSaveKnowledge: true,
    fullAccess: false
  },
  fulfillment: {
    canReadChannels: ['ng-fullfillment-ops'],
    canPostChannels: ['ng-fullfillment-ops'],
    canUseEmail: false,
    canUseCalendar: false,
    canUseGHL: false,
    canUseDrive: true,
    canUseNotion: true,
    canSaveKnowledge: false,
    fullAccess: false
  },
  campaigns: {
    canReadChannels: ['ng-fullfillment-ops'],
    canPostChannels: ['ng-fullfillment-ops'],
    canUseEmail: false,
    canUseCalendar: false,
    canUseGHL: false,
    canUseDrive: true,
    canUseNotion: true,
    canSaveKnowledge: false,
    fullAccess: false
  },
  setter: {
    canReadChannels: ['ng-sales-goats'],
    canPostChannels: ['ng-sales-goats'],
    canUseEmail: false,
    canUseCalendar: false,
    canUseGHL: true,
    canUseDrive: false,
    canUseNotion: false,
    canSaveKnowledge: false,
    fullAccess: false
  },
  closer: {
    canReadChannels: ['ng-sales-goats'],
    canPostChannels: ['ng-sales-goats'],
    canUseEmail: false,
    canUseCalendar: false,
    canUseGHL: true,
    canUseDrive: false,
    canUseNotion: false,
    canSaveKnowledge: false,
    fullAccess: false
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
    client_success: `You are speaking with Tania, the Client Success Operations Manager at NeuroGrowth. She manages active client relationships, account health, AR, contract processing, and case study collection. Help her with client status queries, drafting client communications, checking fulfillment channel activity, and searching client knowledge. She cannot access Ron's email, calendar, or GHL.`,
    tech_ops: `You are speaking with Josue, the Technical Operations Manager at NeuroGrowth. He handles activation calls, campaign operations, and client launch sequencing. Help him with fulfillment channel activity, client launch status, technical blockers, and Notion SOPs. He cannot access Ron's email, calendar, or GHL.`,
    tech_lead: `You are speaking with David, the Lead Technology and Automation specialist at NeuroGrowth. He builds and maintains Make.com scenarios, Supabase infrastructure, and the Neurogrowth Portal. Help him with technical questions, systems channel activity, and process documentation. He cannot access Ron's email, calendar, or GHL.`,
    fulfillment: `You are speaking with a fulfillment team member at NeuroGrowth. Help them with campaign questions, client delivery status, and fulfillment channel activity. Keep responses focused on delivery operations.`,
    campaigns: `You are speaking with Felipe, the Campaign Operations specialist at NeuroGrowth. He implements campaigns, manages Prosp, and handles technical escalations. Help him with campaign status, client delivery questions, and fulfillment channel activity.`,
    setter: `You are speaking with Joseph, the Appointment Setter at NeuroGrowth. He books discovery calls with qualified prospects. Help him with prospect information from GHL, sales channel activity, and call scheduling context.`,
    closer: `You are speaking with Jose, the High-Ticket Closer at NeuroGrowth. He closes prospects after the appointment-setting stage. Help him with GHL conversation data, prospect status, and sales channel activity.`
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

  // Railway: read from environment variables
  if (process.env.GOOGLE_CREDENTIALS) {
    credentials = JSON.parse(process.env.GOOGLE_CREDENTIALS);
    token = JSON.parse(process.env.GOOGLE_TOKEN);
  } else {
    // Local: read from files
    credentials = JSON.parse(fs.readFileSync('./credentials.json'));
    token = JSON.parse(fs.readFileSync('./token.json'));
  }

  const { client_id, client_secret } = credentials.installed;
  const oauth2Client = new google.auth.OAuth2(
    client_id, client_secret, 'http://localhost:3000/callback'
  );
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
    const from = headers.find(h => h.name === 'From')?.value || 'Unknown';
    const date = headers.find(h => h.name === 'Date')?.value || '';

    // Extract full email body
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
    const apiKey = process.env.GHL_API_KEY;

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
        timeZone: 'America/Costa_Rica',
        month: 'short', day: 'numeric',
        hour: '2-digit', minute: '2-digit'
      });
      const age = Math.floor((now - c.lastMessageDate) / oneDayMs);
      const unread = c.unreadCount > 0 ? ` [UNREAD: ${c.unreadCount}]` : '';
      const direction = c.lastMessageDirection === 'inbound' ? '<-- inbound' : '--> outbound';
      const channel = c.lastMessageType?.replace('TYPE_', '') || 'unknown';
      const stale = age >= 3 ? ` [${age}d ago - needs follow-up]` : '';

      return `${c.contactName || c.fullName || 'Unknown'} | ${channel} | ${direction}${unread}${stale}\nLast: "${(c.lastMessageBody || '').substring(0, 120)}" (${lastDate})`;
    });

    const unreadCount = convos.filter(c => c.unreadCount > 0).length;
    const staleCount = convos.filter(c => (now - c.lastMessageDate) / oneDayMs >= 3).length;

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

    const messages = history.messages
      .reverse()
      .map(m => {
        const time = new Date(parseFloat(m.ts) * 1000).toLocaleString('en-US', {
          timeZone: 'America/Costa_Rica',
          month: 'short', day: 'numeric',
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
      if (!messages.includes('not found')) {
        digest += `\n\n=== ${ch} ===\n${messages}`;
      }
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

    const text = response.content.filter(b => b.type === 'text').map(b => b.text).join('\n');
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
    const staleAlerts = data.filter(a => {
      const age = now - new Date(a.updated_at).getTime();
      return age > oneDayMs;
    });

    if (!staleAlerts.length) return;

    const alertText = staleAlerts.map(a =>
      `${a.key}: ${a.value}`
    ).join('\n\n');

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
  // Skip resize for GIF and PDF
  if (mimeType === 'application/pdf' || mimeType === 'image/gif') return { buffer: fileBuffer, mimeType };

  try {
    const image = sharp(fileBuffer);
    const metadata = await image.metadata();

    // Only resize if image is large (over 1200px on longest side)
    const maxDimension = 1200;
    if (metadata.width <= maxDimension && metadata.height <= maxDimension) {
      return { buffer: fileBuffer, mimeType };
    }

    const isLandscape = metadata.width > metadata.height;
    const resized = await image
      .resize(
        isLandscape ? maxDimension : null,
        isLandscape ? null : maxDimension,
        { fit: 'inside', withoutEnlargement: true }
      )
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
  let finalBuffer = fileBuffer;
  let finalMimeType = mimeType;

  // Resize images to reduce token usage
  if (mimeType.startsWith('image/')) {
    const resized = await resizeImageIfNeeded(fileBuffer, mimeType);
    finalBuffer = resized.buffer;
    finalMimeType = resized.mimeType;
  }

  const base64 = finalBuffer.toString('base64');

  let contentBlock;
  if (finalMimeType === 'application/pdf') {
    contentBlock = {
      type: 'document',
      source: { type: 'base64', media_type: 'application/pdf', data: base64 }
    };
  } else if (finalMimeType.startsWith('image/')) {
    contentBlock = {
      type: 'image',
      source: { type: 'base64', media_type: finalMimeType, data: base64 }
    };
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
    'pdf': 'application/pdf',
    'png': 'image/png',
    'jpg': 'image/jpeg',
    'jpeg': 'image/jpeg',
    'gif': 'image/gif',
    'webp': 'image/webp'
  };
  return map[ext] || mimeType;
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
            input_schema: {
              type: "object",
              properties: { query: { type: "string" } },
              required: ["query"]
            }
          },
          {
            name: "get_notion_page",
            description: "Get the content of a specific Notion page by its ID",
            input_schema: {
              type: "object",
              properties: { page_id: { type: "string" } },
              required: ["page_id"]
            }
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
              properties: {
                to: { type: "string" },
                subject: { type: "string" },
                body: { type: "string" }
              },
              required: ["to", "subject", "body"]
            }
          },
          {
            name: "get_calendar_events",
            description: "Get calendar events. daysFromNow: 0=today, 1=tomorrow, -1=yesterday. daysRange: 1=day, 7=week, 14=two weeks.",
            input_schema: {
              type: "object",
              properties: {
                daysFromNow: { type: "number" },
                daysRange: { type: "number" }
              }
            }
          },
          {
            name: "search_drive",
            description: "Search Ron's Google Drive for files and documents",
            input_schema: {
              type: "object",
              properties: { query: { type: "string" } },
              required: ["query"]
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
              properties: {
                channelName: { type: "string" },
                message: { type: "string" }
              },
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
              properties: {
                category: { type: "string", description: "client, team, process, decision, alert, or intel" }
              },
              required: ["category"]
            }
          }
        ]
      });

      if (response.stop_reason === 'tool_use') {
        const toolUses = response.content.filter(b => b.type === 'tool_use');
        const toolResults = await Promise.all(toolUses.map(async (toolUse) => {
          let result;
          try {
            if (toolUse.name === 'search_notion') {
              result = await searchNotion(toolUse.input.query);
            } else if (toolUse.name === 'get_notion_page') {
              result = await getNotionPage(toolUse.input.page_id);
            } else if (toolUse.name === 'get_recent_emails') {
              result = await getRecentEmails();
            } else if (toolUse.name === 'send_email') {
              result = await sendEmail(toolUse.input.to, toolUse.input.subject, toolUse.input.body);
            } else if (toolUse.name === 'get_calendar_events') {
              result = await getCalendarEvents(toolUse.input.daysFromNow || 0, toolUse.input.daysRange || 1);
            } else if (toolUse.name === 'search_drive') {
              result = await searchDrive(toolUse.input.query);
            } else if (toolUse.name === 'read_slack_channel') {
              result = await readSlackChannel(toolUse.input.channelName, toolUse.input.messageCount || 20);
            } else if (toolUse.name === 'draft_channel_post') {
              result = `APPROVAL_NEEDED|${toolUse.input.channelName}|${toolUse.input.message}`;
            } else if (toolUse.name === 'get_ghl_conversations') {
              result = await getGHLConversations(toolUse.input.limit || 20, toolUse.input.unreadOnly || false);
            } else if (toolUse.name === 'search_knowledge') {
              result = await searchKnowledge(toolUse.input.query, toolUse.input.category);
            } else if (toolUse.name === 'save_knowledge') {
              result = await upsertKnowledge(toolUse.input.category, toolUse.input.key, toolUse.input.value, 'conversation');
            } else if (toolUse.name === 'get_knowledge_category') {
              result = await getAllKnowledgeByCategory(toolUse.input.category);
            }
          } catch (err) {
            result = `Error running tool ${toolUse.name}: ${err.message}`;
          }
          return {
            type: 'tool_result',
            tool_use_id: toolUse.id,
            content: JSON.stringify(result)
          };
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
        return followUp.content.filter(b => b.type === 'text').map(b => b.text).join('\n');
      }

      return response.content.filter(b => b.type === 'text').map(b => b.text).join('\n');

    } catch (err) {
      lastErr = err;
      if (err.status === 529 || err.status === 503 || err.status === 500) {
        const wait = (attempt + 1) * 4000;
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
  if (!text || !text.trim()) {
    console.error('postToSlack called with empty text, skipping.');
    return;
  }
  // Strip # prefix — Slack API uses channel names without #
  const channelName = channel.startsWith('#') ? channel.slice(1) : channel;
  const payload = { channel: channelName, text };
  if (threadTs) payload.thread_ts = threadTs;
  await slack.client.chat.postMessage(payload);
}

async function executeChannelPost(channelName, message, say) {
  try {
    const result = await slack.client.conversations.list({
      limit: 200,
      types: 'public_channel,private_channel,mpim,im'
    });
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
  const parts = reply.split('|');
  const channelName = parts[1];
  const draftMessage = parts.slice(2).join('|');
  pendingApprovals[userId] = { channelName, message: draftMessage };
  say(`Here is what I would post to *${channelName}*:\n\n"${draftMessage}"\n\nReply *yes* to send it or *no* to cancel.`);
  return true;
}

// ─── SLACK HANDLERS ───────────────────────────────────────────────────────────
slack.message(async ({ message, say }) => {
  if (message.bot_id) return;
  if (message.channel_type !== 'im') return;

  const isApproval = await checkApproval(message, say, message.user);
  if (isApproval) return;

  const userId = message.user;

  // Handle file uploads
  if (message.subtype === 'file_share' && message.files?.length > 0) {
    const file = message.files[0];
    const instruction = message.text || null;
    const mimeType = getFileMimeType(file.name, file.mimetype);

    const supported = ['application/pdf', 'image/png', 'image/jpeg', 'image/gif', 'image/webp'];
    if (!supported.includes(mimeType)) {
      await say(`I can process images (PNG, JPG, GIF, WEBP) and PDFs. This file type (${mimeType}) is not supported yet.`);
      return;
    }

    await say(`Got the ${mimeType.includes('pdf') ? 'PDF' : 'image'}. Give me a moment to analyze it...`);

    try {
      const fileBuffer = await downloadSlackFile(file.url_private);
      const result = await processFileWithClaude(fileBuffer, mimeType, instruction, SYSTEM_PROMPT);
      await saveMessage(userId, 'user', `[File: ${file.name}] ${instruction || 'analyze this'}`);
      await saveMessage(userId, 'assistant', result);
      await say(result);
    } catch (err) {
      console.error('File processing error:', err);
      await say(`Had trouble processing that file — ${err.message}`);
    }
    return;
  }

  if (message.subtype) return;

  const member = getMemberContext(userId);
  const history = await loadHistory(userId);
  history.push({ role: 'user', content: message.text });

  try {
    const reply = await callClaude(history, 3, userId);
    if (handleDraftReply(reply, userId, say)) return;
    await saveMessage(userId, 'user', message.text);
    await saveMessage(userId, 'assistant', reply);
    if (reply && reply.trim()) await say(reply);
  } catch (err) {
    console.error('Claude API error (DM):', err);
    await say('Got turned around for a second — go ahead and ask again.');
  }
});

slack.event('app_mention', async ({ event, say }) => {
  if (event.bot_id) return;
  const cleanText = event.text.replace(/<@[A-Z0-9]+>/g, '').trim();
  if (!cleanText) return;

  const userId = event.user;
  const history = await loadHistory(userId);
  history.push({ role: 'user', content: cleanText });

  try {
    const reply = await callClaude(history, 3, userId);
    if (handleDraftReply(reply, userId, say)) return;
    await saveMessage(userId, 'user', cleanText);
    await saveMessage(userId, 'assistant', reply);
    if (reply && reply.trim()) await say({ text: reply, thread_ts: event.thread_ts || event.ts });
  } catch (err) {
    console.error('Claude API error (mention):', err);
    await say({ text: 'Got turned around — try again.', thread_ts: event.thread_ts || event.ts });
  }
});

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

  // Handle file uploads in channel
  if (message.subtype === 'file_share' && message.files?.length > 0) {
    const file = message.files[0];
    const instruction = message.text || null;
    const mimeType = getFileMimeType(file.name, file.mimetype);

    const supported = ['application/pdf', 'image/png', 'image/jpeg', 'image/gif', 'image/webp'];
    if (!supported.includes(mimeType)) {
      await say({ text: `I can process images and PDFs. This file type (${mimeType}) is not supported yet.`, thread_ts: message.thread_ts || message.ts });
      return;
    }

    await say({ text: `Got the ${mimeType.includes('pdf') ? 'PDF' : 'image'}. Analyzing...`, thread_ts: message.thread_ts || message.ts });

    try {
      const fileBuffer = await downloadSlackFile(file.url_private);
      const result = await processFileWithClaude(fileBuffer, mimeType, instruction, SYSTEM_PROMPT);
      await saveMessage(userId, 'user', `[File: ${file.name}] ${instruction || 'analyze this'}`);
      await saveMessage(userId, 'assistant', result);
      await say({ text: result, thread_ts: message.thread_ts || message.ts });
    } catch (err) {
      console.error('File processing error:', err);
      await say({ text: `Had trouble processing that file — ${err.message}`, thread_ts: message.thread_ts || message.ts });
    }
    return;
  }

  if (message.subtype) return;

  const history = await loadHistory(userId);
  history.push({ role: 'user', content: message.text });

  try {
    const reply = await callClaude(history, 3, userId);
    if (handleDraftReply(reply, userId, say)) return;
    await saveMessage(userId, 'user', message.text);
    await saveMessage(userId, 'assistant', reply);
    if (reply && reply.trim()) await say({ text: reply, thread_ts: message.thread_ts || message.ts });
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

// Proactive alerts — 9:00 AM and 2:00 PM CST
cron.schedule('0 15 * * *', async () => {
  await runProactiveAlerts();
}, { timezone: 'America/Costa_Rica' });

cron.schedule('0 20 * * *', async () => {
  await runProactiveAlerts();
}, { timezone: 'America/Costa_Rica' });

// Nightly learning — 11:30 PM CST
cron.schedule('30 5 * * *', async () => {
  await runNightlyLearning();
}, { timezone: 'America/Costa_Rica' });

// ─── START ────────────────────────────────────────────────────────────────────
(async () => {
  await slack.start();
  console.log('NeuroGrowth PM Agent is running.');
})();