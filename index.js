require('dotenv').config();
const { App } = require('@slack/bolt');
const Anthropic = require('@anthropic-ai/sdk');
const fs = require('fs');
const cron = require('node-cron');

const slack = new App({
  token: process.env.SLACK_BOT_TOKEN,
  appToken: process.env.SLACK_APP_TOKEN,
  socketMode: true,
});

const anthropic = new Anthropic({ apiKey: process.env.ANTHROPIC_API_KEY });

const SYSTEM_PROMPT = fs.readFileSync('./system_prompt.txt', 'utf8');

const AGENT_CHANNEL   = process.env.AGENT_CHANNEL   || '#ng-pm-agent';
const STANDUP_CHANNEL = process.env.STANDUP_CHANNEL || '#ng-daily-standup';
const OPS_CHANNEL     = process.env.OPS_CHANNEL     || '#ng-ops-management';
const BOT_USER_ID     = process.env.SLACK_BOT_USER_ID;

// In-memory conversation store (keyed by Slack thread_ts or channel+user)
const conversations = {};

function getConversationKey(channelId, threadTs, userId) {
  return threadTs ? `${channelId}:${threadTs}` : `${channelId}:${userId}`;
}

async function callClaude(messages) {
  const response = await anthropic.messages.create({
    model: 'claude-sonnet-4-20250514',
    max_tokens: 1024,
    system: SYSTEM_PROMPT,
    messages,
  });
  return response.content[0].text;
}

async function postToSlack(channel, text, threadTs = null) {
  const payload = { channel, text };
  if (threadTs) payload.thread_ts = threadTs;
  await slack.client.chat.postMessage(payload);
}

// ─── DIRECT MESSAGE HANDLER ───────────────────────────────────────────────────
slack.message(async ({ message, say }) => {
  if (message.subtype || message.bot_id) return;
  if (message.channel_type !== 'im') return;

  const key = getConversationKey(message.channel, null, message.user);
  if (!conversations[key]) conversations[key] = [];

  conversations[key].push({ role: 'user', content: message.text });

  try {
    const reply = await callClaude(conversations[key]);
    conversations[key].push({ role: 'assistant', content: reply });
    await say(reply);
  } catch (err) {
    console.error('Claude API error (DM):', err);
    await say('Something went wrong reaching Claude. Please try again.');
  }
});

// ─── CHANNEL MENTION HANDLER ──────────────────────────────────────────────────
slack.event('app_mention', async ({ event, say }) => {
  if (event.bot_id) return;

  const key = getConversationKey(event.channel, event.thread_ts, event.user);
  if (!conversations[key]) conversations[key] = [];

  const cleanText = event.text.replace(/<@[A-Z0-9]+>/g, '').trim();
  conversations[key].push({ role: 'user', content: cleanText });

  try {
    const reply = await callClaude(conversations[key]);
    conversations[key].push({ role: 'assistant', content: reply });
    await say({ text: reply, thread_ts: event.thread_ts || event.ts });
  } catch (err) {
    console.error('Claude API error (mention):', err);
    await say({ text: 'Something went wrong. Please try again.', thread_ts: event.thread_ts || event.ts });
  }
});

// ─── AGENT CHANNEL HANDLER (no mention needed) ────────────────────────────────
slack.message(async ({ message, say }) => {
  if (message.subtype || message.bot_id) return;
  if (message.channel_type === 'im') return;

  let channelInfo;
  try {
    channelInfo = await slack.client.conversations.info({ channel: message.channel });
  } catch {
    return;
  }

  const channelName = channelInfo.channel?.name || '';
  if (!channelName.includes('ng-pm-agent')) return;

  const key = getConversationKey(message.channel, message.thread_ts, message.user);
  if (!conversations[key]) conversations[key] = [];

  conversations[key].push({ role: 'user', content: message.text });

  try {
    const reply = await callClaude(conversations[key]);
    conversations[key].push({ role: 'assistant', content: reply });
    await say({ text: reply, thread_ts: message.thread_ts || message.ts });
  } catch (err) {
    console.error('Claude API error (channel):', err);
  }
});

// ─── CRON: DAILY STANDUP — 9:00 AM CST (UTC-6) = 15:00 UTC ──────────────────
cron.schedule('0 15 * * 1-5', async () => {
  const prompt = `Generate a daily standup message for the NeuroGrowth team. 
Post it in ${STANDUP_CHANNEL}. 
Tag @Tania, @Josue, @David, @Valeria, and @Felipe. 
Ask each person: (1) what they are working on today, (2) if there are any blockers. 
Keep it direct and brief. Today is ${new Date().toLocaleDateString('en-US', { weekday: 'long', month: 'long', day: 'numeric' })}.`;

  try {
    const message = await callClaude([{ role: 'user', content: prompt }]);
    await postToSlack(STANDUP_CHANNEL, message);
    console.log('Daily standup posted.');
  } catch (err) {
    console.error('Standup cron error:', err);
  }
}, { timezone: 'America/Costa_Rica' });

// ─── CRON: EOD CHECK — 5:00 PM CST = 23:00 UTC ───────────────────────────────
cron.schedule('0 23 * * 1-5', async () => {
  const prompt = `It is end of day at NeuroGrowth. 
Generate a brief EOD accountability check message for ${OPS_CHANNEL}. 
Remind the team to log what they completed today. 
Flag that any open items from this morning's standup that are unresolved should be noted. 
Keep it to 3-4 lines. Direct tone.`;

  try {
    const message = await callClaude([{ role: 'user', content: prompt }]);
    await postToSlack(OPS_CHANNEL, message);
    console.log('EOD check posted.');
  } catch (err) {
    console.error('EOD cron error:', err);
  }
}, { timezone: 'America/Costa_Rica' });

// ─── CRON: WEEKLY DIGEST — FRIDAY 4:00 PM CST = 22:00 UTC ───────────────────
cron.schedule('0 22 * * 5', async () => {
  const prompt = `It is Friday end of day at NeuroGrowth. 
Generate a weekly digest message for Ron to review. Post it in ${OPS_CHANNEL}. 
Structure it as: (1) What should have been completed this week across sales, delivery, and ops. 
(2) Key open items to address first thing Monday. 
(3) One question for Ron to answer over the weekend if needed. 
Tone: executive summary, no fluff.`;

  try {
    const message = await callClaude([{ role: 'user', content: prompt }]);
    await postToSlack(OPS_CHANNEL, message);
    console.log('Weekly digest posted.');
  } catch (err) {
    console.error('Weekly digest cron error:', err);
  }
}, { timezone: 'America/Costa_Rica' });

// ─── CRON: SALES CALL PREP — DAILY CHECK 7:00 PM CST ─────────────────────────
// This fires daily and checks if there are sales calls tomorrow.
// Phase 2 will wire this to Google Calendar. For now it posts a reminder prompt to Ron.
cron.schedule('0 1 * * 1-5', async () => {
  const prompt = `Check if Ron has any sales calls scheduled for tomorrow. 
If so, post a reminder in ${AGENT_CHANNEL} asking Tania to confirm that prospect research briefs are ready. 
If you do not have calendar access yet, post a general reminder that tomorrow is a potential call day 
and ask Ron to confirm if prep notes are needed. Keep it to 2 lines.`;

  try {
    const message = await callClaude([{ role: 'user', content: prompt }]);
    await postToSlack(AGENT_CHANNEL, message);
    console.log('Sales prep reminder posted.');
  } catch (err) {
    console.error('Sales prep cron error:', err);
  }
}, { timezone: 'America/Costa_Rica' });

// ─── CONVERSATION MEMORY CLEANUP (every 24h) ──────────────────────────────────
cron.schedule('0 6 * * *', () => {
  const keys = Object.keys(conversations);
  keys.forEach(k => {
    if (conversations[k].length > 40) {
      conversations[k] = conversations[k].slice(-20);
    }
  });
  console.log(`Memory cleanup done. Active conversations: ${keys.length}`);
}, { timezone: 'America/Costa_Rica' });

// ─── START ─────────────────────────────────────────────────────────────────────
(async () => {
  await slack.start();
  console.log('NeuroGrowth PM Agent is running.');
})();
