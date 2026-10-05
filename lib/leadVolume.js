// Per-channel lead volume watchdog. Recipe: ~/automations/ops/recipes/lead-volume-watchdog.md
//
// On 2026-10-02/03 organic Instagram DMs produced no GHL card and no Slack lead
// post for about 21 hours: the intake workflow was narrowed before its
// replacement was published. Nothing alerted. The workflow drift check only
// sees unpublished or deleted workflows, and lead_posts.source could not see
// it either, because "Social media" lumps WhatsApp, Instagram and Messenger
// together and WhatsApp kept flowing through the whole outage.
//
// So this watches CHANNELS (GHL attributionSource.medium, stored on lead_posts
// at intake) and asks one question per channel: given how this channel has
// been producing leads, is the current silence too long to be chance?
//
// Pure: no Slack, no DB. index.js owns the I/O and the cron.

const H = 60 * 60 * 1000;
// Costa Rica is UTC-6 with no DST, the same assumption the cron next-fire
// arithmetic in index.js makes.
const CR_OFFSET_MS = 6 * H;

// Thresholds, backtested against lead_posts + GHL medium for 2026-09-04 to
// 2026-10-04 (564 non-personal posts). See the recipe for the full table.
const CONFIG = {
  windowHours: 168,     // baseline: the 7 days before the silence started
  awakeFrom: 8,         // CR hour; silence only accrues 08:00 to midnight CR,
  awakeTo: 24,          //   so a quiet night can never fire on its own
  minActiveHours: 7,    // warmup: at least 7 distinct hours with a post in the window
  ageFloorAwakeHours: 12, // a channel younger than this is rated as if it were this old
  minExpected: 5,       // alert when the silence should have held >= 5 lead-hours (P(0) ~ 0.7%)
};

const CHANNELS = {
  fb_form:   { label: 'Facebook lead form', check: 'Meta ads delivery (spend, billing) and the GHL workflow "New Lead Intake and Assignment (NON-VSL PIPELINES)".' },
  whatsapp:  { label: 'WhatsApp',           check: 'the WhatsApp connection in GHL and the GHL workflow "New Lead Intake and Assignment (NON-VSL PIPELINES)".' },
  instagram: { label: 'Instagram DM',       check: 'the GHL workflow "Social DM Intake (on reply)." (published? both reply triggers present?) and the Instagram toggle in GHL Conversations.' },
  messenger: { label: 'Messenger',          check: 'the GHL workflow "Social DM Intake (on reply)." (published? both reply triggers present?) and the Facebook page connection in GHL.' },
};

// GHL contact attribution medium (instagram / whatsapp / whatsapp_coex /
// facebook) plus Max's mapped source label → channel key. Medium wins; the
// source label is the fallback for rows where the contact lookup failed.
// "facebook" medium is both the Meta instant form and Messenger: the form
// arrives with source "Facebook", a Messenger DM with "Social media".
function leadChannel({ source, medium } = {}) {
  const m = String(medium || '').trim().toLowerCase();
  const s = String(source || '').trim().toLowerCase();
  if (m.startsWith('whatsapp')) return 'whatsapp';
  if (m === 'instagram') return 'instagram';
  if (m === 'facebook') return s === 'facebook' ? 'fb_form' : 'messenger';
  if (m) return 'other';
  if (s === 'facebook') return 'fb_form';
  if (s === 'whatsapp') return 'whatsapp';
  if (s === 'instagram') return 'instagram';
  return s ? 'other' : null;
}

const crHour = (ms) => new Date(ms - CR_OFFSET_MS).getUTCHours();

// Whole clock hours in [aMs, bMs) whose Costa Rica hour is a waking hour.
function awakeHoursBetween(aMs, bMs, cfg = CONFIG) {
  let n = 0;
  for (let t = Math.ceil(aMs / H) * H; t < bMs; t += H) {
    const h = crHour(t);
    if (h >= cfg.awakeFrom && h < cfg.awakeTo) n++;
  }
  return n;
}

// timestamps: ms, any order. Returns the evaluation for one channel.
function evaluateChannel(channel, timestamps, nowMs, cfg = CONFIG) {
  const ts = timestamps.filter(t => Number.isFinite(t) && t <= nowMs).sort((a, b) => a - b);
  if (!ts.length) return { channel, firing: false, reason: 'no_history' };
  const lastAt = ts[ts.length - 1];
  const firstAt = ts[0];
  const windowStart = lastAt - cfg.windowHours * H;
  const inWindow = ts.filter(t => t > windowStart);
  // Distinct clock hours, not posts: one burst (a sync flood, 12 Messenger
  // contacts in 25 minutes on 2026-08-27) is one piece of evidence, not twelve.
  const activeHours = new Set(inWindow.map(t => Math.floor(t / H))).size;
  const baselineAwake = Math.max(cfg.ageFloorAwakeHours, awakeHoursBetween(Math.max(firstAt, windowStart), lastAt, cfg));
  const ratePerAwakeHour = activeHours / baselineAwake;
  const silentHours = (nowMs - lastAt) / H;
  const silentAwakeHours = awakeHoursBetween(lastAt, nowMs, cfg);
  const expected = ratePerAwakeHour * silentAwakeHours;
  const windowDays = Math.min(cfg.windowHours, Math.max(24, (lastAt - firstAt) / H)) / 24;
  const base = {
    channel, lastAt, silentHours, silentAwakeHours, activeHours,
    postsInWindow: inWindow.length, postsPerDay: inWindow.length / windowDays,
    ratePerAwakeHour, expected,
  };
  if (activeHours < cfg.minActiveHours) return { ...base, firing: false, reason: 'warmup' };
  if (expected < cfg.minExpected) return { ...base, firing: false, reason: 'within_normal' };
  return { ...base, firing: true, reason: 'silent' };
}

// rows: [{ posted_at, channel }] from lead_posts (personal rows already
// excluded by the query). Only the four watched channels are evaluated.
function evaluateLeadVolume(rows, nowMs, cfg = CONFIG) {
  const byChannel = {};
  for (const r of rows || []) {
    const c = r.channel || leadChannel({ source: r.source, medium: r.medium });
    if (!CHANNELS[c]) continue;
    (byChannel[c] = byChannel[c] || []).push(Date.parse(r.posted_at));
  }
  return Object.keys(CHANNELS).map(c => evaluateChannel(c, byChannel[c] || [], nowMs, cfg));
}

// One alert per silence episode: the episode is identified by the channel and
// the timestamp of the last post before it. Survives a restart because index.js
// stores it in agent_knowledge.
const alertKey = (f) => `lead-volume:${f.channel}:${new Date(f.lastAt).toISOString()}`;
const recoveryKey = (channel, lastAtIso) => `lead-volume-ok:${channel}:${lastAtIso}`;
function parseAlertKey(key) {
  const m = /^lead-volume:([a-z_]+):(.+)$/.exec(String(key || ''));
  return m ? { channel: m[1], lastAtIso: m[2], lastAt: Date.parse(m[2]) } : null;
}

function fmtCR(ms) {
  const d = new Date(ms - CR_OFFSET_MS);
  const mon = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'][d.getUTCMonth()];
  let h = d.getUTCHours();
  const ampm = h >= 12 ? 'PM' : 'AM';
  h = h % 12 || 12;
  return `${mon} ${d.getUTCDate()}, ${h}:${String(d.getUTCMinutes()).padStart(2, '0')} ${ampm} CR`;
}

function renderLeadVolumeAlert(f) {
  const ch = CHANNELS[f.channel];
  return [
    `⚠️ *LEAD INTAKE SILENT*: ${ch.label} has produced no lead post for ${f.silentHours.toFixed(0)}h (last one ${fmtCR(f.lastAt)}).`,
    `Trailing week: ${f.postsInWindow} posts, about ${f.postsPerDay.toFixed(1)} a day. At that pace the ${f.silentAwakeHours} waking hours since should have brought leads in about ${f.expected.toFixed(1)} separate hours; there were none.`,
    `Other channels are evaluated separately, so this is specific to ${ch.label}.`,
    `Check ${ch.check}`,
  ].join('\n');
}

function renderLeadVolumeRecovery({ channel, lastAt, resumedAt }) {
  const ch = CHANNELS[channel];
  return `✅ *LEAD INTAKE BACK*: ${ch.label} posted a lead again at ${fmtCR(resumedAt)}, after ${((resumedAt - lastAt) / H).toFixed(0)}h silent. Leads that arrived during the gap may still need a card: check GHL for contacts on this channel since ${fmtCR(lastAt)}.`;
}

module.exports = {
  CONFIG, CHANNELS,
  leadChannel, awakeHoursBetween, evaluateChannel, evaluateLeadVolume,
  alertKey, recoveryKey, parseAlertKey,
  renderLeadVolumeAlert, renderLeadVolumeRecovery, fmtCR,
};
