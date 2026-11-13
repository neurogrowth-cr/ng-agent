'use strict';
// Rebuild lead_posts rows from the lead cards themselves.
//
// Intake posts the card to Slack first and writes its lead_posts row after.
// When Max's database is down the card lands and the row does not, and a
// retry in memory dies with the next deploy. On 2026-11-12 two cards posted
// during a two-hour database freeze lost their rows: the setter claimed both
// from Slack, the claim could not be mirrored, and one was reported as
// "got a GHL card but NO Slack lead post" while its post sat in the channel.
//
// The card is the durable copy: its Slack metadata (event_type ghl_lead)
// carries what the row needs. Pure; index.js owns the Slack and database I/O.

const LEAD_EVENT = 'ghl_lead';
// A card this young may still be waiting on its own row write.
const MIN_AGE_MS = 2 * 60 * 1000;

const str = (v) => (v == null || v === '' ? null : String(v));

// Top-level lead cards in a conversations.history page (include_all_metadata).
function leadCards(messages) {
  return (messages || []).filter((m) => m && m.ts && !(m.thread_ts && m.thread_ts !== m.ts)
    && m.metadata && m.metadata.event_type === LEAD_EVENT
    && m.metadata.event_payload && m.metadata.event_payload.contact_id);
}

// The lead_posts row a card stands for. Fields the card does not carry stay
// null (cards posted before 2026-11-13 hold only the id, name and email):
// never guessed from the text.
function rowFromCard(m, channelId) {
  const p = m.metadata.event_payload;
  return {
    contact_id: String(p.contact_id),
    slack_message_ts: m.ts,
    slack_channel_id: channelId,
    posted_at: new Date(Math.round(Number(m.ts) * 1000)).toISOString(),
    phone_last10: str(p.phone_last10),
    email_lower: p.email ? String(p.email).trim().toLowerCase() : null,
    source: str(p.source),
    channel: str(p.channel),
    full_name: str(p.full_name),
    name_prefix3: str(p.name_prefix3),
  };
}

// Rows to write: one per contact that has a card and no row. haveIds is the
// set of contact ids lead_posts already holds. The oldest card wins when a
// contact was carded twice, as the first write would have.
function missingRows(messages, haveIds, { channelId, now, minAgeMs = MIN_AGE_MS }) {
  const have = haveIds instanceof Set ? haveIds : new Set(haveIds || []);
  const byContact = new Map();
  for (const m of leadCards(messages)) {
    const tsMs = Number(m.ts) * 1000;
    if (!Number.isFinite(tsMs) || now - tsMs < minAgeMs) continue;
    const id = String(m.metadata.event_payload.contact_id);
    if (have.has(id)) continue;
    const seen = byContact.get(id);
    if (!seen || Number(m.ts) < Number(seen.ts)) byContact.set(id, m);
  }
  return [...byContact.values()].map((m) => rowFromCard(m, channelId));
}

module.exports = { LEAD_EVENT, MIN_AGE_MS, leadCards, rowFromCard, missingRows };
