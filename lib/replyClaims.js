'use strict';
// Reply claims: a setter who answers a New Lead from the GHL inbox (web or the
// LeadConnector app, WhatsApp / Instagram / Messenger / SMS / email) owns the
// lead, exactly as if they had reacted ✅ on the card in #ng-sales-goats.
//
// What GHL gives us (shapes read live 2026-10-04, see test/hot-replies.test.js):
//   - a send typed in the GHL inbox carries the sender's `userId`;
//   - a send from the WhatsApp phone app or the Meta inbox is echoed back with
//     source 'app' and an EMPTY userId, so nobody can be credited;
//   - workflow / campaign sends carry the contact OWNER's userId and are not a
//     human reply.
// Pure module: no I/O, so index.js wires the GHL + Slack + Supabase calls and
// test/reply-claims.test.js covers the decisions.

const REPLY_MESSAGE_TYPES = new Set([
  'TYPE_WHATSAPP', 'TYPE_INSTAGRAM', 'TYPE_FACEBOOK', 'TYPE_SMS', 'TYPE_EMAIL', 'TYPE_LIVE_CHAT',
]);
// `source` values GHL stamps on sends no human typed in the moment.
const AUTOMATED_SOURCES = new Set(['workflow', 'campaign', 'bulk_actions', 'bulk_action', 'automation', 'api', 'trigger']);

function normId(v) { return String(v || '').trim().toLowerCase(); }

// Builds the lookup the sweep passes around: lowercase GHL user id → canonical
// id. `slackToGhl` is the SLACK_TO_GHL_USER map; `excludeSlackIds` drops people
// who may answer leads without owning them (Ron).
function setterIdLookup(slackToGhl, excludeSlackIds = []) {
  const skip = new Set(excludeSlackIds);
  const out = new Map();
  for (const [slackId, ghlId] of Object.entries(slackToGhl || {})) {
    if (skip.has(slackId) || !ghlId) continue;
    out.set(normId(ghlId), { ghlUserId: ghlId, slackUserId: slackId });
  }
  return out;
}

// First human setter reply in a thread (messages oldest-first). Returns
// { setter, messageType, atMs, unattributed } or null. `unattributed` counts the
// outbound human-looking sends that carry no userId, so the sweep can report how
// often a phone reply went uncredited.
//
// `defaultSetter` (Ron, 2026-10-26): with one setter taking new leads
// (Sebastián; Oscar leaves 2026-10-31 and only works his own pipeline), a
// human reply with no userId is credited to that setter, flagged
// `defaulted: true`. `isAutomatedBody` keeps Meta's auto replies (outbound,
// no userId, same shape as a phone reply) from counting as a human reply.
function firstSetterReply(messages, { setterIds, sinceMs = 0, defaultSetter = null, isAutomatedBody = null } = {}) {
  let unattributed = 0;
  let firstPhoneReply = null;
  for (const m of messages || []) {
    if (String(m.direction || '').toLowerCase() !== 'outbound') continue;
    const type = String(m.messageType || m.type || '');
    if (!REPLY_MESSAGE_TYPES.has(type)) continue;
    if (AUTOMATED_SOURCES.has(String(m.source || '').toLowerCase())) continue;
    const atMs = Date.parse(m.dateAdded || m.createdAt || '') || 0;
    if (atMs && atMs < sinceMs) continue;
    const uid = normId(m.userId);
    if (!uid) {
      if (isAutomatedBody && isAutomatedBody(m.body || m.message || '')) continue;
      unattributed++;
      if (!firstPhoneReply) firstPhoneReply = { messageType: type, atMs };
      continue;
    }
    const setter = setterIds && setterIds.get(uid);
    if (!setter) continue; // Max AI, a closer, Ron: not a setter claim
    return { setter, messageType: type, atMs, unattributed, defaulted: false };
  }
  if (!unattributed) return null;
  if (defaultSetter) return { setter: defaultSetter, messageType: firstPhoneReply.messageType, atMs: firstPhoneReply.atMs, unattributed, defaulted: true };
  return { setter: null, messageType: null, atMs: 0, unattributed, defaulted: false };
}

// Which unclaimed cards have a conversation that moved since the last sweep.
// `unclaimed` rows carry contactId; `convos` are GHL conversation search rows.
function matchConversations(unclaimed, convos) {
  const byContact = new Map();
  for (const l of unclaimed || []) if (l.contactId) byContact.set(l.contactId, l);
  const out = [];
  const seen = new Set();
  for (const c of convos || []) {
    const lead = byContact.get(c.contactId);
    if (!lead || seen.has(c.id)) continue;
    seen.add(c.id);
    out.push({ lead, conversationId: c.id });
  }
  return out;
}

const CHANNEL_LABEL = {
  TYPE_WHATSAPP: 'WhatsApp', TYPE_INSTAGRAM: 'Instagram', TYPE_FACEBOOK: 'Messenger',
  TYPE_SMS: 'SMS', TYPE_EMAIL: 'email', TYPE_LIVE_CHAT: 'live chat',
};
function channelLabel(messageType) { return CHANNEL_LABEL[messageType] || 'GHL'; }

module.exports = { REPLY_MESSAGE_TYPES, AUTOMATED_SOURCES, setterIdLookup, firstSetterReply, matchConversations, channelLabel };
