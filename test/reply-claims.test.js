// Reply claims: a setter answering from the GHL inbox claims the card.  Run:  node test/reply-claims.test.js
// lib/replyClaims.js is pure. Message shapes follow test/hot-replies.test.js
// (phone-app send: source 'app', empty userId; inbox send: userId set;
// workflow send: owner's userId, source 'workflow').
const rc = require('../lib/replyClaims');

let failures = 0;
function check(label, actual, expected) {
  const ok = JSON.stringify(actual) === JSON.stringify(expected);
  if (!ok) { failures++; console.error(`FAIL ${label}\n  expected: ${JSON.stringify(expected)}\n  actual:   ${JSON.stringify(actual)}`); }
  else console.log(`ok   ${label}`);
}

const SLACK_TO_GHL = { 'USEB': 'Wdjte1temxfR0lpi5RGV', 'UOSC': 'ZcmdIz2EEraPd80W2zop', 'URON': 'zoGW530iDnPOFqQNfssc' };
const setterIds = rc.setterIdLookup(SLACK_TO_GHL, ['URON']);
const T0 = Date.parse('2026-10-26T15:00:00-06:00');
let seq = 0;
const msg = (over) => ({ id: `m${++seq}`, direction: 'outbound', messageType: 'TYPE_WHATSAPP', body: 'hola', dateAdded: new Date(T0 + seq * 60e3).toISOString(), source: 'app', userId: '', ...over });
const inbound = (over) => msg({ direction: 'inbound', ...over });

// 1. lookup
check('1a lookup keys are lowercase and keep the canonical id', setterIds.get('wdjte1temxfr0lpi5rgv'), { ghlUserId: 'Wdjte1temxfR0lpi5RGV', slackUserId: 'USEB' });
check('1b excluded Slack users are not setters', setterIds.has('zogw530idnpofqqnfssc'), false);

// 2. first setter reply
const pick = (msgs, opts) => { const r = rc.firstSetterReply(msgs, { setterIds, ...opts }); return r && { slack: r.setter && r.setter.slackUserId, type: r.messageType, un: r.unattributed }; };
check('2a inbox WhatsApp reply by Sebastian claims', pick([inbound(), msg({ userId: 'Wdjte1temxfR0lpi5RGV' })]), { slack: 'USEB', type: 'TYPE_WHATSAPP', un: 0 });
check('2b inbox Instagram reply claims (lowercase id)', pick([inbound({ messageType: 'TYPE_INSTAGRAM' }), msg({ messageType: 'TYPE_INSTAGRAM', userId: 'wdjte1temxfr0lpi5rgv' })]), { slack: 'USEB', type: 'TYPE_INSTAGRAM', un: 0 });
check('2c phone-app reply has no userId: nobody credited, counted', pick([inbound(), msg()]), { slack: null, type: null, un: 1 });
check('2d workflow send with the owner id is not a reply', pick([msg({ userId: 'Wdjte1temxfR0lpi5RGV', source: 'workflow' })]), null);
check('2e Ron answering does not claim for a setter', pick([inbound(), msg({ userId: 'zoGW530iDnPOFqQNfssc' })]), null);
check('2f Max AI send is ignored', pick([msg({ userId: 'cfLku0XE7BznqtmBSDmX' })]), null);
check('2g inbound only: nothing', pick([inbound(), inbound()]), null);
check('2h stage-change activity is not a reply', pick([msg({ messageType: 'TYPE_ACTIVITY_OPPORTUNITY', userId: 'Wdjte1temxfR0lpi5RGV' })]), null);
check('2i first setter wins when two answered', pick([msg({ userId: 'ZcmdIz2EEraPd80W2zop' }), msg({ userId: 'Wdjte1temxfR0lpi5RGV' })]), { slack: 'UOSC', type: 'TYPE_WHATSAPP', un: 0 });
check('2j a phone reply before the inbox reply still credits the inbox one', pick([msg(), msg({ userId: 'Wdjte1temxfR0lpi5RGV' })]), { slack: 'USEB', type: 'TYPE_WHATSAPP', un: 1 });
check('2k replies older than sinceMs are skipped', pick([msg({ userId: 'Wdjte1temxfR0lpi5RGV', dateAdded: new Date(T0 - 48 * 3600e3).toISOString() })], { sinceMs: T0 - 3600e3 }), null);
check('2l email reply from the inbox claims', pick([msg({ messageType: 'TYPE_EMAIL', userId: 'Wdjte1temxfR0lpi5RGV' })]), { slack: 'USEB', type: 'TYPE_EMAIL', un: 0 });

// 2bis. default setter for phone replies (Ron, 2026-10-26: Sebastián is the only setter on new leads)
const SEB = { ghlUserId: 'Wdjte1temxfR0lpi5RGV', slackUserId: 'USEB' };
const isAuto = (b) => /para responderle con algo util/i.test(String(b));
const pickD = (msgs) => { const r = rc.firstSetterReply(msgs, { setterIds, defaultSetter: SEB, isAutomatedBody: isAuto }); return r && { slack: r.setter && r.setter.slackUserId, type: r.messageType, un: r.unattributed, d: r.defaulted }; };
check('2m phone reply with a default setter credits him, flagged defaulted', pickD([inbound(), msg()]), { slack: 'USEB', type: 'TYPE_WHATSAPP', un: 1, d: true });
check('2n Instagram reply from the Meta inbox defaults too', pickD([inbound({ messageType: 'TYPE_INSTAGRAM' }), msg({ messageType: 'TYPE_INSTAGRAM' })]), { slack: 'USEB', type: 'TYPE_INSTAGRAM', un: 1, d: true });
check('2o Meta auto reply alone is not a human reply', pickD([inbound(), msg({ body: 'Gracias por escribir, para responderle con algo util...' })]), null);
check('2p auto reply then a real phone reply defaults', pickD([msg({ body: 'para responderle con algo util' }), inbound(), msg({ body: 'hola, soy Seb' })]), { slack: 'USEB', type: 'TYPE_WHATSAPP', un: 1, d: true });
check('2q an inbox reply by Oscar beats the default', pickD([msg(), msg({ userId: 'ZcmdIz2EEraPd80W2zop' })]), { slack: 'UOSC', type: 'TYPE_WHATSAPP', un: 1, d: false });
check('2r workflow sends never trigger the default', pickD([msg({ userId: 'Wdjte1temxfR0lpi5RGV', source: 'workflow' })]), null);
check('2s no default configured keeps the old behavior', pick([inbound(), msg()]), { slack: null, type: null, un: 1 });

// 3. matching convos to unclaimed cards
const unclaimed = [{ contactId: 'K1', fullName: 'A' }, { contactId: 'K2', fullName: 'B' }];
const convos = [{ id: 'C1', contactId: 'K1' }, { id: 'C9', contactId: 'K9' }, { id: 'C1', contactId: 'K1' }, { id: 'C2', contactId: 'K2' }];
check('3a only unclaimed contacts, each conversation once', rc.matchConversations(unclaimed, convos).map(m => `${m.lead.contactId}:${m.conversationId}`), ['K1:C1', 'K2:C2']);
check('3b no convos: nothing to read', rc.matchConversations(unclaimed, []), []);

// 4. labels
check('4a channel labels', ['TYPE_INSTAGRAM', 'TYPE_WHATSAPP', 'TYPE_FACEBOOK', 'TYPE_X'].map(rc.channelLabel), ['Instagram', 'WhatsApp', 'Messenger', 'GHL']);

if (failures) { console.error(`\n${failures} failure(s)`); process.exit(1); }
console.log('\nall reply-claims tests passed');
