// Hot reply alerts + social intake gap.  Run:  node test/hot-replies.test.js
// Plan: ~/.claude/plans/reply-1-hashed-galaxy.md
//
// lib/hotReplies.js is pure, so it is required directly. Message shapes are the
// real GHL v2 shapes read on 2026-10-04 (phone-app sends: source 'app', empty
// userId; workflow sends: source 'workflow' with the owner's userId; stage
// changes: TYPE_ACTIVITY_OPPORTUNITY). Bodies are shortened, and names and
// numbers are made up because this repo is public.
const hr = require('../lib/hotReplies');

let failures = 0;
function check(label, actual, expected) {
  const ok = JSON.stringify(actual) === JSON.stringify(expected);
  if (!ok) { failures++; console.error(`FAIL ${label}\n  expected: ${JSON.stringify(expected)}\n  actual:   ${JSON.stringify(actual)}`); }
  else console.log(`ok   ${label}`);
}
const MIN = 60e3, H = 60 * MIN;
// CR wall clock → epoch ms (UTC-6, no DST).
const cr = (iso) => Date.parse(`${iso}-06:00`);
const isAuto = (b) => /para responderle con algo util/i.test(String(b).normalize('NFD').replace(/[̀-ͯ]/g, ''));

let seq = 0;
const inMsg  = (t, body, type = 'TYPE_WHATSAPP') => ({ id: `m${++seq}`, direction: 'inbound', messageType: type, body, dateAdded: new Date(t).toISOString(), userId: '' });
const phone  = (t, body, type = 'TYPE_WHATSAPP') => ({ id: `m${++seq}`, direction: 'outbound', messageType: type, body, dateAdded: new Date(t).toISOString(), userId: '', source: 'app' });
const ghlUi  = (t, body) => ({ id: `m${++seq}`, direction: 'outbound', messageType: 'TYPE_WHATSAPP', body, dateAdded: new Date(t).toISOString(), userId: 'SETTER1', source: 'app' });
const wf     = (t, body) => ({ id: `m${++seq}`, direction: 'outbound', messageType: 'TYPE_WHATSAPP', body, dateAdded: new Date(t).toISOString(), userId: 'SETTER1', source: 'workflow' });
const stage  = (t) => ({ id: `m${++seq}`, direction: 'outbound', messageType: 'TYPE_ACTIVITY_OPPORTUNITY', body: 'Opportunity updated', dateAdded: new Date(t).toISOString(), source: 'app' });

const NOW = cr('2026-10-05T15:00:00'); // a Monday, 3 PM CR
const convo = (over = {}) => ({
  id: 'C1', contactId: 'K1', contactName: 'Lead', lastMessageDirection: 'inbound', lastMessageType: 'TYPE_WHATSAPP',
  lastMessageDate: NOW - 30 * MIN, tags: ['fb lead form', 'intake-done'], opportunities: [{ id: 'O1', status: 'open', pipelineStageId: 'x' }],
  assignedTo: 'Wdjte1temxfR0lpi5RGV', email: 'lead@example.com', ...over,
});

// ── 1. Conversation screen ──────────────────────────────────────────────────
const clients = new Set(['client@example.com']);
const screen = (c) => hr.screenConversation(c, { now: NOW, clientEmails: clients }).reason;
check('1a an ordinary inbound lead chat is kept', screen(convo()), null);
check('1b team spoke last is skipped', screen(convo({ lastMessageDirection: 'outbound' })), 'team_spoke_last');
check('1c personal contact is skipped', screen(convo({ tags: ['Personal'] })), 'personal');
check('1d no-fit tag is skipped', screen(convo({ tags: ['no-fit', 'intake-done'] })), 'tag_no-fit');
check('1e only lost cards is skipped', screen(convo({ opportunities: [{ status: 'lost' }] })), 'lost');
check('1f an active client is skipped', screen(convo({ email: 'Client@Example.com' })), 'client');
check('1g email channel is out of scope', screen(convo({ lastMessageType: 'TYPE_EMAIL' })), 'other_channel');
check('1h older than the 48h lookback is skipped', screen(convo({ lastMessageDate: NOW - 49 * H })), 'too_old');
check('1i Instagram is in scope', screen(convo({ lastMessageType: 'TYPE_INSTAGRAM' })), null);

// ── 2. Thread analysis (the Alejandro shape, 2026-10-04) ───────────────────
const t0 = cr('2026-10-04T12:21:00');
const alejandro = [
  phone(t0, 'Te quería preguntar si aún mantienes el interés...'),
  inMsg(t0 + 40 * MIN, 'Si de que se trata'),
  phone(t0 + 80 * MIN, 'https://docs.google.com/document/d/abc'),
  phone(t0 + 83 * MIN, 'Te comparto este documento... A qué te dedicas?'),
  inMsg(t0 + 122 * MIN, 'Cual es el costo, el principal tema es que la inversion inicial es alta'),
];
const a1 = hr.analyzeThread(alejandro, { isAutomatedBody: isAuto });
check('2a the price question is the waiting anchor', [a1.anchorId, a1.waitingCount], [alejandro[4].id, 1]);
check('2b the quote is what the lead said', a1.quote.startsWith('Cual es el costo'), true);

const answered = [...alejandro, stage(t0 + 265 * MIN), phone(t0 + 265 * MIN, 'A qué te dedicas Alejandro?')];
check('2c a phone-app reply (source app, empty userId) answers it', hr.analyzeThread(answered, { isAutomatedBody: isAuto }), null);
check('2d a reply sent from the GHL UI answers it', hr.analyzeThread([inMsg(t0, 'hola'), ghlUi(t0 + MIN, 'hola!')], {}), null);
check('2e a stage change is not an answer',
  hr.analyzeThread([inMsg(t0, 'Cual es el costo'), stage(t0 + MIN)], {}).waitingCount, 1);
check('2f a workflow send is not an answer',
  hr.analyzeThread([inMsg(t0, 'Me interesa'), wf(t0 + MIN, 'Hola, Ron Duarte por acá...')], {}).waitingCount, 1);
check('2g a Meta auto reply is not an answer',
  hr.analyzeThread([inMsg(t0, 'Info', 'TYPE_FACEBOOK'), phone(t0 + MIN, 'Para responderle con algo útil y no información genérica...', 'TYPE_FACEBOOK')], { isAutomatedBody: isAuto }).waitingCount, 1);
const burst = [phone(t0, '¿Podría darme contexto?'), inMsg(t0 + MIN, 'Hago terapias'), inMsg(t0 + 2 * MIN, 'Unas 10 sesiones'), inMsg(t0 + 3 * MIN, 'se pueden cobrar en $600')];
const ab = hr.analyzeThread(burst.slice().reverse(), {}); // newest-first input, as GHL returns it
check('2h three messages in a row are one ladder anchored on the first', [ab.anchorId, ab.newestId, ab.waitingCount], [burst[1].id, burst[3].id, 3]);
check('2i a lead who never got an answer waits from their first message', hr.analyzeThread([inMsg(t0, 'Hola')], {}).anchorAt, t0);

// ── 3. Classifier prompt and verdict parsing ────────────────────────────────
const prompt = hr.buildClassifierPrompt({ contactName: 'Lead', tail: alejandro, callBooked: true });
check('3a prompt marks who spoke', /LEAD: Cual es el costo/.test(prompt) && /EQUIPO: Te comparto/.test(prompt), true);
check('3b call-booked leads get the hot rule', /already has a call booked/.test(prompt), true);
check('3c no call-booked line otherwise', /already has a call booked/.test(hr.buildClassifierPrompt({ tail: alejandro })), false);
check('3d valid JSON parses', hr.parseVerdict('{"verdict":"hot","reason_es":"Pregunta el precio","summary_es":"precio"}').verdict, 'hot');
check('3e JSON wrapped in prose still parses', hr.parseVerdict('Here: {"verdict":"noise","reason_es":"auto"}').verdict, 'noise');
check('3f garbage falls back to normal', hr.parseVerdict('no idea').verdict, 'normal');
check('3g an unknown verdict falls back to normal', hr.parseVerdict('{"verdict":"urgent"}').verdict, 'normal');

// ── 4. Poke window and ladder ───────────────────────────────────────────────
check('4a Monday 3 PM CR is inside the window', hr.inPokeWindow(NOW), true);
check('4b Sunday is outside', hr.inPokeWindow(cr('2026-10-04T15:00:00')), false);
check('4c 7:59 AM is outside, 8:00 AM inside', [hr.inPokeWindow(cr('2026-10-05T07:59:00')), hr.inPokeWindow(cr('2026-10-05T08:00:00'))], [false, true]);
check('4d 8 PM is outside', hr.inPokeWindow(cr('2026-10-05T20:00:00')), false);
check('4e Saturday 9 PM starts the clock Monday 8 AM', hr.clockStart(cr('2026-10-03T21:00:00')), cr('2026-10-05T08:00:00'));
check('4f Tuesday 6 AM starts the clock Tuesday 8 AM', hr.clockStart(cr('2026-10-06T06:00:00')), cr('2026-10-06T08:00:00'));
check('4g inside the window the clock is the message time', hr.clockStart(NOW - H), NOW - H);

const at = NOW - 3 * H; // message at noon Monday
const step = (now, sent = {}, verdict = 'hot', anchorAt = at) => hr.nextAlertStep({ now, anchorAt, verdict, sent });
check('4h 14 min: nothing yet', step(at + 14 * MIN), null);
check('4i 15 min: first poke', step(at + 15 * MIN), 'poke1');
check('4j after poke1, nothing until 2h', step(at + 90 * MIN, { poke1At: at + 15 * MIN }), null);
check('4k 2h: second poke', step(at + 2 * H, { poke1At: at + 15 * MIN }), 'poke2');
check('4l both sent: silence', step(at + 5 * H, { poke1At: at + 15 * MIN, poke2At: at + 2 * H }), null);
check('4m normal and noise never poke', [step(at + 3 * H, {}, 'normal'), step(at + 3 * H, {}, 'noise')], [null, null]);
check('4n classified hot late: poke1 now, poke2 waits 45 min',
  [step(at + 3 * H), step(at + 3 * H + 30 * MIN, { poke1At: at + 3 * H }), step(at + 3 * H + 45 * MIN, { poke1At: at + 3 * H })], ['poke1', null, 'poke2']);
check('4o nothing goes out on Sunday', step(cr('2026-10-04T15:00:00'), {}, 'hot', cr('2026-10-04T10:00:00')), null);
check('4p a Sunday message pokes Monday 8:15',
  [step(cr('2026-10-05T08:10:00'), {}, 'hot', cr('2026-10-04T10:00:00')), step(cr('2026-10-05T08:15:00'), {}, 'hot', cr('2026-10-04T10:00:00'))], [null, 'poke1']);

// ── 5. Who gets poked ───────────────────────────────────────────────────────
const SETTERS = { 'wdjte1temxfr0lpi5rgv': 'U_SEB' };
check('5a a setter-owned chat DMs the setter', hr.routeOwner('Wdjte1temxfR0lpi5RGV', SETTERS), { kind: 'setter', slackId: 'U_SEB' });
check('5b unassigned goes to the channel', hr.routeOwner('', SETTERS), { kind: 'unassigned' });
check('5c a closer-owned chat is not a setter\'s', hr.routeOwner('closerId', SETTERS), { kind: 'other' });

// ── 6. Formatting ───────────────────────────────────────────────────────────
const card = hr.formatPokeCard('poke1', { contactName: 'Lead', channel: 'TYPE_WHATSAPP', quote: 'Cual es el costo', reason_es: 'Pregunta el precio', link: 'https://x', anchorAt: NOW - 20 * MIN }, { now: NOW });
check('6a poke card carries quote, reason, link and wait', [/hace 20 min/.test(card), /> Cual es el costo/.test(card), /Por qué: Pregunta el precio/.test(card), /<https:\/\/x\|Abrir en GHL>/.test(card)], [true, true, true, true]);
check('6b second poke reads as a reminder', /Sigue sin respuesta/.test(hr.formatPokeCard('poke2', { contactName: 'L', channel: 'TYPE_INSTAGRAM', quote: 'q', link: 'l', anchorAt: NOW - 2 * H }, { now: NOW })), true);
check('6c unassigned card carries the @setters mention', hr.formatPokeCard('poke1', { contactName: 'L', channel: 'TYPE_FACEBOOK', quote: 'q', link: 'l', anchorAt: NOW }, { now: NOW, mention: '<!subteam^S1>' }).startsWith('<!subteam^S1>'), true);
const eod = hr.formatEodSection([
  { ownerLabel: 'Sebastian', contactName: 'A', channel: 'TYPE_WHATSAPP', quote: 'precio?', anchorAt: NOW - 5 * H },
  { ownerLabel: 'Sebastian', contactName: 'B', channel: 'TYPE_INSTAGRAM', quote: 'llamada?', anchorAt: NOW - 2 * H },
  { ownerLabel: 'Oscar', contactName: 'C', channel: 'TYPE_WHATSAPP', quote: 'mi correo', anchorAt: NOW - 1 * H },
], { now: NOW });
check('6d EOD section groups by setter, biggest first', [/HOT REPLIES STILL UNANSWERED \(3\)/.test(eod), eod.indexOf('*Sebastian* (2)') < eod.indexOf('*Oscar* (1)')], [true, true]);
check('6e an empty EOD section is empty', hr.formatEodSection([], { now: NOW }), '');

// ── 7. Social intake gap (zero LLM calls) ───────────────────────────────────
const ig = (over = {}) => convo({ lastMessageType: 'TYPE_INSTAGRAM', opportunities: [], tags: [], ...over });
const gap = (o) => hr.intakeGapVerdict({ now: NOW, hasLeadPost: false, oppCreatedAt: null, ...o });
check('7a stranded IG DM (wrote 2h ago, no card, no post) flags', gap({ convo: ig(), firstInboundAt: NOW - 2 * H }), { code: 'no_card' });
check('7b Messenger too', gap({ convo: ig({ lastMessageType: 'TYPE_FACEBOOK' }), firstInboundAt: NOW - 2 * H }), { code: 'no_card' });
check('7c Ron\'s own outreach DM with no reply never flags', gap({ convo: ig({ lastMessageDirection: 'outbound' }), firstInboundAt: null }), null);
check('7d a personal contact never flags', gap({ convo: ig({ tags: ['personal'] }), firstInboundAt: NOW - 2 * H }), null);
check('7e under 30 min the intake still has time', gap({ convo: ig(), firstInboundAt: NOW - 20 * MIN }), null);
check('7f a lead post clears it', gap({ convo: ig(), firstInboundAt: NOW - 2 * H, hasLeadPost: true }), null);
check('7g a fresh card with no Slack post flags', gap({ convo: ig({ opportunities: [{ id: 'O9' }] }), firstInboundAt: NOW - 2 * H, oppCreatedAt: NOW - H }), { code: 'no_post' });
check('7h an old card with no post is not this rule\'s business', gap({ convo: ig({ opportunities: [{ id: 'O9' }] }), firstInboundAt: NOW - 2 * H, oppCreatedAt: NOW - 30 * 24 * H }), null);
check('7i WhatsApp is not an intake channel here', gap({ convo: ig({ lastMessageType: 'TYPE_WHATSAPP' }), firstInboundAt: NOW - 2 * H }), null);
check('7j older than 24h is out of the window', gap({ convo: ig(), firstInboundAt: NOW - 25 * H }), null);
const alert = hr.formatIntakeGapAlert([
  { code: 'no_card', contactName: 'Stranded', channel: 'TYPE_INSTAGRAM', firstInboundAt: NOW - 2 * H, link: 'l1' },
  { code: 'no_post', contactName: 'NoPost', channel: 'TYPE_FACEBOOK', firstInboundAt: NOW - H, link: 'l2' },
], { now: NOW });
check('7k alert names both cases and where to look', [/NO GHL card/.test(alert), /Social DM Intake \(on reply\)\./.test(alert), /NO Slack lead post/.test(alert), /ghl-lead/.test(alert)], [true, true, true, true]);

// ── 8. Fail closed ──────────────────────────────────────────────────────────
let c = 0; const seen = [];
for (const failed of [true, true, true, true, false, true]) { const s = hr.nextFailureState(c, failed); c = s.count; seen.push([s.count, s.postBroken]); }
check('8a one broken post on the third failure in a row, reset by a good run',
  seen, [[1, false], [2, false], [3, true], [4, false], [0, false], [1, false]]);

if (failures) { console.error(`\n${failures} check(s) failed`); process.exit(1); }
console.log('\nall hot reply checks passed');
