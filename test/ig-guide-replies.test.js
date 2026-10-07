// Rules test for Instagram guide replies (phase A).  Run:  node test/ig-guide-replies.test.js
//
// lib/igGuideReplies.js is pure, so it is required directly. Messages have the
// GHL v2 conversation-message shape; every name and number is made up (public repo).
const ig = require('../lib/igGuideReplies');

let failures = 0;
function check(label, actual, expected) {
  const ok = JSON.stringify(actual) === JSON.stringify(expected);
  if (!ok) { failures++; console.error(`FAIL ${label}\n  expected: ${JSON.stringify(expected)}\n  actual:   ${JSON.stringify(actual)}`); }
  else console.log(`ok   ${label}`);
}

const BOOK = 'https://api.leadconnectorhq.com/widget/bookings/linkedin-flywheel-appointment';
const NOW = Date.parse('2026-10-08T16:00:00Z');
const at = (minAgo) => new Date(NOW - minAgo * 60000).toISOString();
const msg = (id, dir, minAgo, body, extra = {}) => ({ id, direction: dir, messageType: 'TYPE_INSTAGRAM', dateAdded: at(minAgo), body, ...extra });

// ── conversation screen ──
const convo = { lastMessageType: 'TYPE_INSTAGRAM', lastMessageDirection: 'inbound', tags: ['IG-Guia-LinkedIn'] };
check('tagged inbound IG conversation is in', ig.isGuideConversation(convo, { tag: 'ig-guia-linkedin' }), true);
check('untagged is out', ig.isGuideConversation({ ...convo, tags: ['otro'] }, { tag: 'ig-guia-linkedin' }), false);
check('personal tag wins', ig.isGuideConversation({ ...convo, tags: ['ig-guia-linkedin', 'personal'] }, { tag: 'ig-guia-linkedin' }), false);
check('whatsapp is out', ig.isGuideConversation({ ...convo, lastMessageType: 'TYPE_WHATSAPP' }, { tag: 'ig-guia-linkedin' }), false);
check('team spoke last is out', ig.isGuideConversation({ ...convo, lastMessageDirection: 'outbound' }, { tag: 'ig-guia-linkedin' }), false);

// ── what to answer ──
const guideDm = msg('o1', 'outbound', 60, 'Aquí tiene la guía...', { source: 'workflow' });
const thread = [msg('i0', 'inbound', 61, 'LinkedIn'), guideDm, msg('i1', 'inbound', 10, 'Gracias, la leí.'), msg('i2', 'inbound', 9, 'Vendo consultoría a pymes.')];
check('a burst after the workflow DM is one pending reply, newest id', ig.latestPendingInbound(thread, { now: NOW }),
  { id: 'i2', at: NOW - 9 * 60000, text: 'Gracias, la leí.\nVendo consultoría a pymes.' });
check('a human answer after it means nothing pending',
  ig.latestPendingInbound([...thread, msg('o2', 'outbound', 5, 'Hola', { source: 'app' })], { now: NOW }), null);
check('still settling (under 2 minutes) waits', ig.latestPendingInbound([msg('i9', 'inbound', 1, 'hola')], { now: NOW }), null);
check('24 h window closed is skipped', ig.latestPendingInbound([msg('i9', 'inbound', 24 * 60 + 1, 'hola')], { now: NOW }), null);
check('no inbound at all', ig.latestPendingInbound([guideDm], { now: NOW }), null);

check('window remaining', ig.fmtRemaining(ig.windowRemainingMs(at(60), NOW)), '23 h 0 min');
check('window closed label', ig.fmtRemaining(ig.windowRemainingMs(at(25 * 60), NOW)), 'cerrada');

// ── prompt ──
const sys = ig.buildSystemPrompt({ bookingUrl: BOOK, guideUrl: 'https://neurogrowth.io/recursos/plantillas' });
check('system prompt carries the booking link', sys.includes(BOOK), true);
check('system prompt is in usted and forbids tú', sys.includes('siempre de usted'), true);
check('thread labels the guide DM as automatic', ig.threadForPrompt(thread).includes('NOSOTROS (automático): Aquí tiene la guía...'), true);

// ── parse ──
const good = JSON.stringify({ intent: 'interested', stage: 'discover', collected: { offer: 'consultoría', buyer: 'pymes', price: null }, draft: 'Gracias por escribir. ¿Cuánto cobra por cliente, más o menos?', reason_es: 'falta precio' });
const p = ig.parseDraft(`Aquí va:\n${good}`);
check('parse ok with text around', p.ok, true);
check('parse fills every field', Object.keys(p.collected).length, ig.FIELDS.length);
check('parse keeps known fields, nulls the rest', [p.collected.offer, p.collected.price, p.collected.challenge], ['consultoría', null, null]);
check('unknown intent becomes other', ig.parseDraft('{"intent":"hot","draft":"x"}').intent, 'other');
check('broken JSON never throws', ig.parseDraft('no json').ok, false);

// ── validate ──
const v = (draft, intent = 'interested') => ig.validateDraft(draft, { bookingUrl: BOOK, intent });
check('clean draft passes', v('Gracias por escribir. ¿Qué vende y a quién?'), { ok: true, problems: [] });
check('exclamation fails', v('¡Gracias! ¿Qué vende?').problems.includes('exclamation mark'), true);
check('long dash fails', v('Gracias — ¿qué vende?').problems.includes('long dash'), true);
check('tú fails', v('¿Qué vendes y a quién?').problems.includes('uses tú'), true);
check('price fails', v('Cuesta $5,000 al mes.').problems.includes('mentions a price'), true);
check('the disqualification floor is allowed', v('Funciona con servicios de 1,500 dólares en adelante.').ok, true);
check('banned word fails', v('Le mando más leads.').problems.includes('banned word "leads"'), true);
check('word containing a banned word is fine', v('Gracias por su clientela.').ok, true);
check('booking link is allowed', v(`Puede agendar aquí: ${BOOK}`).ok, true);
check('any other link fails', v('Mire https://example.com').problems[0].startsWith('unexpected link'), true);
check('empty draft fails unless not interested', [v('').ok, v('', 'not_interested').ok], [false, true]);
check('emoji fails', v('Gracias 🙌 ¿qué vende?').problems.includes('emoji'), true);

// ── Ron's DM ──
const dm = ig.formatRonDm({ contactName: 'Ana P.', intent: 'interested', stage: 'discover', collected: p.collected,
  draft: 'Gracias. ¿Cuánto cobra?', leadText: 'Vendo consultoría a pymes.', remainingMs: 5 * 3600000, problems: [], reason: '' });
const L = dm.split('\n');
check('dm header', L[0], '📸 *Instagram · Ana P. respondió a la guía* · interesado · ventana: 5 h 0 min');
check('dm quotes the lead', L[1], '> Vendo consultoría a pymes.');
check('dm lists what is known', L[2], '_Vende: consultoría · A quién: pymes_');
check('dm ends with the controls', L[L.length - 1], '✅ enviar · ❌ descartar · responda en este hilo para editar (se envía su texto)');

// ── what gets sent ──
const RON = 'U1';
check('no thread reply sends the draft', ig.pickSendText({ draft: 'Hola', threadReplies: [{ user: 'B', text: 'x', ts: '1' }], ronUserId: RON }), { text: 'Hola', edited: false });
check("Ron's latest thread reply wins", ig.pickSendText({ draft: 'Hola', threadReplies: [{ user: RON, text: 'uno', ts: '2' }, { user: RON, text: 'dos', ts: '3' }], ronUserId: RON }), { text: 'dos', edited: true });
check('slack link markup is unwrapped', ig.unSlack(`<${BOOK}|${BOOK}> &amp; listo`), `${BOOK} & listo`);

if (failures > 0) { console.error(`\n${failures} check(s) failed`); process.exit(1); }
console.log('\nall checks passed');
