// Rules test for the pipeline guardian, phase A.  Run:  node test/pipeline-guardian.test.js
// Spec: tasks/specs/pipeline-guardian-phase-a.md (sections 3.1, 3.2, 4a, 4b, 7).
//
// lib/pipelineGuardian.js is pure, so it is required directly. Messages have the
// GHL v2 conversation-message shape; every name and number is made up (public repo).
const pg = require('../lib/pipelineGuardian');
const hr = require('../lib/hotReplies');

let failures = 0;
function check(label, actual, expected) {
  const ok = JSON.stringify(actual) === JSON.stringify(expected);
  if (!ok) { failures++; console.error(`FAIL ${label}\n  expected: ${JSON.stringify(expected)}\n  actual:   ${JSON.stringify(actual)}`); }
  else console.log(`ok   ${label}`);
}

const MIN = 60e3, H = 60 * MIN, DAY = 24 * H;
const cr = (iso) => Date.parse(`${iso}-06:00`);
const NOW = cr('2026-11-04T10:00:00'); // a Wednesday, 10:00 CR
const at = (msAgo) => new Date(NOW - msAgo).toISOString();
let seq = 0;
const inMsg = (msAgo, body, type = 'TYPE_WHATSAPP') => ({ id: `m${++seq}`, direction: 'inbound', messageType: type, body, dateAdded: at(msAgo), userId: '' });
const outMsg = (msAgo, body, type = 'TYPE_WHATSAPP', extra = {}) => ({ id: `m${++seq}`, direction: 'outbound', messageType: type, body, dateAdded: at(msAgo), source: 'app', userId: '', ...extra });
const wf = (msAgo, body, type = 'TYPE_WHATSAPP') => outMsg(msAgo, body, type, { source: 'workflow', userId: 'SETTER1' });
const stageRow = (msAgo) => ({ id: `m${++seq}`, direction: 'outbound', messageType: 'TYPE_ACTIVITY_OPPORTUNITY', body: 'Opportunity updated', dateAdded: at(msAgo), source: 'app' });

const S = pg.STAGES;
const opp = (stage, over = {}) => ({ id: 'opp1', pipelineId: pg.SALES_PIPELINE_ID, pipelineStageId: stage, status: 'open', ...over });
const convo = (over = {}) => ({
  id: 'c1', contactId: 'ct1', contactName: 'Mariana Solís', lastMessageDirection: 'inbound', lastMessageType: 'TYPE_WHATSAPP',
  lastMessageDate: NOW - 10 * MIN, tags: [], email: 'mariana@example.test', phone: '+50600000000',
  opportunities: [opp(S.INITIAL_CONTACT)], ...over,
});

// ── screen ──
const CLIENTS = new Set(['cliente@example.test']);
check('open card in Initial Contact is kept', pg.guardianScreen(convo(), { now: NOW, clientEmails: CLIENTS }), { keep: true, reason: null });
check('guardian-off tag wins', pg.guardianScreen(convo({ tags: ['Guardian-Off'] }), { now: NOW, clientEmails: CLIENTS }).reason, 'guardian_off');
check('personal tag skips', pg.guardianScreen(convo({ tags: ['personal'] }), { now: NOW, clientEmails: CLIENTS }).reason, 'personal');
check('client email skips', pg.guardianScreen(convo({ email: 'Cliente@example.test' }), { now: NOW, clientEmails: CLIENTS }).reason, 'client');
check('Call Booked skips', pg.guardianScreen(convo({ opportunities: [opp(S.CALL_BOOKED)] }), { now: NOW, clientEmails: CLIENTS }).reason, 'call_booked_or_later');
check('lost card skips', pg.guardianScreen(convo({ opportunities: [opp(S.STRIKE_1, { status: 'lost' })] }), { now: NOW, clientEmails: CLIENTS }).reason, 'lost');
check('no sales card skips', pg.guardianScreen(convo({ opportunities: [] }), { now: NOW, clientEmails: CLIENTS }).reason, 'no_sales_card');
check('webinar registrant without a card skips', pg.guardianScreen(convo({ tags: ['webinar 2026-10-27'], opportunities: [] }), { now: NOW, clientEmails: CLIENTS }).reason, 'webinar');
check('ig guide held skips', pg.guardianScreen(convo(), { now: NOW, clientEmails: CLIENTS, igHeld: new Set(['c1']) }).reason, 'ig_guide_held');
check('silent lead (team spoke last, 5 days ago) is still kept', pg.guardianScreen(convo({ lastMessageDirection: 'outbound', lastMessageDate: NOW - 5 * DAY }), { now: NOW, clientEmails: CLIENTS }).keep, true);
check('Hot Strike is in the population', pg.guardianScreen(convo({ opportunities: [opp(S.HOT_STRIKE)] }), { now: NOW, clientEmails: CLIENTS }).keep, true);
check('an SMS inbound is another channel', pg.guardianScreen(convo({ lastMessageType: 'TYPE_SMS' }), { now: NOW, clientEmails: CLIENTS }).reason, 'other_channel');

// ── window rules per channel ──
check('Instagram 23h59 kept', pg.windowState({ channel: 'instagram', lastInboundAt: NOW - (23 * H + 59 * MIN) }, NOW).open, true);
check('Instagram 24h01 skipped', pg.windowState({ channel: 'instagram', lastInboundAt: NOW - (24 * H + 1 * MIN) }, NOW).open, false);
check('WhatsApp 24h01 skipped (templates are a later step)', pg.windowState({ channel: 'TYPE_WHATSAPP', lastInboundAt: NOW - (24 * H + 1 * MIN) }, NOW).open, false);
check('email always open', pg.windowState({ channel: 'email', lastInboundAt: NOW - 30 * DAY }, NOW), { channel: 'email', open: true, closesAt: null, remainingMs: null });
check('Messenger with no inbound ever has no window', pg.windowState({ channel: 'facebook', lastInboundAt: null }, NOW).reason, 'no_inbound');
check('window closesAt is lead message + 24h', pg.windowState({ channel: 'whatsapp', lastInboundAt: NOW - H }, NOW).closesAt, NOW - H + 24 * H);

// ── cadence ──
check('strike 1 not due at 23h', pg.strikeStep(NOW - 23 * H, 0, NOW), null);
check('strike 1 due at 1 day', pg.strikeStep(NOW - DAY, 0, NOW), 'strike_1');
check('strike 2 waits 2 days after strike 1', [pg.strikeStep(NOW - 1.5 * DAY, 1, NOW), pg.strikeStep(NOW - 2 * DAY, 1, NOW)], [null, 'strike_2']);
check('strike 3 at 3 days, strike 4 at 4, strike 5 at 7', [pg.strikeStep(NOW - 3 * DAY, 2, NOW), pg.strikeStep(NOW - 4 * DAY, 3, NOW), pg.strikeStep(NOW - 6 * DAY, 4, NOW), pg.strikeStep(NOW - 7 * DAY, 4, NOW)], ['strike_3', 'strike_4', null, 'strike_5']);
check('re-nurture 1 at 60 days after strike 5', [pg.strikeStep(NOW - 59 * DAY, 5, NOW), pg.strikeStep(NOW - 60 * DAY, 5, NOW)], [null, 'renurture_1']);
check('re-nurture 2 at 75 days (15 after re-nurture 1)', [pg.strikeStep(NOW - 14 * DAY, 6, NOW), pg.strikeStep(NOW - 15 * DAY, 6, NOW)], [null, 'renurture_2']);
check('sequence ends after re-nurture 2', pg.strikeStep(NOW - 400 * DAY, 7, NOW), null);
check('no last message, nothing due', pg.strikeStep(null, 0, NOW), null);

// cadence from a real-shaped thread: workflow first message, lead replied, our answer, then two chases
const realThread = [
  wf(9 * DAY, 'Hola, gracias por escribirnos. Para responderle con algo útil...'),
  inMsg(9 * DAY - 10 * MIN, 'Hola, me interesa saber más'),
  outMsg(9 * DAY - 40 * MIN, 'Con gusto. A quién le vende, empresas o personas?'),
  stageRow(9 * DAY - 39 * MIN),
  outMsg(7 * DAY, 'Le escribo de nuevo, seguimos aquí por si quiere retomar.'),
  outMsg(4 * DAY, 'Y hoy cómo le llegan los clientes?', 'TYPE_WHATSAPP', { userId: 'SETTER1' }),
];
const ts = pg.threadState(realThread, { isAutomatedBody: (b) => /para responderle con algo útil/i.test(b) });
check('thread state: team spoke last, two strikes sent since the lead wrote', [ts.leadSpokeLast, ts.strikesSent, ts.outboundSinceInbound, ts.channel], [false, 2, 3, 'whatsapp']);
check('thread state: last outbound is the newest chase', ts.lastOutboundAt, NOW - 4 * DAY);
check('cadence from the thread: strike 3 is due (4 days since strike 2)', pg.strikeStep(ts.lastOutboundAt, ts.strikesSent, NOW), 'strike_3');
const neverReplied = [wf(2 * DAY, 'Hola, bienvenido.')];
const ts2 = pg.threadState(neverReplied, { isAutomatedBody: () => false });
check('a workflow first message starts the clock but is not a strike', [ts2.strikesSent, ts2.lastOutboundAt, ts2.lastInboundAt], [0, NOW - 2 * DAY, null]);
check('lead spoke last reads as waiting', pg.threadState([outMsg(2 * H, 'Hola'), inMsg(H, 'Buenas')]).leadSpokeLast, true);
check('activity rows never count as messages', pg.threadState([inMsg(H, 'Buenas'), stageRow(50 * MIN)]).leadSpokeLast, true);

// ── decision table (3.1) ──
const analysisFor = (msgs) => hr.analyzeThread(msgs, { isAutomatedBody: () => false });
const waiting = analysisFor([outMsg(2 * H, 'Hola, a quién le vende?'), inMsg(30 * MIN, 'Vendo consultoría a empresas, me interesa')]);
const base = { analysis: waiting, verdict: 'normal', signals: {}, thread: null, contact: { email: 'x@example.test', phone: '+506' }, stageId: S.INITIAL_CONTACT, now: NOW };
const dec = (over) => pg.decideAction({ ...base, ...over });
const kindStep = (d) => [d.kind, d.step, d.reason, d.channel];

check('row 1: interested → draft reply', kindStep(dec({ intent: 'interested' })), ['reply', 'reply', 'interested', 'whatsapp']);
check('row 1: wants_info → reply', dec({ intent: 'wants_info' }).kind, 'reply');
check('row 1: referral → reply', dec({ intent: 'referral' }).kind, 'reply');
check('row 1: positive_not_ready → reply', dec({ intent: 'positive_not_ready' }).kind, 'reply');
check('row 1: question inside the FAQ → reply', dec({ intent: 'question', signals: { faqCovered: true } }).kind, 'reply');
const callAsk = analysisFor([outMsg(2 * H, 'Hola'), inMsg(30 * MIN, 'Sí, llámeme mañana a las 3 por favor')]);
check('row 2: call request → signal, no draft', kindStep(dec({ analysis: callAsk, intent: 'interested' })), ['signal', 'signal', 'call_request', 'whatsapp']);
const phoneShare = analysisFor([outMsg(2 * H, 'Hola'), inMsg(30 * MIN, 'Mi número es 8888 1234, escríbame ahí')]);
check('row 2: shares a phone → signal', dec({ analysis: phoneShare, intent: 'interested' }).reason, 'call_request');
check('row 2: classifier call_request flag → signal', dec({ intent: 'interested', signals: { callRequest: true } }).reason, 'call_request');
check('row 3: question outside the FAQ → signal with suggestion', kindStep(dec({ intent: 'question', signals: { faqCovered: false } })), ['signal', 'signal', 'outside_faq', 'whatsapp']);
check('row 3: objection_budget → signal', dec({ intent: 'objection_budget' }).reason, 'objection');
check('row 3: objection_timing → signal', dec({ intent: 'objection_timing' }).kind, 'signal');
check('row 3: competitor_mention → signal', dec({ intent: 'competitor_mention' }).kind, 'signal');
check('row 4: hot verdict → signal', kindStep(dec({ intent: 'interested', verdict: 'hot' })), ['signal', 'signal', 'hot', 'whatsapp']);
for (const i of ['unsubscribe', 'angry', 'already_client']) check(`row 5: ${i} → stop, Ron not paged`, kindStep(dec({ intent: i })).concat([dec({ intent: i }).ron]), ['stop', 'stop', i, 'whatsapp', false]);
for (const i of ['legal_risk', 'sensitive']) check(`row 5: ${i} → stop and DM Ron`, [dec({ intent: i }).kind, dec({ intent: i }).ron], ['stop', true]);
check('stop beats a call request', dec({ analysis: callAsk, intent: 'angry' }).kind, 'stop');
const silent = { analysis: null, thread: ts, intent: null, verdict: null };
check('row 6: silent 4 days after strike 2 → strike 3 on WhatsApp? no: window closed, email exists → email', kindStep(dec(silent)), ['strike', 'strike_3', 'silence', 'email']);
check('row 6: window closed and no email → skipped window_closed', kindStep(dec({ ...silent, contact: { email: '', phone: '+506' } })), ['skip', 'strike_3', 'window_closed', 'whatsapp']);
const freshSilent = pg.threadState([inMsg(26 * H, 'Hola'), outMsg(25 * H, 'Hola, a quién le vende?')]);
check('row 6: lead wrote 26h ago, our answer 25h ago: strike 1 due, window closed → email', kindStep(dec({ ...silent, thread: freshSilent })), ['strike', 'strike_1', 'silence', 'email']);
const insideWindow = pg.threadState([inMsg(23 * H, 'Hola'), outMsg(25 * H, 'Hola'), outMsg(24.5 * H, 'Sigue ahí?')]);
check('row 6: lead spoke last is not a strike', dec({ ...silent, thread: insideWindow }).reason, 'lead_waiting_unanalyzed');
const igSilent = pg.threadState([inMsg(30 * H, 'Hola', 'TYPE_INSTAGRAM'), outMsg(29 * H, 'Hola, a quién le vende?', 'TYPE_INSTAGRAM')]);
check('row 6: Instagram strike with closed window and no email → window_closed', kindStep(dec({ ...silent, thread: igSilent, contact: {} })), ['skip', 'strike_1', 'window_closed', 'instagram']);
check('row 6: not due yet → skip not_due', dec({ ...silent, thread: pg.threadState([outMsg(2 * H, 'Hola')]) }).reason, 'not_due');
check('row 7: silent after strike 5 → re-nurture 1 at 60 days', dec({ ...silent, thread: { ...ts, strikesSent: 5, lastOutboundAt: NOW - 61 * DAY, lastInboundAt: NOW - 70 * DAY } }).step, 'renurture_1');
check('row 8: unclear → one clarifying question', kindStep(dec({ intent: 'unclear' })), ['reply', 'clarify', 'unclear', 'whatsapp']);
const igWaiting = analysisFor([outMsg(2 * H, 'Hola', 'TYPE_INSTAGRAM'), inMsg(30 * MIN, 'Me interesa', 'TYPE_INSTAGRAM')]);
check('row 9: Instagram lead with no phone and no email → askContact', dec({ analysis: igWaiting, intent: 'interested', contact: {} }).askContact, true);
check('row 9: Instagram lead with a phone → no ask', dec({ analysis: igWaiting, intent: 'interested', contact: { phone: '+506' } }).askContact, false);
check('reply window closed, no email → skip window_closed', kindStep(dec({ analysis: analysisFor([inMsg(25 * H, 'Hola, me interesa')]), intent: 'interested', contact: {} })), ['skip', 'reply', 'window_closed', 'whatsapp']);
check('a burst still settling waits', dec({ analysis: analysisFor([inMsg(MIN, 'Hola')]), intent: 'interested' }).reason, 'settling');
check('Call Booked never gets an action', dec({ intent: 'interested', stageId: S.CALL_BOOKED }).reason, 'call_booked_or_later');

// ── validateDraft (4a) ──
const v = (t) => pg.validateDraft(t);
check('good usted draft passes', v('Con gusto. Para orientarlo mejor, a quién le vende hoy, empresas o personas?'), { ok: true, problems: [] });
check('placeholder fails', v('Hola [Nombre], a quién le vende?').problems.includes('placeholder'), true);
check('{{ fails', v('Hola {{name}}').problems.includes('placeholder'), true);
check('undefined fails', v('Hola undefined, qué vende?').problems.includes('placeholder'), true);
check('tú fails', v('Cuéntame qué vendes y a quién, tú decides.').problems.includes('uses tú'), true);
check('tienes as a whole word fails', v('Si tienes tiempo hablamos.').problems.includes('uses tú'), true);
check('a word containing tienes (mantienes) is not a tú form', v('Lo que usted mantienes hoy, cómo le llegan clientes?').problems.includes('uses tú'), false);
check('em dash fails', v('Perfecto \u2014 a quién le vende?').problems.includes('em dash'), true);
check('foreign URL fails', v('Mire https://example.com/oferta').problems[0].startsWith('unexpected link'), true);
check('approved booking link passes', v(`Aquí puede agendar: ${pg.APPROVED_URLS.booking}`).ok, true);
check('approved guide link passes', v(`Le dejo la guía: ${pg.APPROVED_URLS.guide}`).ok, true);
check('price with $ fails', v('Cuesta $5,000 al mes.').problems.includes('mentions a price'), true);
check('price with USD fails', v('Son 5000 USD por el sistema.').problems.includes('mentions a price'), true);
check('the FAQ floor without a currency sign passes', v('Funciona para servicios de 1,500 dólares en adelante.').ok, true);
check('4 paragraphs fail', v('Uno.\n\nDos.\n\nTres.\n\nCuatro.').problems.includes('4 paragraphs'), true);
check('over 700 chars fails', v('a'.repeat(701)).problems[0].startsWith('too long'), true);
check('empty fails', v('').ok, false);
check('guarantee fails', v('Le garantizo resultados en un mes.').problems.includes('banned word "garantizo"'), true);
check('exclamation fails', v('Hola! qué vende?').problems.includes('exclamation mark'), true);

// ── card round trip (3.2, 4a) ──
const LINK = 'https://app.gohighlevel.com/v2/location/LOC/contacts/detail/ct1';
const replyCard = pg.formatCard({
  kind: 'reply', step: 'reply', contactName: 'Mariana Solís', channel: 'whatsapp', intent: 'interested',
  leadQuote: 'Vendo consultoría a empresas, me interesa', leadAt: NOW - 30 * MIN, stageName: 'Initial Contact',
  draft: 'Con gusto. Para orientarlo mejor, cuánto cobra por cliente, más o menos?', problems: [],
  closesAt: NOW - 30 * MIN + 24 * H, contactLink: LINK,
}, { now: NOW });
const RL = replyCard.split('\n');
check('reply card header', RL[0], '🛡️ Guardián · Mariana Solís · WhatsApp · interesado');
check('reply card lead line', RL[1], 'Lead (hace 30 min): "Vendo consultoría a empresas, me interesa"');
check('reply card stage and step', RL[2], 'Etapa: Initial Contact · Paso: respuesta');
check('reply card draft label then draft', [RL[3], RL[4]], ['Borrador:', 'Con gusto. Para orientarlo mejor, cuánto cobra por cliente, más o menos?']);
check('reply card expiry is the Meta window in CR time', RL[5], 'Vence: 09:30 CR (ventana de Meta cierra 09:30)');
check('reply card controls', RL[6], '✅ enviar · ❌ descartar · responda en este hilo para enviar su texto');
check('reply card link', RL[7], `🔗 ${LINK}`);
check('reply card passes validateCard', pg.validateCard(replyCard), { ok: true, problems: [] });

const signalCard = pg.formatCard({
  kind: 'signal', step: 'signal', contactName: 'Diego Araya', channel: 'instagram', intent: 'objection_budget',
  leadQuote: 'Y eso cuánto cuesta? Tengo poco presupuesto', leadAt: NOW - 5 * MIN, stageName: 'New Lead',
  draft: 'El precio depende de su caso y se ve en la llamada. Qué vende y a quién?', problems: [],
  closesAt: NOW - 5 * MIN + 24 * H, ficha: { empresa: 'Araya Consultores', vende_a: 'pymes', dolor: null, urgencia: null }, contactLink: LINK,
}, { now: NOW });
const SL = signalCard.split('\n');
check('signal card header', SL[0], '🛡️ Guardián · Diego Araya · Instagram · objeción de presupuesto');
check('signal card uses Sugerencia', SL[3], 'Sugerencia:');
check('signal card ficha', SL[5], 'Ficha: empresa: Araya Consultores · a quién vende: pymes');
check('signal card booking link', SL[6], `Agendar: ${pg.APPROVED_URLS.booking}`);
check('signal card expires in 15 min', SL[7], 'Vence: 10:15 CR (ventana de Meta cierra 09:55)');
check('signal card passes validateCard', pg.validateCard(signalCard), { ok: true, problems: [] });

const strikeCard = pg.formatCard({
  kind: 'strike', step: 'strike_3', contactName: 'Lucía Mora', channel: 'email', intent: null, leadQuote: 'Hola, me interesa saber más',
  leadAt: NOW - 9 * DAY, stageName: 'Strike 2', draft: 'Le escribo por correo porque por WhatsApp no nos cruzamos.\n\nLe sirve si vemos su caso en una llamada corta esta semana?', problems: [], closesAt: null, contactLink: LINK,
}, { now: NOW });
check('strike card step and no window', [strikeCard.split('\n')[2], strikeCard.includes('(sin ventana)')], ['Etapa: Strike 2 · Paso: strike 3 de 5', true]);
check('strike card intent reads silencio', strikeCard.split('\n')[0].endsWith('· Email · silencio'), true);
check('strike card passes validateCard', pg.validateCard(strikeCard).ok, true);

const invalidCard = pg.formatCard({ kind: 'reply', step: 'reply', contactName: 'X', channel: 'whatsapp', intent: 'interested', leadQuote: 'hola', leadAt: NOW - MIN, stageName: 'New Lead', draft: 'tú', problems: ['uses tú'], closesAt: NOW + H, contactLink: LINK }, { now: NOW });
check('invalid draft card shows the mark and still validates', [invalidCard.includes(`Borrador: ${pg.INVALID_DRAFT_MARK} (uses tú)`), pg.validateCard(invalidCard).ok], [true, true]);
check('validateCard red on a missing link', pg.validateCard(replyCard.replace(`🔗 ${LINK}`, '')).problems, ['missing GHL link']);
check('validateCard red on an empty draft', pg.validateCard(replyCard.replace(RL[4], '')).problems.includes('empty draft'), true);
check('validateCard red on a signal without Agendar', pg.validateCard(signalCard.replace(SL[6], 'Agendar: ')).problems.includes('signal without Agendar'), true);
check('validateCard red on a placeholder', pg.validateCard(replyCard.replace('Mariana Solís', 'undefined')).problems.includes('placeholder'), true);

// ── digest and verdict line (7) ──
const rows = [
  { status: 'drafted', contact_name: 'Mariana Solís', channel: 'whatsapp', step: 'reply', window_closes_at: new Date(NOW + 2 * H).toISOString() },
  { status: 'signal', contact_name: 'Diego Araya', channel: 'instagram', step: 'signal', window_closes_at: new Date(NOW + 20 * H).toISOString() },
  { status: 'drafted', contact_name: 'Lucía Mora', channel: 'email', step: 'strike_3', window_closes_at: null },
  { status: 'drafted', contact_name: 'Pedro Vega', channel: 'facebook', step: 'strike_1', window_closes_at: new Date(NOW - H).toISOString() },
  { status: 'sent', contact_name: 'Ya enviado', channel: 'whatsapp', step: 'reply', window_closes_at: null },
];
const digest = pg.formatDigest(rows, { now: NOW, stats: { leadsRead: 42, drafts: 6, signals: 2, sent: 4, expired: 1, medianMinutes: 12.4, approvalRate: 0.8 } });
check('digest header counts only waiting rows', digest.split('\n')[0], '🛡️ *Guardián · cierre del día* · 4 tarjeta(s) esperando su ✅');
check('digest groups by expiry', [digest.includes('*Vencen hoy* (1)'), digest.includes('*Vencen mañana* (1)'), digest.includes('*Sin ventana* (1)'), digest.includes('*Ventana cerrada* (1)')], [true, true, true, true]);
check('digest verdict line', digest.split('\n').pop(), '✅ ALL GREEN · guardián: 42 leads leídos, 6 borradores, 2 señales a Sebastián, 4 enviados, 1 vencidos, mediana 12 min, aprobación 80%');
check('issues verdict line', pg.verdictLine({ issues: ['3 corridas fallaron', 'reintento automático ok', 'nada para Ron'] }), '⚠️ ISSUES · 3 corridas fallaron · reintento automático ok · nada para Ron');
check('a quiet day with nothing waiting is no post', pg.formatDigest([], { now: NOW, stats: {} }), '');
check('no em dash anywhere in the digest', /\u2014/.test(digest), false);

// ── watchdog (4b) ──
const warm = { daysActive: 7, runsToday: 52, capStreak: 0, cardsToday: 12, isWeekday: true, inboundToday: 9, approvals: 8, rejections: 1, medianMinutes: 10, expiredShare: 0.05, sentInShadowOrOff: 0, sentWithoutApproval: 0, divergence: 0, maxDraftsPerRun: 8 };
const levels = (m) => Object.fromEntries(pg.evaluateWatchdog(m).map((r) => [r.metric, r.level]));
check('all green', Object.values(levels(warm)).every((l) => l === 'green'), true);
check('runs 50 yellow, 47 red', [levels({ ...warm, runsToday: 50 }).runs, levels({ ...warm, runsToday: 47 }).runs], ['yellow', 'red']);
check('cap hit 3 runs in a row red', levels({ ...warm, capStreak: 3 }).drafts_cap, 'red');
check('zero cards on a weekday with inbound is red', levels({ ...warm, cardsToday: 0 }).cards, 'red');
check('zero cards on a weekday with no inbound is fine', levels({ ...warm, cardsToday: 0, inboundToday: 0 }).cards, 'green');
check('61 cards red, 45 yellow', [levels({ ...warm, cardsToday: 61 }).cards, levels({ ...warm, cardsToday: 45 }).cards], ['red', 'yellow']);
check('approval 60% yellow, 40% red', [levels({ ...warm, approvals: 6, rejections: 4 }).approval, levels({ ...warm, approvals: 4, rejections: 6 }).approval], ['yellow', 'red']);
check('median 20 yellow, 31 red', [levels({ ...warm, medianMinutes: 20 }).median, levels({ ...warm, medianMinutes: 31 }).median], ['yellow', 'red']);
check('expired 20% yellow, 30% red', [levels({ ...warm, expiredShare: 0.2 }).expired, levels({ ...warm, expiredShare: 0.3 }).expired], ['yellow', 'red']);
check('warmup keeps the gated rows green', levels({ ...warm, daysActive: 2, runsToday: 3, cardsToday: 0 }).runs, 'green');
check('zero tolerance ignores warmup', [levels({ ...warm, daysActive: 0, sentInShadowOrOff: 1 }).sent_in_shadow, levels({ ...warm, daysActive: 0, sentWithoutApproval: 1 }).sent_without_approval], ['red', 'red']);
check('divergence 1 yellow, 2 red', [levels({ ...warm, divergence: 1 }).divergence, levels({ ...warm, divergence: 2 }).divergence], ['yellow', 'red']);
check('median helper', [pg.median([5, 1, 3]), pg.median([1, 2, 3, 4]), pg.median([])], [3, 2.5, null]);

// ── prompts and parsing ──
const prompt = pg.buildDraftPrompt({ action: { kind: 'strike', step: 'strike_5', askContact: true }, thread: 'NOSOTROS: hola', contactName: 'Ana', intentLabel: 'silencio', stageName: 'Strike 3', channel: 'whatsapp' });
check('system prompt carries usted, the playbook and both links', [prompt.system.includes('siempre de usted'), prompt.system.includes('PLAYBOOK'), prompt.system.includes(pg.APPROVED_URLS.booking), prompt.system.includes(pg.APPROVED_URLS.guide)], [true, true, true, true]);
check('user prompt names the step and the move-off-channel ask', [prompt.user.includes('Strike 5 de 5'), prompt.user.includes('termine pidiendo su WhatsApp o su correo')], [true, true]);
check('no em dash in the prompts', /\u2014/.test(prompt.system + prompt.user), false);
check('draft parse with text around', pg.parseDraft('Aquí va:\n{"draft":"Hola, a quién le vende?","reason_es":"falta el cliente"}'), { ok: true, draft: 'Hola, a quién le vende?', reason_es: 'falta el cliente' });
check('broken draft JSON never throws', pg.parseDraft('nope').ok, false);
const cls = pg.parseClassification('{"verdict":"hot","intent":"question","call_request":false,"faq_covered":true,"reason_es":"pregunta cómo funciona","summary_es":"quiere saber el proceso","ficha":{"empresa":"Vega SA","vende_a":null,"dolor":"","urgencia":"null"}}');
check('classification parses intent, flags and ficha', [cls.verdict, cls.intent, cls.faqCovered, cls.ficha], ['hot', 'question', true, { empresa: 'Vega SA', vende_a: null, dolor: null, urgencia: null }]);
check('unknown intent becomes unclear, bad JSON is normal/unclear', [pg.parseClassification('{"intent":"foo","verdict":"hot"}').intent, pg.parseClassification('x').verdict], ['unclear', 'normal']);
check('classifier prompt lists every intent class', pg.INTENT_CLASSES.every((i) => pg.buildClassifierPrompt({ contactName: 'A', tail: [] }).includes(i)), true);
check('thread for prompt labels the workflow send as automatic', pg.threadForPrompt(realThread).startsWith('NOSOTROS (automático): Hola, gracias'), true);

// ── send text ──
check('no thread reply sends the draft', pg.pickSendText({ draft: 'Hola', threadReplies: [{ user: 'B', text: 'x', ts: '1' }], approverIds: ['SETTER'] }), { text: 'Hola', edited: false });
check('the approver\'s latest thread reply wins', pg.pickSendText({ draft: 'Hola', threadReplies: [{ user: 'SETTER', text: 'uno', ts: '2' }, { user: 'RON', text: 'dos', ts: '3' }], approverIds: ['SETTER', 'RON'] }), { text: 'dos', edited: true });
check('slack markup is unwrapped', pg.unSlack(`<${pg.APPROVED_URLS.booking}|agende> &amp; listo`), `${pg.APPROVED_URLS.booking} & listo`);

if (failures > 0) { console.error(`\n${failures} check(s) failed`); process.exit(1); }
console.log('\nall checks passed');
