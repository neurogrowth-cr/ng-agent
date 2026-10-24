// Sequence email reply alerts.  Run:  node test/sequence-replies.test.js
//
// lib/sequenceReplies.js is pure, so it is required directly. Email bodies are
// the real formats GHL returned on 2026-10-23 (Gmail web and mobile, Outlook
// for Android, Roundcube, plain-text Outlook), shortened, with names, addresses
// and companies made up because this repo is public.
const fs = require('fs');
const path = require('path');
const sr = require('../lib/sequenceReplies');

let failures = 0;
function check(label, actual, expected) {
  const ok = JSON.stringify(actual) === JSON.stringify(expected);
  if (!ok) { failures++; console.error(`FAIL ${label}\n  expected: ${JSON.stringify(expected)}\n  actual:   ${JSON.stringify(actual)}`); }
  else console.log(`ok   ${label}`);
}
const MIN = 60e3, H = 60 * MIN;
const cr = (iso) => Date.parse(`${iso}-06:00`); // CR wall clock (UTC-6, no DST)

// ── 1. Quote stripping, real client formats ─────────────────────────────────
const GMAIL = '<div dir="auto">Buenos d\u00edas,\u00a0<div dir="auto"><br></div><div dir="auto">Sigue igual\u00a0</div></div><br>'
  + '<div class="gmail_quote gmail_quote_container"><div dir="ltr" class="gmail_attr">El mi\u00e9, 21 de oct de 2026, 8:06\u202fa.m., Ron Duarte &lt;<a href="mailto:x">contact+x@mail.example.com</a>&gt; escribi\u00f3:<br></div>'
  + '<blockquote class="gmail_quote"><div style="display:none">Hablamos hace un tiempo.</div><p>Erika, hablamos hace un tiempo sobre conseguir clientes en LinkedIn. \u00bfSu situaci\u00f3n con los referidos cambi\u00f3 o sigue igual?</p><p>Ron</p></blockquote></div>\r\n';
check('1a Gmail mobile: only the answer survives', sr.cleanReply(GMAIL), 'Buenos días,\n\nSigue igual');

const ROUNDCUBE = '<html><head><meta http-equiv="Content-Type" content="text/html; charset=UTF-8" /></head><body>\r\n'
  + '<div class="pre">hola Ron&nbsp;</div>\r\n<div class="pre">gracias por tu interes, en este momento la inversi&oacute;n est&aacute; fuera de mis posibilidades ... me mantengo atento</div>\r\n'
  + '<div class="pre">&nbsp;</div>\r\n<div class="pre">Mario&nbsp;</div>\r\n<div class="pre">&nbsp;</div>\r\n'
  + '<div class="pre">El 2026-10-21 08:04, Ron Duarte escribi&oacute;:\r\n<blockquote type="cite">Hablamos hace un tiempo.<br />&iquest;Su situaci&oacute;n cambi&oacute; o sigue igual?</blockquote>\r\n</div>\r\n</body></html>';
check('1b Roundcube: attribution line and blockquote cut, entities decoded',
  sr.cleanReply(ROUNDCUBE), 'hola Ron\ngracias por tu interes, en este momento la inversión está fuera de mis posibilidades ... me mantengo atento\n\nMario');

const OUTLOOK_ANDROID = '<html>\r\n<head>\r\n<meta http-equiv="Content-Type" content="text/html; charset=iso-8859-1">\r\n</head>\r\n<body>\r\n'
  + '<div dir="auto" style="font-size: 12pt;">\r\nDe momento, ser\u00eda un no.&nbsp;</div>\r\n<div dir="auto">\r\n<br>\r\n</div>\r\n'
  + '<div dir="auto">\r\nSaludos.&nbsp;</div>\r\n'
  + '<div id="ms-outlook-mobile-signature" dir="auto">\r\n<span>Obtener\r\n</span><span><a href="https://aka.ms/x">Outlook para Android</a></span></div>\r\n'
  + '<hr style="display:inline-block;width:98%" tabindex="-1">\r\n<div id="divRplyFwdMsg" dir="ltr"><font><b>From:</b> contact+x@mail.example.com<br>\r\n<b>Sent:</b> Thursday, 22 October 2026 08:05:37<br>\r\n<b>Subject:</b> Juan, lo que le garantizo por contrato</font></div>\r\n'
  + '<div><p>Hola Juan,</p><p>Se las garantizo por contrato: <strong>10 o m\u00e1s al mes</strong>.</p></div>\r\n</body>\r\n</html>\r\n';
check('1c Outlook for Android: signature and From block cut', sr.cleanReply(OUTLOOK_ANDROID), 'De momento, sería un no.\n\nSaludos.');

const OUTLOOK_PLAIN = 'Me interesa, ¿podemos hablar el jueves a las 10 am?\n\nCarlos Pérez\nGerente General\n\n'
  + '________________________________\nDe: Ron Duarte <contact+x@mail.example.com>\nEnviado: martes, 21 de octubre de 2026 8:04\nPara: carlos@example.com\nAsunto: Carlos, ¿cambió o sigue igual?\n\nCarlos, hablamos hace un tiempo...';
check('1d Outlook plain text: underscore rule and De/Enviado header cut',
  sr.cleanReply(OUTLOOK_PLAIN), 'Me interesa, ¿podemos hablar el jueves a las 10 am?\n\nCarlos Pérez\nGerente General');

const HEADER_NO_RULE = 'PAUSA por favor\n\nDe: Ron Duarte <contact+x@mail.example.com>\nFecha: 21 oct 2026\nAsunto: Pedro, ARRANCO o PAUSA\n\ntexto viejo';
check('1e a De:/Fecha: header block with no rule above is cut', sr.cleanReply(HEADER_NO_RULE), 'PAUSA por favor');

const PLAIN_GT = 'ARRANCO\n\nOn Wed, Oct 21, 2026 at 8:04 AM Ron Duarte <contact+x@mail.example.com>\nwrote:\n> Responda ARRANCO o PAUSA\n> Ron';
check('1f English attribution wrapped over two lines, ">" quotes', sr.cleanReply(PLAIN_GT), 'ARRANCO');

const ORIGINAL = 'Socio con Laura, martes o jueves en la tarde.\n-----Mensaje original-----\nDe: Ron\nAsunto: Para su socio';
check('1g "-----Mensaje original-----" cut', sr.cleanReply(ORIGINAL), 'Socio con Laura, martes o jueves en la tarde.');

const EL_SENTENCE = 'El proyecto ya no se hizo, parece que no lo requirieron.\n\nEl mié, 21 de oct de 2026, 8:06 a.m., Ron Duarte <contact+x@mail.example.com>\nescribió:\n> texto';
check('1h a real sentence starting with "El" survives, the attribution below it is cut',
  sr.cleanReply(EL_SENTENCE), 'El proyecto ya no se hizo, parece que no lo requirieron.');

const ZOHO = 'Hola !\nEl proyecto ya no se hizo, no requirieron de mis servicios\nSaludos !\nPablo\n\n---- On Wed, 21 Oct 2026 08:05:36 -0600 Ron Duarte <contact+x@mail.example.com> wrote ----\n\nPablo, hablamos hace un tiempo';
check('1h2 Zoho dashed attribution cut', sr.cleanReply(ZOHO), 'Hola !\nEl proyecto ya no se hizo, no requirieron de mis servicios\nSaludos !\nPablo');
check('1h3 an opening <div> breaks the line (Gmail web)',
  sr.cleanReply('<div dir="ltr">Buen día cuál es el costo del sistema<div><br></div><div>Saludos</div><div>Lic. Mario Ruiz</div></div>'),
  'Buen día cuál es el costo del sistema\n\nSaludos\nLic. Mario Ruiz');
check('1h4 trailing signature contact data trimmed, name and title kept',
  sr.cleanReply('Sigue igual\n\nAndrés López\nBusiness Development Manager\nAv. Central 4014, Parque Industrial\nMonterrey, N.L 64310, Mexico.\nPhone: 812-555-0199\nandres@example.com\nwww.example.com\nhttps://vimeo.com/123456'),
  'Sigue igual\n\nAndrés López\nBusiness Development Manager\nAv. Central 4014, Parque Industrial');
check('1h5 a phone number inside the answer stays',
  sr.cleanReply('Confírmeme al +52 33 5555 0101 por WhatsApp.\nGracias\nSaludos'), 'Confírmeme al +52 33 5555 0101 por WhatsApp.\nGracias\nSaludos');
check('1i iPhone signature removed', sr.cleanReply('Me interesa\n\nEnviado desde mi iPhone'), 'Me interesa');
check('1j a client that quotes first (nothing above the quote) falls back to the whole body',
  sr.cleanReply('<blockquote>viejo</blockquote><div>Sí, me interesa</div>').includes('Sí, me interesa'), true);
check('1k clip keeps 600 chars on a word boundary', (() => {
  const c = sr.clip('palabra '.repeat(200));
  return [c.length <= 600, c.endsWith('…')];
})(), [true, true]);

// ── 2. Skips ────────────────────────────────────────────────────────────────
const skip = (from, subject, text) => sr.skipReason({ from, subject, text });
check('2a ordinary reply', skip('Ana <ana@example.com>', 'Re: Ana, ¿cambió o sigue igual?', 'Sigue igual'), null);
check('2b Spanish out-of-office subject', skip('ana@example.com', 'Respuesta automática: Ana, ¿cambió o sigue igual?', 'Estoy fuera'), 'auto_reply');
check('2c English automatic reply subject', skip('ana@example.com', 'Automatic reply: Ana, lo que le garantizo', 'Thanks'), 'auto_reply');
check('2d out of office body', skip('ana@example.com', 'Re: Ana', 'I am out of the office until Monday.'), 'auto_reply');
check('2e "fuera de la oficina" body', skip('ana@example.com', 'Re: Ana', 'Me encuentro fuera de la oficina sin acceso a mi correo.'), 'auto_reply');
check('2f vacation responder (vacaciones + return date + cue)',
  skip('ana@example.com', 'Re: Ana', 'Estoy de vacaciones hasta el 3 de noviembre. Para temas urgentes contactar a soporte@example.com.'), 'auto_reply');
check('2g a person on a trip who wants to talk is NOT an auto-reply',
  skip('ana@example.com', 'Re: Ana', 'Ando de viaje, a mi regreso hablamos con gusto.'), null);
check('2h mailer-daemon bounce', skip('Mail Delivery Subsystem <mailer-daemon@googlemail.com>', 'Delivery Status Notification (Failure)', 'Address not found'), 'bounce');
check('2i Outlook undeliverable subject', skip('postmaster@example.com', 'Undeliverable: Ana, lo que le garantizo', 'x'), 'bounce');
check('2j internal sender', skip('Jose <jose.carranza@neurogrowth.io>', 'Re: Ana', 'Le escribo yo'), 'internal');
check('2k internal subdomain sender', skip('contact+x@mail.neurogrowthconsulting.com', 'Re: Ana', 'x'), 'internal');
check('2l calendar acceptance (English body)', skip('ana@example.com', 'LinkedIn Flywheel Quick Sync', 'Ana Ruiz has accepted this invitation.'), 'calendar');
check('2m calendar acceptance (Spanish subject)', skip('ana@example.com', 'Aceptado: LinkedIn Flywheel', ''), 'calendar');
check('2n empty after stripping', skip('ana@example.com', 'Re: Ana', ' \n '), 'empty');

// ── 3. Intent tags ──────────────────────────────────────────────────────────
const tags = (t) => sr.tagIntents(t);
check('3a ARRANCO', tags('ARRANCO'), ['listo para contrato']);
check('3b arranquemos / firmemos, accents and case tolerant', tags('Listo, ¡arranquemos! Firmemos esta semana'), ['listo para contrato']);
check('3c CONTRATO', tags('Contrato por favor'), ['listo para contrato']);
check('3d PAUSA', tags('pausa'), ['pausa']);
check('3e SOCIO with two times is socio + quiere agendar', tags('SOCIO: martes 10 am o jueves 4 pm'), ['quiere agendar', 'socio']);
check('3f PATEK', tags('Me encaja PATEK'), ['eligió PATEK']);
check('3g Rolex in lower case', tags('rolex'), ['eligió ROLEX']);
check('3h job title and country on one line', tags('Gerentes de compras en México'), ['pidió la búsqueda']);
check('3i "cargo" and "país" words', tags('El cargo sería director comercial y el país Costa Rica'), ['pidió la búsqueda']);
check('3j title alone in a signature is not a search', tags('Sigue igual\n\nAna Ruiz\nBusiness Development Manager'), ['sigue igual']);
check('3k wants a call', tags('¿Qué horarios tiene para una llamada?'), ['quiere agendar']);
check('3k2 "reunión" in passing is not a request to book', tags('En la reunión su producto me sonó bien, pero tengo dudas'), ['respuesta']);
check('3l weekday plus a time', tags('El jueves a las 3:30 pm me queda bien'), ['quiere agendar']);
check('3m no me interesa', tags('Gracias pero no me interesa'), ['no interesado']);
check('3n "sería un no" (real Outlook reply)', tags('De momento, sería un no.\n\nSaludos.'), ['no interesado']);
check('3o darse de baja', tags('Favor de darme de baja de su lista'), ['no interesado']);
check('3p price question', tags('Buen día cuál es el costo del sistema'), ['pregunta el precio']);
check('3q sigue igual', tags('Buenos días,\n\nSigue igual'), ['sigue igual']);
check('3r anything else is "respuesta"', tags('Hola Ron, una disculpa, creo que hubo una confusión de mi parte'), ['respuesta']);
check('3s "usa" the verb is not a country', tags('El gerente usa LinkedIn a diario'), ['respuesta']);

// ── 4. Subject, group, mentions ─────────────────────────────────────────────
check('4a Re: stripped', sr.answeredSubject('Re: Ana, ¿cambió o sigue igual?'), 'Ana, ¿cambió o sigue igual?');
check('4b RE: RV: stacked prefixes stripped', sr.answeredSubject('RE: RV: Re: Para su socio'), 'Para su socio');
check('4c reactivation label wins', sr.groupLabel({ tags: ['reactivacion-oct26', 'react:no_show'], opportunities: [] }), 'reactivación · no-show');
check('4d abierto label', sr.groupLabel({ tags: ['react:abierto'] }), 'reactivación · deal abierto');
check('4e open deal stage = post-llamada', sr.groupLabel({ tags: [], opportunities: [{ status: 'open', pipelineStageId: '63d30181-4ec0-4daa-8832-a8eebe1afbeb' }] }), 'post-llamada');
check('4f call booked stage = pre-llamada', sr.groupLabel({ tags: [], opportunities: [{ status: 'open', pipelineStageId: 'dc1fba03-abeb-4b47-9d29-6c308002b6c1' }] }), 'pre-llamada');
check('4g follow-up tag = post-llamada', sr.groupLabel({ tags: ['follow-up'], opportunities: [] }), 'post-llamada');
check('4h nothing known = null', sr.groupLabel({ tags: ['fb lead form'], opportunities: [] }), null);

const MAPS = {
  ownerSlackByGhlId: { setter1: 'U_SETTER', closer1: 'U_JOSE', ron1: 'U_RON' },
  closerSlackByEmail: { 'jose@example.com': 'U_JOSE', 'jose.old@example.com': 'U_JOSE', 'ron@example.com': 'U_RON' },
  closerSlackByName: { 'jose carranza': 'U_JOSE', 'ron duarte': 'U_RON' },
  fallbackSlackId: 'U_RON',
};
const men = (o) => sr.resolveMentions({ ...MAPS, ...o });
check('4i setter and closer both mentioned', men({ assignedTo: 'SETTER1', closerEmail: 'Jose@example.com' }).mentions, ['U_SETTER', 'U_JOSE']);
check('4j same person once', men({ assignedTo: 'CLOSER1', closerEmail: 'jose.old@example.com' }).mentions, ['U_JOSE']);
check('4k closer only', men({ assignedTo: null, closerEmail: 'ron@example.com' }).mentions, ['U_RON']);
check('4l closer by name when the email is empty', men({ assignedTo: 'nobody', closerName: 'José Carranza' }).mentions, ['U_JOSE']);
check('4m Ron fallback when neither maps', (() => { const m = men({ assignedTo: 'nobody', closerEmail: 'x@example.com' }); return [m.mentions, m.fallback]; })(), [['U_RON'], true]);
check('4n custom field read', sr.customFieldValue({ customFields: [{ id: 'f1', value: ' jose@example.com ' }] }, 'f1'), 'jose@example.com');

// ── 5. Conversation screen ──────────────────────────────────────────────────
const convo = (o = {}) => ({ id: 'C1', contactId: 'K1', lastMessageDirection: 'inbound', lastMessageType: 'TYPE_EMAIL', email: 'ana@example.com',
  tags: ['reactivacion-oct26', 'react:abierto'], opportunities: [], ...o });
const scr = (c) => sr.screenConversation(c, { clientEmails: new Set(['client@example.com']) }).reason;
check('5a reactivation contact kept', scr(convo()), null);
check('5b personal tag skipped', scr(convo({ tags: ['react:abierto', 'personal'] })), 'personal');
check('5c won-deal tag skipped', scr(convo({ tags: ['won-deal'] })), 'client');
check('5d client roster email skipped', scr(convo({ email: 'Client@example.com' })), 'client');
check('5e internal contact skipped', scr(convo({ email: 'josue@neurogrowth.io' })), 'internal');
check('5f no tag and no sales opportunity (mailbox sync, webinar) skipped', scr(convo({ tags: ['webinar oct'], opportunities: [{ pipelineId: 'OTHER' }] })), 'no_sales_context');
check('5g a sales-pipeline opportunity is enough', scr(convo({ tags: [], opportunities: [{ pipelineId: 'KH1IQuaN8aNB1lfRpvP4', status: 'open' }] })), null);
check('5h last message outbound skipped', scr(convo({ lastMessageDirection: 'outbound' })), 'team_spoke_last');

// ── 6. Thread walk, claims, window ──────────────────────────────────────────
(async () => {
  const NOW = cr('2026-10-23T10:00:00'); // a Friday
  const since = NOW - 3 * 24 * H;
  const store = {
    ours1: { direction: 'outbound', at: NOW - 50 * H },
    rep1: { direction: 'inbound', at: NOW - 20 * H },
    rep2: { direction: 'inbound', at: NOW - 2 * H },
    old: { direction: 'inbound', at: NOW - 5 * 24 * H },
  };
  const reads = [];
  const fetchMeta = async (id) => { reads.push(id); return store[id] || null; };
  const ids = (r) => r.map(x => x.id);
  check('6a two unanswered replies after our email, oldest first',
    ids(await sr.collectNewReplies({ messageIds: ['ours1', 'rep1', 'rep2'], fetchMeta, since })), ['rep1', 'rep2']);
  check('6b the walk stops at our outbound email', reads.includes('ours1'), true);
  check('6c a handled reply stops the walk (restarts never re-alert)',
    ids(await sr.collectNewReplies({ messageIds: ['ours1', 'rep1', 'rep2'], fetchMeta, since, isKnown: (id) => id === 'rep1' })), ['rep2']);
  check('6d a reply older than the window is never alerted',
    ids(await sr.collectNewReplies({ messageIds: ['ours1', 'old'], fetchMeta, since })), []);
  check('6e a newer outbound email means the earlier reply was answered',
    ids(await sr.collectNewReplies({ messageIds: ['rep1', 'ours1'], fetchMeta, since })), []);

  check('6f claim states', [
    sr.claimState(undefined, NOW),
    sr.claimState(JSON.stringify({ s: 'posted', at: NOW }), NOW),
    sr.claimState(JSON.stringify({ s: 'skipped', reason: 'auto_reply' }), NOW),
    sr.claimState(JSON.stringify({ s: 'claimed', at: NOW - 2 * MIN }), NOW),
    sr.claimState(JSON.stringify({ s: 'claimed', at: NOW - 20 * MIN }), NOW),
  ], ['free', 'done', 'done', 'running', 'stale']);

  check('6g first run backfills exactly 3 days', sr.sinceFor({ now: NOW, watermark: null }), { since: NOW - 72 * H, firstRun: true });
  check('6h later runs go back to the watermark minus 1h', sr.sinceFor({ now: NOW, watermark: NOW - 10 * MIN }), { since: NOW - 70 * MIN, firstRun: false });
  check('6i a week-old watermark is capped at 3 days', sr.sinceFor({ now: NOW, watermark: NOW - 7 * 24 * H }).since, NOW - 72 * H);

  // ── 7. Reminder ladder: one reminder, 2h of window time, never again ──────
  const due = (o) => sr.reminderDue({ now: NOW, ...o });
  check('7a 2h after the post, inside the window: due', due({ postedAt: NOW - 2 * H - MIN }), true);
  check('7b 1h after the post: not yet', due({ postedAt: NOW - H }), false);
  check('7c handled with ✅: never', due({ postedAt: NOW - 3 * H, handledAt: NOW - H }), false);
  check('7d already reminded: no further nagging', due({ postedAt: NOW - 5 * H, remindedAt: NOW - 3 * H }), false);
  check('7e posted at 23:00, due 10:00 next day (clock starts at 08:00)',
    [sr.reminderDue({ now: cr('2026-10-23T09:30:00'), postedAt: cr('2026-10-22T23:00:00') }),
     sr.reminderDue({ now: cr('2026-10-23T10:01:00'), postedAt: cr('2026-10-22T23:00:00') })], [false, true]);
  check('7f no reminder at 21:00 CR', sr.reminderDue({ now: cr('2026-10-23T21:00:00'), postedAt: cr('2026-10-23T17:00:00') }), false);
  check('7g an alert older than 2 days is never reminded', due({ postedAt: NOW - 3 * 24 * H }), false);

  // ── 8. Answered in GHL ────────────────────────────────────────────────────
  const replyAt = NOW - 3 * H;
  const t = (ms) => new Date(ms).toISOString();
  check('8a a typed WhatsApp after the reply counts as answered', sr.answeredInGhl({ replyAt, messages: [
    { id: 'th', direction: 'inbound', messageType: 'TYPE_EMAIL', dateAdded: t(replyAt), source: 'workflow' },
    { id: 'w1', direction: 'outbound', messageType: 'TYPE_WHATSAPP', dateAdded: t(replyAt + H), source: 'app', userId: '' },
  ], threadId: 'th' }), true);
  check('8b a closer reply inside the email thread counts as answered', sr.answeredInGhl({ replyAt, messages: [
    { id: 'th', direction: 'outbound', messageType: 'TYPE_EMAIL', dateAdded: t(replyAt + H), source: 'workflow' },
  ], threadId: 'th' }), true);
  check('8c a workflow WhatsApp is not an answer', sr.answeredInGhl({ replyAt, messages: [
    { id: 'th', direction: 'inbound', messageType: 'TYPE_EMAIL', dateAdded: t(replyAt), source: 'workflow' },
    { id: 'w1', direction: 'outbound', messageType: 'TYPE_WHATSAPP', dateAdded: t(replyAt + H), source: 'workflow', userId: 'U1' },
  ], threadId: 'th' }), false);

  // ── 9. Formatting ─────────────────────────────────────────────────────────
  const card = sr.formatAlert({
    contactName: 'Ana Ruiz', intents: ['pausa'], subject: 'Ana, ARRANCO o PAUSA', group: 'post-llamada',
    quote: 'PAUSA por ahora <gracias>', link: 'https://app.gohighlevel.com/v2/location/L/contacts/detail/K1',
    mentions: ['U_SETTER', 'U_JOSE'], fallback: false,
  });
  check('9a card shape', card.split('\n'), [
    '<@U_SETTER> <@U_JOSE> 📧 *Respuesta por correo* · *Ana Ruiz*',
    '*Intención:* pausa',
    '*Respondió a:* Ana, ARRANCO o PAUSA · *Grupo:* post-llamada',
    '> PAUSA por ahora &lt;gracias&gt;',
    '⏸️ Pausa: ya pasa al correo mensual.',
    '<https://app.gohighlevel.com/v2/location/L/contacts/detail/K1|Abrir en GHL> · Reaccione ✅ cuando esté atendido.',
  ]);
  check('9b a valid card passes its contract', sr.validateAlert(card), null);
  check('9c a card with no quote fails', sr.validateAlert(card.split('\n').filter(l => !l.startsWith('> ')).join('\n')), 'missing quote');
  check('9d "undefined" outside the quote fails', sr.validateAlert(card.replace('Ana Ruiz', 'undefined')), 'placeholder value');
  check('9e Ron fallback note', sr.formatAlert({ contactName: 'X', intents: [], subject: '', group: null, quote: 'hola', link: 'https://x.example/a', mentions: ['U_RON'], fallback: true }).split('\n'), [
    '<@U_RON> 📧 *Respuesta por correo* · *X*', '*Intención:* respuesta', '*Respondió a:* (sin asunto)', '> hola',
    '_Sin setter ni closer asignado en GHL._', '<https://x.example/a|Abrir en GHL> · Reaccione ✅ cuando esté atendido.',
  ]);
  check('9e2 a backfilled reply says how old it is',
    sr.formatAlert({ contactName: 'Ana', intents: ['respuesta'], subject: 's', quote: 'hola', link: 'https://x.example/a', mentions: ['U1'], replyAt: NOW - 26 * H, now: NOW }).split('\n')[0],
    '<@U1> 📧 *Respuesta por correo* · *Ana* (hace 1d 2h)');
  check('9f reminder mentions the same people',
    sr.formatReminder({ mentions: ['U_SETTER', 'U_JOSE'], contactName: 'Ana Ruiz', postedAt: NOW - 2 * H, now: NOW }),
    '<@U_SETTER> <@U_JOSE> ⏰ La respuesta de *Ana Ruiz* sigue sin atender (hace 2h). Reaccione ✅ en el mensaje de arriba cuando esté atendida.');

  // ── 10. Wiring in index.js (text checks, the monolith is never required) ──
  const SRC = fs.readFileSync(path.join(__dirname, '..', 'index.js'), 'utf8');
  check('10a declared in STATIC_CRON_SCHEDULES every 10 min', /runSequenceReplyAlerts:\s*'\*\/10 \* \* \* \*'/.test(SRC), true);
  check('10b registered with the same schedule', SRC.includes("cron.schedule('*/10 * * * *', wrapCronJob('runSequenceReplyAlerts'"), true);
  check('10c kill switch read from SEQ_REPLY_ALERTS_DISABLED', /process\.env\.SEQ_REPLY_ALERTS_DISABLED === 'true'/.test(SRC), true);
  check('10d ✅ route reads the alert metadata', /metadata\?\.event_type === SEQ_REPLY_EVENT/.test(SRC), true);
  check('10e posts to #ng-sales-goats by id', /channel: LEAD_CHANNEL_ID, text: card/.test(SRC), true);
  check('10f no em dash in the new code', /\u2014/.test(fs.readFileSync(path.join(__dirname, '..', 'lib', 'sequenceReplies.js'), 'utf8').replace(/'\\u2014'/g, '')), false);

  if (failures) { console.error(`\n${failures} failure(s)`); process.exit(1); }
  console.log('\nAll sequence reply checks passed.');
})().catch((err) => { console.error(err); process.exit(1); });
