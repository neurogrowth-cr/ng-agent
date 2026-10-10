// Rules test for the reels briefs (daily DM + monthly ranking).  Run:  node test/reels-briefs.test.js
//
// lib/reelsBriefs.js is pure, so it is required directly. Media rows have the
// shape of Meta GET /{ig-user}/media with insights.metric(reach,saved,shares)
// field expansion; schedule rows have the shape of GHL posts/list (type all).
// All values are synthetic (public repo, never prod rows).
const rb = require('../lib/reelsBriefs');

let failures = 0;
function check(label, actual, expected) {
  const ok = JSON.stringify(actual) === JSON.stringify(expected);
  if (!ok) { failures++; console.error(`FAIL ${label}\n  expected: ${JSON.stringify(expected)}\n  actual:   ${JSON.stringify(actual)}`); }
  else console.log(`ok   ${label}`);
}

// ── windows ──────────────────────────────────────────────────────────────────
// Wednesday 2026-10-07 07:40 CR = 13:40Z.
const WED = new Date('2026-10-07T13:40:00Z');
const w = rb.dayWindows(WED);
check('today starts 00:00 CR', w.todayStart.toISOString(), '2026-10-07T06:00:00.000Z');
check('week starts Monday 00:00 CR', w.weekStart.toISOString(), '2026-10-05T06:00:00.000Z');
check('month starts day 1 00:00 CR', w.monthStart.toISOString(), '2026-10-01T06:00:00.000Z');
check('wednesday is not monday', w.isMonday, false);
check('late Sunday night CR still belongs to that week',
  rb.dayWindows(new Date('2026-10-12T05:30:00Z')).weekStart.toISOString(), '2026-10-05T06:00:00.000Z');
const mw = rb.monthWindows(new Date('2026-10-01T14:00:00Z'));
check('monthly reports the previous month', [mw.label, mw.period, mw.previousName], ['septiembre 2026', '2026-09', 'agosto']);
check('monthly window bounds', [mw.previousStart.toISOString(), mw.reportedStart.toISOString(), mw.reportedEnd.toISOString()],
  ['2026-08-01T06:00:00.000Z', '2026-09-01T06:00:00.000Z', '2026-10-01T06:00:00.000Z']);
check('january report compares with november', rb.monthWindows(new Date('2027-01-01T14:00:00Z')).label, 'diciembre 2026');
check('time formats in CR 12h', [rb.crTime(new Date('2026-10-08T00:00:00Z')), rb.crTime(new Date('2026-10-08T18:00:00Z'))], ['6:00 pm', '12:00 pm']);

// ── media ────────────────────────────────────────────────────────────────────
const media = (id, ts, reach, likes, comments, caption, over = {}) => ({
  id, timestamp: ts, media_product_type: 'REELS', caption, permalink: `https://www.instagram.com/reel/${id}/`,
  like_count: likes, comments_count: comments,
  insights: reach === undefined ? undefined : { data: [
    { name: 'reach', values: [{ value: reach }] }, { name: 'saved', values: [{ value: 2 }] }, { name: 'shares', values: [{ value: 1 }] }] },
  ...over,
});
const MEDIA = [
  media('w1', '2026-10-07T03:00:00+0000', 300, 4, 0, 'Uno de la semana.\n\nComente "LinkedIn".'),   // Tue 9 pm CR
  media('w2', '2026-10-06T12:00:00+0000', 1200, 30, 3, 'El mejor de la semana'),                     // Tue 6 am CR, 25 h before WED
  media('w3', '2026-10-07T12:00:00+0000', undefined, 1, 0, 'Sin insights todavía'),                  // Wed 6 am CR
  media('m1', '2026-10-02T15:00:00+0000', 5000, 50, 1, 'El mejor del mes'),
  media('p1', '2026-09-30T15:00:00+0000', 900, 9, 0, 'De septiembre'),
  { id: 'img', timestamp: '2026-10-06T15:00:00+0000', media_product_type: 'FEED', like_count: 99, comments_count: 9 },
];
const reels = rb.normalizeMedia(MEDIA);
check('only REELS are kept, newest first', reels.map((r) => r.id), ['w3', 'w1', 'w2', 'm1', 'p1']);
check('missing insights are null, never 0', [reels[0].reach, reels[0].saved], [null, null]);
check('insights are read from the field expansion', [reels[1].reach, reels[1].saved, reels[1].shares], [300, 2, 1]);

// ── schedule ─────────────────────────────────────────────────────────────────
const ghl = (over) => ({ _id: 'x', platform: 'instagram', status: 'scheduled', deleted: false, summary: 'Caption', ...over });
const SCHED = [
  ghl({ _id: 'parentA', scheduleDate: '2026-10-06T22:00:00.000Z', summary: 'Publicado ayer' }),
  ghl({ _id: 'childA', parentPostId: 'parentA', status: 'published', scheduleDate: '2026-10-06T22:00:00.000Z', publishedAt: '2026-10-06T22:07:00.000Z', summary: 'Publicado ayer', previewLink: 'https://www.instagram.com/reel/w2/' }),
  ghl({ _id: 'missed', scheduleDate: '2026-10-07T12:00:00.000Z', summary: 'Este debió salir a las 6 am' }),
  ghl({ _id: 'today', scheduleDate: '2026-10-08T00:00:00.000Z', summary: 'La acción cura todo.\nResto' }),
  ghl({ _id: 'thu', platform: 'google', parentPostId: '7adf18d8-uuid-group', scheduleDate: '2026-10-08T18:00:00.000Z', summary: 'Jueves' }), // GHL labels queued IG posts "google"
  ghl({ _id: 'gone', deleted: true, scheduleDate: '2026-10-08T18:00:00.000Z' }),
];
const schedule = rb.normalizeSchedule(SCHED);
check('a parent with a published child is dropped (no double count)', schedule.map((p) => p.id), ['childA', 'missed', 'today', 'thu']);
check('missed = scheduled, not published, past by more than 30 min', rb.missedPosts(schedule, WED).map((p) => p.id), ['missed']);
check('a post due 20 min ago is not missed yet', rb.missedPosts(schedule, new Date('2026-10-07T12:20:00Z')).map((p) => p.id), []);
check('failed status is always missed', rb.missedPosts(rb.normalizeSchedule([ghl({ _id: 'f', status: 'failed', scheduleDate: '2026-10-09T00:00:00.000Z' })]), WED).length, 1);

// ── daily ────────────────────────────────────────────────────────────────────
const daily = rb.formatDaily({ now: WED, schedule, reels });
const D = daily.split('\n');
check('daily header', D[0], '*📅 Reels · miércoles 7 oct*');
check('verdict rides on the Hoy header and warns when something missed', D[2].startsWith('*Hoy se publica* · ⚠️ ') && D[2].includes('un reel agendado no salió'), true);
check('today table: time right-aligned, caption, status', D.slice(3, 7), ['```', '6:00 am  Este debió salir a las 6 am  no salió', '6:00 pm  La acción cura todo.', '```']);
check('missed post is flagged', D.some((l) => l.startsWith('⚠️ No se publicó: Mié 7 oct 6:00 am · Este debió salir')), true);
check('upcoming counts only after today', D.includes('Próximos esta semana: 1'), true);
check('week header carries the totals and flags incomplete reach', D.includes('*📊 Semana* · 3 reels · alcance 1,500 (incompleto) · 35 likes · 3 coment.'), true);
const wk = D.indexOf('*📊 Semana* · 3 reels · alcance 1,500 (incompleto) · 35 likes · 3 coment.');
check('week table sorted by reach, fresh marked *, missing reach s/d', D.slice(wk + 1, wk + 6), [
  '```',
  'Día  Reel                   Alcance  Likes  Com  Guard',
  'Mar  El mejor de la semana    1,200     30    3      2',
  'Mar  Uno de la semana.*         300      4    0      2',
  'Mié  Sin insights todavía*      s/d      1    0    s/d',
]);
check('footnote and commented reels with links', D.includes('_* menos de 24 h_ · Con comentarios: <https://www.instagram.com/reel/w2/|El mejor de la semana>'), true);
check('month to date includes earlier October reels, not September', D.includes('*🗓️ Mes en curso* · 4 reels · alcance 6,500 (incompleto) · 85 likes · 4 coment.'), true);
check('best of the month links the reel', D.includes('Mejor: <https://www.instagram.com/reel/m1/|El mejor del mes> (alcance 5,000)'), true);
check('no last-week line on a wednesday', D.some((l) => l.startsWith('Semana pasada')), false);
check('no views section when none is given', daily.includes('Vistas'), false);
check('daily passes its own contract', rb.validateDaily(daily), { ok: true, problems: [] });

const withViews = rb.formatDaily({ now: WED, schedule, reels, viewsBlock: '*👁️ Vistas de ayer: 50*\n```\nTikTok  50\n```' });
const V = withViews.split('\n');
check('views section sits between Hoy and Semana', V.indexOf('*👁️ Vistas de ayer: 50*') > V.indexOf('Próximos esta semana: 1') && V.indexOf('*👁️ Vistas de ayer: 50*') < V.findIndex((l) => l.startsWith('*📊 Semana*')), true);
check('daily with views passes its contract', rb.validateDaily(withViews).ok, true);

const cleanSched = rb.normalizeSchedule(SCHED.filter((p) => p._id !== 'missed'));
const cleanReels = reels.filter((r) => r.id !== 'w3');
check('all good = green verdict', rb.formatDaily({ now: WED, schedule: cleanSched, reels: cleanReels }).includes('*Hoy se publica* · ✅ Todo publicado a tiempo.'), true);

const ghlDown = rb.formatDaily({ now: WED, schedule: null, reels });
check('GHL down: says so, still sends metrics', [ghlDown.includes('⚠️ No pude leer el calendario de GHL.'), ghlDown.includes('*📊 Semana* · 3 reels')], [true, true]);
const metaDown = rb.formatDaily({ now: WED, schedule, reels: null });
check('Meta down: no numbers invented', [metaDown.includes('⚠️ No pude leer las métricas de Meta.'), /alcance \d/.test(metaDown)], [true, false]);
check('Meta down still passes the contract (it is a valid warning brief)', rb.validateDaily(metaDown).ok, true);
check('validate catches an unclosed table', rb.validateDaily(`${daily}\n\`\`\``).problems, ['unclosed table']);

const MON = new Date('2026-10-12T13:40:00Z');
const monday = rb.formatDaily({ now: MON, schedule: [], reels });
check('monday adds last week', monday.split('\n').some((l) => l.startsWith('Semana pasada: 3 reels')), true);
check('empty day says so', monday.includes('Hoy no hay reels agendados.'), true);

// ── monthly ──────────────────────────────────────────────────────────────────
const NOV1 = new Date('2026-11-01T14:00:00Z');
const OCT = [
  media('a', '2026-10-03T15:00:00+0000', 4000, 40, 4, 'Alto'),
  media('b', '2026-10-10T15:00:00+0000', 2000, 100, 10, 'Medio con mucha interacción'),
  media('c', '2026-10-12T15:00:00+0000', 1000, 10, 0, 'Bajo'),
  media('d', '2026-10-20T15:00:00+0000', 50, 20, 5, 'Muy bajo, alcance menor a 100'),
  media('e', '2026-10-31T15:00:00+0000', 500, 5, 1, 'Cerró el mes'),
  media('s1', '2026-09-05T15:00:00+0000', 3000, 30, 2, 'Septiembre uno'),
  media('s2', '2026-09-25T15:00:00+0000', 1000, 10, 0, 'Septiembre dos'),
  media('n1', '2026-11-01T07:00:00+0000', 999, 9, 9, 'Ya es noviembre en CR'),
];
const monthly = rb.formatMonthly({ now: NOV1, reels: rb.normalizeMedia(OCT) });
const M = monthly.split('\n');
check('monthly header', M[0], '*🗓️ Reels de octubre 2026*');
check('summary compares with september', M[2], '*Resumen* · contra septiembre');
check('summary table', M.slice(3, 12), [
  '```',
  '              octubre  septiembre  Cambio',
  'Reels               5           2   +150%',
  'Alcance         7,550       4,000    +89%',
  'Alcance/reel    1,510       2,000    -24%',
  'Likes             175          40   +338%',
  'Comentarios        20           2   +900%',
  'Guardados          10           4   +150%',
  'Compartidos         5           2   +150%',
]);
check('top 3 by reach in order', M.filter((l) => /^[1-3] {2}\S/.test(l)).map((l) => l.slice(3, 31).trim()), ['Alto', 'Medio con mucha interacción', 'Bajo']);
check('top 3 links under the table', M.includes('Ver: <https://www.instagram.com/reel/a/|1> · <https://www.instagram.com/reel/b/|2> · <https://www.instagram.com/reel/c/|3>'), true);
check('best engagement ignores reach under 100', M.includes('*Mejor en interacción* · <https://www.instagram.com/reel/b/|Medio con mucha interacción> · 5.7%'), true);
const lo = M.indexOf('*Los 3 más bajos*');
check('bottom 3, lowest first', M.slice(lo + 3, lo + 6).map((l) => l.replace(/\s+[\d,]+$/, '')), ['Muy bajo, alcance menor a 100', 'Cerró el mes', 'Bajo']);
check('caveat line closes the report', M[M.length - 1], '_Métricas acumuladas a hoy; un reel de fin de mes tuvo menos días para sumar._');
check('monthly passes its contract', rb.validateMonthly(monthly, { reportedCount: 5 }), { ok: true, problems: [] });
check('a reel at 1 am CR on the 1st belongs to the new month', monthly.includes('Ya es noviembre'), false);

const rows = rb.monthRows(NOV1, rb.normalizeMedia(OCT));
check('snapshot rows: one per reel of the month, keyed by period', [rows.length, rows[0].iso_week, rows[0].kind], [5, '2026-10', 'month']);
check('snapshot row keeps nulls as nulls', rb.monthRows(NOV1, rb.normalizeMedia([media('z', '2026-10-05T15:00:00+0000', undefined, 1, 0, 'x')]))[0].reach, null);

check('meta down: monthly says so and has no numbers', rb.formatMonthly({ now: NOV1, reels: null }).includes('⚠️ No pude leer las métricas de Meta'), true);
check('empty month says so', rb.formatMonthly({ now: NOV1, reels: [] }).includes('No se publicaron reels en octubre 2026.'), true);
check('validate catches a short top list', rb.validateMonthly('*🗓️ Reels de octubre 2026*\n*Resumen*\n*Top 3 por alcance*\n```\n1  a  5\n```', { reportedCount: 5 }).problems, ['top list has 1 rows']);
check('validate catches an em dash', rb.validateDaily(`${daily}\nx \u2014 y`).problems, ['contains an em dash']);
check('validate catches undefined', rb.validateDaily(`${daily}\nundefined`).ok, false);

// ── lectura ──────────────────────────────────────────────────────────────────
const octReels = rb.normalizeMedia(OCT);
const prompt = rb.lecturaPrompt(NOV1, octReels);
check('lectura prompt names the month and the previous count', prompt.startsWith('Estos son los reels de Instagram de @linkedin.papi publicados en octubre 2026, ordenados por alcance (el mes anterior se publicaron 2).'), true);
check('lectura prompt ranks only the reported month by reach', (prompt.match(/^\d+\. alcance \d+/gm) || []).map((l) => l.split(' ')[0]), ['1.', '2.', '3.', '4.', '5.']);
check('lectura prompt carries no November reel', prompt.includes('Ya es noviembre'), false);
check('cleanLectura strips bullets, header, exclamations and em dashes',
  rb.cleanLectura('*Lectura*\n- Los reels en español ganaron \u2014 sobre todo los de mentalidad!\n- Sugerencia: publique 4 por semana con una idea concreta.'),
  'Los reels en español ganaron, sobre todo los de mentalidad.\nSugerencia: publique 4 por semana con una idea concreta.');
check('cleanLectura keeps at most 3 lines', rb.cleanLectura('Línea uno con suficiente texto para pasar.\nDos\nTres\nCuatro').split('\n').length, 3);
check('cleanLectura rejects empty and too short output', [rb.cleanLectura(''), rb.cleanLectura('ok')], [null, null]);
check('cleanLectura rejects output with undefined', rb.cleanLectura('El reel undefined tuvo el mayor alcance del mes entero.'), null);
const withRead = rb.withLectura(monthly, 'Los de mayor alcance son frases en español con una idea.');
const WR = withRead.split('\n');
check('lectura sits right before the caveat line', WR.slice(-4), ['*Lectura*', 'Los de mayor alcance son frases en español con una idea.', '', '_Métricas acumuladas a hoy; un reel de fin de mes tuvo menos días para sumar._']);
check('report with lectura still passes the contract', rb.validateMonthly(withRead, { reportedCount: 5 }).ok, true);
check('no lectura leaves the report untouched', rb.withLectura(monthly, null), monthly);

if (failures > 0) { console.error(`\n${failures} check(s) failed`); process.exit(1); }
console.log('\nall checks passed');
