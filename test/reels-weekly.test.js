// Rules test for the Monday reels summary.  Run:  node test/reels-weekly.test.js
//
// lib/reelsWeekly.js is pure (no Slack, no network, no Supabase), so it is
// required directly. The posts below have the shape of GHL Social Planner
// child posts (POST /social-media-posting/{locationId}/posts/list); the stats
// object has the shape of POST /social-media-posting/statistics. All values
// are synthetic (public repo, never prod rows).
const rw = require('../lib/reelsWeekly');

let failures = 0;
function check(label, actual, expected) {
  const ok = JSON.stringify(actual) === JSON.stringify(expected);
  if (!ok) { failures++; console.error(`FAIL ${label}\n  expected: ${JSON.stringify(expected)}\n  actual:   ${JSON.stringify(actual)}`); }
  else console.log(`ok   ${label}`);
}

// Monday 2026-10-12 08:00 CR = 14:00Z. Last week = Mon 10-05 00:00 CR .. Sun 10-11 24:00 CR.
const NOW = new Date('2026-10-12T14:00:00Z');
const W = rw.weekWindow(NOW);
check('window start is Mon 00:00 CR (06:00Z)', W.start.toISOString(), '2026-10-05T06:00:00.000Z');
check('window end is next Mon 00:00 CR', W.end.toISOString(), '2026-10-12T06:00:00.000Z');
check('iso week label', W.isoWeek, '2026-W41');
check('range label', W.label, '5 a 11 oct');
check('week window works on a Sunday night CR', rw.weekWindow(new Date('2026-10-12T04:00:00Z')).isoWeek, '2026-W40');
check('range label across months', rw.weekWindow(new Date('2026-11-02T14:00:00Z')).label, '26 oct a 1 nov');

const post = (over) => ({
  _id: 'g1', platform: 'instagram', status: 'published', deleted: false, type: 'reel', postId: '17900000000000001',
  publishedAt: '2026-10-07T17:48:28.528Z', previewLink: 'https://www.instagram.com/reel/AAA/',
  summary: 'En Facebook está el que regatea. En LinkedIn, el que firma.\n\nComente "LinkedIn" y le paso una guía.',
  insights: { like: 72, share: 9, comment: 4 }, ...over,
});
const POSTS = [
  post(),
  post({ _id: 'g2', postId: '17900000000000002', publishedAt: '2026-10-07T22:01:00Z', summary: 'Dos garantías por contrato, no por promesa.\n\nGarantía de tiempo: ...', insights: { like: 40, share: 2, comment: 1 } }),
  post({ _id: 'g3', postId: '17900000000000003', publishedAt: '2026-10-04T22:00:00Z' }),            // previous week: out
  post({ _id: 'g4', postId: '17900000000000004', status: 'scheduled', publishedAt: null }),        // not published: out
  post({ _id: 'g5', postId: '17900000000000005', platform: 'facebook' }),                           // other platform: out
  post({ _id: 'g6', postId: undefined }),                                                           // no media id: out
];

const norm = rw.normalizePosts(POSTS, W);
check('keeps only published instagram posts inside the window', norm.map((p) => p.ghlPostId), ['g1', 'g2']);
check('hook is the first sentence of the caption', norm[0].hook, 'En Facebook está el que regatea.');
check('hook is truncated with an ellipsis', rw.hookOf('x'.repeat(100)).length, 70);
check('hook falls back when the caption is empty', rw.hookOf(''), '(sin caption)');
check('insights are copied as numbers', [norm[0].likes, norm[0].comments, norm[0].shares], [72, 4, 9]);
check('reach is null before Meta', norm[0].reach, null);

const merged = rw.mergeMetaInsights(norm, { '17900000000000001': { reach: 12400, views: 15000, saved: 31 } });
check('meta insights merge by media id', [merged[0].reach, merged[0].views, merged[0].saved], [12400, 15000, 31]);
check('posts without meta data are untouched', merged[1].reach, null);
check('no meta map returns the same list', rw.mergeMetaInsights(norm, null), norm);

check('ranking by engagement when no reach', rw.rankReels(norm).map((p) => p.ghlPostId), ['g1', 'g2']);
const flipped = rw.mergeMetaInsights(norm, { '17900000000000002': { reach: 50000 } });
check('ranking by reach when any reel has it', rw.rankReels(flipped).map((p) => p.ghlPostId), ['g2', 'g1']);

const STATS = { results: { totals: { posts: 6, likes: 285, followers: 64, impressions: 50325, comments: 12 },
  breakdowns: { reach: { total: 31132 }, engagement: { instagram: { likes: 285, comments: 12, shares: 60 } } } } };
const acct = rw.accountTotals(STATS);
check('account totals flatten the GHL statistics', acct, { impressions: 50325, reach: 31132, likes: 285, comments: 12, shares: 60, followers: 64, posts: 6 });
check('account totals tolerate a missing body', rw.accountTotals(null), null);

const rows = rw.buildRows(merged, acct, W, NOW);
check('one row per reel plus the account row', rows.map((r) => r.post_id), ['17900000000000001', '17900000000000002', 'account']);
check('account row carries impressions as views and reach', [rows[2].views, rows[2].reach, rows[2].followers], [50325, 31132, 64]);
check('rows are keyed by iso week', new Set(rows.map((r) => r.iso_week)).size, 1);

check('delta up', rw.fmtDelta(10, 7), ' (▲ +3 WoW)');
check('delta flat', rw.fmtDelta(10, 10), ' (= flat WoW)');
check('delta missing side', rw.fmtDelta(10, null), '');

const text = rw.formatPost({ window: W, posts: merged, account: acct, prevAccount: { impressions: 40000, reach: 30000, followers: 70 }, metaWired: false });
const L = text.split('\n');
check('header line', L[0], '*📊 Reels · semana 2026-W41 (5 a 11 oct)*');
check('count line', L[1], 'Reels publicados: 2');
check('account table with deltas', L.slice(2, 7), ['```', '                   Semana  vs anterior', 'Impresiones        50,325      +10,325', 'Alcance            31,132       +1,132', 'Seguidores nuevos      64           -6']);
// Two reels, one with Meta data, no history: the median floor (5) is not met, so
// there is no multiple and the ranking falls back to sends per 1,000 reached.
check('ranking header names env/1k when no median is possible', L[9], '*Ranking por env/1k (sin mediana)*');
check('ranking table, sends first, unknown = s/d, reach and views kept', L.slice(11, 14), [
  '#  Reel                            Env/1k  Alcance  Vistas  Likes  Com  Comp  Guard',
  '1  En Facebook está el que regat…     0.7   12,400  15,000     72    4     9     31',
  '2  Dos garantías por contrato, n…     s/d      s/d     s/d     40    1     2    s/d',
]);
check('links go under the table', L[15], 'Ver: <https://www.instagram.com/reel/AAA/|1> · <https://www.instagram.com/reel/AAA/|2>');
check('footnote says the median floor was not met', L[16], '_xMed: s/d, la mediana necesita al menos 5 reels con vistas (hay 1). Env/1k = compartidos por cada 1,000 alcanzados._');
check('verdict is s/d under the floor, never a diagnosis', L[17], '_Veredicto: s/d (hacen falta 5 reels con alcance; hay 1)._');
check('caveat when meta is not wired', L[18].startsWith('_Alcance y vistas por reel: pendiente'), true);
check('no caveat when meta is wired', rw.formatPost({ window: W, posts: merged, account: acct, prevAccount: null, metaWired: true }).includes('pendiente'), false);
check('no delta column without last week', rw.formatPost({ window: W, posts: merged, account: acct, prevAccount: null, metaWired: true }).includes('vs anterior'), false);
check('delta cell', [rw.deltaCell(10, 7), rw.deltaCell(5, 11), rw.deltaCell(3, 3), rw.deltaCell(3, null)], ['+3', '-6', '=', '']);
check('validate accepts the post', rw.validatePost(text, { posts: merged, account: acct }), { ok: true, problems: [] });

const quiet = rw.formatPost({ window: W, posts: [], account: acct, prevAccount: null, metaWired: true });
check('quiet week is two lines', quiet.split('\n').length, 2);
check('quiet week says zero', quiet.split('\n')[1], 'Reels publicados: 0. Sin publicaciones esta semana, no hay nada que medir.');
check('validate accepts the quiet week', rw.validatePost(quiet, { posts: [], account: acct }).ok, true);

check('validate rejects a bad header', rw.validatePost('hola\nReels publicados: 2').ok, false);
check('validate rejects a banned token', rw.validatePost(`${L[0]}\nReels publicados: undefined`).problems.includes('contains "undefined"'), true);
check('validate rejects a leftover placeholder', rw.validatePost(`${L[0]}\nReels publicados: 2 · {reach}`).ok, false);
check('validate rejects an unclosed table', rw.validatePost(`${L[0]}\nReels publicados: 2\n\`\`\``).problems, ['unclosed table']);
check('validate fails closed on zero impressions with reels published (4b)',
  rw.validatePost(text, { posts: merged, account: { ...acct, impressions: 0 } }).problems[0],
  'reels published but account impressions are 0 (stats feed suspect)');
check('validate does not treat a word containing null as banned', rw.validatePost(`${L[0]}\nReels publicados: 1 · anulado`).ok, true);


// ── outlier multiple, sends per reach, verdict (lib/reelsOutlier.js) ─────────
// Five reels with Meta data and 12 weeks of synthetic history: the median comes
// from reel_stats (views 1,000 to 2,100, median 1,550) and every reel gets xMed.
const o = require('../lib/reelsOutlier');
const FIVE = rw.mergeMetaInsights(rw.normalizePosts([
  post(),
  post({ _id: 'g2', postId: '17900000000000002', publishedAt: '2026-10-07T22:01:00Z', summary: 'Dos garantías por contrato, no por promesa.', insights: { like: 40, share: 2, comment: 1 } }),
  post({ _id: 'g3', postId: '17900000000000003', publishedAt: '2026-10-08T22:01:00Z', summary: 'Tres.', insights: { like: 10, share: 20, comment: 1 } }),
  post({ _id: 'g4', postId: '17900000000000004', publishedAt: '2026-10-09T22:01:00Z', summary: 'Cuatro.', insights: { like: 10, share: 0, comment: 1 } }),
  post({ _id: 'g5', postId: '17900000000000005', publishedAt: '2026-10-10T22:01:00Z', summary: 'Cinco.', insights: { like: 10, share: 1, comment: 1 } }),
], W), {
  '17900000000000001': { reach: 12400, views: 15000, saved: 31, shares: 9 },
  '17900000000000002': { reach: 3000, views: 3500, saved: 2, shares: 2 },
  '17900000000000003': { reach: 2000, views: 2400, saved: 1, shares: 20 },
  '17900000000000004': { reach: 1500, views: 1800, saved: 0, shares: 0 },
  '17900000000000005': { reach: 900, views: 1000, saved: 0, shares: 1 },
});
const HIST = [];
for (let i = 0; i < 12; i++) HIST.push({ iso_week: '2026-W40', post_id: `h${i}`, kind: 'reel', reach: 800 + i * 100, views: 1000 + i * 100, shares: 1 });
HIST.push({ iso_week: '2026-W40', post_id: 'account', kind: 'account', reach: 30000, views: 40000, shares: 10, followers: 50 });

check('trailing weeks are the 8 before the reported week, oldest first', o.trailingWeeks(W.start), ['2026-W33', '2026-W34', '2026-W35', '2026-W36', '2026-W37', '2026-W38', '2026-W39', '2026-W40']);
check('median: even, odd, nothing positive', [o.median([1, 2, 3, 4]), o.median([5, 1, 3]), o.median([0, null, -1])], [2.5, 3, null]);
check('sends per 1,000 reached, null without reach', [o.sendsPer1k(9, 12400), o.sendsPer1k(3, 0), o.sendsPer1k(3, null)], [0.7, null, null]);
check('weeks starting in October 2026', o.weeksStartingIn(new Date('2026-10-01T06:00:00Z'), new Date('2026-11-01T06:00:00Z')), ['2026-W41', '2026-W42', '2026-W43', '2026-W44']);
check('median base prefers views and the history (n=12)', (({ basis, median, n, source }) => ({ basis, median, n, source }))(rw.medianBase(FIVE, HIST)), { basis: 'views', median: 1550, n: 12, source: 'history' });
check('median base falls back to the batch below the floor', (({ median, n, source, historyN }) => ({ median, n, source, historyN }))(rw.medianBase(FIVE, HIST.slice(0, 2))), { median: 2400, n: 5, source: 'batch', historyN: 2 });
check('median base knows an unreadable history from a short one', rw.medianBase(FIVE, null).historyN, null);
check('median base gives no median under the floor on both sides', rw.medianBase(FIVE.slice(0, 2), []).median, null);
check('account rows never feed the median', rw.medianBase(FIVE.slice(0, 2), [HIST[12]]).median, null);
check('rankReels: multiple first', rw.rankReels(FIVE, HIST).map((p) => [p.ghlPostId, p.multiple, p.sendsPer1k]),
  [['g1', 9.7, 0.7], ['g2', 2.3, 0.7], ['g3', 1.5, 10], ['g4', 1.2, 0], ['g5', 0.6, 1.1]]);
const TIE = FIVE.map((p) => ({ ...p, views: 3100, reach: 1000 }));
check('rankReels: equal multiple, more sends per reach first', rw.rankReels(TIE, HIST).map((p) => p.ghlPostId), ['g3', 'g1', 'g2', 'g5', 'g4']);
const GAP = FIVE.map((p) => (p.ghlPostId === 'g2' ? { ...p, reach: null, views: null } : p));
check('rankReels: a reel without views or reach sits last with s/d, never 0', (({ ghlPostId, multiple, sendsPer1k }) => [ghlPostId, multiple, sendsPer1k])(rw.rankReels(GAP, HIST)[4]), ['g2', null, null]);

const PREV = { impressions: 40000, reach: 30000, followers: 70 };
const full = rw.formatPost({ window: W, posts: FIVE, account: acct, prevAccount: PREV, metaWired: true, history: HIST });
const F = full.split('\n');
check('ranking header names the multiple', F[9], '*Ranking por múltiplo, luego env/1k*');
check('table ranked by multiple then env/1k, reach and views kept as columns', F.slice(11, 17), [
  '#  Reel                            xMed  Env/1k  Alcance  Vistas  Likes  Com  Comp  Guard',
  '1  En Facebook está el que regat…  9.7x     0.7   12,400  15,000     72    4     9     31',
  '2  Dos garantías por contrato, n…  2.3x     0.7    3,000   3,500     40    1     2      2',
  '3  Tres.                           1.5x    10.0    2,000   2,400     10    1    20      1',
  '4  Cuatro.                         1.2x     0.0    1,500   1,800     10    1     0      0',
  '5  Cinco.                          0.6x     1.1      900   1,000     10    1     1      0',
]);
check('footnote names the history median and its n', F[19], '_xMed = vistas del reel / mediana de vistas de las 8 semanas anteriores (n=12). Env/1k = compartidos por cada 1,000 alcanzados._');
check('verdict: a hit with few new followers is a profile problem', F[20], '*Veredicto:* problema de perfil, no de gancho. El mejor reel hizo 9.7x la mediana y la cuenta sumó pocos seguidores (2.1 por 1,000 alcanzados).');
check('full post passes the contract', rw.validatePost(full, { posts: FIVE, account: acct }), { ok: true, problems: [] });
const short = rw.formatPost({ window: W, posts: FIVE, account: acct, prevAccount: PREV, metaWired: true, history: HIST.slice(0, 2) });
check('short history: batch median, footnote says so', short.includes('_xMed = vistas del reel / mediana de esta semana (historial corto: 2 reels en reel_stats, mínimo 5). Env/1k'), true);
check('short history: the multiple moves with the median', short.split('\n')[12].includes('6.3x'), true);
check('unreadable history is named in the footnote', rw.formatPost({ window: W, posts: FIVE, account: acct, prevAccount: PREV, metaWired: true, history: null }).includes('(reel_stats ilegible)'), true);
const gapPost = rw.formatPost({ window: W, posts: GAP, account: acct, prevAccount: PREV, metaWired: true, history: HIST });
check('missing reach on one reel prints s/d in xMed, Env/1k, Alcance and Vistas', gapPost.split('\n')[16], '5  Dos garantías por contrato, n…   s/d     s/d      s/d     s/d     40    1     2      2');
check('fewer than 5 reels with reach: no verdict, s/d with the count', gapPost.includes('_Veredicto: s/d (hacen falta 5 reels con alcance; hay 4)._'), true);
check('no meta: ranking by interactions, no multiple, no verdict', (() => {
  const t = rw.formatPost({ window: W, posts: rw.normalizePosts(POSTS, W), account: acct, prevAccount: null, metaWired: false, history: HIST });
  return [t.includes('*Ranking por interacciones (likes + comentarios + compartidos)*'), t.includes('xMed'), t.includes('Veredicto')];
})(), [true, false, false]);

// Verdict branches (ig-audit: views but no follows = profile; no views = hook).
const scored = (multiples) => multiples.map((m, i) => ({ reach: 1000, multiple: m, ghlPostId: `v${i}` }));
check('verdict floor: fewer than 5 reels with reach', o.verdict({ reels: scored([3, 3, 3, 3]), reach: 30000, prevReach: 20000, newFollowers: 30 }).kind, 'floor');
check('verdict perfil: a 2x+ reel and under 5 followers per 1,000 reached', o.verdict({ reels: scored([3, 1, 1, 1, 1]), reach: 30000, prevReach: 20000, newFollowers: 30 }), { kind: 'perfil', best: 3, followsPer1k: 1, deltaPct: 50, newFollowers: 30 });
check('verdict ok: a hit that converted', o.verdict({ reels: scored([3, 1, 1, 1, 1]), reach: 30000, prevReach: 20000, newFollowers: 300 }).kind, 'ok');
check('verdict gancho: flat reach, nothing travelled', o.verdict({ reels: scored([1.5, 1, 1, 1, 1]), reach: 30000, prevReach: 29000, newFollowers: 300 }), { kind: 'gancho', best: 1.5, followsPer1k: 10, deltaPct: 3, newFollowers: 300 });
check('verdict gancho: multiples known, none travelled, no previous week', o.verdict({ reels: scored([1.5, 1, 1, 1, 1]), reach: 30000, prevReach: null, newFollowers: null }).kind, 'gancho');
check('verdict ok: reach grew and no multiples to judge', o.verdict({ reels: scored([null, null, null, null, null]), reach: 40000, prevReach: 30000, newFollowers: null }).kind, 'ok');
check('verdict none: nothing to compare', o.verdict({ reels: scored([null, null, null, null, null]), reach: 40000, prevReach: null, newFollowers: null }).kind, 'none');
check('verdict text: gancho', o.formatVerdict({ kind: 'gancho', best: 1.5, deltaPct: 3, followsPer1k: 10 }, 'la semana anterior'), '*Veredicto:* problema de gancho. Alcance plano (+3% vs la semana anterior) y ningún reel pasó de 2x la mediana (mejor: 1.5x).');
check('verdict text: ok with a missing follower count', o.formatVerdict({ kind: 'ok', best: 2.4, deltaPct: 25, followsPer1k: null }, 'la semana anterior'), '*Veredicto:* sin alarma. Mejor reel 2.4x la mediana, alcance +25% vs la semana anterior, seguidores nuevos s/d.');
check('verdict text: none', o.formatVerdict({ kind: 'none' }, 'la semana anterior'), '_Veredicto: s/d (sin alcance de la semana anterior ni mediana para comparar)._');
check('no verdict text has an em dash', [o.formatVerdict({ kind: 'perfil', best: 3, followsPer1k: 1 }), o.formatVerdict({ kind: 'floor', n: 2 })].some((t) => t.includes('—')), false);

// Contract: the ranking header must name the key the table is sorted by.
check('validate rejects a ranking header that no longer names the sort key',
  rw.validatePost(full.replace('*Ranking por múltiplo, luego env/1k*', '*Ranking por alcance*'), { posts: FIVE, account: acct }).problems,
  ['ranking header says "alcance" but the table sorts by "xMed"']);
check('validate rejects an unknown sort key', rw.validatePost(full.replace('múltiplo, luego env/1k', 'magia'), { posts: FIVE, account: acct }).problems, ['ranking header does not name a known sort key: "magia"']);
check('validate rejects a missing ranking header', rw.validatePost(full.replace('*Ranking por múltiplo, luego env/1k*', 'Ranking'), { posts: FIVE, account: acct }).problems, ['missing ranking header']);
check('validate skips the ranking check on a quiet week', rw.validatePost(quiet, { posts: [], account: acct }).ok, true);

if (failures > 0) { console.error(`\n${failures} check(s) failed`); process.exit(1); }
console.log('\nall checks passed');
