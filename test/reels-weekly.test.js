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
check('header line', L[0], '📊 Reels · semana 2026-W41 (5 a 11 oct)');
check('totals line with deltas', L[1], 'Reels publicados: 2 · Impresiones: 50,325 (▲ +10,325 WoW) · Alcance: 31,132 (▲ +1,132 WoW) · Seguidores nuevos: 64 (▼ −6 WoW)');
check('ranking header says reach once any reel has it', L[2], 'Ranking por alcance:');
check('first ranked reel line', L[3], '1. En Facebook está el que regatea. <https://www.instagram.com/reel/AAA/|ver> · 12,400 alcance · 15,000 reproducciones · 72 likes · 4 comentarios · 9 compartidos · 31 guardados');
check('winner line quotes the hook', L[5], 'Lo que más funcionó: "En Facebook está el que regatea."');
check('caveat when meta is not wired', L[6].startsWith('Alcance y reproducciones por reel: pendiente'), true);
check('no caveat when meta is wired', rw.formatPost({ window: W, posts: merged, account: acct, prevAccount: null, metaWired: true }).split('\n').length, 6);
check('validate accepts the post', rw.validatePost(text, { posts: merged, account: acct }), { ok: true, problems: [] });

const quiet = rw.formatPost({ window: W, posts: [], account: acct, prevAccount: null, metaWired: true });
check('quiet week is two lines', quiet.split('\n').length, 2);
check('quiet week says zero', quiet.split('\n')[1], 'Reels publicados: 0. Sin publicaciones esta semana, no hay nada que medir.');
check('validate accepts the quiet week', rw.validatePost(quiet, { posts: [], account: acct }).ok, true);

check('validate rejects a bad header', rw.validatePost('hola\nReels publicados: 2').ok, false);
check('validate rejects a banned token', rw.validatePost(`${L[0]}\nReels publicados: undefined`).problems.includes('contains "undefined"'), true);
check('validate rejects a leftover placeholder', rw.validatePost(`${L[0]}\nReels publicados: 2 · {reach}`).ok, false);
check('validate fails closed on zero impressions with reels published (4b)',
  rw.validatePost(text, { posts: merged, account: { ...acct, impressions: 0 } }).problems[0],
  'reels published but account impressions are 0 (stats feed suspect)');
check('validate does not treat a word containing null as banned', rw.validatePost(`${L[0]}\nReels publicados: 1 · anulado`).ok, true);

if (failures > 0) { console.error(`\n${failures} check(s) failed`); process.exit(1); }
console.log('\nall checks passed');
