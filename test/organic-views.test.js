// Rules test for organic views across platforms.  Run:  node test/organic-views.test.js
//
// lib/organicViews.js is pure, so it is required directly. All values are
// synthetic (public repo, never prod rows).
const ov = require('../lib/organicViews');

let failures = 0;
function check(label, actual, expected) {
  const ok = JSON.stringify(actual) === JSON.stringify(expected);
  if (!ok) { failures++; console.error(`FAIL ${label}\n  expected: ${JSON.stringify(expected)}\n  actual:   ${JSON.stringify(actual)}`); }
  else console.log(`ok   ${label}`);
}

// ── dates ────────────────────────────────────────────────────────────────────
check('crDate: 23:55 CR stays on the CR day', ov.crDate(new Date('2026-10-10T05:55:00Z')), '2026-10-09');
check('crDate: 00:05 CR is the next day', ov.crDate(new Date('2026-10-10T06:05:00Z')), '2026-10-10');
const wk = ov.weekPeriods(new Date('2026-10-12T14:00:00Z')); // Mon 08:00 CR
check('week = last Mon..Sun', [wk.cur.from, wk.cur.to, wk.cur.label], ['2026-10-05', '2026-10-11', '5 oct a 11 oct']);
check('previous week', [wk.prev.from, wk.prev.to], ['2026-09-28', '2026-10-04']);
check('week on a Sunday night CR is still the week before', ov.weekPeriods(new Date('2026-10-12T05:00:00Z')).cur.from, '2026-09-28');
const mo = ov.monthPeriods(new Date('2026-11-01T14:00:00Z'));
check('month = last calendar month', [mo.cur.from, mo.cur.to, mo.cur.label], ['2026-10-01', '2026-10-31', 'octubre 2026']);
check('previous month', [mo.prev.from, mo.prev.to], ['2026-09-01', '2026-09-30']);
check('month across a year', ov.monthPeriods(new Date('2027-01-01T14:00:00Z')).cur.from, '2026-12-01');
check('yesterday', ov.dayPeriod(new Date('2026-10-12T14:00:00Z')).from, '2026-10-11');

// ── normalizers ──────────────────────────────────────────────────────────────
const ig = ov.normalizeIg([
  { id: '1790001', timestamp: '2026-10-09T18:05:04+0000', caption: 'Hook uno.\n\nResto', insights: { data: [{ name: 'views', values: [{ value: 454 }] }] } },
  { id: '1790002', timestamp: '2026-10-09T18:05:04+0000', caption: 'Sin insights' },
]);
check('IG: views read, media without views skipped', ig.map((v) => [v.videoId, v.views, v.title]), [['1790001', 454, 'Hook uno.']]);
const yt = ov.normalizeYt([{ id: 'abcDEF12345', snippet: { publishedAt: '2026-10-09T06:38:14Z', title: 'Título #shorts' }, statistics: { viewCount: '14' } }]);
check('YT: string viewCount becomes a number', yt.map((v) => [v.platform, v.views]), [['youtube', 14]]);
check('TikTok: playCount of the video itself', ov.parseTikTokPlayCount('x"stats":{"diggCount":0,"shareCount":0,"commentCount":0,"playCount":164,"collectCount":"0"}y'), 164);
check('TikTok: blocked page is null, never 0', ov.parseTikTokPlayCount('<html>captcha</html>'), null);
const tt = ov.tiktokPostsFromGhl([
  { platform: 'tiktok', status: 'published', postId: '7690001', previewLink: 'https://www.tiktok.com/@x/video/7690001', summary: 'A' },
  { platform: 'tiktok', status: 'published', postId: '7690001', previewLink: 'https://www.tiktok.com/@x/video/7690001', summary: 'A dup' },
  { platform: 'tiktok', status: 'scheduled', postId: null, previewLink: null },
  { platform: 'instagram', status: 'published', postId: '1790001', previewLink: 'https://instagram.com/reel/x' },
]);
check('TikTok posts: published only, deduped', tt.map((p) => p.videoId), ['7690001']);

// ── period math ──────────────────────────────────────────────────────────────
const r = (d, platform, id, views, published_at = '2026-09-01T00:00:00Z') => ({ snapshot_date: d, platform, video_id: id, views, published_at });
const rows = [
  // old IG reel tracked since before the week: 100 → 160 = +60
  r('2026-10-04', 'instagram', 'a', 100), r('2026-10-08', 'instagram', 'a', 140), r('2026-10-11', 'instagram', 'a', 160),
  // IG reel published inside the week (Wed 10-07 noon CR): counts from 0 = +300
  r('2026-10-07', 'instagram', 'b', 120, '2026-10-07T18:00:00Z'), r('2026-10-11', 'instagram', 'b', 300, '2026-10-07T18:00:00Z'),
  // YouTube: count revised down by the platform, never negative
  r('2026-10-04', 'youtube', 'y', 50), r('2026-10-11', 'youtube', 'y', 45),
  // snapshot after the period is ignored
  r('2026-10-12', 'instagram', 'a', 999),
];
const p = ov.periodViews(rows, wk.cur);
check('IG gained = growth of old + new from 0', p.byPlatform.instagram, 360);
check('YT revised down = 0, not negative', p.byPlatform.youtube, 0);
check('TikTok with no snapshot = null (sin datos), not 0', p.byPlatform.tiktok, null);
check('total sums known platforms', p.total, 360);
check('lifetime = latest value at period end', p.lifetime, { instagram: 460, youtube: 45, tiktok: null });
check('baselines known = not partial', p.partialSince, null);

// tracking starts mid-week on a video published before the week: partial, counts from first snapshot
const partial = ov.periodViews([r('2026-10-09', 'tiktok', 't', 150), r('2026-10-11', 'tiktok', 't', 200)], wk.cur);
check('first-seen old video counts from its first snapshot', partial.byPlatform.tiktok, 50);
check('and marks the period partial', partial.partialSince, '2026-10-09');
check('no rows at all = total null', ov.periodViews([], wk.cur).total, null);
// a video deleted before the period adds nothing and does not make its platform "seen"
check('video gone before the period is ignored', ov.periodViews([r('2026-10-01', 'youtube', 'gone', 10)], wk.cur).byPlatform.youtube, null);

// ── formatting ───────────────────────────────────────────────────────────────
const prev = { total: 300, byPlatform: {}, lifetime: {}, partialSince: null };
const block = ov.formatViewsBlock({ title: 'semana 5 oct a 11 oct', cur: p, prev, funnel: { comments: 4, conversations: 1, booked: null } });
check('block: header', block.split('\n')[0], '*👁️ Vistas orgánicas, semana 5 oct a 11 oct*');
check('block: total with % vs previous', block.split('\n')[1], 'Total: 360 (▲ 20% vs anterior)');
check('block: per platform, missing = sin datos', block.split('\n')[2], 'Instagram 360 · YouTube 0 · TikTok sin datos');
check('block: funnel names the missing step', block.split('\n')[3], 'Embudo: 360 vistas → 4 comentarios "LinkedIn" → 1 conversación → sin datos de llamadas agendadas');
check('block passes its validator', ov.validateBlock(block).ok, true);
check('no lifetime line anymore', /por vida/.test(block), false);
const mtdP = ov.monthToDatePeriod(wk.cur.to);
check('month to date period', [mtdP.from, mtdP.to, mtdP.label], ['2026-10-01', '2026-10-11', 'octubre']);
const mtd = { ...ov.periodViews(rows, mtdP), label: mtdP.label };
const mBlock = ov.formatViewsBlock({ title: 's', cur: p, prev, funnel: null, monthToDate: mtd });
check('block: acumulado mensual line', mBlock.split('\n')[3], 'Acumulado mensual (octubre): 360 (Instagram 360 · YouTube 0)');
check('mtd block passes its validator', ov.validateBlock(mBlock).ok, true);
const pBlock = ov.formatViewsBlock({ title: 'semana', cur: partial, prev: null, funnel: null });
check('partial block says so', /Parcial: la medición empezó el 9 oct/.test(pBlock), true);
check('partial previous period gives no % delta',
  ov.formatViewsBlock({ title: 's', cur: p, prev: { ...prev, partialSince: '2026-09-30' }, funnel: null }).split('\n')[1], 'Total: 360');
check('no data block is explicit', /Sin datos de vistas/.test(ov.formatViewsBlock({ title: 's', cur: ov.periodViews([], wk.cur), prev: null })), true);
check('validator catches undefined', ov.validateBlock('*👁️ Vistas orgánicas, x*\nTotal: undefined').ok, false);
check('daily line', ov.formatDailyLine(p), '👁️ Vistas de ayer: 360 (Instagram 360 · YouTube 0)');
check('daily line absent without data', ov.formatDailyLine(ov.periodViews([], wk.cur)), null);

// ── funnel helpers ───────────────────────────────────────────────────────────
const s = Date.parse('2026-10-05T06:00:00Z'); const e = Date.parse('2026-10-12T06:00:00Z');
check('LinkedIn comments in window, any casing', ov.countLinkedInComments([
  { text: 'LinkedIn', timestamp: '2026-10-06T10:00:00+0000' },
  { text: 'linked in porfa', timestamp: '2026-10-07T10:00:00+0000' },
  { text: '🔥', timestamp: '2026-10-07T10:00:00+0000' },
  { text: 'linkedin', timestamp: '2026-10-12T07:00:00+0000' },
], s, e), 2);

if (failures) { console.error(`\n${failures} failure(s)`); process.exit(1); }
console.log('\nall organic-views checks passed');
