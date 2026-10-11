// Routing test for the reels posts (weekly + daily brief).  Run:  node test/reels-channel.test.js
//
// Extracts the real REELS WEEKLY + REELS BRIEFS I/O block straight out of
// index.js (same approach as make-watchdog.test.js — index.js boots the Slack
// app on require) and drives runReelsWeekly / runReelsDailyBrief with stubbed
// GHL, Meta, Supabase, Slack and organic-views helpers. The formatting rules
// live in lib/reelsWeekly.js and lib/reelsBriefs.js and have their own tests;
// this one checks only WHERE each post goes (Ron, 2026-10-11: everything reels
// on #ng-content-machine by default, no Railway variable needed) and that the
// mode knob and kill switches still work.
const fs   = require('fs');
const path = require('path');

const ROOT = path.join(__dirname, '..');
const SRC = fs.readFileSync(process.argv[2] || path.join(ROOT, 'index.js'), 'utf8');
const block = SRC.slice(
  SRC.indexOf('// ─── REELS WEEKLY'),
  SRC.indexOf('async function runReelsMonthlyReport'),
);
if (block.length < 1000) { console.error('FAIL could not locate the reels block in index.js'); process.exit(1); }

const RON = 'U05HXGX18H3';
const CONTENT = 'C0C7HFQ8THU';

let failures = 0;
function check(label, actual, expected) {
  const ok = JSON.stringify(actual) === JSON.stringify(expected);
  if (!ok) { failures++; console.error(`FAIL ${label}\n  expected: ${JSON.stringify(expected)}\n  actual:   ${JSON.stringify(actual)}`); }
  else console.log(`ok   ${label}`);
}

// ── fixtures ─────────────────────────────────────────────────────────────────
// Daily brief runs Thu 2026-10-08 07:40 CR; weekly runs Mon 2026-10-12 08:00 CR.
const DAILY_NOW = new Date('2026-10-08T13:40:00Z');
const WEEKLY_NOW = new Date('2026-10-12T14:00:00Z');

const ghlSchedule = { results: { posts: [
  { _id: 'q1', status: 'scheduled', platform: 'google', scheduleDate: '2026-10-08T14:00:00.000Z', summary: 'Pagar publicidad depende de su presupuesto.' },
  { _id: 'p1', status: 'published', platform: 'instagram', postId: '18001', publishedAt: '2026-10-07T17:48:00.000Z',
    summary: 'La acción cura todo.', previewLink: 'https://www.instagram.com/reel/DeNmaEQAKEB/', insights: { like: 13, comment: 0, share: 0 } },
] } };
const ghlStats = { results: { totals: { impressions: 1200, likes: 21, comments: 0, followers: 1500, posts: 1 },
  breakdowns: { reach: { total: 900 }, engagement: { instagram: { shares: 2 } } } } };
const metaMedia = { data: [
  { id: '18001', timestamp: '2026-10-07T17:48:00+0000', media_product_type: 'REELS', caption: 'La acción cura todo.',
    permalink: 'https://www.instagram.com/reel/DeNmaEQAKEB/', like_count: 13, comments_count: 0,
    insights: { data: [{ name: 'reach', values: [{ value: 354 }] }, { name: 'saved', values: [{ value: 3 }] }, { name: 'shares', values: [{ value: 0 }] }] } },
] };

const okJson = (body) => ({ ok: true, status: 200, json: async () => body });

// Builds the block with stubbed I/O. `env` is the process.env the block sees.
function build({ env = {}, ghlDown = false } = {}) {
  const posted = [];
  const logged = [];
  const ghlFetch = async (url) => {
    if (ghlDown) return { ok: false, status: 503 };
    if (url.includes('/posts/list')) return okJson(ghlSchedule);
    if (url.includes('/statistics')) return okJson(ghlStats);
    throw new Error(`unexpected GHL url ${url}`);
  };
  const fetchStub = async (url) => {
    if (url.includes('graph.facebook.com') && url.includes('/media?')) return okJson(metaMedia);
    if (url.includes('graph.facebook.com') && url.includes('/insights?')) return okJson({ data: [{ name: 'reach', values: [{ value: 354 }] }] });
    throw new Error(`unexpected fetch ${url}`);
  };
  const supabaseStub = { from: () => {
    const q = { select: () => q, eq: () => q, maybeSingle: async () => ({ data: null, error: null }), upsert: async () => ({ error: null }) };
    return q;
  } };
  const slackStub = { client: { chat: { postMessage: async (payload) => { posted.push(payload); return { ok: true }; } } } };
  const requireShim = (m) => require(m.startsWith('./') ? path.join(ROOT, m) : m);
  // Organic views (#310) are rendered outside this block; stubbed to a fixed line.
  const organicViews = { weekPeriods: () => ({ cur: {}, prev: {} }) };
  const organicViewsBlock = async () => '\n👁 Vistas: stub';
  const organicViewsDailyBlock = async () => '';
  const factory = new Function(
    'require', 'process', 'fetch', 'ghlFetch', 'slack', 'supabase', 'logActivity', 'RON_SLACK_ID', 'cron', 'wrapCronJob', 'console',
    'organicViews', 'organicViewsBlock', 'organicViewsDailyBlock',
    `${block}; return { runReelsWeekly, runReelsDailyBrief, REELS_CHANNEL, reelsWeeklyLive, reelsBriefsTarget };`
  );
  const api = factory(
    requireShim, { env: { GHL_API_KEY: 'k', GHL_LOCATION_ID: 'loc', META_IG_INSIGHTS_TOKEN: 'tok', ...env } },
    fetchStub, ghlFetch, slackStub, supabaseStub, (row) => logged.push(row), RON,
    { schedule: () => {} }, (name, fn) => fn,
    { log: () => {}, warn: () => {}, error: () => {} },
    organicViews, organicViewsBlock, organicViewsDailyBlock,
  );
  return { ...api, posted, logged };
}

(async () => {
  // 1. No REELS_* variables set: the channel is the default for everything.
  {
    const t = build();
    check('1a REELS_CHANNEL defaults to #ng-content-machine', t.REELS_CHANNEL, CONTENT);
    check('1b briefs target the content channel', t.reelsBriefsTarget(), CONTENT);
    check('1c weekly is live by default', t.reelsWeeklyLive(), true);
  }

  // 2. Weekly, default env: live to the content channel, links not unfurled.
  {
    const t = build();
    await t.runReelsWeekly({ now: WEEKLY_NOW });
    check('2a weekly posts exactly once', t.posted.length, 1);
    check('2b weekly goes to the content channel', t.posted[0].channel, CONTENT);
    check('2c weekly is the real post, not a dry run', t.posted[0].text.startsWith('[DRY RUN'), false);
    check('2d weekly names the published reel', t.posted[0].text.includes('La acción cura todo'), true);
    check('2e weekly carries the views block', t.posted[0].text.includes('Vistas: stub'), true);
    check('2f weekly links do not unfurl', [t.posted[0].unfurl_links, t.posted[0].unfurl_media], [false, false]);
    const row = t.logged.find((r) => r.action === 'runReelsWeekly' && r.event_type === 'report');
    check('2g weekly activity row records live routing', row && row.status === 'ok' && row.output.live, true);
  }

  // 3. REELS_WEEKLY_MODE=live (what Railway would carry) still means live.
  {
    const t = build({ env: { REELS_WEEKLY_MODE: 'live' } });
    await t.runReelsWeekly({ now: WEEKLY_NOW });
    check('3  explicit live is live', t.posted.map((p) => p.channel), [CONTENT]);
  }

  // 4. REELS_WEEKLY_MODE=dry: Ron's DM, marked with the channel it skipped.
  {
    const t = build({ env: { REELS_WEEKLY_MODE: 'dry' } });
    await t.runReelsWeekly({ now: WEEKLY_NOW });
    check('4a dry weekly goes to Ron', t.posted.map((p) => p.channel), [RON]);
    check('4b dry weekly is marked with its target', t.posted[0].text.startsWith(`[DRY RUN → ${CONTENT}]\n`), true);
    check('4c dry weekly still carries the views block', t.posted[0].text.includes('Vistas: stub'), true);
  }

  // 5. REELS_CHANNEL override is honoured by the weekly and the briefs alike.
  {
    const t = build({ env: { REELS_CHANNEL: 'C0OVERRIDE' } });
    await t.runReelsWeekly({ now: WEEKLY_NOW });
    await t.runReelsDailyBrief({ now: DAILY_NOW });
    check('5  REELS_CHANNEL override routes both posts', t.posted.map((p) => p.channel), ['C0OVERRIDE', 'C0OVERRIDE']);
  }

  // 6. Daily brief, default env: content channel, real brief, no unfurl.
  {
    const t = build();
    await t.runReelsDailyBrief({ now: DAILY_NOW });
    check('6a daily brief goes to the content channel', t.posted.map((p) => p.channel), [CONTENT]);
    check('6b daily brief is the real brief', t.posted[0].text.startsWith('*📅 Reels · jueves 8 oct*'), true);
    check('6c daily brief lists today\'s queue', t.posted[0].text.includes('Pagar publicidad depende de su presupuesto.'), true);
    check('6d daily brief links do not unfurl', [t.posted[0].unfurl_links, t.posted[0].unfurl_media], [false, false]);
  }

  // 7. Weekly fails closed: GHL down → one DM to Ron, nothing in the channel.
  {
    const t = build({ ghlDown: true });
    await t.runReelsWeekly({ now: WEEKLY_NOW });
    check('7a weekly failure DMs Ron only', t.posted.map((p) => p.channel), [RON]);
    check('7b weekly failure names the channel it skipped', t.posted[0].text.includes(`Nothing was sent to ${CONTENT}`), true);
  }

  // 8. Kill switches post nothing anywhere.
  {
    const t = build({ env: { REELS_WEEKLY_DISABLED: 'true', REELS_BRIEFS_DISABLED: 'true' } });
    await t.runReelsWeekly({ now: WEEKLY_NOW });
    await t.runReelsDailyBrief({ now: DAILY_NOW });
    check('8  kill switches post nothing', t.posted.length, 0);
  }

  if (failures) { console.error(`\n${failures} failure(s)`); process.exit(1); }
  console.log('\nall reels-channel checks passed');
})().catch((err) => { console.error('FAIL threw', err); process.exit(1); });
