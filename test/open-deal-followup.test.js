// Rules test for the open-deal follow-up sweep (runOpenDealFollowupSweep +
// runOpenDealZombieDigest).
//   Run:  node test/open-deal-followup.test.js
//
// Extracts the real pure block straight out of index.js so the test cannot
// drift from shipped behaviour — same approach as provisioning-lag.test.js
// (index.js boots Slack on require).
const fs   = require('fs');
const path = require('path');

const SRC = fs.readFileSync(process.argv[2] || path.join(__dirname, '..', 'index.js'), 'utf8');
const block = SRC.slice(
  SRC.indexOf('const OPEN_DEAL_FIRST_NUDGE_DAYS'),
  SRC.indexOf('// ─── end open-deal followup pure block'),
);
if (!block || block.length < 100) { console.error('FAIL: could not extract the open-deal pure block'); process.exit(1); }

const {
  encodeOpenDealNudgeState, parseOpenDealNudgeState, evaluateOpenDealNudge,
  evaluateOutcomePromotion, parseOpenDealReply, classifyOpenDeals,
  buildOpenDealCardText, formatOpenDealZombieDigest, sortOpenDealsOldestFirst,
  OPEN_DEAL_FIRST_NUDGE_DAYS, OPEN_DEAL_RENUDGE_DAYS, OPEN_DEAL_SNOOZE_DAYS,
  OPEN_DEAL_ZOMBIE_AGE_DAYS, OPEN_DEAL_ZOMBIE_NUDGES, OPEN_DEAL_SANITY_MAX, OPEN_DEAL_LIST_CAP,
  OPEN_DEAL_DIGEST_ONLY_DAYS,
} = new Function(`${block}; return {
  encodeOpenDealNudgeState, parseOpenDealNudgeState, evaluateOpenDealNudge,
  evaluateOutcomePromotion, parseOpenDealReply, classifyOpenDeals,
  buildOpenDealCardText, formatOpenDealZombieDigest, sortOpenDealsOldestFirst,
  OPEN_DEAL_FIRST_NUDGE_DAYS, OPEN_DEAL_RENUDGE_DAYS, OPEN_DEAL_SNOOZE_DAYS,
  OPEN_DEAL_ZOMBIE_AGE_DAYS, OPEN_DEAL_ZOMBIE_NUDGES, OPEN_DEAL_SANITY_MAX, OPEN_DEAL_LIST_CAP,
  OPEN_DEAL_DIGEST_ONLY_DAYS };`)();

let failures = 0;
function check(label, actual, expected) {
  const ok = JSON.stringify(actual) === JSON.stringify(expected);
  if (!ok) { failures++; console.error(`FAIL ${label}\n  expected: ${JSON.stringify(expected)}\n  actual:   ${JSON.stringify(actual)}`); }
  else console.log(`ok   ${label}`);
}

const NOW = Date.parse('2026-08-24T16:00:00Z'); // a Monday, 10:00 CR
const daysAgoISO = (d) => new Date(NOW - d * 86400000).toISOString();

// ── 1. Nudge cadence (evaluateOpenDealNudge) ─────────────────────────────────
check('1a constants: 3d / 4d / 7d / 21d / 3 nudges',
  [OPEN_DEAL_FIRST_NUDGE_DAYS, OPEN_DEAL_RENUDGE_DAYS, OPEN_DEAL_SNOOZE_DAYS, OPEN_DEAL_ZOMBIE_AGE_DAYS, OPEN_DEAL_ZOMBIE_NUDGES],
  [3, 4, 7, 21, 3]);
check('1b 2d old, no card yet → too_fresh',
  evaluateOpenDealNudge({ ageDays: 2, state: null, ghlStageTerminal: false }, NOW), { skip: 'too_fresh' });
check('1c 3d old, no card yet → first_nudge',
  evaluateOpenDealNudge({ ageDays: 3, state: null, ghlStageTerminal: false }, NOW), { act: 'first_nudge' });
check('1d 61d old, never carded → digest_only (cold-start guard)',
  evaluateOpenDealNudge({ ageDays: 61, state: null, ghlStageTerminal: false }, NOW), { skip: 'digest_only' });
check('1e GHL card already terminal → ghl_drift, beats everything',
  evaluateOpenDealNudge({ ageDays: 10, state: null, ghlStageTerminal: true }, NOW), { skip: 'ghl_drift' });
const carded = (over = {}) => ({ firstNudgedISO: daysAgoISO(5), nudgeCount: 1, lastNudgedISO: daysAgoISO(5), snoozeUntilISO: null, snoozeCount: 0, cardChannel: 'D1', cardTs: '1.1', driftISO: null, ...over });
check('1f nudged 2d ago → renudge_gap (4d cadence)',
  evaluateOpenDealNudge({ ageDays: 6, state: carded({ lastNudgedISO: daysAgoISO(2) }), ghlStageTerminal: false }, NOW), { skip: 'renudge_gap' });
check('1g nudged 4d ago → renudge',
  evaluateOpenDealNudge({ ageDays: 8, state: carded({ lastNudgedISO: daysAgoISO(4) }), ghlStageTerminal: false }, NOW), { act: 'renudge' });
check('1h snoozed until tomorrow → snoozed, even though bump is due',
  evaluateOpenDealNudge({ ageDays: 8, state: carded({ snoozeUntilISO: daysAgoISO(-1) }), ghlStageTerminal: false }, NOW), { skip: 'snoozed' });
check('1i snooze expired yesterday → renudge resumes',
  evaluateOpenDealNudge({ ageDays: 12, state: carded({ snoozeUntilISO: daysAgoISO(1) }), ghlStageTerminal: false }, NOW), { act: 'renudge' });
check('1j carded deal older than 60d keeps its bumps (digest_only is first-card only)',
  evaluateOpenDealNudge({ ageDays: 65, state: carded(), ghlStageTerminal: false }, NOW), { act: 'renudge' });

// ── 2. State round-trip ──────────────────────────────────────────────────────
const st = { firstNudgedISO: '2026-08-20T16:00:00.000Z', nudgeCount: 2, lastNudgedISO: '2026-08-24T16:00:00.000Z', snoozeUntilISO: '', snoozeCount: 1, cardChannel: 'D0AAA', cardTs: '1724.5', driftISO: '' };
const encoded = encodeOpenDealNudgeState(st);
const round = parseOpenDealNudgeState(encoded);
check('2a round-trip keeps counts and pointers',
  [round.nudgeCount, round.snoozeCount, round.cardChannel, round.cardTs], [2, 1, 'D0AAA', '1724.5']);
check('2b empty ISO fields survive as null', [round.snoozeUntilISO, round.driftISO], [null, null]);
check('2c re-encode is stable', encodeOpenDealNudgeState(round), encoded);
check('2d malformed value → null, not a crash', parseOpenDealNudgeState('resolved|2026-08-24|won'), null);
check('2e legacy/garbage → null', [parseOpenDealNudgeState(''), parseOpenDealNudgeState(null), parseOpenDealNudgeState('v1|only|three')], [null, null, null]);

// ── 3. Promotion guard ───────────────────────────────────────────────────────
check('3a follow_up → won without revenue → refused',
  evaluateOutcomePromotion('follow_up', 'won', null), { ok: false, reason: 'won_needs_revenue' });
check('3b follow_up → won with revenue → ok',
  evaluateOutcomePromotion('follow_up', 'won', 3500), { ok: true });
check('3c follow_up → lost → ok', evaluateOutcomePromotion('follow_up', 'lost', null), { ok: true });
check('3d follow_up → no_show → refused (not terminal)',
  evaluateOutcomePromotion('follow_up', 'no_show', null), { ok: false, reason: 'not_terminal' });
check('3e won → lost → refused (terminal never regresses)',
  evaluateOutcomePromotion('won', 'lost', null), { ok: false, reason: 'not_follow_up' });
check('3f follow_up → follow_up → refused',
  evaluateOutcomePromotion('follow_up', 'follow_up', null), { ok: false, reason: 'not_terminal' });

// ── 4. Reply parsing ─────────────────────────────────────────────────────────
check('4a won 3.5k → 3500', parseOpenDealReply('won 3.5k'), { outcome: 'won', revenue: 3500 });
check('4b won without amount → won, null revenue (caller prompts)', parseOpenDealReply('won'), { outcome: 'won', revenue: null });
check('4c lost', parseOpenDealReply('lost'), { outcome: 'lost', revenue: null });
check('4d no fit → disqualified', parseOpenDealReply('no fit'), { outcome: 'disqualified', revenue: null });
check('4e snooze tokens', [parseOpenDealReply('snooze'), parseOpenDealReply('still working it'), parseOpenDealReply('sigo en ello')],
  [{ snooze: true }, { snooze: true }, { snooze: true }]);
check('4f no show → null (nonsense on a held deal, falls to LLM)', parseOpenDealReply('no show'), null);
check('4g open deal → null (the row is already there)', parseOpenDealReply('open deal'), null);
check('4h long conversational text → null', parseOpenDealReply('he said he wants to loop in his partner and circle back after their board meeting next month'), null);

// ── 5. Zombie classification ─────────────────────────────────────────────────
const deals = [
  { display: 'Old Silent Deal', closer: 'Jose Carranza', openedAt: daysAgoISO(22), nudgeCount: 0, snoozeCount: 0 },
  { display: 'Ignored Nudges', closer: 'Jonathan Madriz', openedAt: daysAgoISO(10), nudgeCount: 3, snoozeCount: 0 },
  { display: 'Working Fine', closer: 'Jose Carranza', openedAt: daysAgoISO(10), nudgeCount: 2, snoozeCount: 0 },
  { display: 'Serial Snoozer', closer: 'Jonathan Madriz', openedAt: daysAgoISO(30), nudgeCount: 1, snoozeCount: 4 },
  { display: 'Drifted Deal', closer: 'Jose Carranza', openedAt: daysAgoISO(15), nudgeCount: 1, snoozeCount: 0, driftISO: '2026-08-20T00:00:00Z' },
  { display: 'Fresh Deal', closer: 'Jose Carranza', openedAt: daysAgoISO(1), nudgeCount: 0, snoozeCount: 0 },
  { display: 'Bad Date', closer: 'Jose Carranza', openedAt: 'not-a-date', nudgeCount: 0, snoozeCount: 0 },
];
const b = classifyOpenDeals(deals, NOW);
check('5a zombies: age OR nudges qualify, snoozed still zombie, oldest first',
  b.zombies.map(z => z.display), ['Serial Snoozer', 'Old Silent Deal', 'Ignored Nudges']);
check('5b snooze count carried to Ron', b.zombies[0].snoozeCount, 4);
check('5c drift bucketed separately with its date', [b.drift.length, b.drift[0].driftDate], [1, '2026-08-20']);
check('5d active/fresh counts; bad date dropped', [b.active, b.fresh], [1, 1]);

// ── 6. Card structural contract (criteria 4a of the recipe) ──────────────────
const acts = [{ outcome: 'lost', label: 'Lost', emoji: '👎', emojiName: '-1' }, { outcome: 'disqualified', label: 'No Fit', emoji: '🙅', emojiName: 'no_good' }];
const card = buildOpenDealCardText({ prospectName: 'Ana Lopez', ageDays: 5, nudgeCount: 2, snoozeCount: 1, callDateStr: 'Aug 12', acts, wonLabel: 'Closed' });
check('6a card names the prospect and the day count', card.includes('*Ana Lopez*') && card.includes('5 days'), true);
check('6b card surfaces nudge and snooze counts', card.includes('nudged 2×') && card.includes('snoozed 1×'), true);
check('6c card offers snooze + close verbs, won is typed-only',
  card.includes('💤 *Still working it*') && card.includes('👎 *Lost*') && card.includes('🙅 *No Fit*') && card.includes('`won <amount>`') && !card.includes('tap won'), true);
check('6d card never renders undefined/null/NaN', /undefined|NaN|\bnull\b/.test(card), false);

// ── 7. Digest structural contract ────────────────────────────────────────────
const digest = formatOpenDealZombieDigest({ zombies: b.zombies, drift: b.drift, unmatchedGhl: 2, ghlCheckFailed: false });
check('7a header literal', digest.includes('*OPEN DEAL ZOMBIES — deals nobody is closing*'), true);
check('7b every zombie bullet matches the contract regex',
  digest.split('\n').filter(l => l.startsWith('• ') && l.includes('nudged')).every(l => /^• .+ — open \d+d — closer .+ — nudged \d+× · snoozed \d+×$/.test(l)), true);
check('7c drift section present with its own header', digest.includes('*PORTAL/GHL DRIFT — portal says open deal, GHL card already closed*'), true);
check('7d unmatched GHL count surfaced', digest.includes('📎 2 GHL Open-Deal card(s)'), true);
check('7e no undefined/null/NaN in digest', /undefined|NaN|\bnull\b/.test(digest), false);
check('7f healthy book → null (silent-when-healthy)',
  formatOpenDealZombieDigest({ zombies: [], drift: [], unmatchedGhl: 0, ghlCheckFailed: false }), null);
check('7g healthy book + failed GHL check → still null (log line carries it)',
  formatOpenDealZombieDigest({ zombies: [], drift: [], unmatchedGhl: 0, ghlCheckFailed: true }), null);

// ── 8. Sanity bound (fails closed) ───────────────────────────────────────────
const mkZombie = (i) => ({ display: `Zombie ${i}`, closer: 'Jose Carranza', ageDays: 30 + i, nudgeCount: 5, snoozeCount: 0 });
const atBound = formatOpenDealZombieDigest({ zombies: Array.from({ length: OPEN_DEAL_SANITY_MAX }, (_, i) => mkZombie(i)), drift: [], unmatchedGhl: 0, ghlCheckFailed: false });
check('8a exactly at the bound → still lists, capped with "and N more"',
  atBound.includes(`…and ${OPEN_DEAL_SANITY_MAX - OPEN_DEAL_LIST_CAP} more.`)
  && atBound.split('\n').filter(l => l.startsWith('• ')).length === OPEN_DEAL_LIST_CAP, true);
const overBound = formatOpenDealZombieDigest({ zombies: Array.from({ length: OPEN_DEAL_SANITY_MAX + 1 }, (_, i) => mkZombie(i)), drift: [], unmatchedGhl: 0, ghlCheckFailed: false });
check('8b over the bound → SANITY BOUND EXCEEDED, names withheld',
  overBound.includes('SANITY BOUND EXCEEDED') && !overBound.includes('Zombie 0'), true);

// ── 9. Historical backlog is its own bucket, not 200 urgent zombies ──────────
// Calibrated on the book measured in production on 2026-08-24: 299 follow_up
// rows, 2 fresh (<3d), 31 in 3-21d, 73 actionable (21-60d), 193 older than 60d,
// oldest 2026-05-19. The first draft called all 266 of the 21d+ rows zombies,
// which tripped the fail-closed bound and would have told Ron his query was
// broken when the only thing wrong was three months of unworked deals.
const realBook = [
  ...Array.from({ length: 2 },   (_, i) => ({ display: `Fresh ${i}`,      closer: 'Jose Carranza',   openedAt: daysAgoISO(1) })),
  ...Array.from({ length: 31 },  (_, i) => ({ display: `Working ${i}`,    closer: 'Jonathan Madriz', openedAt: daysAgoISO(10) })),
  ...Array.from({ length: 73 },  (_, i) => ({ display: `Actionable ${i}`, closer: 'Jose Carranza',   openedAt: daysAgoISO(40) })),
  ...Array.from({ length: 193 }, (_, i) => ({ display: `Ancient ${i}`,    closer: 'Ron Duarte',      openedAt: daysAgoISO(90) })),
];
const rb = classifyOpenDeals(realBook, NOW);
check('9a the 21-60d band is what counts as a zombie', rb.zombies.length, 73);
check('9b everything past the card floor is historical, not a zombie', rb.historical.length, 193);
check('9c fresh and working deals stay out of both', [rb.fresh, rb.active], [2, 31]);
check('9d the real book does NOT trip the fail-closed bound', rb.zombies.length <= OPEN_DEAL_SANITY_MAX, true);
const rbMsg = formatOpenDealZombieDigest({ zombies: rb.zombies, drift: [], historical: rb.historical, unmatchedGhl: 0, ghlCheckFailed: false });
check('9e it reports real names, not "SANITY BOUND EXCEEDED"', rbMsg.includes('SANITY BOUND EXCEEDED'), false);
check('9f the backlog is one honest count line, never 193 bullets',
  /📦 \*193 historical\* — open more than 60 days \(oldest 90d\)/.test(rbMsg), true);
check('9g still only LIST_CAP names in the zombie section',
  rbMsg.split('\n').filter(l => l.startsWith('• ')).length, OPEN_DEAL_LIST_CAP);
check('9h a purely historical book still speaks up (it is not "healthy")',
  formatOpenDealZombieDigest({ zombies: [], drift: [], historical: rb.historical, unmatchedGhl: 0, ghlCheckFailed: false }) !== null, true);
check('9i a genuinely empty book stays silent',
  formatOpenDealZombieDigest({ zombies: [], drift: [], historical: [], unmatchedGhl: 0, ghlCheckFailed: false }), null);
check('9j a capped GHL scan says the count is incomplete',
  formatOpenDealZombieDigest({ zombies: [], drift: [], historical: [], unmatchedGhl: 4, ghlCheckFailed: false, scanCapped: true })
    .includes('stage scan hit its page cap'), true);
check('9k no undefined/NaN anywhere in the real-book digest', /undefined|NaN/.test(rbMsg), false);

// ── 10. Card ordering — which three deals a closer is asked about ───────────
// Ron's real book from the first dry run (2026-08-24), with opened_at as the
// Date OBJECTS node-postgres actually returns. The original comparator sorted
// String(Date) alphabetically — weekday name, then month NAME, so "Jul" < "Jun"
// — and produced 49d, 55d, 36d, silently pushing a 53-day-old deal past the
// 3-per-closer cap. ISO strings would have sorted fine; only Date objects
// expose it, which is why this fixture uses them.
const ronBook = [
  { name: 'Flor Lizano Bolanos (49d)', opened_at: new Date('2026-07-06T12:49:39Z') },
  { name: 'Mario Cardona (55d)',       opened_at: new Date('2026-06-29T22:18:49Z') },
  { name: 'Liz Zamora (36d)',          opened_at: new Date('2026-07-19T17:42:10Z') },
  { name: 'Flor Lizano Bolanos (53d)', opened_at: new Date('2026-07-02T18:03:21Z') },
];
const ordered = sortOpenDealsOldestFirst(ronBook).map(d => d.name);
check('10a truly oldest first, with pg Date objects',
  ordered, ['Mario Cardona (55d)', 'Flor Lizano Bolanos (53d)', 'Flor Lizano Bolanos (49d)', 'Liz Zamora (36d)']);
check('10b the 3-card cap now takes the three STALEST, not an alphabetical accident',
  ordered.slice(0, 3).includes('Liz Zamora (36d)'), false);
check('10c ISO strings sort identically — the fix is shape-agnostic',
  sortOpenDealsOldestFirst(ronBook.map(d => ({ ...d, opened_at: d.opened_at.toISOString() }))).map(d => d.name),
  ordered);
check('10d unparseable dates sort last instead of poisoning the order',
  sortOpenDealsOldestFirst([{ name: 'bad', opened_at: 'not-a-date' }, ...ronBook])[0].name,
  'Mario Cardona (55d)');
check('10e the input array is not mutated', ronBook[0].name, 'Flor Lizano Bolanos (49d)');

// ── 11. Inherited deals say so on the card ──────────────────────────────────
// A deal that was Jonathan's arrives in Jose's DM after 2026-08-24. Without a
// line explaining why, it reads as Max nagging him about someone else's call.
const inherited = buildOpenDealCardText({
  prospectName: 'Kendall Rodríguez', ageDays: 52, nudgeCount: 0, snoozeCount: 0,
  callDateStr: 'Jul 3', acts, wonLabel: 'Closed', inheritedFrom: 'Jonathan Madriz',
});
check('11a the card explains the handover', inherited.includes("Originally Jonathan Madriz's call — you own the follow-up now."), true);
check('11b it still carries the normal verbs', inherited.includes('💤 *Still working it*') && inherited.includes('`won <amount>`'), true);
check('11c a deal that was never inherited says nothing about it',
  /Originally|own the follow-up now/.test(card), false);
check('11d no undefined leaks when inheritedFrom is absent', /undefined/.test(card), false);

// ── 12. The query only returns deals a closer can actually act on ───────────
// Source-level, because the gates live in SQL. Measured 2026-08-24: 299
// `follow_up` rows, of which 263 were pre-cutover iClosed history with exactly
// ONE GHL link between them — no opportunity card to move, so a 👎 tap would
// write against a card that does not exist. Six of the nine cards the first
// live run would have sent were such ghosts, one of them a test record named
// "Prueba". These assertions are what stop that floor being dropped again.
const fetchSrc = SRC.slice(SRC.indexOf('async function fetchOpenDeals'), SRC.indexOf('async function ghlGetOpportunityStage'));
check('12a the query floors on the GHL cutover', /a\.scheduled_start >= \$1/.test(fetchSrc), true);
check('12b bound to the shared constant, not a copied literal',
  [/\[GHL_CUTOVER_ISO\]/.test(fetchSrc), /'2026-07-23/.test(fetchSrc)], [true, false]);
check('12c past calls only — a booked prospect is not a stalled deal',
  /a\.scheduled_start <= now\(\)/.test(fetchSrc), true);
check('12d still scoped to open deals', /o\.outcome = 'follow_up'/.test(fetchSrc), true);
const cutoverDecl = SRC.indexOf('const GHL_CUTOVER_ISO');
check('12e the constant is declared before every use (no temporal dead zone)',
  [...SRC.matchAll(/GHL_CUTOVER_ISO/g)].map(m => m.index).filter(i => i !== cutoverDecl + 6).every(i => i > cutoverDecl), true);

// ─────────────────────────────────────────────────────────────────────────────
if (failures) { console.error(`\n${failures} failure(s).`); process.exit(1); }
console.log('\nAll open-deal follow-up rules tests passed.');
