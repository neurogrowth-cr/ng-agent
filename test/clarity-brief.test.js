// Rules test for the Clarity Brief.  Run:  node test/clarity-brief.test.js
//
// lib/clarityBrief.js is pure (no Slack, no network), so it is required directly.
// Texts below are synthetic (public repo): no real lead names, emails or phones.
const cb = require('../lib/clarityBrief');
const fs = require('fs');
const path = require('path');

let failures = 0;
function check(label, actual, expected) {
  const ok = JSON.stringify(actual) === JSON.stringify(expected);
  if (!ok) { failures++; console.error(`FAIL ${label}\n  expected: ${JSON.stringify(expected)}\n  actual:   ${JSON.stringify(actual)}`); }
  else console.log(`ok   ${label}`);
}

// ── 1. Window ───────────────────────────────────────────────────────────────
const mon = new Date('2026-11-09T12:50:00Z'); // Monday 06:50 CR
const fri = new Date('2026-11-06T12:52:00Z'); // Friday's brief posted
const w1 = cb.briefWindow({ lastPostedAt: fri, now: mon });
check('1a Monday covers the weekend (hours since Friday)', w1.hours, 72);
check('1b not clamped at 3 days', w1.clamped, false);
check('1c no last brief = 24h', cb.briefWindow({ now: mon }).hours, 24);
const w2 = cb.briefWindow({ lastPostedAt: new Date('2026-10-01T12:50:00Z'), now: mon });
check('1d a month-long gap is clamped to 7 days and says so', [w2.hours, w2.clamped], [168, true]);
check('1e a future lastPostedAt falls back to 24h', cb.briefWindow({ lastPostedAt: new Date('2026-12-01T00:00:00Z'), now: mon }).hours, 24);

// ── 2. Keys ─────────────────────────────────────────────────────────────────
check('2a topic key drops stop words, digits and accents', cb.topicKey('Re: Factura 2026-10 vencida para Acción Legal'), 'factura-vencida-accion');
check('2b empty topic', cb.topicKey('123 456'), 'general');
check('2c signature', cb.signatureFor({ source: 'gmail_ron', actor: 'Billing@Example.com', title: 'Payment failed for invoice 44' }), 'gmail_ron|billing@example.com|payment-failed-invoice');
check('2d actor signature', cb.actorSignatureFor({ source: 'slack', actor: 'C0AJANQBYUE' }), 'slack|c0ajanqbyue|*');
check('2e tier by kind', [cb.tierFor({ kind: 'payment' }), cb.tierFor({ kind: 'check_broken' }), cb.tierFor({ kind: 'pr_review' }), cb.tierFor({})], [1, 2, 3, 3]);
check('2f explicit tier wins', cb.tierFor({ kind: 'payment', tier: 2 }), 2);

// ── 3. Suppression ──────────────────────────────────────────────────────────
const now = new Date('2026-11-10T12:50:00Z');
const cands = [
  { source: 'gmail_ron', actor: 'billing@example.com', title: 'Payment failed for invoice 44', url: 'https://mail.example/1', dedupeKey: 'gmail_ron:t1', createdAt: '2026-11-10T03:00:00Z', kind: 'payment' },
  { source: 'gmail_ron', actor: 'news@example.com', title: 'Weekly newsletter', url: 'https://mail.example/2', dedupeKey: 'gmail_ron:t2', createdAt: '2026-11-10T03:00:00Z' },
  { source: 'slack', actor: 'C0AJANQBYUE', title: 'Ron can you approve the proposal', url: 'https://slack.example/3', dedupeKey: 'slack:1.1', createdAt: '2026-11-09T20:00:00Z', kind: 'approval' },
  { source: 'github', actor: 'neurogrowth-cr/ng-agent', title: 'PR 400 awaiting merge', url: 'https://github.example/4', dedupeKey: 'github:pr400', createdAt: '2026-11-08T20:00:00Z', kind: 'pr_review' },
  { source: 'slack', actor: 'C0B48H7NT41', title: 'Ron the client asked again', url: 'https://slack.example/5', dedupeKey: 'slack:2.2', createdAt: '2026-11-08T20:00:00Z', updatedAt: '2026-11-10T01:00:00Z', kind: 'mention' },
  { source: 'wiki', actor: 'wiki', title: 'Old follow-up', url: 'https://wiki.example/6', dedupeKey: 'wiki:abc', createdAt: '2026-10-01', kind: 'followup' },
];
const dismissals = [
  { signature: 'gmail_ron|news@example.com|weekly-newsletter', muted: false },
  { signature: 'wiki|wiki|*', muted: true },
];
const shown = [
  { dedupe_key: 'github:pr400', status: 'shown', shown_at: '2026-11-09T12:50:00Z' },
  { dedupe_key: 'slack:2.2', status: 'shown', shown_at: '2026-11-09T12:50:00Z' },
];
const sup = cb.applySuppression({ candidates: cands, dismissals, shownItems: shown, now });
check('3a kept the right ones (payment, approval, escalated mention)', sup.kept.map(k => k.dedupeKey), ['gmail_ron:t1', 'slack:1.1', 'slack:2.2']);
check('3b drop reasons', sup.dropped.map(d => [d.candidate.dedupeKey, d.reason]), [['gmail_ron:t2', 'dismissed'], ['github:pr400', 'repeat'], ['wiki:abc', 'muted']]);
check('3c kept items carry signature and tier', [sup.kept[0].signature, sup.kept[0].tier], ['gmail_ron|billing@example.com|payment-failed-invoice', 1]);
const sup2 = cb.applySuppression({ candidates: [cands[3]], shownItems: [{ dedupe_key: 'github:pr400', status: 'snoozed', shown_at: '2026-11-09T12:50:00Z', snoozed_until: '2026-11-10T08:00:00Z' }], now });
check('3d a snoozed item whose time passed re-enters', sup2.kept.length, 1);
const sup3 = cb.applySuppression({ candidates: [cands[3]], shownItems: [{ dedupe_key: 'github:pr400', status: 'accepted', shown_at: '2026-11-09T12:50:00Z' }], now });
check('3e an accepted item stays out of block 1', sup3.dropped[0].reason, 'accepted');

// ── 4. Rank and cap ─────────────────────────────────────────────────────────
const many = Array.from({ length: 10 }, (_, i) => ({ source: 'slack', actor: 'c', title: `item ${i}`, url: `https://x.example/${i}`, dedupeKey: `k${i}`, tier: (i % 3) + 1, createdAt: new Date(2026, 10, 1 + i).toISOString() }));
const r = cb.rankAndCap(many, { now });
check('4a never more than 7', r.shown.length, 7);
check('4b tier 1 first, older first within a tier', r.shown.map(s => s.title).slice(0, 4), ['item 0', 'item 3', 'item 6', 'item 9']);
check('4c overflow counted', r.overflow.length, 3);
const r2 = cb.rankAndCap([{ source: 'max', actor: 'max', title: 'no link', dedupeKey: 'nl', tier: 1 }], { now });
check('4d an item without a URL goes to overflow with a reason', [r2.shown.length, r2.overflow[0].reason], [0, 'no_url']);

// ── 5. Wiki follow-ups ──────────────────────────────────────────────────────
const md = fs.readFileSync(path.join(__dirname, 'fixtures', 'clarity-followups.md'), 'utf8');
const rows = cb.parseFollowupTable(md);
check('5a parses only the Ron section rows', rows.length, 3);
check('5b first row fields', rows[0], { item: 'Rotate the test key printed to logs', noted: '2026-10-05', source: 'dash test-key' });
const elig = cb.eligibleFollowups({ rows, now: new Date('2026-11-10T12:50:00Z'), pageUrl: 'https://wiki.example/followups' });
check('5c two per brief, oldest first, under 14 days excluded', elig.map(e => e.title), ['Decide on the former closer open deals', 'Rotate the test key printed to logs']);
check('5d dedupe key is a hash of the item text', elig[0].dedupeKey.startsWith('wiki:'), true);
const elig2 = cb.eligibleFollowups({ rows, now: new Date('2026-11-10T12:50:00Z'), shownItems: [{ source: 'wiki', dedupe_key: elig[0].dedupeKey, status: 'shown', shown_at: '2026-11-08T12:50:00Z' }] });
check('5e one shown two days ago rotates out', elig2.map(e => e.title), ['Rotate the test key printed to logs']);

// ── 6. Reactions ────────────────────────────────────────────────────────────
const RON = 'U05HXGX18H3';
const item = { status: 'shown', signature: 'gmail_ron|billing@example.com|payment-failed-invoice', source: 'gmail_ron', title: 'Payment failed' };
check('6a ✅ on shown = accepted', cb.applyReaction({ item, reaction: 'white_check_mark', userId: RON, ronId: RON, now }).status, 'accepted');
check('6b ✅ on accepted = done', cb.applyReaction({ item: { ...item, status: 'accepted' }, reaction: 'white_check_mark', userId: RON, ronId: RON, now }).status, 'done');
const dis = cb.applyReaction({ item, reaction: 'x', userId: RON, ronId: RON, now });
check('6c ❌ dismisses and teaches a signature', [dis.status, dis.dismissal.signature], ['dismissed', item.signature]);
const sn = cb.applyReaction({ item, reaction: 'next_track_button', userId: RON, ronId: RON, now });
check('6d ⏭ snoozes ~20h', [sn.status, sn.set.snoozed_until], ['snoozed', '2026-11-11T08:50:00.000Z']);
check('6e someone else is ignored', cb.applyReaction({ item, reaction: 'x', userId: 'U_OTHER', ronId: RON, now }).ignore, true);
check('6f other emoji ignored', cb.applyReaction({ item, reaction: 'eyes', userId: RON, ronId: RON, now }).reason, 'other_emoji');
check('6g ✅ on dismissed ignored', cb.applyReaction({ item: { ...item, status: 'dismissed' }, reaction: 'white_check_mark', userId: RON, ronId: RON, now }).ignore, true);
const mute = cb.muteDecision({ actorSignature: 'gmail_ron|news@example.com|*', now, dismissalsForActor: [
  { signature: 'gmail_ron|news@example.com|weekly-newsletter', count: 2, last_at: '2026-11-01T00:00:00Z' },
  { signature: 'gmail_ron|news@example.com|product-update', count: 1, last_at: '2026-11-05T00:00:00Z' },
  { signature: 'gmail_ron|news@example.com|old-topic', count: 5, last_at: '2026-08-01T00:00:00Z' },
] });
check('6h three dismissals in 30 days mute the sender (old ones do not count)', [mute.mute, mute.count], [true, 3]);
check('6i two do not', cb.muteDecision({ actorSignature: 'a|b|*', now, dismissalsForActor: [{ signature: 'a|b|c', count: 2, last_at: '2026-11-01T00:00:00Z' }] }).mute, false);
check('6j expire unreacted older than 20h', cb.expireUnreacted([{ id: 1, status: 'shown', shown_at: '2026-11-09T12:50:00Z' }, { id: 2, status: 'shown', shown_at: '2026-11-10T12:00:00Z' }, { id: 3, status: 'accepted', shown_at: '2026-11-01T00:00:00Z' }], now), [1]);

// ── 7. Formatting and the structural contract ───────────────────────────────
const date = new Date('2026-11-10T12:50:00Z'); // Tue Nov 10 CR
const items = sup.kept.map(k => ({ ...k, whyRon: 'Only you hold the card.', action: 'Open the invoice and pay or dispute.' }));
const counts = { slack: { scanned: 31 }, max: { scanned: 4 }, gmail_ron: { scanned: 48 }, gmail_contact: { scanned: 0, failed: 'auth' }, calendar: { scanned: 3 }, github: { scanned: 6 }, wiki: { scanned: 37 } };
const scanned = cb.scannedLine(counts, { dropped: 94, expired: 2, muted: 1 });
check('7a scanned line', scanned, 'Scanned: slack 31 · max 4 · gmail_ron 48 · ⚠️ gmail_contact unavailable · calendar 3 · github 6 · wiki 37 · dropped 94 · expired 2 · muted 1');
const projects = cb.projectsInFlight({ now, accepted: [{ id: 9, title: 'Fix the intake leak', url: 'https://x.example/p', tier: 2, accepted_at: '2026-11-01T00:00:00Z', shown_at: '2026-11-01T00:00:00Z' }], prs: [{ title: 'feat: clarity brief', url: 'https://github.example/pr', updatedAt: '2026-11-09T00:00:00Z', tier: 3 }] });
check('7b projects ranked by tier, stale flagged after 7 days', [projects[0].title, projects[0].stale, projects[1].quietDays], ['Fix the intake leak', true, 1]);
const parent = cb.formatParent({ date, items, projects: projects.map((p, i) => ({ ...p, why: i ? 'Ships the brief.' : 'Leads keep leaking.' })), scanned, today: [{ hm: '10:00', label: 'Sales call: Acme' }] });
const replies = items.map((it, i) => cb.formatItemReply(it, i + 1));
check('7c header line', parent.split('\n')[0], 'CLARITY BRIEF · Tue Nov 10 · 3 for you · 2 projects');
check('7d a good brief validates', cb.validateClarityBrief({ parent, replies }), { ok: true, problems: [] });
check('7e item reply shape', replies[0].split('\n').map(l => l.split(':')[0]), ['*1. Payment failed for invoice 44*  [T1 Revenue now · gmail_ron]', 'Source', 'Why you', 'Do']);
check('7f stale project line asks keep or drop', /7\+ days: ✅ done or ❌ drop/.test(parent), true);
// Failure paths (the check must be able to go red)
const bad1 = replies.slice(); bad1[1] = bad1[1].replace(/^Do: .*$/m, 'Do: ');
check('7g an empty Do fails', cb.validateClarityBrief({ parent, replies: bad1 }).problems, ['reply2:do', 'empty_label']);
check('7h an em dash fails', cb.validateClarityBrief({ parent: parent.replace('Nothing', 'x'), replies: [replies[0].replace('Only you', 'Only \u2014 you'), replies[1], replies[2]] }).problems, ['forbidden:em_dash']);
check('7i header count must match the replies', cb.validateClarityBrief({ parent, replies: replies.slice(0, 2) }).problems, ['header_count:3!=2']);
check('7j a missing source on the Scanned line fails', cb.validateClarityBrief({ parent: parent.replace('calendar 3 · ', ''), replies }).problems, ['scanned_missing:calendar']);
const empty = cb.formatParent({ date, items: [], projects: [], scanned });
check('7k zero items says so and validates', [empty.includes('Nothing needs you today.'), cb.validateClarityBrief({ parent: empty, replies: [] }).ok], [true, true]);
check('7l zero items without the line fails', cb.validateClarityBrief({ parent: empty.replace('Nothing needs you today.', 'Quiet.'), replies: [] }).problems, ['zero_items_line']);
check('7m undefined in text fails', cb.validateClarityBrief({ parent: parent.replace('Acme', 'undefined'), replies }).problems, ['forbidden:undefined']);
check('7n fallback replies meet the contract', cb.validateClarityBrief({ parent, replies: cb.fallbackReplies(items) }).ok, true);
check('7o the lib itself has no em dash', /\u2014/.test(fs.readFileSync(path.join(__dirname, '..', 'lib', 'clarityBrief.js'), 'utf8').replace(/\\u2014/g, '')), false);

// ── 8. Verdict and model parsing ────────────────────────────────────────────
check('8a green verdict', cb.verdictLine({ ok: true, postedAt: date, items: 3, projects: 2, sourcesOk: 6, sourcesTotal: 7, costUsd: 0.0712 }), '✅ ALL GREEN: Clarity Brief posted 06:50, 3 for you, 2 projects, sources 6/7, cost $0.07');
check('8b issues verdict', cb.verdictLine({ ok: false, issues: ['gmail_contact unavailable', 'fallback used'] }), '⚠️ ISSUES: gmail_contact unavailable · fallback used');
const cls = cb.parseClassification('```json\n[{"idx":0,"verdict":"needs_ron","tier":1,"title":"Billing: payment failed","why_ron":"Card on file is yours","action":"Update the card"},{"idx":1,"verdict":"junk","tier":9},{"idx":7,"verdict":"noise"}]\n```', 2);
check('8c classification parses, defaults bad enums, drops out-of-range idx', [cls.size, cls.get(0).tier, cls.get(1).verdict, cls.get(1).tier], [2, 1, 'noise', 3]);
check('8d junk classification is null', cb.parseClassification('sure, here you go', 2), null);
// 2026-11-05: the model answered every candidate but left idx out, and the whole batch vanished.
const noIdx = cb.parseClassification('[{"verdict":"noise"},{"verdict":"needs_ron","tier":1,"title":"Laura asks about terms"}]', 2);
check('8d2 an answer without idx that covers the batch in order matches by position', [noIdx.size, noIdx.get(1).verdict, noIdx.get(1).title], [2, 'needs_ron', 'Laura asks about terms']);
check('8d3 an answer with nothing matchable is junk, not an empty result', cb.parseClassification('[{"verdict":"noise"}]', 2), null);
check('8d4 partial idx answers do not fall back to position', cb.parseClassification('[{"idx":1,"verdict":"fyi"},{"verdict":"noise"}]', 2).size, 1);
check('8d5 the prompt requires idx and keeps non needs_ron answers short', /MUST start with "idx"/.test(cb.buildClassifyPrompt([{ idx: 0, source: 's', actor: 'a', title: 't' }])) && /just \{"idx": n, "verdict"/.test(cb.buildClassifyPrompt([])), true);
check('8d6 a full batch of needs_ron answers fits the token budget', cb.CLASSIFY_BATCH * 150 < cb.CLASSIFY_MAX_TOKENS, true);
check('8d7 both brief crons recover a missed minute', (fs.readFileSync(path.join(__dirname, '..', 'index.js'), 'utf8').match(/cron\.schedule\('(50 6|15 7) \* \* 1-5', wrapCronJob\('(runClarityBrief|checkClarityBriefPosted)'.*recoverMissedExecutions: true \}\);/g) || []).length, 2);
const draft = cb.parseDraftJson('{"items":[{"idx":0,"title":"Pay invoice 44","why_ron":"Your card","action":"Pay it"}],"projects":[{"idx":0,"why":"Leaks cost leads"}]}', { nItems: 1, nProjects: 1 });
check('8e draft parses', [draft.items.get(0).action, draft.projects.get(0).why], ['Pay it', 'Leaks cost leads']);
check('8f a draft missing an item is rejected', cb.parseDraftJson('{"items":[],"projects":[]}', { nItems: 1, nProjects: 0 }), null);
const merged = cb.mergeDraft({ items: [items[0]], projects: [projects[0]], draft });
check('8g merge keeps facts, takes prose', [merged.items[0].url, merged.items[0].action, merged.projects[0].why], ['https://mail.example/1', 'Pay it', 'Leaks cost leads']);
check('8h prompts carry no em dash', /\u2014/.test(cb.buildClassifyPrompt([{ idx: 0, source: 's', actor: 'a', title: 't' }]) + cb.buildDraftPrompt({ items: [{ idx: 0, ...items[0] }], projects: [{ idx: 0, ...projects[0] }] })), false);

// 9. Gmail links name the mailbox; /u/0/ would open contact@ threads in Ron's inbox
check('9a contact link names the mailbox', cb.gmailThreadUrl('contact@example.com', 'abc123'), 'https://mail.google.com/mail/u/contact@example.com/#inbox/abc123');
check('9b no mailbox falls back to the first account', cb.gmailThreadUrl('', 'abc123'), 'https://mail.google.com/mail/u/0/#inbox/abc123');

// 10. On-demand trigger (Ron DMs Max)
check('10a trigger phrases', ['brief now', 'Brief now!', 'clarity brief', 'Clarity brief now', 'brief ahora'].map(cb.isBriefNowCommand), [true, true, true, true, true]);
check('10a2 the footer Claude adds from Ron\'s account is ignored', ['brief now *Sent using* Claude', 'brief now\n*Sent using* Claude'].map(cb.isBriefNowCommand), [true, true]);
check('10b sentences about the brief go to the chat model', ['brief', 'why was the brief empty?', 'send the brief now to Gerald', 'brief now please'].map(cb.isBriefNowCommand), [false, false, false, false]);
check('10c the DM handler runs it only for Ron, outside threads', /userId === RON_SLACK_ID && !dmThreadTs && clarityBrief\.isBriefNowCommand\(message\.text\)/.test(fs.readFileSync(path.join(__dirname, '..', 'index.js'), 'utf8')), true);

if (failures) { console.error(`\n${failures} failure(s)`); process.exit(1); }
console.log('\nall clarity-brief checks passed');
