// Run: node test/blocked-summary.test.js
const assert = require('assert');
const { blockedSummary, MAX_REASON } = require('../lib/blockedSummary');

// 1. The roster never appears: 17 blocked activities, no notes = one short line.
const roster = Array.from({ length: 17 }, (_, i) => ({ status: 'blocked', notes: null, updated_at: `2026-11-05T15:34:${String(10 + i).padStart(2, '0')}Z`, template_id: `t${i}` }));
assert.strictEqual(blockedSummary(roster), 'blocked since 2026-11-05: no reason logged in the portal');

// 2. A logged note is the reason; the newest note wins.
const noted = [
  { status: 'blocked', notes: 'waiting on LinkedIn access', updated_at: '2026-11-03T10:00:00Z' },
  { status: 'blocked', notes: '  client asked to pause until Nov 20  ', updated_at: '2026-11-05T10:00:00Z' },
  { status: 'blocked', notes: null, updated_at: '2026-11-01T10:00:00Z' },
];
assert.strictEqual(blockedSummary(noted), 'blocked since 2026-11-01: client asked to pause until Nov 20');

// 3. Long notes are clipped so the brief stays one line.
const long = [{ status: 'blocked', notes: 'x'.repeat(400), updated_at: '2026-11-05' }];
const out = blockedSummary(long);
assert.ok(out.length <= 'blocked since 2026-11-05: '.length + MAX_REASON, 'clipped');
assert.ok(out.endsWith('…'));

// 4. Nothing blocked, or no rows, = empty string (callers drop the segment).
assert.strictEqual(blockedSummary([{ status: 'phase_1', notes: 'hi' }]), '');
assert.strictEqual(blockedSummary([]), '');
assert.strictEqual(blockedSummary(undefined), '');

// 5. No updated_at at all still reads.
assert.strictEqual(blockedSummary([{ status: 'blocked' }]), 'blocked: no reason logged in the portal');

// 6. The portal reason (client_dashboards.blocked_reason, dash #430) wins over
//    activity notes, and blocked_at is the date.
const dash = { customer_status: 'blocked', blocked_reason: '  Client has not sent  LinkedIn access ', blocked_at: '2026-11-07T14:02:00Z' };
assert.strictEqual(blockedSummary(noted, dash), 'blocked since 2026-11-07: Client has not sent LinkedIn access');
assert.strictEqual(blockedSummary(roster, dash), 'blocked since 2026-11-07: Client has not sent LinkedIn access');

// 7. A blocked client with no blocked activity rows still reads (the dash row alone is enough).
assert.strictEqual(blockedSummary([], dash), 'blocked since 2026-11-07: Client has not sent LinkedIn access');
assert.strictEqual(blockedSummary([], { customer_status: 'blocked', blocked_reason: null, blocked_at: null }), 'blocked: no reason logged in the portal');

// 8. A dash row that is not blocked changes nothing.
assert.strictEqual(blockedSummary(roster, { customer_status: 'phase_1', blocked_reason: 'stale', blocked_at: '2026-01-01' }), 'blocked since 2026-11-05: no reason logged in the portal');
assert.strictEqual(blockedSummary([], { customer_status: 'phase_1' }), '');

console.log('blocked-summary: all tests passed');
