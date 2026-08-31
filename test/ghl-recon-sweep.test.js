// Rules test for the GHL↔revops reconciliation diff.  Run:  node test/ghl-recon-sweep.test.js
//
// The sweep starts from GHL's calendar API (source of truth) and flags any
// event booked in the lookback window with no revops_appointments row. This
// pins the pure diff: window filtering on dateAdded, GHL tombstones excluded,
// CANCELLED events still checked (dash records cancels — a missing cancelled
// row is still a gap), and known ids never re-flagged.
const fs   = require('fs');
const path = require('path');

const SRC = fs.readFileSync(process.argv[2] || path.join(__dirname, '..', 'index.js'), 'utf8');
const block = SRC.slice(
  SRC.indexOf('function diffGhlEventsAgainstRevops'),
  SRC.indexOf('async function runGhlRevopsReconciliation'),
);
const diffGhlEventsAgainstRevops = new Function(`${block}; return diffGhlEventsAgainstRevops;`)();

const NOW = Date.parse('2026-08-31T03:00:00.000Z');
const H = 3600 * 1000;
const win = { sinceMs: NOW - 48 * H, untilMs: NOW - 0.75 * H };
const ev = (id, hoursAgo, extra = {}) => ({ id, dateAdded: new Date(NOW - hoursAgo * H).toISOString(), ...extra });

const cases = [];
const check = (name, got, want) => cases.push({ name, ok: JSON.stringify(got) === JSON.stringify(want), got, want });

// 1. A booking in-window with no row is a gap — the ONLY RON failure mode.
check('missing in-window booking is flagged',
  diffGhlEventsAgainstRevops([ev('appt-1', 5)], new Set(), win).map(e => e.id), ['appt-1']);

// 2. A booking that has a row is not a gap.
check('ingested booking passes',
  diffGhlEventsAgainstRevops([ev('appt-1', 5)], new Set(['appt-1']), win), []);

// 3. GHL tombstones are not gaps.
check('deleted event excluded',
  diffGhlEventsAgainstRevops([ev('appt-1', 5, { deleted: true })], new Set(), win), []);

// 4. CANCELLED is not deleted — dash records cancels, so a missing row is still a gap.
check('cancelled-but-not-deleted event IS flagged',
  diffGhlEventsAgainstRevops([ev('appt-1', 5, { appointmentStatus: 'cancelled' })], new Set(), win).map(e => e.id),
  ['appt-1']);

// 5. Older than the lookback: out of scope (yesterday's run owned it).
check('event older than lookback excluded',
  diffGhlEventsAgainstRevops([ev('appt-1', 49)], new Set(), win), []);

// 6. Inside the grace window: webhook/hydration may still be in flight.
check('event inside grace window excluded',
  diffGhlEventsAgainstRevops([ev('appt-1', 0.5)], new Set(), win), []);

// 7. Garbage never crashes the sweep and never counts as a gap.
check('null/undefined/bad-date events excluded',
  diffGhlEventsAgainstRevops([null, undefined, { id: 'x' }, ev('appt-1', 5, { dateAdded: 'not-a-date' })], new Set(), win),
  []);

// 8. Empty input → empty output (a quiet night is not an error).
check('no events → no gaps', diffGhlEventsAgainstRevops([], new Set(), win), []);

const failed = cases.filter(c => !c.ok);
for (const c of cases) console.log(`${c.ok ? '✓' : '✗'} ${c.name}${c.ok ? '' : ` got=${JSON.stringify(c.got)} want=${JSON.stringify(c.want)}`}`);
console.log(`${cases.length - failed.length}/${cases.length} passed`);
if (failed.length) process.exit(1);
