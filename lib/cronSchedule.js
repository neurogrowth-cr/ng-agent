'use strict';
// Every Max cron goes through here, so every one recovers a minute its tick missed.
// index.js binds: cron = wrapNodeCron(node-cron module), on one line.
//
// node-cron 3.0.3 checks the clock once a second per task. With the default
// recoverMissedExecutions: false it only fires when a tick lands inside second 0
// of the scheduled minute; a tick that steps over second 0 (event-loop lag, often
// from the other crons that start at the same :00) drops the run with no trace.
// From 2026-10-08 to 2026-11-07 that silently dropped 352 runs across 21 crons
// while the process was up, including the 2026-11-06 Clarity Brief, two Guardian
// digests and two Setter Hot Replies digests.
//
// With recovery on, the scheduler replays the seconds it stepped over since its
// previous tick (so at most a few seconds late). It never replays anything from
// before the process started.
//
// BUT node-cron 3.0.3 recovery also double-fires: it zeroes the milliseconds of
// lastExecution after a fire, then on the next tick (about 1002 ms later) it looks
// one second back, re-matches the same second 0 with its milliseconds intact, and
// fires again. Seen in prod right after #448 deployed (2026-11-07 09:20 CR: every
// cron due started twice, about a second apart) and reproduced locally. A long
// stall can also replay several minutes in one synchronous loop.
//
// So the guard below drops any fire less than 30 s after the previous fire of the
// same task. That removes the duplicate and collapses a replay burst into one run.
// Every Max schedule is 5-field (minute resolution), so no legitimate fire of one
// task comes within 30 s of the last.
//
// Pure wrapper, tested in test/cron-recovery.test.js (which also fails if any
// cron in index.js bypasses it).

const MIN_GAP_MS = 30 * 1000;

// ─── Step-on-range check ─────────────────────────────────────────────────────
// node-cron 3.0.3 reads a step as "values divisible by the step", not "every
// step-th value from the start of the range". '0 7-21/2' runs at 8, 10, ..., 20
// (standard cron: 7, 9, ..., 21) and '20 7-19/3' at 9:20, 12:20, 15:20, 18:20
// (standard: 7:20 to 19:20). Found 2026-11-07 on runAutoStrikeMover and
// runApptDeletionSweep, which had run at the wrong hours since they shipped.
// stepMismatch(expr) says when node-cron and standard cron disagree, so a stepped
// field that only reads right by luck ('*/15' on minutes, starting at 0) passes
// and one that does not is caught: by test/cron-step-ranges.test.js for index.js,
// and by a loud log line at registration for dynamic crons from the DB.
const FIELD_RANGES = [[0, 59], [0, 23], [1, 31], [1, 12], [0, 7]];
const FIELD_NAMES = ['minute', 'hour', 'day-of-month', 'month', 'day-of-week'];

function expandField(field, [lo, hi], reading) {
  const out = [];
  for (const part of String(field).split(',')) {
    const [rng, stepStr] = part.split('/');
    const step = stepStr === undefined ? 1 : Number(stepStr);
    let a; let b;
    if (rng === '*') { a = lo; b = hi; }
    else if (rng.includes('-')) { [a, b] = rng.split('-').map(Number); }
    else { a = Number(rng); b = (stepStr !== undefined && reading === 'standard') ? hi : a; }
    if (![a, b, step].every(Number.isInteger) || step < 1) return null; // names etc: cannot judge
    for (let v = a; v <= b; v++) {
      if (stepStr === undefined) out.push(v);
      else if (reading === 'standard' ? (v - a) % step === 0 : v % step === 0) out.push(v);
    }
  }
  return [...new Set(out)].sort((x, y) => x - y);
}

function stepMismatch(expression) {
  let fields = String(expression || '').trim().split(/\s+/);
  let ranges = FIELD_RANGES;
  let names = FIELD_NAMES;
  if (fields.length === 6) { ranges = [[0, 59], ...FIELD_RANGES]; names = ['second', ...FIELD_NAMES]; }
  else if (fields.length !== 5) return null;
  const problems = [];
  fields.forEach((f, i) => {
    if (!f.includes('/')) return;
    const nodeReads = expandField(f, ranges[i], 'node');
    const standard = expandField(f, ranges[i], 'standard');
    if (!nodeReads || !standard) return;
    if (nodeReads.join(',') !== standard.join(',')) {
      problems.push(`${names[i]} '${f}' runs at ${nodeReads.join(',') || 'never'}, not ${standard.join(',')}`);
    }
  });
  return problems.length ? problems.join('; ') : null;
}

function makeSchedule(nodeCron, now = () => Date.now()) {
  return function schedule(expression, fn, options = {}) {
    const mismatch = stepMismatch(expression);
    if (mismatch) console.error(`⚠️ cron '${expression}' does not run when it reads: ${mismatch}. List the values instead of a stepped range.`);
    let lastFireMs = -Infinity;
    const onceAtATime = (...args) => {
      const t = now();
      if (t - lastFireMs < MIN_GAP_MS) return undefined;
      lastFireMs = t;
      return fn(...args);
    };
    return nodeCron.schedule(expression, onceAtATime, { ...options, recoverMissedExecutions: true });
  };
}

function wrapNodeCron(nodeCron) {
  return { ...nodeCron, schedule: makeSchedule(nodeCron) };
}

// node-cron is passed in, not required here, so the tests run with no npm install.
module.exports = { makeSchedule, wrapNodeCron, stepMismatch, MIN_GAP_MS };
