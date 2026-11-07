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

function makeSchedule(nodeCron, now = () => Date.now()) {
  return function schedule(expression, fn, options = {}) {
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
module.exports = { makeSchedule, wrapNodeCron, MIN_GAP_MS };
