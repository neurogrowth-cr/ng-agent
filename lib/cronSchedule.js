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
// previous tick (so at most a few seconds late) and its lastExecution guard stops
// the same minute firing twice. It never replays anything from before the process
// started. One edge stays: a stall longer than a cron's interval would replay
// several minutes in one synchronous loop. The guard below collapses any fires
// less than a second apart into one, so a long stall recovers one run, never a
// burst. No 5-field schedule legitimately fires twice inside a second.
//
// Pure wrapper, tested in test/cron-recovery.test.js (which also fails if any
// cron in index.js bypasses it).

const MIN_GAP_MS = 1000;

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
