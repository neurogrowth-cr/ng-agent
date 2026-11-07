// Run: node test/cron-recovery.test.js
// Every cron in Max must recover a minute its node-cron tick stepped over.
// The default (recoverMissedExecutions: false) dropped 352 runs in 30 days, so
// this fails if any registration can reach node-cron without lib/cronSchedule.js.
const assert = require('assert');
const fs = require('fs');
const path = require('path');
const { makeSchedule, wrapNodeCron, MIN_GAP_MS } = require('../lib/cronSchedule');

const root = path.join(__dirname, '..');
const indexSrc = fs.readFileSync(path.join(root, 'index.js'), 'utf8');

// 1. index.js gets `cron` from the wrapper, never from node-cron directly.
assert.ok(/^const cron = require\('\.\/lib\/cronSchedule'\);$/m.test(indexSrc), 'index.js must bind cron to lib/cronSchedule');
assert.ok(!/require\(\s*['"]node-cron['"]\s*\)/.test(indexSrc), 'index.js must not require node-cron directly');

// 2. Nothing else in the app requires node-cron (a second door would bypass the wrapper).
const appFiles = ['lib', 'scripts']
  .map(d => path.join(root, d))
  .filter(d => fs.existsSync(d))
  .flatMap(d => fs.readdirSync(d).filter(f => f.endsWith('.js')).map(f => path.join(d, f)));
for (const file of appFiles) {
  if (path.basename(file) === 'cronSchedule.js') continue;
  assert.ok(!/require\(\s*['"]node-cron['"]\s*\)/.test(fs.readFileSync(file, 'utf8')), `${path.relative(root, file)} requires node-cron directly`);
}

// 3. Every .schedule( registration in index.js goes through `cron`, and none opts out.
const scheduleCalls = indexSrc.match(/\b(\w+)\.schedule\(/g) || [];
const viaCron = scheduleCalls.filter(c => c === 'cron.schedule(');
assert.ok(viaCron.length >= 60, `expected 60+ cron.schedule calls, found ${viaCron.length} (scan broken?)`);
assert.deepStrictEqual(scheduleCalls.filter(c => c !== 'cron.schedule('), [], 'a .schedule( call that is not cron.schedule');
assert.ok(!/recoverMissedExecutions\s*:\s*false/.test(indexSrc), 'a cron opts out of recovery');

// 4. The wrapper forces recovery and keeps the caller's options (timezone).
const calls = [];
const fake = { schedule: (expr, fn, opts) => { calls.push({ expr, fn, opts }); return { expr }; }, validate: () => true };
const wrapped = wrapNodeCron(fake);
wrapped.schedule('50 6 * * 1-5', () => {}, { timezone: 'America/Costa_Rica' });
wrapped.schedule('0 9 * * *', () => {}, { timezone: 'America/Costa_Rica', recoverMissedExecutions: false });
wrapped.schedule('*/5 * * * *', () => {});
assert.deepStrictEqual(calls.map(c => c.opts), [
  { timezone: 'America/Costa_Rica', recoverMissedExecutions: true },
  { timezone: 'America/Costa_Rica', recoverMissedExecutions: true },
  { recoverMissedExecutions: true },
]);
assert.strictEqual(calls[0].expr, '50 6 * * 1-5', 'schedule unchanged');
assert.strictEqual(wrapped.validate(), true, 'other node-cron exports pass through');

// 5. A catch-up loop that replays several minutes in one go runs the job once.
let clock = 1_000_000;
const runs = [];
const fake2 = { schedule: (expr, fn) => ({ fire: (arg) => fn(arg) }) };
const schedule = makeSchedule(fake2, () => clock);
const taskA = schedule('*/5 * * * *', (arg) => { runs.push(['A', arg]); return 'ran'; });
const taskB = schedule('*/5 * * * *', (arg) => { runs.push(['B', arg]); });
assert.strictEqual(taskA.fire('10:00'), 'ran', 'return value passes through');
taskA.fire('10:05');                       // same synchronous replay, same ms
clock += MIN_GAP_MS - 1; taskA.fire('10:10'); // still inside the guard
taskB.fire('10:00');                       // other task, own guard
clock += 1; taskA.fire('10:15');           // a second later: a real next run
assert.deepStrictEqual(runs, [['A', '10:00'], ['B', '10:00'], ['A', '10:15']]);

// 6. Against the real node-cron, the scheduler is built with recovery on.
const real = require('../lib/cronSchedule');
const task = real.schedule('50 6 * * 1-5', () => {}, { timezone: 'America/Costa_Rica', scheduled: false });
assert.strictEqual(task._scheduler.autorecover, true, 'node-cron scheduler autorecover');
assert.strictEqual(task.options.timezone, 'America/Costa_Rica');
task.stop();

console.log(`cron-recovery: all checks passed (${viaCron.length} cron.schedule calls guarded)`);
