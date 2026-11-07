// Run: node test/cron-step-ranges.test.js
// node-cron 3.0.3 reads a step as "divisible by the step", so '0 7-21/2' runs at
// 8, 10, ..., 20 instead of 7, 9, ..., 21. This fails if any cron in index.js has a
// stepped field that node-cron reads differently from standard cron.
const assert = require('assert');
const fs = require('fs');
const path = require('path');
const { stepMismatch, makeSchedule } = require('../lib/cronSchedule');

// 1. The checker itself.
assert.match(stepMismatch('0 7-21/2 * * *'), /hour '7-21\/2' runs at 8,10,12,14,16,18,20, not 7,9,11,13,15,17,19,21/);
assert.match(stepMismatch('20 7-19/3 * * *'), /hour '7-19\/3' runs at 9,12,15,18, not 7,10,13,16,19/);
assert.match(stepMismatch('0 9 */2 * *'), /day-of-month/, 'dom * starts at 1, so */2 differs');
assert.match(stepMismatch('5/10 * * * *'), /runs at never/);
for (const ok of ['*/5 * * * *', '*/30 7-20 * * *', '0 */2 * * *', '0 0-22/2 * * *', '0 7,9,11 * * *', '0 9,15 * * 1-6', '50 6 * * 1-5', '0 8 1 * *', '* * * * * *']) {
  assert.strictEqual(stepMismatch(ok), null, ok);
}
assert.strictEqual(stepMismatch('0 9 * JAN-MAR/2 *'), null, 'names: cannot judge, never a false alarm');

// 2. Every expression in index.js: the cron.schedule literals and STATIC_CRON_SCHEDULES.
const src = fs.readFileSync(path.join(__dirname, '..', 'index.js'), 'utf8');
const literals = [...src.matchAll(/cron\.schedule\(\s*'([^']+)'/g)].map(m => m[1]);
const block = src.slice(src.indexOf('const STATIC_CRON_SCHEDULES = {'), src.indexOf('};', src.indexOf('const STATIC_CRON_SCHEDULES = {')));
const declared = [...block.matchAll(/:\s*'([^']+)'/g)].map(m => m[1]);
assert.ok(literals.length >= 60 && declared.length >= 50, `scan broken? ${literals.length} literals, ${declared.length} declared`);
const bad = [...new Set([...literals, ...declared])].map(e => [e, stepMismatch(e)]).filter(([, m]) => m);
assert.deepStrictEqual(bad, [], 'a cron in index.js does not run when it reads');

// 3. The two crons fixed 2026-11-07 run at their intended hours.
assert.ok(src.includes("cron.schedule('0 7,9,11,13,15,17,19,21 * * *', wrapCronJob('runAutoStrikeMover'"));
assert.ok(src.includes("cron.schedule('20 7,10,13,16,19 * * *', wrapCronJob('runApptDeletionSweep'"));

// 4. A dynamic cron from the DB with a misread step is logged loudly at registration.
const logged = [];
const origError = console.error;
console.error = (msg) => logged.push(String(msg));
makeSchedule({ schedule: () => ({}) })('0 7-21/2 * * *', () => {});
makeSchedule({ schedule: () => ({}) })('0 8 * * 1', () => {});
console.error = origError;
assert.strictEqual(logged.length, 1);
assert.match(logged[0], /does not run when it reads/);

console.log(`cron-step-ranges: all checks passed (${literals.length + declared.length} expressions checked)`);
