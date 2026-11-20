// Rules test for the inbox watch liveness check.  Run:  node test/axon-inbox-watch.test.js
//
// AXON's inbox_watch cron (12,42 6-20 * * * Costa Rica) stamps
// axon.engine_heartbeat id 'cron:inbox_watch' after each run that read the
// inbox. lib/axonHealth.js decides, from that one timestamp, when the cron has
// stopped: 75 minutes with no completed run inside 06:00 to 20:59 Costa Rica.
// One alert per outage, a recovery when a newer run appears, quiet at night.
const ax = require('../lib/axonHealth');

let failures = 0;
function check(label, actual, expected) {
  const ok = JSON.stringify(actual) === JSON.stringify(expected);
  if (!ok) { failures++; console.error(`FAIL ${label}\n  expected: ${JSON.stringify(expected)}\n  actual:   ${JSON.stringify(actual)}`); }
  else console.log(`ok   ${label}`);
}

const MIN = 60 * 1000;
// Costa Rica is UTC-6 all year: 10:00 CR = 16:00 UTC.
const cr = (day, hhmm) => new Date(`2026-11-${day}T${hhmm}:00-06:00`).getTime();
const row = (ms) => [{ beat_at: new Date(ms).toISOString() }];
const at = (now, beatMs) => ax.classifyInboxWatch({ data: beatMs == null ? [] : row(beatMs), now });

// ── 1. the id is a contract with ng-axon lib/pure.js cronBeatRow
check('1a  beat id', ax.INBOX_WATCH_BEAT_ID, 'cron:inbox_watch');
check('1b  threshold is 75 minutes', ax.INBOX_WATCH_STALE_MS, 75 * MIN);

// ── 2. classify inside the cron's hours
check('2a  run 17 min ago is ok', at(cr(20, '10:30'), cr(20, '10:13')).kind, 'ok');
check('2b  one missed run (47 min) is ok', at(cr(20, '11:00'), cr(20, '10:13')).kind, 'ok');
check('2c  74 min is ok', at(cr(20, '11:27'), cr(20, '10:13')).kind, 'ok');
check('2d  75 min is stale', at(cr(20, '11:28'), cr(20, '10:13')).kind, 'stale');
check('2e  no row at midday is stale', at(cr(20, '12:00'), null).kind, 'stale');
check('2f  read error is blind', ax.classifyInboxWatch({ error: { message: 'upstream request timeout' }, now: cr(20, '12:00') }).kind, 'blind');
check('2g  throw is blind', ax.classifyInboxWatch({ thrown: new Error('fetch failed'), now: cr(20, '12:00') }).kind, 'blind');
check('2h  unreadable timestamp counts as no run', at(cr(20, '12:00'), null).beatAt, ax.classifyInboxWatch({ data: [{ beat_at: 'not a date' }], now: cr(20, '12:00') }).beatAt);

// ── 3. the night never counts
const lastNight = cr(19, '20:43');
check('3a  21:00, last run 20:43: quiet', at(cr(19, '21:00'), lastNight).kind, 'quiet');
check('3b  03:00 with a 6 hour old run: quiet', at(cr(20, '03:00'), lastNight).kind, 'quiet');
check('3c  05:59: quiet', at(cr(20, '05:59'), lastNight).kind, 'quiet');
check('3d  06:00: the day starts, ok', at(cr(20, '06:00'), lastNight).kind, 'ok');
check('3e  07:10, no run yet today: still ok (70 min)', at(cr(20, '07:10'), lastNight).kind, 'ok');
check('3f  07:20, no run yet today: stale', at(cr(20, '07:20'), lastNight).kind, 'stale');
check('3g  07:20 with the 06:13 run: ok', at(cr(20, '07:20'), cr(20, '06:13')).kind, 'ok');
check('3h  20:59 is still inside the hours', at(cr(20, '20:59'), cr(20, '19:13')).kind, 'stale');
check('3i  cron dead at 19:50: 20:50 poll not yet stale, then the night is quiet',
  [at(cr(20, '20:50'), cr(20, '19:43')).kind, at(cr(20, '21:00'), cr(20, '19:43')).kind], ['ok', 'quiet']);
check('3j  a dead night with no row at all: quiet', at(cr(20, '02:00'), null).kind, 'quiet');

// ── 4. one alert per outage, recovery clears it
function run(polls, start = ax.newInboxWatchState()) {
  let state = start;
  const out = [];
  const events = [];
  for (const [now, beat] of polls) {
    const step = ax.stepInboxWatch(state, at(now, beat));
    state = step.state;
    out.push(step.posts);
    events.push(step.event);
  }
  return { state, out, events };
}
const B = cr(20, '10:13');
const outage = run([
  [cr(20, '10:20'), B], [cr(20, '11:20'), B],             // fine, one missed run
  [cr(20, '11:30'), B],                                   // 77 min: alert
  [cr(20, '11:40'), B], [cr(20, '14:00'), B], [cr(20, '20:50'), B], // same outage: silent
  [cr(20, '23:00'), B],                                   // night: silent
  [cr(21, '07:30'), B], [cr(21, '12:00'), B],             // next day, same outage: silent
  [cr(21, '12:20'), cr(21, '12:13')],                     // a run completed: recovery
  [cr(21, '12:30'), cr(21, '12:13')],                     // back to normal: silent
]);
check('4a  posts per poll', outage.out.map((p) => p.length), [0, 0, 1, 0, 0, 0, 0, 0, 0, 1, 0]);
check('4b  alert event carries the last completed run', outage.events[2], { action: 'stale_alert', beat_key: new Date(B).toISOString() });
check('4c  recovery event', outage.events[9], { action: 'recovered', beat_key: new Date(cr(21, '12:13')).toISOString() });
check('4d  state is clean after recovery', outage.state, { alertedKey: null });
check('4e  a second outage alerts again', run([[cr(21, '14:00'), cr(21, '12:13')]], outage.state).out[0].length, 1);

// ── 5. a failed read decides nothing and keeps the state
const alerted = run([[cr(20, '11:30'), B]]).state;
const blindStep = ax.stepInboxWatch(alerted, ax.classifyInboxWatch({ error: { message: 'x' }, now: cr(20, '11:40') }));
check('5a  blind: no post, state kept', [blindStep.posts.length, blindStep.state], [0, alerted]);
check('5b  blind with a clean state: no alert', ax.stepInboxWatch(ax.newInboxWatchState(), { kind: 'blind' }).posts, []);

// ── 6. recovery is seen at any hour; the night alone never alerts
const night = run([[cr(20, '20:55'), cr(20, '19:13')], [cr(20, '21:10'), cr(20, '20:57')]]);
check('6a  alert at 20:55, recovery seen at 21:10', night.out.map((p) => p.length), [1, 1]);
check('6b  a whole quiet night: nothing', run([[cr(20, '22:00'), lastNight], [cr(20, '04:00'), lastNight]]).out.flat(), []);

// ── 7. never beaten (row missing): alert once, recover on the first run
const never = run([[cr(20, '09:00'), null], [cr(20, '09:10'), null], [cr(20, '09:20'), cr(20, '09:13')]]);
check('7a  alert once, then recovery', never.out.map((p) => p.length), [1, 0, 1]);
check('7b  never key', never.events[0], { action: 'stale_alert', beat_key: 'never' });
check('7c  never alert text', /nunca ha registrado una lectura completa/.test(never.out[0][0]), true);
check('7d  recovery after never has no span', never.out[2][0],
  '✅ *La vigilancia del inbox de AXON volvió a correr*: completó una lectura a las 09:13, hora de Costa Rica.');

// ── 8. a Max redeploy in the middle of an outage does not repeat the alert
const restored = ax.restoreInboxWatchState([{ action: 'stale_alert', metadata: { beat_key: new Date(B).toISOString() } }]);
check('8a  restored state', restored, { alertedKey: new Date(B).toISOString() });
check('8b  same outage after the boot: silent', run([[cr(20, '13:00'), B]], restored).out[0], []);
check('8c  recovered while Max was down: recovery line', run([[cr(20, '13:00'), cr(20, '12:43')]], restored).out[0].length, 1);
check('8d  last row is a recovery: clean', ax.restoreInboxWatchState([{ action: 'recovered', metadata: { beat_key: 'x' } }]), { alertedKey: null });
check('8e  no rows: clean', [ax.restoreInboxWatchState([]), ax.restoreInboxWatchState(null)], [{ alertedKey: null }, { alertedKey: null }]);
check('8f  a row with no metadata: clean', ax.restoreInboxWatchState([{ action: 'stale_alert' }]), { alertedKey: null });

// ── 9. the text the team reads (Spanish, usted, exact)
const alertText = outage.out[2][0];
check('9a  alert first line', alertText.split('\n')[0],
  '🔴 *La vigilancia del inbox de AXON dejó de correr*: no completa una lectura desde las 10:13, hora de Costa Rica (hace 77 min). Debe correr cada 30 minutos entre las 06:00 y las 20:59.');
check('9b  alert says what to check', [/node scripts\/prosp-web-session\.js/.test(alertText), /ng-fulfillment-AXON en Railway/.test(alertText), /PROSP_INBOX_WATCH/.test(alertText)], [true, true, true]);
check('9c  alert says it is sent once', /una sola vez por caída/.test(alertText), true);
check('9d  morning alert names yesterday\'s run', run([[cr(20, '07:20'), lastNight]]).out[0][0].split('\n')[0],
  '🔴 *La vigilancia del inbox de AXON dejó de correr*: hoy no ha completado ninguna lectura (la última fue el 19 nov a las 20:43, hora de Costa Rica). Debe correr cada 30 minutos entre las 06:00 y las 20:59.');
check('9e  recovery text', outage.out[9][0],
  '✅ *La vigilancia del inbox de AXON volvió a correr*: completó una lectura a las 12:13, hora de Costa Rica, después de 26 h sin correr.');
const everything = [alertText, never.out[0][0], outage.out[9][0], never.out[2][0], run([[cr(20, '07:20'), lastNight]]).out[0][0]];
check('9f  no em dash, en dash, undefined, NaN or null', everything.some((t) => t.includes(String.fromCharCode(0x2014)) || t.includes(String.fromCharCode(0x2013)) || /undefined|NaN|null/.test(t)), false);

if (failures) { console.error(`\n${failures} failure(s)`); process.exit(1); }
console.log('\nall inbox watch liveness checks passed');
