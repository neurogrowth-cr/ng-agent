// Rules test for the Prosp reader liveness check.  Run:  node test/axon-prosp-reader-watch.test.js
//
// AXON's Prosp reader cron (minutes 5, 15, 25, 35, 45, 55 of every hour, every
// day) stamps axon.engine_heartbeat id 'cron:prosp_web_reader' at the end of
// every run, in shadow and in live. lib/axonHealth.js decides, from that one
// timestamp, when the cron has stopped: 40 minutes with no run, at any hour.
// One alert per outage, a recovery when a newer run appears.
const fs = require('fs');
const path = require('path');
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
const at = (now, beatMs) => ax.classifyProspReader({ data: beatMs == null ? [] : row(beatMs), now });

// ── 1. the id is a contract with ng-axon; the live-only row is never watched
check('1a  beat id', ax.PROSP_READER_BEAT_ID, 'cron:prosp_web_reader');
check('1b  threshold is 40 minutes', ax.PROSP_READER_STALE_MS, 40 * MIN);

// ── 2. classify: the same rule at every hour
check('2a  run 5 min ago is ok', at(cr(23, '10:00'), cr(23, '09:55')).kind, 'ok');
check('2b  three missed runs (35 min) is ok', at(cr(23, '10:30'), cr(23, '09:55')).kind, 'ok');
check('2c  39 min is ok', at(cr(23, '10:34'), cr(23, '09:55')).kind, 'ok');
check('2d  40 min is stale', at(cr(23, '10:35'), cr(23, '09:55')).kind, 'stale');
check('2e  03:00 on a Sunday, 45 min old: stale (no quiet hours)', at(cr(22, '03:00'), cr(22, '02:15')).kind, 'stale');
check('2f  03:00 on a Sunday, 5 min old: ok', at(cr(22, '03:00'), cr(22, '02:55')).kind, 'ok');
check('2g  no row is stale', at(cr(23, '12:00'), null).kind, 'stale');
check('2h  unreadable timestamp counts as no run', ax.classifyProspReader({ data: [{ beat_at: 'not a date' }], now: cr(23, '12:00') }), { kind: 'stale', beatAt: null, beatMs: null, now: cr(23, '12:00') });
check('2i  read error is blind', ax.classifyProspReader({ error: { message: 'upstream request timeout' }, now: cr(23, '12:00') }).kind, 'blind');
check('2j  throw is blind', ax.classifyProspReader({ thrown: new Error('fetch failed'), now: cr(23, '12:00') }).kind, 'blind');

// ── 3. one alert per outage, recovery clears it
function run(polls, start = ax.newInboxWatchState()) {
  let state = start;
  const out = [];
  const events = [];
  for (const [now, beat] of polls) {
    const step = ax.stepProspReaderWatch(state, at(now, beat));
    state = step.state;
    out.push(step.posts);
    events.push(step.event);
  }
  return { state, out, events };
}
const B = cr(23, '13:55');
const outage = run([
  [cr(23, '14:00'), B], [cr(23, '14:30'), B],             // fine: 5 and 35 min
  [cr(23, '14:40'), B],                                   // 45 min: alert
  [cr(23, '14:50'), B], [cr(23, '20:00'), B], [cr(24, '03:00'), B], // same outage: silent, night included
  [cr(24, '08:10'), cr(24, '08:05')],                     // a run completed: recovery
  [cr(24, '08:20'), cr(24, '08:15')],                     // back to normal: silent
]);
check('3a  posts per poll', outage.out.map((p) => p.length), [0, 0, 1, 0, 0, 0, 1, 0]);
check('3b  alert event carries the last run', outage.events[2], { action: 'stale_alert', beat_key: new Date(B).toISOString() });
check('3c  recovery event', outage.events[6], { action: 'recovered', beat_key: new Date(cr(24, '08:05')).toISOString() });
check('3d  state is clean after recovery', outage.state, { alertedKey: null });
check('3e  a second outage alerts again', run([[cr(24, '09:00'), cr(24, '08:15')]], outage.state).out[0].length, 1);

// ── 4. a failed read decides nothing and keeps the state
const alerted = run([[cr(23, '14:40'), B]]).state;
const blindStep = ax.stepProspReaderWatch(alerted, ax.classifyProspReader({ error: { message: 'x' }, now: cr(23, '14:50') }));
check('4a  blind: no post, state kept', [blindStep.posts.length, blindStep.state], [0, alerted]);
check('4b  blind with a clean state: no alert', ax.stepProspReaderWatch(ax.newInboxWatchState(), { kind: 'blind' }).posts, []);

// ── 5. never beaten (row missing): alert once, recover on the first run
const never = run([[cr(23, '09:00'), null], [cr(23, '09:10'), null], [cr(23, '09:20'), cr(23, '09:15')]]);
check('5a  alert once, then recovery', never.out.map((p) => p.length), [1, 0, 1]);
check('5b  never key', never.events[0], { action: 'stale_alert', beat_key: 'never' });
check('5c  never alert text', /nunca ha registrado una corrida/.test(never.out[0][0]), true);
check('5d  recovery after never has no span', never.out[2][0],
  '✅ *El lector de Prosp de AXON volvió a correr*: corrió a las 09:15, hora de Costa Rica.');

// ── 6. a Max redeploy in the middle of an outage does not repeat the alert
const restored = ax.restoreInboxWatchState([{ action: 'stale_alert', metadata: { beat_key: new Date(B).toISOString() } }]);
check('6a  same outage after the boot: silent', run([[cr(23, '16:00'), B]], restored).out[0], []);
check('6b  recovered while Max was down: recovery line', run([[cr(23, '16:00'), cr(23, '15:55')]], restored).out[0].length, 1);

// ── 7. the text Ron reads (Spanish, usted, exact)
const alertText = outage.out[2][0];
check('7a  alert first line', alertText.split('\n')[0],
  '🔴 *El lector de Prosp de AXON dejó de correr*: no corre desde las 13:55, hora de Costa Rica (hace 45 min). Debe correr cada 10 minutos, a toda hora.');
check('7b  alert says what it means', alertText.split('\n')[1],
  'Mientras no corra, AXON no lee las campañas en Prosp y el QA no se hace: ni antes del lanzamiento, ni en el lanzamiento, ni en la revisión diaria, ni cuando alguien escribe `@AXON QA` con el nombre del cliente.');
check('7c  alert says what to check, Railway first', [/\(1\) primero el servicio ng-fulfillment-AXON en Railway/.test(alertText), /PROSP_WEB_READER/.test(alertText)], [true, true]);
check('7d  alert says it is sent once', /una sola vez por caída/.test(alertText), true);
const overMidnight = run([[cr(24, '00:30'), cr(23, '23:45')]]).out[0][0];
check('7e  a run from yesterday is named with its day', overMidnight.split('\n')[0],
  '🔴 *El lector de Prosp de AXON dejó de correr*: no corre desde el 23 nov a las 23:45, hora de Costa Rica (hace 45 min). Debe correr cada 10 minutos, a toda hora.');
check('7f  recovery text', outage.out[6][0],
  '✅ *El lector de Prosp de AXON volvió a correr*: corrió a las 08:05, hora de Costa Rica, después de 18 h 10 min sin correr.');
const everything = [alertText, never.out[0][0], overMidnight, outage.out[6][0], never.out[2][0]];
check('7g  no em dash, en dash, undefined, NaN or null', everything.some((t) => t.includes(String.fromCharCode(0x2014)) || t.includes(String.fromCharCode(0x2013)) || /undefined|NaN|null/.test(t)), false);
check('7h  no angle brackets (Slack reads them as links)', everything.some((t) => /[<>]/.test(t)), false);

// ── 8. index.js runs it: in the 10-minute AXON poll, on the right row
const SRC = fs.readFileSync(path.join(__dirname, '..', 'index.js'), 'utf8');
const poll = SRC.slice(SRC.indexOf("wrapCronJob('checkAxonHealth'"), SRC.indexOf('Registered static cron: AXON heartbeat watch'));
check('8a  the poll calls all three checks', ['checkAxonHealth()', 'checkAxonInboxWatch()', 'checkAxonProspReader()'].map((c) => poll.includes(c)), [true, true, true]);
check('8b  the reader check reads the reader row', /function checkAxonProspReader\(\) \{[\s\S]{0,300}beatId: axonHealthLib\.PROSP_READER_BEAT_ID, classify: axonHealthLib\.classifyProspReader, step: axonHealthLib\.stepProspReaderWatch/.test(SRC), true);
check('8c  the live-only row is not read anywhere', SRC.includes('prosp_web_reader_live'), false);

// ── 9. the glue in index.js, run for real with stubs (it is shared with the inbox watch)
const glue = SRC.slice(SRC.indexOf('const axonCronWatchState = {};'), SRC.indexOf('async function checkMakeScenarioHealth('));
function harness({ beats, lastActivity = {}, env = {} }) {
  const seen = { posts: [], activity: [], restoredFor: [], rowsRead: [] };
  const query = (resolve) => { const q = { select: () => q, eq: (col, v) => { q.v = v; return q; }, order: () => q, limit: () => Promise.resolve(resolve(q.v)) }; return q; };
  const deps = {
    axonHealthLib: ax,
    axonSupabase: { from: () => query((id) => { seen.rowsRead.push(id); if (beats[id] === 'throw') throw new Error('fetch failed'); return { data: beats[id] == null ? [] : row(beats[id]), error: null }; }) },
    portalSupabase: { from: () => query((type) => { seen.restoredFor.push(type); return { data: lastActivity[type] || [], error: null }; }) },
    slack: { client: { chat: { postMessage: async (m) => { seen.posts.push(m); } } } },
    logActivity: (a) => { seen.activity.push(a); },
    AXON_OPS_CHANNEL: 'C_AXON', RON_SLACK_ID: 'U_RON',
    process: { env }, console: { warn() {}, error() {} },
  };
  const fns = new Function(...Object.keys(deps), `${glue}; return { checkAxonInboxWatch, checkAxonProspReader };`)(...Object.values(deps));
  return { seen, ...fns };
}
(async () => {
  const now = Date.now();
  const dead = harness({ beats: { 'cron:prosp_web_reader': now - 50 * MIN, 'cron:inbox_watch': now - 5 * MIN } });
  await dead.checkAxonProspReader();
  await dead.checkAxonProspReader();
  check('9a  stale reader: one alert, to AXON\'s channel and to Ron, not repeated', dead.seen.posts.map((m) => m.channel), ['C_AXON', 'U_RON']);
  check('9b  it is the reader alert', /El lector de Prosp de AXON dejó de correr/.test(dead.seen.posts[0].text), true);
  check('9c  recorded under its own event type', dead.seen.activity.map((a) => [a.event_type, a.action]), [['axon_prosp_reader_watch', 'stale_alert']]);
  check('9d  reads the reader row, restores from its own rows, once', [dead.seen.rowsRead, dead.seen.restoredFor], [['cron:prosp_web_reader', 'cron:prosp_web_reader'], ['axon_prosp_reader_watch']]);
  await dead.checkAxonInboxWatch();
  check('9e  the inbox watch keeps its own row, event type and state', [dead.seen.rowsRead[2], dead.seen.restoredFor[1], dead.seen.activity.length], ['cron:inbox_watch', 'axon_inbox_watch', 1]);

  const fresh = harness({ beats: { 'cron:prosp_web_reader': now - 5 * MIN } });
  await fresh.checkAxonProspReader();
  check('9f  fresh reader: nothing posted, nothing recorded', [fresh.seen.posts, fresh.seen.activity], [[], []]);

  const beatKey = new Date(now - 50 * MIN).toISOString();
  const rebooted = harness({ beats: { 'cron:prosp_web_reader': now - 50 * MIN }, lastActivity: { axon_prosp_reader_watch: [{ action: 'stale_alert', metadata: { beat_key: beatKey } }] } });
  await rebooted.checkAxonProspReader();
  check('9g  Max redeployed mid outage: silent', rebooted.seen.posts, []);

  const off = harness({ beats: { 'cron:prosp_web_reader': now - 50 * MIN }, env: { AXON_PROSP_READER_CHECK: 'off' } });
  await off.checkAxonProspReader();
  check('9h  kill switch: reads nothing, posts nothing', [off.seen.rowsRead, off.seen.posts], [[], []]);

  const blind = harness({ beats: { 'cron:prosp_web_reader': 'throw' } });
  await blind.checkAxonProspReader();
  check('9i  a failed read posts nothing', blind.seen.posts, []);

  if (failures) { console.error(`\n${failures} failure(s)`); process.exit(1); }
  console.log('\nall Prosp reader liveness checks passed');
})().catch((e) => { console.error(e); process.exit(1); });
