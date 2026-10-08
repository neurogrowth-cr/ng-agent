// Lead volume watchdog.  Run:  node test/lead-volume-watchdog.test.js
// Recipe: ~/automations/ops/recipes/lead-volume-watchdog.md
//
// lib/leadVolume.js is pure, so it is required directly. Fixtures are
// synthetic but shaped like the 2026-10-02/03 incident: Instagram intake went
// live on Oct 2 (about a dozen lead posts that day), then produced nothing from
// 6:32 PM CR Oct 2 until the fix at 3:56 PM CR Oct 3, while WhatsApp kept
// flowing. The real 30-day backtest is in the recipe; production rows are not
// committed here because this repo is public.
const fs   = require('fs');
const path = require('path');
const lv   = require('../lib/leadVolume');

let failures = 0;
function check(label, actual, expected) {
  const ok = JSON.stringify(actual) === JSON.stringify(expected);
  if (!ok) { failures++; console.error(`FAIL ${label}\n  expected: ${JSON.stringify(expected)}\n  actual:   ${JSON.stringify(actual)}`); }
  else console.log(`ok   ${label}`);
}
const H = 3600e3;
const utc = (s) => Date.parse(s);
const rowsOf = (channel, isoList) => isoList.map(t => ({ posted_at: t, channel }));
const at = (rows, iso) => lv.evaluateLeadVolume(rows.filter(r => Date.parse(r.posted_at) <= utc(iso)), utc(iso));
const find = (findings, c) => findings.find(f => f.channel === c);

// ── 1. Channel mapping (GHL medium + Max's mapped source label) ─────────────
check('instagram medium', lv.leadChannel({ source: 'Social media', medium: 'instagram' }), 'instagram');
check('paid instagram is still instagram', lv.leadChannel({ source: 'Paid Social', medium: 'instagram' }), 'instagram');
check('whatsapp medium', lv.leadChannel({ source: 'Social media', medium: 'whatsapp' }), 'whatsapp');
check('whatsapp_coex medium', lv.leadChannel({ source: 'Social media', medium: 'whatsapp_coex' }), 'whatsapp');
check('facebook medium + Facebook source = lead form', lv.leadChannel({ source: 'Facebook', medium: 'facebook' }), 'fb_form');
check('facebook medium + Social media source = Messenger', lv.leadChannel({ source: 'Social media', medium: 'facebook' }), 'messenger');
check('calendar booking is other', lv.leadChannel({ source: 'LinkedIn Flywheel - Appointment', medium: 'calendar' }), 'other');
check('no medium, Facebook source falls back to form', lv.leadChannel({ source: 'Facebook' }), 'fb_form');
check('no medium, Social media source is not guessed', lv.leadChannel({ source: 'Social media' }), 'other');
check('nothing at all', lv.leadChannel({}), null);

// ── 2. Waking hours: only 08:00 to midnight CR count ────────────────────────
check('8 PM CR to 8 AM CR next day = 4 waking hours', lv.awakeHoursBetween(utc('2026-10-03T02:00Z'), utc('2026-10-03T14:00Z')), 4);
check('midnight to 8 AM CR = 0 waking hours', lv.awakeHoursBetween(utc('2026-10-03T06:00Z'), utc('2026-10-03T14:00Z')), 0);

// ── 3. The incident replay ───────────────────────────────────────────────────
const igDay = ['2026-10-02T03:20Z', '2026-10-02T03:50Z', '2026-10-02T05:15Z', '2026-10-02T05:40Z',
  '2026-10-02T18:15Z', '2026-10-02T18:20Z', '2026-10-02T20:15Z', '2026-10-02T22:35Z',
  '2026-10-02T23:25Z', '2026-10-02T23:50Z', '2026-10-03T00:25Z', '2026-10-03T00:32Z'];
const ig = rowsOf('instagram', igDay);
// WhatsApp keeps flowing through the whole outage, every two hours.
const wa = [];
for (let t = utc('2026-09-26T14:00Z'); t <= utc('2026-10-03T22:00Z'); t += 2 * H) wa.push({ posted_at: new Date(t).toISOString(), channel: 'whatsapp' });
const incident = [...ig, ...wa];

check('7 AM CR Oct 3: not yet (night hours carry no evidence)', find(at(incident, '2026-10-03T13:00Z'), 'instagram').firing, false);
check('1 PM CR Oct 3: Instagram fires', find(at(incident, '2026-10-03T19:00Z'), 'instagram').firing, true);
check('the fire lands before the 3:56 PM CR fix', [13, 14, 15, 16, 17, 18, 19].map(h => find(at(incident, `2026-10-03T${String(h).padStart(2, '0')}:00Z`), 'instagram').firing).indexOf(true) >= 0, true);
check('WhatsApp flowing does not mask the Instagram silence', find(at(incident, '2026-10-03T19:00Z'), 'whatsapp').firing, false);
check('one episode, one key', lv.alertKey(find(at(incident, '2026-10-03T19:00Z'), 'instagram')), lv.alertKey(find(at(incident, '2026-10-03T21:00Z'), 'instagram')));

// ── 4. Normal quiet nights never fire ────────────────────────────────────────
// A steady Facebook form channel: 10 leads a day, 9 AM to 9 PM CR, for 7 days.
const steady = [];
for (let d = 0; d < 7; d++) for (let k = 0; k < 10; k++) steady.push({ posted_at: new Date(utc('2026-09-27T15:00Z') + d * 24 * H + k * 72 * 60e3).toISOString(), channel: 'fb_form' });
const lastSteady = Math.max(...steady.map(r => Date.parse(r.posted_at)));
for (const iso of ['2026-10-04T06:00Z', '2026-10-04T10:00Z', '2026-10-04T13:59Z']) {
  check(`steady channel overnight, ${lv.fmtCR(utc(iso))}: silent`, find(at(steady, iso), 'fb_form').firing, false);
}
check('the steady fixture is past warmup (so the night test is real)', find(at(steady, '2026-10-04T13:59Z'), 'fb_form').reason, 'within_normal');
check('but a steady channel silent into the next afternoon does fire', find(at(steady, '2026-10-04T20:00Z'), 'fb_form').firing, true);
// Structural: any silence that sits entirely between midnight and 8 AM CR has
// zero waking hours, so it can never reach the threshold, whatever the rate.
const busy = [];
for (let t = utc('2026-09-27T06:00Z'); t < utc('2026-10-04T06:00Z'); t += H) busy.push({ posted_at: new Date(t).toISOString(), channel: 'whatsapp' });
check('even a 24/7 channel: midnight to 7:59 AM CR never fires', find(at(busy, '2026-10-04T13:59Z'), 'whatsapp').firing, false);
check('last steady post is 7:48 PM CR', lv.fmtCR(lastSteady), 'Oct 3, 7:48 PM CR');

// ── 5. Warmup and bursts ────────────────────────────────────────────────────
const thin = rowsOf('messenger', ['2026-09-28T16:00Z', '2026-09-29T16:00Z', '2026-09-30T16:00Z', '2026-10-01T16:00Z', '2026-10-02T16:00Z', '2026-10-03T16:00Z']);
check('6 active hours in a week: warmup, never fires', find(at(thin, '2026-10-07T20:00Z'), 'messenger').reason, 'warmup');
const burst = [];
for (let i = 0; i < 30; i++) burst.push({ posted_at: new Date(utc('2026-10-01T17:00Z') + i * 60e3).toISOString(), channel: 'messenger' });
check('a 30-post sync burst counts as one active hour', find(at(burst, '2026-10-03T20:00Z'), 'messenger').activeHours, 1);
check('so a burst alone cannot arm the alarm', find(at(burst, '2026-10-03T20:00Z'), 'messenger').firing, false);
check('rows with no channel and no watched source are ignored', at([{ posted_at: '2026-10-01T17:00Z', channel: null, source: 'Social media' }], '2026-10-03T20:00Z').every(f => f.reason === 'no_history'), true);

// ── 5b. A post later marked personal still proves the pipe works ─────────
// 2026-10-07: Instagram carded leads all morning, the last real one at 12:33 PM
// CR, then a contact carded at 9:55 PM CR was tagged personal. The watchdog
// ignored that row and fired at 11:05 PM CR on a quiet evening.
const igWeek = [];
for (let d = 0; d < 7; d++) for (let k = 0; k < 10; k++) igWeek.push({ posted_at: new Date(utc('2026-09-30T14:00Z') + d * 24 * H + k * H).toISOString(), channel: 'instagram' });
igWeek.push({ posted_at: '2026-10-07T18:33Z', channel: 'instagram' });
const personalLate = { posted_at: '2026-10-08T03:55Z', channel: 'instagram', personal: true };
check('without the personal post, the quiet evening fires', find(at(igWeek, '2026-10-08T05:05Z'), 'instagram').firing, true);
const withPersonal = find(at([...igWeek, personalLate], '2026-10-08T05:05Z'), 'instagram');
check('a personal post ends the silence', withPersonal.firing, false);
check('silence counts from the personal post', lv.fmtCR(withPersonal.lastAt), 'Oct 7, 9:55 PM CR');
check('personal posts stay out of the rate', withPersonal.postsInWindow, find(at(igWeek, '2026-10-08T05:05Z'), 'instagram').postsInWindow);
check('personal posts alone are not history', find(at([personalLate], '2026-10-08T05:05Z'), 'instagram').reason, 'no_history');

// ── 6. Text contract (4a) ───────────────────────────────────────────────────
const fire = find(at(incident, '2026-10-03T19:00Z'), 'instagram');
const alert = lv.renderLeadVolumeAlert(fire);
const recovery = lv.renderLeadVolumeRecovery({ channel: 'instagram', lastAt: fire.lastAt, resumedAt: utc('2026-10-03T22:18Z') });
for (const [name, text] of [['alert', alert], ['recovery', recovery]]) {
  check(`${name}: no undefined/null/NaN/[object Object]`, /undefined|null|NaN|\[object Object\]/.test(text), false);
  check(`${name}: no em or en dash`, /[\u2013\u2014]/.test(text), false);
  check(`${name}: names the channel`, text.includes('Instagram DM'), true);
}
check('alert: verdict line', alert.startsWith('⚠️ *LEAD INTAKE SILENT*: Instagram DM'), true);
check('alert: last post time in CR', alert.includes('Oct 2, 6:32 PM CR'), true);
check('alert: names the workflow to check', alert.includes('Social DM Intake (on reply).'), true);
check('recovery: verdict line', recovery.startsWith('✅ *LEAD INTAKE BACK*: Instagram DM'), true);
check('every channel has a label and a check', Object.values(lv.CHANNELS).every(c => c.label && c.check), true);

// ── 7. Episode keys survive a round trip ────────────────────────────────────
const parsed = lv.parseAlertKey(lv.alertKey(fire));
check('alert key parses back', [parsed.channel, parsed.lastAt], ['instagram', fire.lastAt]);
check('a recovery key is not an alert key', lv.parseAlertKey(lv.recoveryKey('instagram', parsed.lastAtIso)), null);

// ── 8. Wiring in index.js ───────────────────────────────────────────────────
const SRC = fs.readFileSync(path.join(__dirname, '..', 'index.js'), 'utf8');
check('declared in STATIC_CRON_SCHEDULES', /runLeadVolumeWatchdog:\s*'5 \* \* \* \*'/.test(SRC), true);
check('registered with the same expression', SRC.includes("cron.schedule('5 * * * *', wrapCronJob('runLeadVolumeWatchdog'"), true);
check('every lead_posts write carries channel', (SRC.match(/channel: leadChannelKey,/g) || []).length, 3);
const runner = SRC.slice(SRC.indexOf('async function runLeadVolumeWatchdog'), SRC.indexOf('// ─── end lead volume watchdog'));
check('runner reads personal rows as liveness', /personal: !!r\.personal_excluded_at/.test(SRC.slice(SRC.indexOf('async function fetchLeadVolumeRows'), SRC.indexOf('async function runLeadVolumeWatchdog'))), true);
check('runner posts only to #ng-pm-agent', [...runner.matchAll(/channel: ([A-Z_]+)/g)].every(m => m[1] === 'AGENT_CHANNEL'), true);
check('no self-termination', /disable|cron\.stop|\.stop\(\)|process\.exit/.test(runner), false);

if (failures) { console.error(`\n${failures} failure(s)`); process.exit(1); }
console.log('\nall lead volume watchdog checks passed');
