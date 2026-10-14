// Content Bridge 24h setter card.  Run:  node test/bridge-reminder.test.js
// lib/bridgeReminder.js is pure, so it is required directly. Names are made up
// because this repo is public.
const fs = require('fs');
const path = require('path');
const br = require('../lib/bridgeReminder');

let failures = 0;
function check(label, actual, expected) {
  const ok = JSON.stringify(actual) === JSON.stringify(expected);
  if (!ok) { failures++; console.error(`FAIL ${label}\n  expected: ${JSON.stringify(expected)}\n  actual:   ${JSON.stringify(actual)}`); }
  else console.log(`ok   ${label}`);
}
const H = 3600e3;
// CR wall clock -> epoch ms (UTC-6, no DST).
const cr = (iso) => Date.parse(`${iso}-06:00`);

// ── 1. CR dates and times ───────────────────────────────────────────────────
check('1a CR date of 11:30 PM CR stays that day', br.crDateKey(cr('2026-10-14T23:30:00')), '2026-10-14');
check('1b CR date of 00:30 CR is the next day', br.crDateKey(cr('2026-10-15T00:30:00')), '2026-10-15');
check('1c tomorrow key', br.crTomorrowKey(cr('2026-10-14T20:15:00')), '2026-10-15');
check('1d morning time', br.crTimeEs(cr('2026-10-15T10:00:00')), '10:00 a. m.');
check('1e afternoon time', br.crTimeEs(cr('2026-10-15T15:30:00')), '3:30 p. m.');
check('1f noon is p. m.', br.crTimeEs(cr('2026-10-15T12:00:00')), '12:00 p. m.');
check('1g midnight is 12 a. m.', br.crTimeEs(cr('2026-10-15T00:05:00')), '12:05 a. m.');
check('1h long date in Spanish', br.crDateTimeEs(cr('2026-10-15T10:00:00')), 'jueves 15 de octubre, 10:00 a. m.');

// ── 2. First names ──────────────────────────────────────────────────────────
check('2a first word, title case', br.firstNameOf('MARIA fernanda lopez'), 'Maria');
check('2b accents kept', br.firstNameOf('álvaro Pérez'), 'Álvaro');
check('2c email is not a name', br.firstNameOf('lead@example.com'), '');
check('2d empty', br.firstNameOf(null), '');

// ── 3. Dedupe key ───────────────────────────────────────────────────────────
const start = cr('2026-10-15T10:00:00');
check('3a live key carries the CR call date', br.bridgeKey('A1', start, true), 'bridge-24h:A1:2026-10-15');
check('3b shadow key is separate', br.bridgeKey('A1', start, false), 'bridge-24h-shadow:A1:2026-10-15');
check('3c a reschedule to another day is a new key',
  br.bridgeKey('A1', cr('2026-10-20T10:00:00'), true) !== br.bridgeKey('A1', start, true), true);

// ── 4. Window selection ─────────────────────────────────────────────────────
const base = { startMs: start, cancelled: false, deleted: false, superseded: false, stamped: false, hasOwner: true, ownerSlackId: 'U1' };
const at = (iso, over = {}) => br.classifyBridgeCall({ ...base, ...over }, cr(iso));
check('4a 24h before, tomorrow, owned: due', at('2026-10-14T10:15:00'), 'due');
check('4b 8 AM run catches a 10 AM call (26h)', at('2026-10-14T08:15:00'), 'due');
check('4c 2 days out: not yet', at('2026-10-13T10:15:00'), 'not_yet');
check('4d tomorrow but 29h out: not yet',
  br.classifyBridgeCall({ ...base, startMs: cr('2026-10-15T23:00:00') }, cr('2026-10-14T18:00:00')), 'not_yet');
check('4e the 8 PM run covers a late-night call tomorrow',
  br.classifyBridgeCall({ ...base, startMs: cr('2026-10-15T23:30:00') }, cr('2026-10-14T20:15:00')), 'due');
check('4f already stamped', at('2026-10-14T11:15:00', { stamped: true }), 'already_sent');
check('4g same day, never carded: missed (divergence count)', at('2026-10-15T08:15:00'), 'missed_same_day');
check('4h same day, carded yesterday: already sent', at('2026-10-15T08:15:00', { stamped: true }), 'already_sent');
check('4i started', at('2026-10-15T10:30:00'), 'started');
check('4j cancelled', at('2026-10-14T10:15:00', { cancelled: true }), 'cancelled');
check('4k deleted beats cancelled', at('2026-10-14T10:15:00', { cancelled: true, deleted: true }), 'deleted');
check('4l superseded by a later booking', at('2026-10-14T10:15:00', { superseded: true }), 'superseded');
check('4m self-serve, nobody owns it', at('2026-10-14T10:15:00', { hasOwner: false, ownerSlackId: null }), 'self_serve');
check('4n owned by someone who is not an active setter', at('2026-10-14T10:15:00', { ownerSlackId: null }), 'not_active_setter');
check('4o bad date', at('2026-10-14T10:15:00', { startMs: NaN }), 'bad_date');
// Every call on tomorrow's date gets exactly one due run between 8 AM and 8 PM.
{
  let missed = 0;
  for (let h = 0; h < 24; h++) {
    const callMs = cr(`2026-10-15T${String(h).padStart(2, '0')}:45:00`);
    let dueRuns = 0;
    for (let run = 8; run <= 20; run++) {
      const v = br.classifyBridgeCall({ ...base, startMs: callMs, stamped: dueRuns > 0 }, cr(`2026-10-14T${String(run).padStart(2, '0')}:15:00`));
      if (v === 'due') dueRuns++;
    }
    if (dueRuns !== 1) missed++;
  }
  check('4p every hour of tomorrow is carded exactly once by the 8-20 hourly runs', missed, 0);
}

// ── 5. WhatsApp text (section d) ────────────────────────────────────────────
const wa = br.buildWhatsAppText({ firstName: 'Maria', startMs: start, videoUrl: '' });
check('5a matches section (d) with the placeholder', wa,
  'Maria, mañana nos vemos (10:00 a. m.). Si tiene 10 minutos hoy, vea esto antes, para que la llamada sea sobre sus números, no lo básico:\n(video C pendiente de grabar)\nY si decide con un socio, mándeselo también. Nos vemos mañana.');
check('5b real link replaces the placeholder',
  br.buildWhatsAppText({ firstName: 'Maria', startMs: start, videoUrl: 'https://youtu.be/xyz' }).split('\n')[1], 'https://youtu.be/xyz');
check('5c no name still reads right',
  br.buildWhatsAppText({ firstName: '', startMs: start, videoUrl: '' }).startsWith('Mañana nos vemos (10:00 a. m.). Si tiene'), true);

// ── 6. Card + contract ──────────────────────────────────────────────────────
const shadowCard = br.buildBridgeCard({ prospectName: 'Maria Lopez', startMs: start, closerName: 'Closer Uno', whatsappText: wa, ghlLink: 'https://app.example/c/1', shadowFor: 'Setter Uno', videoMissing: true });
check('6a shadow label first', shadowCard.split('\n')[0], '[SHADOW · iría a Setter Uno]');
check('6b header', shadowCard.split('\n')[1], '📹 *Víspera de llamada* · Maria Lopez · jueves 15 de octubre, 10:00 a. m. (hora CR)');
check('6c WhatsApp inside a code block', shadowCard.includes('```\n' + wa + '\n```'), true);
check('6d warns while video C is missing', shadowCard.includes('⚠️ El video C todavía no existe'), true);
check('6e valid card passes the contract', br.validateBridgeCard(shadowCard, { videoUrl: '' }), []);
const liveCard = br.buildBridgeCard({ prospectName: 'Maria Lopez', startMs: start, closerName: null, whatsappText: br.buildWhatsAppText({ firstName: 'Maria', startMs: start, videoUrl: 'https://youtu.be/xyz' }), ghlLink: null, shadowFor: null, videoMissing: false });
check('6f live card has no shadow label and no warning', [liveCard.includes('SHADOW'), liveCard.includes('⚠️')], [false, false]);
check('6g live card passes with the link', br.validateBridgeCard(liveCard, { videoUrl: 'https://youtu.be/xyz' }), []);
// The contract must be able to go red.
check('6h contract catches a missing link', br.validateBridgeCard(liveCard, { videoUrl: 'https://youtu.be/other' }), ['missing video link or placeholder']);
check('6i contract catches a leaked undefined', br.validateBridgeCard(shadowCard.replace('Maria Lopez', 'undefined'), { videoUrl: '' }), ['leaked empty value']);
check('6j contract catches an em dash', br.validateBridgeCard(shadowCard + ' \u2014', { videoUrl: '' }), ['em dash']);
check('6k no em dash anywhere in the lib', fs.readFileSync(path.join(__dirname, '../lib/bridgeReminder.js'), 'utf8').includes('\u2014'), false);

// ── 7. index.js wiring ──────────────────────────────────────────────────────
const SRC = fs.readFileSync(path.join(__dirname, '../index.js'), 'utf8');
const sched = /cron\.schedule\('([^']+)', wrapCronJob\('runBridge24hCards'/.exec(SRC);
const declared = /runBridge24hCards:\s*'([^']+)'/.exec(SRC);
check('7a cron registered and declared with the same expression', [!!sched, !!declared, sched && declared && sched[1] === declared[1]], [true, true, true]);
check('7b BRIDGE_24H_LIVE is a boolean code constant', /const BRIDGE_24H_LIVE = (true|false);/.test(SRC), true);
check('7c VIDEO_C_URL is a code constant', /const VIDEO_C_URL = '[^']*';/.test(SRC), true);
check('7d shadow posts go to Ron', /channel: live \? slackId : RON_SLACK_ID/.test(SRC), true);

if (failures) { console.error(`\n${failures} failure(s)`); process.exit(1); }
console.log('\nAll bridge reminder checks passed.');
