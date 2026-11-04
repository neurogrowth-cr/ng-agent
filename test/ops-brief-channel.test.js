// Ron, 2026-11-03: the 09:00 ops brief and the Monday 15:30 WBR reminder that
// used to DM Josué (tech_ops, leaves 2026-12-01) post to #ng-marktech-internal
// for now, through one constant (OPS_BRIEF_CHANNEL) so the target moves in one
// place. Source-level guard, like the other tests: a stale copy of index.js that
// brings the DM loop back fails here before it reaches main.
const fs   = require('fs');
const path = require('path');

const SRC = fs.readFileSync(process.argv[2] || path.join(__dirname, '..', 'index.js'), 'utf8');

let failures = 0;
function check(name, ok, detail) {
  if (ok) { console.log(`  ok  ${name}`); }
  else { failures += 1; console.error(`FAIL  ${name}${detail ? `\n      ${detail}` : ''}`); }
}

console.log('OPS_BRIEF_CHANNEL');
check('1a defined once with an env override and the #ng-marktech-internal id as default',
  (SRC.match(/^const OPS_BRIEF_CHANNEL\s*=\s*process\.env\.OPS_BRIEF_CHANNEL\s*\|\|\s*'C0B48H7NT41';$/m) || []).length === 1);

const standup = SRC.slice(SRC.indexOf('async function runFulfillmentStandup('), SRC.indexOf('// ── DM fulfillment role'));
check('1b runFulfillmentStandup slice found', standup.length > 500, `slice length ${standup.length}`);
check('1c the ops brief posts to OPS_BRIEF_CHANNEL',
  /postMessage\(\{ channel: OPS_BRIEF_CHANNEL, text: josueLines\.join/.test(standup));
check('1d the ops brief no longer DMs the tech_ops role',
  !/slackIdsByRole\('tech_ops'\)/.test(standup) && !/U08ABBFNGUW/.test(standup));
check('1e the day-over-day snapshot key is unchanged (delta did not reset)',
  /getYesterdayStandupSnapshot\('josue'\)/.test(standup) && /saveStandupSnapshot\('josue'/.test(standup));
check('1f the greeting no longer addresses one person',
  /Good morning team!/.test(standup) && !/Good morning Josue/.test(standup));

console.log('WBR reminder');
check('2a WBR_REMINDER_TO is the ops brief channel',
  /^const WBR_REMINDER_TO = OPS_BRIEF_CHANNEL;$/m.test(SRC));
check('2b the reminder cron still fires Monday 15:30 CR',
  /cron\.schedule\('30 15 \* \* 1', wrapCronJob\('runCampaignHealthWbrReminder'/.test(SRC));

if (failures) { console.error(`\n${failures} failure(s)`); process.exit(1); }
console.log('\nall ok');
