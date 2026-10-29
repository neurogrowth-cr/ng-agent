// The Monday reflection brief's contract: deterministic sections built by code,
// LLM output parsed through a protocol and never posted raw, proposals one-tap
// with Ron-only approval, 7-day expiry and 30-day dismissal suppression. Run:
//   node test/weekly-brief.test.js
const fs   = require('fs');
const path = require('path');

const SRC = fs.readFileSync(process.argv[2] || path.join(__dirname, '..', 'index.js'), 'utf8');
const slugBlock = SRC.slice(SRC.indexOf('function slugifyReportName'), SRC.indexOf('function reportPostKey'));
const block = SRC.slice(
  SRC.indexOf('const PROPOSAL_ACTIONS'),
  SRC.indexOf('// ─── end weekly brief helpers'),
);
const g = new Function(`${slugBlock}\n${block}; return { parseProposalLine, selectProposals, proposalExpired, formatWeekNumbers, plainAdSweep, WEEK_NUMBERS_PLAIN, PROPOSAL_MAX_PER_WEEK, PROPOSAL_EXPIRY_DAYS, PROPOSAL_SUPPRESS_DAYS };`)();

let failures = 0;
function check(label, actual, expected) {
  if (JSON.stringify(actual) === JSON.stringify(expected)) { console.log(`  ok  ${label}`); }
  else { failures += 1; console.error(`FAIL  ${label}\n      expected ${JSON.stringify(expected)}\n      got      ${JSON.stringify(actual)}`); }
}

console.log('parseProposalLine — the protocol');
{
  const p = g.parseProposalLine('PROPOSAL | notion_task | Audit activation SOP :: Three clients stuck at config.');
  check('action parsed', p.action, 'notion_task');
  check('title is the pre-:: half', p.title, 'Audit activation SOP');
  check('slug derived for suppression matching', p.slug, 'audit-activation-sop');
  check('payload preserved whole', p.payload, 'Audit activation SOP :: Three clients stuck at config.');
}
check('scheduled_task and decision are legal actions',
  ['scheduled_task', 'decision'].map(a => g.parseProposalLine(`PROPOSAL | ${a} | T :: x`)?.action), ['scheduled_task', 'decision']);
check('an action outside the vocabulary is rejected at parse',
  g.parseProposalLine('PROPOSAL | send_email | Email all clients :: now'), null);
check('a titleless proposal is rejected', g.parseProposalLine('PROPOSAL | decision |  :: details'), null);
check('prose lines are not proposals', g.parseProposalLine('No patterns crossed threshold this week.'), null);
check('null does not throw', g.parseProposalLine(null), null);

console.log('\nselectProposals — cap and suppression');
const P = (t) => g.parseProposalLine(`PROPOSAL | notion_task | ${t} :: x`);
{
  const out = g.selectProposals([P('One'), P('Two'), P('Three'), P('Four')], new Set());
  check('capped at 3 per week', out.length, 3);
}
check('a dismissed proposal is suppressed by slug',
  g.selectProposals([P('Audit activation SOP')], new Set(['audit-activation-sop'])).length, 0);
check('duplicate titles collapse to one',
  g.selectProposals([P('Same idea'), P('Same idea')], new Set()).length, 1);
check('empty input → empty output', g.selectProposals([], new Set()), []);
check('nulls are skipped', g.selectProposals([null, P('Real')], new Set()).length, 1);

console.log('\nproposalExpired — the 7-day window');
const NOW = Date.parse('2026-08-31T12:00:00Z');
const tsDaysAgo = (d) => String((NOW - d * 86400000) / 1000);
check('6 days old → still live', g.proposalExpired(tsDaysAgo(6), NOW), false);
check('8 days old → expired', g.proposalExpired(tsDaysAgo(8), NOW), true);

console.log('\nformatWeekNumbers — plain language, honest about noise');
{
  const rows = [
    { name: 'roas_meta_7d', value: 3.2, mean: 2.0, stdDev: 0.5 },              // up, up is good → 🟢
    { name: 'close_rate_7d', value: 0.10, mean: 0.20, stdDev: 0.05 },          // down, up is good → 🔴
    { name: 'cpa_true_7d', value: 365.41, mean: 183.17, stdDev: 50 },          // up, down is good → 🔴
    { name: 'show_rate_7d', value: 0.85, mean: 0.84, stdDev: 0.10 },           // inside noise → ⚪
    { name: 'fulfillment_days_to_launch_avg', value: 21, mean: null, stdDev: null }, // no baseline yet
    { name: 'iclosed_calls_booked_yest', value: 0, mean: 0.64, stdDev: 0.5 },  // daily key → left out
  ];
  const out = g.formatWeekNumbers(rows);
  const line = (k) => out.split('\n').find(l => l.includes(k)) || '';
  check('grouped under plain section headers', ['*Marketing*', '*Sales*', '*Clients*'].every(h => out.includes(h)), true);
  check('a legend explains the dots', /🟢 better than usual · 🔴 worse than usual · ⚪ about normal/.test(out), true);
  check('good move up gets green', line('Sales for every $1').startsWith('🟢') && line('Sales for every $1').includes('*$3.20* (usually $2.00)'), true);
  check('bad move down gets red, rate shown as a percent', line('Calls that became a sale'), '🔴 Calls that became a sale: *10%* (usually 20%)');
  check('a cost going UP is red, money rounded to whole dollars', line('Ad cost per new client'), '🔴 Ad cost per new client: *$365* (usually $183)');
  check('wiggle inside half a sigma is normal, not a fake trend', line('Booked people who showed up').startsWith('⚪'), true);
  check('a metric still in warmup says so instead of faking a baseline', line('Days to launch'), '⚪ Days to launch a new client: *21 days* (too new to compare)');
  check('daily "yesterday" keys stay out of the weekly brief', /yesterday/i.test(out), false);
  check('no decimals on counts', /: \*\d+\.\d+\*/.test(g.formatWeekNumbers([{ name: 'leads_total_7d', value: 81, mean: 45.93, stdDev: 10 }])), false);
}
check('no data at all is stated, not faked', g.formatWeekNumbers([]).startsWith('No numbers yet'), true);
check('null rows are skipped', g.formatWeekNumbers([null, { name: 'leads_total_7d', value: 1, mean: 1, stdDev: 1 }]).includes('New leads: *1*'), true);
check('every plain metric exists in the registry (a renamed key would silently vanish)',
  g.WEEK_NUMBERS_PLAIN.every(m => SRC.includes(`name: '${m.name}'`)), true);

console.log('\nplainAdSweep — stored format translated at print time');
{
  const out = g.plainAdSweep('Form-funnel ad ranking, 7d. Account CPL $1.90.\nSCALE: Ad A — $1.10 CPL on $120 spend (2nd week running)\nKILL: Ad B — $4.50 CPL on $80 spend');
  check('account line in plain words', out.split('\n')[0], '• Average cost per lead: $1.90');
  check('scale line', out.split('\n')[1], '• 🟢 Spend more on "Ad A": $1.10 per lead, $120 spent, 2nd week running');
  check('kill line', out.split('\n')[2], '• 🔴 Think about pausing "Ad B": $4.50 per lead, $80 spent');
  check('no jargon left', /CPL|SCALE|KILL/.test(out), false);
}

console.log('\nwiring (source-level)');
check('the brief posts to the management channel',
  /MGMT_CHANNEL = process\.env\.MGMT_CHANNEL \|\| '#ng-ops-management'/.test(SRC), true);
check('the cron is in the liveness registry',
  /runWeeklyReflectionBrief:\s+'0 8 \* \* 1'/.test(SRC), true);
check('numbers/actions/watching are built by code — the LLM writes only patterns and proposals',
  SRC.indexOf('formatWeekNumbers(numberRows)') > 0 &&
  /const actionsBlock = autoActions\.length/.test(SRC), true);
check('LLM failure degrades to an honest line + Ron DM, never garbage to the channel',
  /Pattern generation failed this week[\s\S]{0,300}numbers above are live/.test(SRC), true);
check('proposals carry Slack metadata for restart-safe one-tap',
  /event_type: 'weekly_proposal', event_payload/.test(SRC), true);
check('ONLY Ron executes — other reactions are ignored in the handler',
  /event\.user !== RON_SLACK_ID[\s\S]{0,120}Ron-only/.test(SRC), true);
check('a non-pending proposal cannot double-execute',
  /row\.status !== 'pending'[\s\S]{0,120}no action taken/.test(SRC), true);
check('dismissal is remembered for the suppression window',
  /status: 'dismissed'[\s\S]{0,400}PROPOSAL_SUPPRESS_DAYS/.test(SRC), true);
check('an empty week still posts (dead-cron distinguishability)',
  /None this week\./.test(SRC), true);

console.log('');
if (failures) { console.error(`${failures} test(s) failed.`); process.exit(1); }
console.log('All weekly-brief tests passed.');
