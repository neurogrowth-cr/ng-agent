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
const g = new Function(`${slugBlock}\n${block}; return { parseProposalLine, selectProposals, proposalExpired, formatWeekNumbers, PROPOSAL_MAX_PER_WEEK, PROPOSAL_EXPIRY_DAYS, PROPOSAL_SUPPRESS_DAYS };`)();

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

console.log('\nformatWeekNumbers — deterministic, honest about noise');
{
  const rows = [
    { label: 'ROAS', domain: 'marketing', value: 3.2, mean: 2.0, stdDev: 0.5 },   // > mean + 0.5σ → ▲
    { label: 'Close rate', domain: 'sales', value: 0.10, mean: 0.20, stdDev: 0.05 }, // < mean − 0.5σ → ▼
    { label: 'Show rate', domain: 'sales', value: 0.85, mean: 0.84, stdDev: 0.10 },  // inside noise → •
    { label: 'Days to launch', domain: 'fulfillment', value: 21, mean: null, stdDev: null }, // no baseline yet
  ];
  const out = g.formatWeekNumbers(rows);
  check('grouped under department headers',
    ['MARKETING', 'SALES', 'FULFILLMENT'].every(h => out.includes(h)), true);
  check('real drift up gets ▲', /ROAS: 3\.20 \(▲/.test(out), true);
  check('real drift down gets ▼', /Close rate: 0\.10 \(▼/.test(out), true);
  check('wiggle inside half a sigma gets a neutral dot, not a fake trend', /Show rate: 0\.85 \(•/.test(out), true);
  check('a metric still in warmup shows its value without a baseline claim',
    /Days to launch: 21\.00(?!.*baseline)/.test(out.split('\n').find(l => l.includes('Days to launch'))), true);
}
check('no data at all is stated, not faked',
  g.formatWeekNumbers([]).includes('No metrics have baselines yet'), true);
check('null rows are skipped', g.formatWeekNumbers([null, { label: 'X', domain: 'sales', value: 1, mean: 1, stdDev: 1 }]).includes('X: 1.00'), true);

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
