// The portal's fake DEMO client must never reach a Max report, alert, count,
// learning or action (Ron, 2026-11-11).
//   Run:  node test/demo-client.test.js
//
// What has to hold:
//   - the shared helpers filter the way they say;
//   - every Kai SQL query that reads Kai's queue, log or workspaces carries a
//     demo guard (a new query without one fails here, not in front of Ron);
//   - every multi-client client_dashboards reader in index.js filters is_demo,
//     except the single-client lookups named below.
const fs = require('fs');
const path = require('path');
const { excludeDemo, isDemoRecord, notDemoDashboard, notDemoWorkspace, DEMO_PROMPT_NOTE } = require('../lib/demoClient');
const kaiHealth = require('../lib/kaiHealth');
const kaiEscalations = require('../lib/kaiEscalations');

let failures = 0;
function check(label, ok, detail = '') {
  if (!ok) { failures++; console.error(`FAIL ${label}${detail ? `\n  ${detail}` : ''}`); }
  else console.log(`ok   ${label}`);
}

// ── helpers ──
const calls = [];
const fakeQuery = { eq(col, val) { calls.push([col, val]); return this; } };
check('excludeDemo adds is_demo = false and chains', excludeDemo(fakeQuery) === fakeQuery && calls.length === 1 && calls[0][0] === 'is_demo' && calls[0][1] === false);
check('isDemoRecord true only for is_demo === true',
  isDemoRecord({ is_demo: true }) && !isDemoRecord({ is_demo: false }) && !isDemoRecord({}) && !isDemoRecord(null) && !isDemoRecord(undefined));
check('notDemoDashboard keeps rows with a null flag', notDemoDashboard('d') === 'COALESCE(d.is_demo, false) = false');
check('notDemoWorkspace keeps rows with no workspace', /NOT EXISTS \(SELECT 1 FROM workspaces demo_w WHERE demo_w\.id = q\.workspace_id AND demo_w\.is_demo\)/.test(notDemoWorkspace('q.workspace_id')));
check('prompt note names the flag on both tables', /client_dashboards\.is_demo/.test(DEMO_PROMPT_NOTE) && /workspaces\.is_demo/.test(DEMO_PROMPT_NOTE));
check('prompt note has no em dash', !DEMO_PROMPT_NOTE.includes('—'));

// ── Kai SQL ──
const guard = /is_demo/;
const touchesKai = /prosp_reply_queue|prosp_reply_log|\bworkspaces\b/;
const kaiQueries = {
  ...Object.fromEntries(Object.entries(kaiHealth.SQL).map(([k, v]) => [`kaiHealth.SQL.${k}`, v])),
  ...Object.fromEntries(Object.entries(kaiHealth.DIGEST_SQL).map(([k, v]) => [`kaiHealth.DIGEST_SQL.${k}`, v])),
  ...Object.fromEntries(Object.entries(kaiEscalations.SQL).map(([k, v]) => [`kaiEscalations.SQL.${k}`, v])),
};
for (const [name, sql] of Object.entries(kaiQueries)) {
  if (!touchesKai.test(sql)) continue; // prospEvents: any Prosp event at all, no client in it
  check(`${name} excludes the demo workspace`, guard.test(sql));
}

// ── index.js client_dashboards readers ──
// Single-client or by-email lookups, deliberately unfiltered.
const ALLOWED_UNFILTERED = [
  'loadActiveClientEmails',  // suppression set: keeping the demo email in it only holds more nudges
  'unlinkedEmails',          // looks up specific prospect emails
  'loadDemoClients',         // reads the demo itself
];
const src = fs.readFileSync(path.join(__dirname, '..', 'index.js'), 'utf8');
const lines = src.split('\n');
lines.forEach((line, i) => {
  if (!line.includes(".from('client_dashboards')")) return;
  const windowText = lines.slice(Math.max(0, i - 12), i + 8).join('\n');
  const allowed = ALLOWED_UNFILTERED.some(name => windowText.includes(name));
  const filtered = /is_demo|excludeDemo\(/.test(windowText);
  check(`index.js:${i + 1} client_dashboards reader filters the demo`, allowed || filtered, line.trim());
});
check('phase_3 reconciliation SQL filters the demo', /FROM client_dashboards d WHERE[^`]*notDemoDashboard\('d'\)/.test(src));
check('phase_3 webhook skips a demo record', /isPhase3 && !isDemoRecord\(record\)/.test(src) && /if \(isDemoRecord\(record\)\)/.test(src));
check('query_portal_db description carries the demo note', /name: 'query_portal_db',[^\n]*\$\{DEMO_PROMPT_NOTE\}/.test(src));
check('every v_phase0_fulfillment read drops the demo',
  (src.match(/\.from\('v_phase0_fulfillment'\)/g) || []).length === (src.match(/await withoutDemoClients\(raw(Data|Phase0|Phase0Gaps|StuckPhase0|Phase0All)\b/g) || []).length);

if (failures) { console.error(`\n${failures} failure(s)`); process.exit(1); }
console.log('\nall demo-client checks passed');
