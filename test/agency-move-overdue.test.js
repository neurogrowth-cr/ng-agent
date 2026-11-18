// Rules test for the agency Prosp waiting DM (dash prosp-key-check → Valeria).
// Run:  node test/agency-move-overdue.test.js
const am = require('../lib/agencyMoveOverdue');

let failures = 0;
function check(label, actual, expected) {
  const ok = JSON.stringify(actual) === JSON.stringify(expected);
  if (!ok) { failures++; console.error(`FAIL ${label}\n  expected: ${JSON.stringify(expected)}\n  actual:   ${JSON.stringify(actual)}`); }
  else console.log(`ok   ${label}`);
}

const OK = { day: '2026-11-18', mode: 'live', clients: [{ name: 'Late <Patek>', rung: 'patek', daysSinceCall: 5 }, { name: 'One Day', rung: 'ROLEX', daysSinceCall: 1 }] };

const r = am.checkPayload(OK);
check('accepts a valid payload', r.ok, true);
check('upper-cases the rung', r.payload.clients[0].rung, 'PATEK');
check('keeps live mode', r.payload.mode, 'live');
check('missing mode is a dry run', am.checkPayload({ ...OK, mode: undefined }).payload.mode, 'dry_run');
check('refuses an empty list', am.checkPayload({ ...OK, clients: [] }).ok, false);
check('refuses a bad day', am.checkPayload({ ...OK, day: 'today' }).ok, false);
check('refuses a bad count', am.checkPayload({ ...OK, clients: [{ name: 'X', rung: 'ROLEX', daysSinceCall: -1 }] }).ok, false);
check('refuses a rung with spaces', am.checkPayload({ ...OK, clients: [{ name: 'X', rung: 'ROLEX <x>', daysSinceCall: 3 }] }).ok, false);
check('refuses too many clients', am.checkPayload({ ...OK, clients: Array.from({ length: am.MAX_CLIENTS + 1 }, () => OK.clients[1]) }).ok, false);

const text = am.formatDm(r.payload);
check('escapes client names', text.includes('*Late &lt;Patek&gt;* (PATEK): 5 días desde la llamada de activación'), true);
check('singular day', text.includes('*One Day* (ROLEX): 1 día desde'), true);
check('plural header', text.startsWith('🔌 2 clientes ROLEX o PATEK siguen'), true);
check('singular header', am.formatDm({ clients: [OK.clients[1]] }).startsWith('🔌 Un cliente ROLEX o PATEK sigue'), true);
check('no em dashes', /—|–/.test(text), false);

if (failures) { console.error(`\n${failures} failing`); process.exit(1); }
console.log('\nall passing');
