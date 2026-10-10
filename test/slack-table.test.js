// Rules test for the aligned Slack tables.  Run:  node test/slack-table.test.js
const st = require('../lib/slackTable');

let failures = 0;
function check(label, actual, expected) {
  const ok = JSON.stringify(actual) === JSON.stringify(expected);
  if (!ok) { failures++; console.error(`FAIL ${label}\n  expected: ${JSON.stringify(expected)}\n  actual:   ${JSON.stringify(actual)}`); }
  else console.log(`ok   ${label}`);
}

check('table pads columns: first two left, the rest right', st.table(['Día', 'Reel', 'Alcance'], [['Mar', 'Uno', '4,764'], ['Vie', 'Más largo', '9']]).split('\n'),
  ['```', 'Día  Reel       Alcance', 'Mar  Uno          4,764', 'Vie  Más largo        9', '```']);
check('explicit alignment and no header row', st.table(null, [['8:00 am', 'A'], ['12:00 pm', 'B']], ['r', 'l']).split('\n'),
  ['```', ' 8:00 am  A', '12:00 pm  B', '```']);
check('accented characters count as one column', st.width('acción…'), 7);
check('trailing spaces are trimmed', st.table(null, [['a', ''], ['bb', 'x']]).split('\n')[1], 'a');
check('empty table is empty', st.table(null, []), '');
check('cell cuts with an ellipsis', st.cell('Sales Navigator es el pilar número uno', 20), 'Sales Navigator es…');
check('cell keeps short text', st.cell('Corto', 20), 'Corto');
check('cell strips backticks so the block cannot close early', st.cell('a ``` b'), "a ''' b");
check('cell collapses newlines', st.cell('uno\n\ndos'), 'uno dos');

if (failures) { console.error(`\n${failures} failure(s)`); process.exit(1); }
console.log('\nall slack-table checks passed');
