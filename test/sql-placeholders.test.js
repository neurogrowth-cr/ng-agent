// Static check: every inline SQL query passes exactly as many values as it
// has placeholders.
//   Run:  node test/sql-placeholders.test.js
//
// Built 2026-09-20. promoteOpenDealOutcome shipped with five placeholders and
// a four-value array (`source` was missing), so Postgres rejected every
// open-deal promotion for three weeks: "bind message supplies 4 parameters,
// but prepared statement requires 5". 50 cards issued, zero resolved. No test
// ran that query, and none could without a database, so this one reads the
// source instead: for each `.query(<sql literal>, [<inline array>])` call the
// highest $N must equal the number of array elements.
//
// Calls whose values are not an inline array (a variable, a spread) cannot be
// counted statically and are skipped, and reported as skipped.
const fs = require('fs');
const path = require('path');

const FILE = process.argv[2] || path.join(__dirname, '..', 'index.js');
const SRC = fs.readFileSync(FILE, 'utf8');

let failures = 0;
function check(label, actual, expected) {
  const ok = JSON.stringify(actual) === JSON.stringify(expected);
  if (!ok) { failures++; console.error(`FAIL ${label}\n  expected: ${JSON.stringify(expected)}\n  actual:   ${JSON.stringify(actual)}`); }
  else console.log(`ok   ${label}`);
}

const lineOf = (i) => SRC.slice(0, i).split('\n').length;
// Skips whitespace AND comments: the promotion query carries a comment between
// its SQL and its values, and a scanner that stopped there skipped the one
// query this test exists for.
const skipWs = (i) => {
  for (;;) {
    while (i < SRC.length && /\s/.test(SRC[i])) i++;
    if (SRC[i] === '/' && SRC[i + 1] === '/') { while (i < SRC.length && SRC[i] !== '\n') i++; continue; }
    if (SRC[i] === '/' && SRC[i + 1] === '*') { i = SRC.indexOf('*/', i + 2); i = i < 0 ? SRC.length : i + 2; continue; }
    return i;
  }
};

// End index (exclusive) of the string or template literal starting at i.
function endOfLiteral(i) {
  const q = SRC[i];
  let j = i + 1;
  while (j < SRC.length) {
    const c = SRC[j];
    if (c === '\\') { j += 2; continue; }
    if (q === '`' && c === '$' && SRC[j + 1] === '{') { j = endOfBraces(j + 1); continue; }
    if (c === q) return j + 1;
    j++;
  }
  return -1;
}

// End index (exclusive) of the bracketed group opening at i, literals skipped.
function endOfBraces(i) {
  const open = SRC[i];
  const close = { '(': ')', '[': ']', '{': '}' }[open];
  let depth = 0;
  let j = i;
  while (j < SRC.length) {
    const c = SRC[j];
    if (c === '"' || c === "'" || c === '`') { j = endOfLiteral(j); if (j < 0) return -1; continue; }
    if (c === '/' && SRC[j + 1] === '/') { while (j < SRC.length && SRC[j] !== '\n') j++; continue; }
    if (c === '(' || c === '[' || c === '{') depth++;
    if (c === ')' || c === ']' || c === '}') { depth--; if (depth === 0 && c === close) return j + 1; }
    j++;
  }
  return -1;
}

// Top-level elements of the array literal spanning [start, end).
function arrayElements(start, end) {
  const out = [];
  let depth = 0;
  let cur = '';
  let j = start + 1;
  const stop = end - 1;
  while (j < stop) {
    const c = SRC[j];
    if (c === '"' || c === "'" || c === '`') { const e = endOfLiteral(j); cur += SRC.slice(j, e); j = e; continue; }
    if (c === '/' && SRC[j + 1] === '/') { while (j < stop && SRC[j] !== '\n') j++; continue; }
    if (c === '(' || c === '[' || c === '{') depth++;
    if (c === ')' || c === ']' || c === '}') depth--;
    if (c === ',' && depth === 0) { out.push(cur.trim()); cur = ''; j++; continue; }
    cur += c;
    j++;
  }
  if (cur.trim()) out.push(cur.trim());
  return out;
}

const checked = [];
const skipped = [];
const mismatches = [];
const re = /\.query\(/g;
let m;
while ((m = re.exec(SRC))) {
  let i = skipWs(m.index + m[0].length);
  if (!['`', "'", '"'].includes(SRC[i])) continue; // SQL held in a variable, nothing to read
  const sqlEnd = endOfLiteral(i);
  if (sqlEnd < 0) continue;
  const sql = SRC.slice(i, sqlEnd);
  const nums = [...sql.matchAll(/\$(\d+)/g)].map(x => Number(x[1]));
  const maxPlaceholder = nums.length ? Math.max(...nums) : 0;
  const line = lineOf(m.index);

  i = skipWs(sqlEnd);
  let count;
  if (SRC[i] === ')') {
    count = 0;
  } else if (SRC[i] === ',') {
    i = skipWs(i + 1);
    if (SRC[i] !== '[') { skipped.push(line); continue; }
    const arrEnd = endOfBraces(i);
    if (arrEnd < 0) { skipped.push(line); continue; }
    const els = arrayElements(i, arrEnd);
    if (els.some(e => e.startsWith('...'))) { skipped.push(line); continue; }
    count = els.length;
  } else {
    skipped.push(line);
    continue;
  }
  checked.push({ line, maxPlaceholder, count });
  if (maxPlaceholder !== count) {
    mismatches.push(`line ${line}: highest placeholder $${maxPlaceholder}, ${count} value(s) passed`);
  }
}

console.log(`scanned ${checked.length} inline queries, skipped ${skipped.length} with non-literal values`);
check('1 every inline query passes as many values as it has placeholders', mismatches, []);

// A scanner that matches nothing passes forever. Pin the floor and the one
// query this test exists for.
// 29 inline queries on 2026-09-20. The floor sits below that so deleting a
// query does not fail CI, and far above zero so a broken scanner does.
check('2a the scan actually covers the file (at least 20 inline queries)', checked.length >= 20, true);
const promoStart = SRC.indexOf('async function promoteOpenDealOutcome');
const promoEnd = SRC.indexOf('// One snooze: push snoozeUntil out', promoStart);
const promoLines = [lineOf(promoStart), lineOf(promoEnd)];
const promoUpdate = checked.find(c => c.line >= promoLines[0] && c.line <= promoLines[1] && c.maxPlaceholder === 5);
check('2b the open-deal promotion UPDATE is one of the scanned queries', !!promoUpdate, true);
check('2c it passes five values for its five placeholders', promoUpdate && promoUpdate.count, 5);

if (failures) { console.error(`\n${failures} failure(s)`); process.exit(1); }
console.log('\nAll sql-placeholders tests passed.');
