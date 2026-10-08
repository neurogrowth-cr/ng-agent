// Rules test for the GHL user id maps in index.js.
//   Run:  node test/ghl-user-id-maps.test.js [path/to/index.js]
//
// Jose Carranza's GHL id was spelled two ways for months: izLTA0jy5OrKyMvyItjV
// (correct, GET /users) and izLTA0jy5OrKyMvyltjV (lowercase L for capital I).
// The lead-claim map carried the typo, so a Slack reaction claim PUT a
// non-existent assignedTo into GHL, and GHL-side assignments to Jose resolved to
// a raw id instead of his name. Nothing errored. These checks make a re-typed or
// half-fixed id fail CI instead. The first run also caught Joseph Salazar's
// lowercase key spelled cuttpcov7... instead of cuttpgov7... (real id
// cUTTPGov7ZTLvyjKHdX8, as stored on his 21 setter_claims rows).
//
//   * Every GHL user id in the maps is 20 alphanumeric characters.
//   * One person, one id: across all maps a person never maps to two ids, and
//     each all-lowercase lookup key is exactly the lowercased real id.
const fs   = require('fs');
const path = require('path');

const SRC = fs.readFileSync(process.argv[2] || path.join(__dirname, '..', 'index.js'), 'utf8');
const GHL_ID_RE = /^[A-Za-z0-9]{20}$/;

// Slice `const NAME = { ... }` out of the source by brace matching and evaluate
// the object literal. The maps are plain string literals with comments.
function sliceMap(name) {
  const start = SRC.indexOf(`const ${name} = {`);
  if (start < 0) throw new Error(`could not find const ${name}`);
  let i = SRC.indexOf('{', start), depth = 0;
  for (; i < SRC.length; i++) {
    if (SRC[i] === '{') depth++;
    else if (SRC[i] === '}' && --depth === 0) break;
  }
  return new Function(`return (${SRC.slice(SRC.indexOf('{', start), i + 1)});`)();
}

let failures = 0;
function check(ok, msg) {
  if (ok) return;
  failures++;
  console.error(`FAIL: ${msg}`);
}

// A key is treated as a GHL id when it is not an email, has no spaces and is
// longer than any first name used as a lookup key.
const isIdKey = k => !k.includes('@') && !/\s/.test(k) && k.length > 12;
const person  = name => String(name).trim().split(/\s+/)[0].toLowerCase();

const NAME_MAPS  = ['SALES_TEAM_MAP', 'GHL_USER_NAMES', 'GHL_USERS', 'GHL_USERS_GAP', 'ghlUserNames']; // id -> name
const LABEL_MAPS = ['HOT_REPLY_SETTERS'];                             // id -> { slackId, label }
const SLACK_MAPS = ['CLOSER_SLACK', 'GHL_TO_SLACK'];                  // id -> Slack id
const ID_MAPS    = ['SLACK_TO_GHL_USER', 'EMAIL_TO_GHL_USER_ID'];     // x -> id

const maps = {};
for (const m of [...NAME_MAPS, ...LABEL_MAPS, ...SLACK_MAPS, ...ID_MAPS]) maps[m] = sliceMap(m);

// Collect (person, id, where) triples from every map.
const pairs = [];
for (const m of NAME_MAPS) {
  for (const [k, v] of Object.entries(maps[m])) if (isIdKey(k)) pairs.push([person(v), k, `${m}['${k}']`]);
}
for (const m of LABEL_MAPS) {
  for (const [k, v] of Object.entries(maps[m])) if (isIdKey(k)) pairs.push([person(v.label), k, `${m}['${k}']`]);
}
const nameById = Object.fromEntries(pairs.map(([p, id]) => [id, p]));
const personBySlack = {};
for (const [slack, id] of Object.entries(maps.SLACK_TO_GHL_USER)) {
  const p = nameById[id];
  check(p, `SLACK_TO_GHL_USER['${slack}'] = '${id}' is not a known id in ${NAME_MAPS.join('/')}`);
  personBySlack[slack] = p || `slack:${slack}`;
  pairs.push([personBySlack[slack], id, `SLACK_TO_GHL_USER['${slack}']`]);
}
for (const m of SLACK_MAPS) {
  for (const [k, slack] of Object.entries(maps[m])) {
    if (isIdKey(k)) pairs.push([personBySlack[slack] || `slack:${slack}`, k, `${m}['${k}']`]);
  }
}
for (const [email, id] of Object.entries(maps.EMAIL_TO_GHL_USER_ID)) {
  const p = nameById[id];
  check(p, `EMAIL_TO_GHL_USER_ID['${email}'] = '${id}' is not a known id in ${NAME_MAPS.join('/')}`);
  pairs.push([p || `email:${email}`, id, `EMAIL_TO_GHL_USER_ID['${email}']`]);
}

check(pairs.length > 30, `expected the maps to yield GHL ids, got ${pairs.length}`);

// 1. Format.
for (const [, id, where] of pairs) check(GHL_ID_RE.test(id), `${where}: '${id}' is not a 20-char alphanumeric GHL id`);

// 2. One person, one id. Mixed-case ids are the real ids; all-lowercase keys are
//    case-folded lookups and must fold from that same id.
const byPerson = {};
for (const [p, id, where] of pairs) (byPerson[p] = byPerson[p] || []).push([id, where]);
for (const [p, list] of Object.entries(byPerson)) {
  const real   = new Set(list.map(([id]) => id).filter(id => id !== id.toLowerCase()));
  const folded = new Set(list.map(([id]) => id.toLowerCase()));
  const where  = list.map(([id, w]) => `${w}=${id}`).join(', ');
  check(real.size <= 1, `${p} maps to ${real.size} different ids: ${where}`);
  check(folded.size === 1, `${p} has ${folded.size} different lowercase ids: ${where}`);
}

// 3. Pin the id that started this (verified via GET /users 2026-10-07).
const jose = byPerson.jose || [];
check(jose.length && jose.every(([id]) => id.toLowerCase() === 'izlta0jy5orkymvyitjv'),
  `Jose Carranza must be izLTA0jy5OrKyMvyItjV everywhere, got ${jose.map(([id]) => id).join(', ')}`);
check(maps.SLACK_TO_GHL_USER.U0AMTEKDCPN === 'izLTA0jy5OrKyMvyItjV',
  `the reaction-claim map must PUT Jose's real id, got ${maps.SLACK_TO_GHL_USER.U0AMTEKDCPN}`);

if (failures) { console.error(`\n${failures} failure(s)`); process.exit(1); }
console.log(`ghl-user-id-maps: OK (${pairs.length} ids across ${Object.keys(maps).length} maps, ${Object.keys(byPerson).length} people)`);
