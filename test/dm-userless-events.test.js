// The DM handler ignores events without a user (message_changed,
// message_replied): a person never sends one, and answering them posted
// "not on Max's roster" into Ron's DM after every Clarity Brief.
const fs = require('fs');
const path = require('path');
const src = fs.readFileSync(path.join(__dirname, '..', 'index.js'), 'utf8');
const start = src.indexOf('slack.message(async ({ message, say }) => {');
const handler = src.slice(start, start + 4000);
const guard = handler.indexOf('if (!message.user) return;');
const roster = handler.indexOf('isRosterMember(userId)');
let failures = 0;
function check(name, ok) { console.log(`${ok ? 'ok  ' : 'FAIL'} ${name}`); if (!ok) failures++; }
check('DM handler found', start >= 0);
check('userless events return before the roster reply', guard > 0 && roster > 0 && guard < roster);
if (failures) { console.error(`\n${failures} failure(s)`); process.exit(1); }
console.log('\nall dm-userless-events checks passed');
