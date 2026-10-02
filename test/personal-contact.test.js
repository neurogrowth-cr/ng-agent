// Personal-contact exclusion.  Run:  node test/personal-contact.test.js
//
// A friend who DMs the brand account gets a GHL contact and a New Lead card
// like any lead. Tagging the contact `personal` must take it out of the sales
// flow everywhere. Two things are checked here:
//   1. isPersonalContact, extracted from index.js (never copied).
//   2. Every lead count / nag query on lead_posts skips flagged rows. A new
//      query that forgets the filter would quietly count friends as leads.
const fs   = require('fs');
const path = require('path');

const SRC = fs.readFileSync(process.argv[2] || path.join(__dirname, '..', 'index.js'), 'utf8');

const constLine = SRC.match(/const PERSONAL_CONTACT_TAG = [^\n]+\n/);
const fnBlock   = SRC.match(/function isPersonalContact\(tags\) \{[\s\S]*?\n\}\n/);
if (!constLine || !fnBlock) { console.log('FAIL  could not extract isPersonalContact from index.js'); process.exit(1); }
const build = (envTag) => {
  const prev = process.env.PERSONAL_CONTACT_TAG;
  if (envTag === undefined) delete process.env.PERSONAL_CONTACT_TAG; else process.env.PERSONAL_CONTACT_TAG = envTag;
  const fn = new Function(`${constLine[0]}${fnBlock[0]}; return isPersonalContact;`)();
  if (prev === undefined) delete process.env.PERSONAL_CONTACT_TAG; else process.env.PERSONAL_CONTACT_TAG = prev;
  return fn;
};
const isPersonalContact = build(undefined);

const cases = [];
const check = (name, got, want) => cases.push({ name, ok: JSON.stringify(got) === JSON.stringify(want), got, want });

check('tagged personal', isPersonalContact(['fb lead form', 'personal']), true);
check('case and spaces do not matter', isPersonalContact([' Personal ']), true);
check('untagged lead', isPersonalContact(['fb lead form', 'call-booked']), false);
check('a tag that merely contains the word is not a match', isPersonalContact(['personal-brand', 'not personal']), false);
check('no tags at all', isPersonalContact([]), false);
check('null tags (GHL omits the field)', isPersonalContact(undefined), false);
check('tag name is overridable from env', build('amigo')(['Amigo']), true);

// Every lead_posts read outside the intake webhook's own dedup lookups must
// carry the personal_excluded_at filter within its query chain.
const DEDUP_OK = new Set(['handleGHLWebhook', 'handleGHLClaimWebhook']);
const lines = SRC.split('\n');
let currentFn = '';
const missing = [];
let checked = 0;
lines.forEach((line, i) => {
  const m = line.match(/^(?:async )?function ([A-Za-z0-9_]+)\(/);
  if (m) currentFn = m[1];
  if (!line.includes(".from('lead_posts')")) return;
  if (DEDUP_OK.has(currentFn)) return;
  // Writes and the exclusion helper's own lookup are not counts.
  const chain = lines.slice(i, i + 8).join('\n');
  if (/\.(upsert|insert|update|delete)\(/.test(chain.split(';')[0])) return;
  if (currentFn === 'excludePersonalContact') return;
  checked++;
  if (!chain.split(';')[0].includes("personal_excluded_at")) missing.push(`${currentFn} (line ${i + 1})`);
});
check('every lead count / nag query skips personal contacts', missing, []);
check('the guard actually found the lead_posts reads', checked >= 7, true);

// The 🫂 Slack route must not collide with any other reaction set, and must
// run before the lead-claim route in the reaction_added handler.
const setOf = (name) => {
  const m = SRC.match(new RegExp(`const ${name}\\s*=\\s*new Set\\((\\[[^\\]]*\\])\\)`));
  return m ? new Set(JSON.parse(m[1].replace(/'/g, '"'))) : null;
};
const mark = setOf('PERSONAL_MARK_EMOJIS');
const others = ['LEAD_CLAIM_EMOJIS', 'CAMPAIGN_APPROVE_EMOJIS', 'CAMPAIGN_SKIP_EMOJIS'].map(n => [n, setOf(n)]);
check('personal mark emoji set found', !!mark && mark.size > 0, true);
check('every other reaction set found', others.every(([, set]) => !!set), true);
check('personal mark emoji collides with no other reaction set',
  others.flatMap(([n, set]) => [...(mark || [])].filter(e => set && set.has(e)).map(e => `${e} in ${n}`)), []);
const handler = SRC.slice(SRC.indexOf("slack.event('reaction_added'"));
const markAt  = handler.indexOf('PERSONAL_MARK_EMOJIS.has(baseEmoji)');
const claimAt = handler.indexOf('LEAD_CLAIM_EMOJIS.has(baseEmoji)');
check('personal mark route runs before the lead-claim route', markAt > -1 && claimAt > -1 && markAt < claimAt, true);

// Card removal policy: early Appointment Setting stages are deleted, anything
// that reached a booked call (or any VSL stage) is only abandoned.
const stageBlock  = SRC.match(/const STRIKE_STAGE = \{[\s\S]*?\n\};\n/);
const deletable   = SRC.match(/const PERSONAL_DELETABLE_STAGE_IDS = new Set\(\[[\s\S]*?\]\);\n/);
const actionFn    = SRC.match(/function personalCardAction\(stageId\) \{[\s\S]*?\n\}\n/);
check('card policy pieces found in index.js', !!(stageBlock && deletable && actionFn), true);
if (stageBlock && deletable && actionFn) {
  const personalCardAction = new Function(`${stageBlock[0]}${deletable[0]}${actionFn[0]}; return personalCardAction;`)();
  const NL = '93de6a09-78a4-4253-bea4-c1528ed6f2b3', IC = '4b936528-794e-40ab-812d-144b9d5e8128',
        S3 = 'e639662d-6b1b-42b5-a89d-7ebd70ca97e3', CALL_BOOKED = 'dc1fba03-abeb-4b47-9d29-6c308002b6c1',
        OPEN_DEAL = '63d30181-4ec0-4daa-8832-a8eebe1afbeb', VSL_BOOKED = '0315ac38-dd43-479b-ad99-aee8b4334bd5';
  check('New Lead friend card is deleted', personalCardAction(NL), 'delete');
  check('Initial Contact friend card is deleted', personalCardAction(IC), 'delete');
  check('Strike 3 friend card is deleted', personalCardAction(S3), 'delete');
  check('Call Booked card is only abandoned', personalCardAction(CALL_BOOKED), 'abandon');
  check('Open Deal card is only abandoned', personalCardAction(OPEN_DEAL), 'abandon');
  check('VSL card is only abandoned', personalCardAction(VSL_BOOKED), 'abandon');
  check('unknown stage is only abandoned', personalCardAction(undefined), 'abandon');
}

let failed = 0;
for (const c of cases) {
  console.log(`${c.ok ? 'PASS' : 'FAIL'}  ${c.name}`);
  if (!c.ok) { failed++; console.log(`      got  ${JSON.stringify(c.got)}\n      want ${JSON.stringify(c.want)}`); }
}
console.log(`\n${cases.length - failed}/${cases.length} passed`);
process.exit(failed ? 1 : 0);
