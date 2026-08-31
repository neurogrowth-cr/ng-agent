// Rules test for the call-prep REVI game plan.  Run:  node test/revi-game-plan.test.js
//
// Extracts the real pure helpers straight out of index.js rather than copying
// them, so the test can never drift from shipped behaviour (same approach as
// won-handoff-notes.test.js — index.js boots the Slack app on require).
const fs   = require('fs');
const path = require('path');

const SRC = fs.readFileSync(process.argv[2] || path.join(__dirname, '..', 'index.js'), 'utf8');
const block = SRC.slice(
  SRC.indexOf('function buildReviGamePlanPrompt'),
  SRC.indexOf('async function buildReviGamePlan'),
);

let failures = 0;
function check(label, actual, expected) {
  const ok = JSON.stringify(actual) === JSON.stringify(expected);
  if (!ok) { failures++; console.error(`FAIL ${label}\n  expected: ${JSON.stringify(expected)}\n  actual:   ${JSON.stringify(actual)}`); }
  else console.log(`ok   ${label}`);
}

const { buildReviGamePlanPrompt, enforceGamePlanLines } = new Function(
  `${block}; return { buildReviGamePlanPrompt, enforceGamePlanLines };`
)();

// ── enforceGamePlanLines: the hard cap Ron asked for ────────────────────────
// "no bs straight up coach lines — if too long, closers will not read it."
check('empty → null', enforceGamePlanLines(''), null);
check('null → null', enforceGamePlanLines(null), null);
check('prose without bullets → null', enforceGamePlanLines('Aquí va el plan:\nsin bullets\nnada'), null);
check('keeps bullet lines only',
  enforceGamePlanLines('Plan de juego:\n• uno\n• dos\nGracias!'),
  '• uno\n• dos');
check('caps at 4 bullets',
  enforceGamePlanLines('• a\n• b\n• c\n• d\n• e\n• f'),
  '• a\n• b\n• c\n• d');
check('dash bullets normalized to •',
  enforceGamePlanLines('- primero\n- segundo'),
  '• primero\n• segundo');
check('whitespace-padded bullets survive',
  enforceGamePlanLines('   • con espacios   '),
  '• con espacios');
const long = `• ${'x'.repeat(300)}`;
const enforced = enforceGamePlanLines(long);
check('long line truncated to 200 chars', enforced.length <= 200, true);
check('truncated line ends with ellipsis', enforced.endsWith('…'), true);

// ── buildReviGamePlanPrompt: hard rules + inputs present ────────────────────
const prompt = buildReviGamePlanPrompt({
  closerFirst: 'Jose',
  prospectLines: 'Nombre: David Lara\nNotas del setter: grúas puente',
  weeklyLines: 'Acción 1: temperature check | Script: "del 1 al 10"',
});
check('prompt addresses the closer', prompt.includes('llamada de Jose'), true);
check('prompt carries the 4-bullet hard rule', prompt.includes('Máximo 4 bullets'), true);
check('prompt forbids inventing data', prompt.includes('No inventes nada'), true);
check('prompt embeds prospect context', prompt.includes('grúas puente'), true);
check('prompt embeds weekly actions', prompt.includes('temperature check'), true);
check('prompt demands bullet prefix', prompt.includes('Empezá cada línea con "• "'), true);

if (failures) { console.error(`\n${failures} failure(s)`); process.exit(1); }
console.log('\nall green');
