// Rules test for lib/openDealDrafts.js (day-5 / day-30 WhatsApp drafts on the
// open-deal cards). Run: node test/open-deal-drafts.test.js
const d = require('../lib/openDealDrafts');

let failures = 0;
function check(label, actual, expected) {
  const ok = JSON.stringify(actual) === JSON.stringify(expected);
  if (!ok) { failures++; console.error(`FAIL ${label}\n  expected: ${JSON.stringify(expected)}\n  actual:   ${JSON.stringify(actual)}`); }
  else console.log(`ok   ${label}`);
}

// ── 1. Archetype choice ───────────────────────────────────────────────────────
check('1a d5, no timeline → one_question', d.pickOpenDealArchetype({ touch: 'd5', signals: {} }), 'one_question');
check('1b d5, timeline stated → goal_reminder', d.pickOpenDealArchetype({ touch: 'd5', signals: { stated_timeline: 'quiere arrancar antes de diciembre' } }), 'goal_reminder');
check('1c d5, timeline "unknown" → one_question', d.pickOpenDealArchetype({ touch: 'd5', signals: { stated_timeline: 'unknown' } }), 'one_question');
check('1d d30, strong signal → new_reason', d.pickOpenDealArchetype({ touch: 'd30', signals: { buying_signal_strength: 'alta' } }), 'new_reason');
check('1e d30, weak signal → retreat', d.pickOpenDealArchetype({ touch: 'd30', signals: { buying_signal_strength: 'low' } }), 'retreat');
check('1f d30, no signal, deal dead → retreat', d.pickOpenDealArchetype({ touch: 'd30', signals: {}, dealStatus: 'dead' }), 'retreat');
check('1g d30, no signal → honest_close', d.pickOpenDealArchetype({ touch: 'd30', signals: {} }), 'honest_close');
check('1h value_delivery is never picked in v1', Object.values(d.ARCHETYPES).filter(a => a.touch === null).map(a => a.name), ['Entrega de valor']);

// ── 2. Prompt contract ────────────────────────────────────────────────────────
const p = d.buildOpenDealDraftPrompt({
  touch: 'd5', archetype: 'goal_reminder', prospectFirstName: 'Ana', closerFirstName: 'Jose', language: 'es',
  callDateStr: 'Oct 6', ageDays: 5,
  revi: { company: 'Acme', recovery: 'quedó en revisar la propuesta con su socio', statedTimeline: 'antes de diciembre', objectionType: 'socio' },
});
check('2a prompt names the archetype and the touch', p.includes('Recordatorio de objetivo') && p.includes('día 5'), true);
check('2b prompt carries the REVI fields', ['Acme', 'revisar la propuesta con su socio', 'antes de diciembre', 'socio'].every(s => p.includes(s)), true);
check('2c prompt enforces usted, 60 words, no price, no banned openers', ['usted', '60 words', 'No price', 'no he tenido noticias', 'dar seguimiento'].every(s => p.includes(s)), true);
check('2d d5 archetype asks for exactly one question', p.includes('End with exactly one question'), true);
const p30 = d.buildOpenDealDraftPrompt({ touch: 'd30', archetype: 'new_reason', prospectFirstName: 'Luis', closerFirstName: 'Jose', revi: {} });
check('2e d30 new_reason carries what is new and asks for nothing', p30.includes('licencias van incluidas') && p30.includes('Do not end with a question'), true);
check('2f no REVI read → prompt says so and forbids inventing', p30.includes('No REVI read for this call'), true);
check('2g feedback round-trips into a retry prompt', d.buildOpenDealDraftPrompt({ touch: 'd5', archetype: 'one_question', feedback: 'contains a price' }).includes('rejected: contains a price'), true);
check('2h prompt never mentions teardown', /teardown/i.test(p) || /teardown/i.test(p30), false);

// ── 3. Validator ──────────────────────────────────────────────────────────────
const good5 = 'Ana, en la llamada me dijo que quería tener el sistema andando antes de diciembre. El plazo avanza. ¿Quiere que lo ayude a lograrlo, o lo dejamos en pausa?\nJose';
check('3a good d5 draft passes', d.validateOpenDealDraft(good5, { archetype: 'goal_reminder' }), { ok: true, problems: [] });
const good30 = 'Luis, dos cosas cambiaron desde que hablamos: las licencias van incluidas y Smartrade cobró $71,000 en 42 días con el sistema. Me acordé de usted.\nJose';
check('3b good d30 new_reason passes (approved figure allowed)', d.validateOpenDealDraft(good30, { archetype: 'new_reason' }), { ok: true, problems: [] });
check('3c tuteo rejected', d.validateOpenDealDraft('Ana, ¿al final te decidiste por alguien o esto sigue en pie?\nJose', { archetype: 'one_question' }).problems, ['tuteo (must be usted)']);
check('3d banned opener rejected', d.validateOpenDealDraft('Ana, no he tenido noticias suyas, así que asumo que el momento no es el adecuado. Todo bien.\nJose', { archetype: 'honest_close' }).problems, ['banned phrase: "no he tenido noticias"']);
check('3e price rejected', d.validateOpenDealDraft('Ana, la inversión queda en $4,997 este mes. ¿Arrancamos?\nJose', { archetype: 'one_question' }).problems, ['contains a price']);
check('3f too long rejected', d.validateOpenDealDraft(Array(70).fill('palabra').join(' ') + '?', { archetype: 'one_question' }).problems, ['too long (70 words, max 60)']);
check('3g question archetype must end with ?', d.validateOpenDealDraft('Ana, al final se decidió por alguien o esto sigue en pie.\nJose', { archetype: 'one_question' }).problems, ['must end with a question']);
check('3h no-ask archetype must not end with ?', d.validateOpenDealDraft('Ana, puede que esto no sea lo adecuado para su negocio y está bien. ¿Le parece?\nJose', { archetype: 'retreat' }).problems, ['must not end with a question']);
check('3i emoji and markdown rejected', d.validateOpenDealDraft('Ana, *esto sigue en pie* 🚀 ¿seguimos?\nJose', { archetype: 'one_question' }).problems, ['emoji', 'markdown or link']);
check('3j empty → not ok', d.validateOpenDealDraft('', {}), { ok: false, problems: ['empty'] });
check('3k english draft skips the tuteo check', d.validateOpenDealDraft('Ana, did you end up going with someone, or is this still on the table?\nJose', { language: 'en', archetype: 'one_question' }).ok, true);

// ── 4. REVI context picker ────────────────────────────────────────────────────
const rec = { signals: { objection_type: 'precio', stated_timeline: 'Q1', buying_signal_strength: 'media', decision_maker_status: 'decide con socio' }, dealRecovery: 'enviar la cláusula' };
const ctx = d.reviContextForDraft(rec, { company: 'Acme', general_read: 'interesado pero cauto', language: 'es', teardown_text: 'RON ONLY' });
check('4a picks closer-safe fields only', Object.keys(ctx).sort(), ['buyingSignal', 'company', 'decisionMaker', 'generalRead', 'language', 'objectionType', 'recovery', 'statedTimeline']);
check('4b never carries teardown_text', JSON.stringify(ctx).includes('RON ONLY'), false);
check('4c recovery falls back to coaching.deal.recovery', d.reviContextForDraft(null, { deal: { recovery: 'plan B' } }).recovery, 'plan B');
check('4d language en detected', d.reviContextForDraft(null, { language: 'English' }).language, 'en');
check('4e nothing → {}', d.reviContextForDraft(null, null), {});

// ── 5. Card block ─────────────────────────────────────────────────────────────
const blk = d.renderOpenDealDraftBlock({ touch: 'd5', archetype: 'one_question', text: 'Ana, ¿sigue en pie?\nJose', hasContact: true, source: 'revi' });
check('5a block names touch and archetype, quotes every line, offers ✅ and revise',
  blk.includes('día 5') && blk.includes('Una sola pregunta') && blk.includes('> Ana, ¿sigue en pie?') && blk.includes('> Jose') && blk.includes('✅') && blk.includes('revise: <your text>'), true);
const blk2 = d.renderOpenDealDraftBlock({ touch: 'd30', archetype: 'honest_close', text: 'x y z', hasContact: false, source: 'generic' });
check('5b no contact → phone fallback, generic flagged', blk2.includes('No GHL contact') && blk2.includes('no REVI read') && !blk2.includes('✅'), true);
check('5c block never renders undefined/null/NaN', /undefined|NaN|\bnull\b/.test(blk + blk2), false);

if (failures) { console.error(`\n${failures} failure(s)`); process.exit(1); }
console.log('\nall open-deal draft checks passed');
