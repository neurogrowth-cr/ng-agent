// Wiring test for opt-out -> email DND in index.js.  Run:  node test/seq-optout-dnd.test.js
//
// index.js boots Slack on require, so the block is sliced out by its section
// markers and run against a fake GHL, a fake Haiku and a fake Slack. Synthetic
// ids only (public repo).
const fs = require('fs');
const path = require('path');
const sequenceReplies = require('../lib/sequenceReplies');

let failures = 0;
function check(label, actual, expected) {
  const ok = JSON.stringify(actual) === JSON.stringify(expected);
  if (!ok) { failures++; console.error(`FAIL ${label}\n  expected: ${JSON.stringify(expected)}\n  actual:   ${JSON.stringify(actual)}`); }
  else console.log(`ok   ${label}`);
}

const SRC = fs.readFileSync(path.join(__dirname, '..', 'index.js'), 'utf8');
const START = SRC.indexOf('// ─── OPT-OUT → EMAIL DND');
const END = SRC.indexOf('// ─── end opt-out → email DND');
if (START < 0 || END < 0) { console.error('FAIL opt-out block markers not found'); process.exit(1); }
const BLOCK = SRC.slice(START, END);

function load({ haiku = '{"optout": true}', putOk = true, env = {}, contact = { dndSettings: {} }, roster = true } = {}) {
  const puts = []; const posts = []; const activity = []; const llm = [];
  const deps = {
    process: { env: { GHL_API_KEY: 'k', ...env } },
    sequenceReplies,
    anthropic: { messages: { create: async (req) => { llm.push(req); return { content: [{ type: 'text', text: haiku }] }; } } },
    MODEL_LIGHT: 'haiku', THINKING_OFF: { type: 'disabled' },
    logLlmFromAnthropicResponse: () => {},
    logActivity: (r) => activity.push(r),
    ghlFetch: async (url, init) => { puts.push({ url, body: JSON.parse(init.body) }); return { ok: putOk, status: putOk ? 200 : 422, text: async () => 'bad' }; },
    ghlGetContact: async () => contact,
    isRosterMember: () => roster,
    slack: { client: { chat: { postMessage: async (m) => { posts.push(m); return { ok: true }; } } } },
  };
  const names = Object.keys(deps);
  const api = new Function(...names, `${BLOCK}\nreturn { seqApplyOptOutDnd, handleSequenceOptOutUndo, SEQ_OPTOUT_DND_UNDO_EMOJI };`)(...names.map(n => deps[n]));
  return { api, puts, posts, activity, llm };
}
const NOW = Date.parse('2026-10-26T15:00:00Z');

(async () => {
  {
    const t = load();
    const r = await t.api.seqApplyOptOutDnd({ contact: { dndSettings: {} }, contactId: 'C1', text: 'me dejas de enviar correos por favor', now: NOW });
    check('1a confirmed opt-out: email DND applied', [r, t.puts.length, t.puts[0].body.dndSettings.Email.status], ['applied', 1, 'active']);
    check('1b only the Email channel is touched', Object.keys(t.puts[0].body.dndSettings), ['Email']);
    check('1c the DND message carries the date', t.puts[0].body.dndSettings.Email.message.includes('2026-10-26'), true);
    check('1d Haiku runs with thinking off', t.llm[0].thinking, { type: 'disabled' });
  }
  {
    const t = load({ haiku: '{"optout": false}' });
    const r = await t.api.seqApplyOptOutDnd({ contact: {}, contactId: 'C1', text: 'no me interesa por ahora' });
    check('2a Haiku says no: nothing written', [r, t.puts.length], ['unconfirmed', 0]);
  }
  {
    const t = load({ haiku: 'I think so' });
    const r = await t.api.seqApplyOptOutDnd({ contact: {}, contactId: 'C1', text: 'x' });
    check('2b junk verdict: nothing written (fails closed)', [r, t.puts.length], ['unconfirmed', 0]);
  }
  {
    const t = load();
    const r = await t.api.seqApplyOptOutDnd({ contact: { dndSettings: { Email: { status: 'active' } } }, contactId: 'C1', text: 'x' });
    check('2c already on: no Haiku call, no write', [r, t.llm.length, t.puts.length], ['already', 0, 0]);
  }
  {
    const t = load({ env: { SEQ_OPTOUT_DND_DISABLED: 'true' } });
    const r = await t.api.seqApplyOptOutDnd({ contact: {}, contactId: 'C1', text: 'x' });
    check('2d kill switch: nothing happens', [r, t.llm.length, t.puts.length], ['off', 0, 0]);
  }
  {
    const t = load({ putOk: false });
    const r = await t.api.seqApplyOptOutDnd({ contact: {}, contactId: 'C1', text: 'x' });
    check('2e GHL refuses: returns failed, never throws, logs an error', [r, t.activity.some(a => a.status === 'error')], ['failed', true]);
  }
  // Undo
  const ev = { user: 'U1', item: { channel: 'C9', ts: '1.1' } };
  {
    const t = load({ contact: { dndSettings: { Email: { status: 'active' } } } });
    await t.api.handleSequenceOptOutUndo(ev, { dnd: 'applied', contact_id: 'C1' });
    check('3a undo turns email DND off and says who', [t.puts[0].body.dndSettings.Email.status, /quitado por <@U1>/.test(t.posts[0].text)], ['inactive', true]);
  }
  {
    const t = load({ contact: { dndSettings: { Email: { status: 'active' } } } });
    await t.api.handleSequenceOptOutUndo(ev, { dnd: 'already', contact_id: 'C1' });
    check('3b no undo when Max did not apply it', t.puts.length, 0);
  }
  {
    const t = load({ roster: false, contact: { dndSettings: { Email: { status: 'active' } } } });
    await t.api.handleSequenceOptOutUndo(ev, { dnd: 'applied', contact_id: 'C1' });
    check('3c someone off the roster cannot undo', t.puts.length, 0);
  }
  {
    const t = load({ contact: { dndSettings: { Email: { status: 'inactive' } } } });
    await t.api.handleSequenceOptOutUndo(ev, { dnd: 'applied', contact_id: 'C1' });
    check('3d already off: nothing to undo, no post', [t.puts.length, t.posts.length], [0, 0]);
  }
  // Wiring
  check('4a the reply flow calls the DND step only for pide baja',
    /const dnd = intents\.includes\('pide baja'\) \? await seqApplyOptOutDnd\(/.test(SRC), true);
  check('4b the card and its metadata carry the DND state',
    [/contactName, intents, group, dnd,/.test(SRC), /conversation_id: convo\.id, dnd \}/.test(SRC)], [true, true]);
  check('4c the reaction router sends ↩️ on a reply card to the undo',
    /baseEmoji === SEQ_OPTOUT_DND_UNDO_EMOJI\) \{\s*await handleSequenceOptOutUndo/.test(SRC), true);

  if (failures) { console.error(`\n${failures} failure(s)`); process.exit(1); }
  console.log('\nall opt-out DND checks passed');
})().catch(err => { console.error('FAIL unexpected error', err); process.exit(1); });
