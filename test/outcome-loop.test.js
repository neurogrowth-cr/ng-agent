// Tests the outcome-loop pure contract: outcome→prospect-status promotion
// (mirrored from dash status-map.ts + outcome-map.ts), the REVI proposal
// gate (won is never proposable), and the 36h recording matcher.
// Run:  node test/outcome-loop.test.js
//
// Same extraction trick as the other tests: the block is sliced out of
// index.js and compiled with new Function, so this can never drift from
// shipped behaviour.
const fs   = require('fs');
const path = require('path');

const SRC = fs.readFileSync(process.argv[2] || path.join(__dirname, '..', 'index.js'), 'utf8');
const block = SRC.slice(
  SRC.indexOf('const OUTCOME_STATUS_RANK'),
  SRC.indexOf('// Writes one outcome row + the matching prospect-status promotion'),
);
const g = new Function(`${block}; return { nextProspectStatusForOutcome, computeOutcomeProposal, parseReviSignals, outcomeStageFor, outcomeActionsFor, resolveAppointmentStatus, appointmentStatusForOutcome, buildOutcomeCardText, buildReviProspectNote, matchRecordingToCall, VALID_LOGGABLE_OUTCOMES, OUTCOME_MATCH_PAD_MS };`)();

let failures = 0;
function check(name, actual, expected) {
  const a = JSON.stringify(actual);
  const e = JSON.stringify(expected);
  if (a === e) { console.log(`  ok  ${name}`); }
  else { failures += 1; console.error(`FAIL  ${name}\n      expected ${e}\n      got      ${a}`); }
}

console.log('nextProspectStatusForOutcome — mirrors dash mergeProspectStatus + prospectStatusFromOutcome');
// Advances
check('follow_up promotes appointment_booked → nurture (30→35)', g.nextProspectStatusForOutcome('follow_up', 'appointment_booked'), 'nurture');
check('no_show promotes appointment_booked → appointment_held (dash contract: the call event happened)', g.nextProspectStatusForOutcome('no_show', 'appointment_booked'), 'appointment_held');
check('won is terminal from anywhere', g.nextProspectStatusForOutcome('won', 'prospect'), 'won');
check('lost is terminal from nurture', g.nextProspectStatusForOutcome('lost', 'nurture'), 'lost');
check('disqualified is terminal', g.nextProspectStatusForOutcome('disqualified', 'qualified'), 'disqualified');
// No-ops and guards
check('same status is a no-op', g.nextProspectStatusForOutcome('follow_up', 'nurture'), null);
check('never regress: follow_up (→nurture, 35) does not downgrade appointment_held (40)', g.nextProspectStatusForOutcome('follow_up', 'appointment_held'), null);
check('terminal is sticky: follow_up cannot leave won', g.nextProspectStatusForOutcome('follow_up', 'won'), null);
check('terminal is sticky: no_show cannot leave lost', g.nextProspectStatusForOutcome('no_show', 'lost'), null);
check('terminal→terminal is allowed (won over lost is an explicit terminal write)', g.nextProspectStatusForOutcome('won', 'lost'), 'won');
check('unknown outcome maps to no change', g.nextProspectStatusForOutcome('garbage', 'prospect'), null);

console.log('computeOutcomeProposal — won is NEVER proposable; deal.status beats deal_outcome');
const REC = { dealOutcome: 'stall' };
const P = (e) => { const r = g.computeOutcomeProposal(e); return { outcome: r.outcome, confidence: r.confidence, wonHint: r.wonHint, rule: r.rule }; };
check('no recording at all → no proposal', P({}), { outcome: null, confidence: 'none', wonHint: false, rule: 'no_evidence' });
check('deal alive → follow_up, high confidence', P({ recording: REC, dealStatus: 'alive' }),
  { outcome: 'follow_up', confidence: 'high', wonHint: false, rule: 'revi_alive' });
check('deal dead + low ICP → disqualified (the Mia Campos case, REVI labelled it lost)',
  P({ recording: { dealOutcome: 'lost' }, dealStatus: 'dead', icpFit: 'Bajo - servicio de belleza B2C, ticket ~$400' }),
  { outcome: 'disqualified', confidence: 'high', wonHint: false, rule: 'revi_dead_low_icp' });
check('deal dead + healthy ICP → lost', P({ recording: REC, dealStatus: 'dead', icpFit: 'Medio-Alto — agencia B2B' }),
  { outcome: 'lost', confidence: 'high', wonHint: false, rule: 'revi_dead' });
check('English "low" ICP also reads as No Fit', P({ recording: REC, dealStatus: 'dead', icpFit: 'Low — B2C, tiny ticket' }),
  { outcome: 'disqualified', confidence: 'high', wonHint: false, rule: 'revi_dead_low_icp' });
check('GHL stage lost outranks a silent REVI read', P({ recording: REC, funnelClass: 'lost' }),
  { outcome: 'lost', confidence: 'high', wonHint: false, rule: 'ghl_stage_lost' });
check('REVI won → hint only, NEVER an outcome', P({ recording: { dealOutcome: 'won' }, dealStatus: 'alive' }),
  { outcome: null, confidence: 'high', wonHint: true, rule: 'revi_won' });
check('GHL stage won → hint only, NEVER an outcome', P({ recording: REC, funnelClass: 'won' }),
  { outcome: null, confidence: 'high', wonHint: true, rule: 'ghl_stage_won' });
check('cash collected → hint only, never one-tap', P({ recording: REC, dealStatus: 'alive', cashCollected: 3500 }),
  { outcome: null, confidence: 'high', wonHint: true, rule: 'revi_won' });
check('cash collected is surfaced as the amount hint',
  g.computeOutcomeProposal({ recording: REC, cashCollected: 3500 }).revenueHint, 3500);
check('recording but no deal read → low confidence, no one-tap', P({ recording: REC }),
  { outcome: null, confidence: 'low', wonHint: false, rule: 'recording_only' });
// GHL stage evidence is independent of REVI: the closer moved that card by
// hand, so it must be read even when REVI never saw the call (~60% of them).
check('NO recording but GHL says lost → still a confident proposal', P({ recording: null, funnelClass: 'lost' }),
  { outcome: 'lost', confidence: 'high', wonHint: false, rule: 'ghl_stage_lost' });
check('NO recording but GHL says won → won hint', P({ recording: null, funnelClass: 'won' }),
  { outcome: null, confidence: 'high', wonHint: true, rule: 'ghl_stage_won' });
check('GHL "No show / Rescheduling" merges two answers → propose, but MEDIUM only',
  P({ recording: null, funnelClass: 'noshow' }),
  { outcome: 'no_show', confidence: 'medium', wonHint: false, rule: 'ghl_stage_noshow' });
check('no recording and no GHL signal → nothing at all', P({ recording: null }),
  { outcome: null, confidence: 'none', wonHint: false, rule: 'no_evidence' });

console.log('parseReviSignals — 129/130 rows are objects, 1 is double-encoded JSON');
check('plain object passes through', g.parseReviSignals({ buying_signal_strength: 'high' }), { buying_signal_strength: 'high' });
check('double-encoded JSON string is recovered',
  g.parseReviSignals(JSON.stringify(JSON.stringify({ buying_signal_strength: 'high' }))), { buying_signal_strength: 'high' });
check('array-wrapped payload unwraps to the first element',
  g.parseReviSignals(JSON.stringify([{ objection_type: 'Liquidez' }])), { objection_type: 'Liquidez' });
check('garbage never throws', g.parseReviSignals('not json at all'), {});
check('null is an empty read', g.parseReviSignals(null), {});

console.log('GHL vocabulary — cards must name stages the closer actually has');
const APPT = 'KH1IQuaN8aNB1lfRpvP4', VSL = '7KU0NBdMhhVifszmT9jo';
check('Appointment Setting: follow_up is Open Deal', g.outcomeStageFor(APPT, 'follow_up').label, 'Open Deal');
check('VSL: follow_up is Follow up - Open Deal', g.outcomeStageFor(VSL, 'follow_up').label, 'Follow up - Open Deal');
check('Appointment Setting separates No Fit from Lost',
  [g.outcomeStageFor(APPT, 'disqualified').label, g.outcomeStageFor(APPT, 'lost').label], ['No Fit', 'Lost']);
check('VSL merges them into ONE stage id',
  g.outcomeStageFor(VSL, 'disqualified').id === g.outcomeStageFor(VSL, 'lost').id, true);
check('so VSL offers 3 actions, not 4', g.outcomeActionsFor(VSL).map(a => a.label), ['Follow up - Open Deal', 'Lost / No Fit', 'No-Show / Rescheduling']);
check('Appointment Setting offers all 4', g.outcomeActionsFor(APPT).map(a => a.label), ['Open Deal', 'Lost', 'No Fit', 'No show / Rescheduling']);
check('closers type a short token, never the long stage label', g.outcomeActionsFor(APPT).map(a => a.say), ['open deal', 'lost', 'no fit', 'no show']);
check('an unknown pipeline never guesses a stage id', g.outcomeStageFor('not-a-pipeline', 'lost'), null);

console.log('resolveAppointmentStatus — REVI proves a show; absence never proves a no-show');
const R = (e) => { const v = g.resolveAppointmentStatus(e); return { status: v.status, action: v.action, reason: v.reason, conflict: v.conflict }; };
check('recording + still confirmed → write showed', R({ ghlStatus: 'confirmed', recording: REC }),
  { status: 'showed', action: 'write', reason: 'revi_recording', conflict: false });
// Measured 2026-08-18: of 27 post-cutover calls with NO recording, 25 were
// actually held. Absence is not evidence, so this must never become a no-show
// AND must never become a separate question to the closer either.
check('NO recording → defer to the outcome card, never auto no-show, never a second question',
  R({ ghlStatus: 'confirmed', recording: null }),
  { status: null, action: 'defer', reason: 'outcome_card_will_settle_it', conflict: false });
check('cancelled upstream → skip, never ask the closer', R({ ghlStatus: 'confirmed', cancelled: true }),
  { status: null, action: 'skip', reason: 'cancelled_upstream', conflict: false });
check('rescheduled → skip, never ask the closer', R({ ghlStatus: 'confirmed', rescheduled: true }),
  { status: null, action: 'skip', reason: 'rescheduled', conflict: false });

console.log('appointmentStatusForOutcome — one answer settles Paso 1 and Paso 2');
check('no_show is the only outcome that means they did not show', g.appointmentStatusForOutcome('no_show'), 'noshow');
['won','lost','follow_up','disqualified'].forEach(o =>
  check(`${o} implies the call was held`, g.appointmentStatusForOutcome(o), 'showed'));
check('no outcome implies nothing', g.appointmentStatusForOutcome(null), null);
check('already showed → skip', R({ ghlStatus: 'showed', recording: REC }),
  { status: null, action: 'skip', reason: 'already_showed', conflict: false });
const CALL = Date.parse('2026-08-04T20:00:00Z');
const AFTER = CALL + 6 * 3600e3, BEFORE = CALL - 4 * 86400e3;
check('GHL says noshow AFTER the call but REVI recorded it → CONFLICT, never flip',
  R({ ghlStatus: 'noshow', recording: REC, statusWrittenAtMs: AFTER, startMs: CALL }),
  { status: null, action: 'skip', reason: 'conflict_recording_vs_status', conflict: true });

// GHL reuses the appointment row on reschedule, so attendance set for an
// EARLIER booking rides onto the new date. The real Daniela Bruno row: no-show
// logged Jul 28 for a Jul 24 call, rebooked to Aug 4, then a 74-min recording.
// 3 of 16 post-cutover terminal statuses were stale exactly this way.
check('attendance written BEFORE the call is a leftover — the recording wins, no conflict',
  R({ ghlStatus: 'noshow', recording: REC, statusWrittenAtMs: BEFORE, startMs: CALL }),
  { status: 'showed', action: 'write', reason: 'stale_status_superseded_by_recording', conflict: false });
check('a stale `showed` is not treated as settled either (the Melina Yáñez row)',
  R({ ghlStatus: 'showed', recording: null, statusWrittenAtMs: BEFORE, startMs: CALL }),
  { status: null, action: 'defer', reason: 'stale_status_no_evidence', conflict: false });
check('cancelling BEFORE a call is normal, never stale',
  R({ ghlStatus: 'cancelled', recording: null, statusWrittenAtMs: BEFORE, startMs: CALL }),
  { status: null, action: 'skip', reason: 'already_cancelled', conflict: false });
check('missing timestamps fall back to trusting the status (never invent staleness)',
  R({ ghlStatus: 'noshow', recording: null }),
  { status: null, action: 'skip', reason: 'already_noshow', conflict: false });
check('GHL says cancelled but REVI recorded it → conflict', R({ ghlStatus: 'cancelled', recording: REC }).conflict, true);
check('noshow with no recording is just a skip, not a conflict', R({ ghlStatus: 'noshow', recording: null }),
  { status: null, action: 'skip', reason: 'already_noshow', conflict: false });
check('cancelled beats a missing recording — no human is asked',
  R({ ghlStatus: 'confirmed', recording: null, cancelled: true }).action, 'skip');

console.log('VALID_LOGGABLE_OUTCOMES — what a human may log');
check('the five loggable outcomes', [...g.VALID_LOGGABLE_OUTCOMES].sort(), ['disqualified', 'follow_up', 'lost', 'no_show', 'won']);

console.log('matchRecordingToCall — 36h window, nearest wins, one recording vouches once');
const H = 3600 * 1000;
const T0 = Date.parse('2026-08-01T18:00:00Z');
const mkRecs = () => ([
  { email: 'a@x.com', at: T0 + 2 * H,  matched: false, tag: 'a-near' },
  { email: 'a@x.com', at: T0 + 30 * H, matched: false, tag: 'a-far' },
  { email: 'b@x.com', at: T0,          matched: false, tag: 'b' },
]);
let recs = mkRecs();
check('matches same email inside window, nearest of two', g.matchRecordingToCall(recs, 'a@x.com', T0)?.tag, 'a-near');
check('a matched recording is consumed — second call gets the other one', g.matchRecordingToCall(recs, 'a@x.com', T0)?.tag, 'a-far');
check('third call for same email finds nothing left', g.matchRecordingToCall(recs, 'a@x.com', T0), null);
recs = mkRecs();
check('wrong email never matches', g.matchRecordingToCall(recs, 'c@x.com', T0), null);
check('email compare is case-insensitive', g.matchRecordingToCall(recs, 'B@X.com', T0)?.tag, 'b');
recs = [{ email: 'a@x.com', at: T0 + g.OUTCOME_MATCH_PAD_MS, matched: false, tag: 'edge' }];
check('exactly 36h away is inside the window', g.matchRecordingToCall(recs, 'a@x.com', T0)?.tag, 'edge');
recs = [{ email: 'a@x.com', at: T0 + g.OUTCOME_MATCH_PAD_MS + 1, matched: false, tag: 'out' }];
check('36h + 1ms is outside', g.matchRecordingToCall(recs, 'a@x.com', T0), null);
check('missing email on the call never matches', g.matchRecordingToCall(mkRecs(), '', T0), null);
check('bad appointment time never matches', g.matchRecordingToCall(mkRecs(), 'a@x.com', NaN), null);

console.log('buildOutcomeCardText — the copy a closer actually reads');
const cardBase = {
  prospectName: 'Aura Bonilla Bravo', whenStr: 'Aug 14, 01:00 PM', heldDays: 3, nudgeCount: 1,
  rec: { durationMin: 51, score: 67, url: 'https://fathom.video/x', dealRecovery: 'Depósito de $500 acordado;\n saldo el lunes 18.', signals: { buying_signal_strength: 'high' }, icpFit: 'Medio-Alto' },
  funnel: { to_stage_name: 'Call Booked', detected_on: '2026-08-12' },
  pipelineId: APPT,
};
const aliveCard = g.buildOutcomeCardText({ ...cardBase, proposal: { outcome: 'follow_up', confidence: 'high', wonHint: false } });
check('states the show was auto-marked', /Marked \*Showed\* automatically/.test(aliveCard), true);
check('names the literal GHL stage the ✅ lands in', /✅ move to \*Open Deal\*/.test(aliveCard), true);
check('flattens REVI multi-line narrative onto one line', /Depósito de \$500 acordado; saldo el lunes 18\./.test(aliveCard), true);
const escaped = g.buildOutcomeCardText({ ...cardBase, rec: { ...cardBase.rec, dealRecovery: 'Estado actual: alive.\\n\\nPlan: llamar el lunes.' },
  proposal: { outcome: 'follow_up', confidence: 'high', wonHint: false } });
check('literal backslash-n from REVI JSON never reaches the closer', /\\n/.test(escaped), false);
check('and the text still reads as one sentence', /Estado actual: alive\. Plan: llamar el lunes\./.test(escaped), true);
check('shows the current GHL stage', /📊 GHL: Call Booked since 2026-08-12/.test(aliveCard), true);
check('never renders [object Object]', /\[object Object\]/.test(aliveCard), false);
check('never renders a bare undefined', /undefined/.test(aliveCard), false);

const wonCard = g.buildOutcomeCardText({ ...cardBase, proposal: { outcome: null, confidence: 'high', wonHint: true, revenueHint: 3500 } });
check('a close asks for a typed amount, never a ✅', /Reply `won <amount>`/.test(wonCard), true);
check('and offers no ✅ move at all', /✅ move to/.test(wonCard), false);
check('surfaces the amount REVI heard', /REVI heard \$3500/.test(wonCard), true);

const vslCard = g.buildOutcomeCardText({ ...cardBase, pipelineId: VSL, proposal: { outcome: 'follow_up', confidence: 'high', wonHint: false } });
check('VSL card names the VSL stage', /✅ move to \*Follow up - Open Deal\*/.test(vslCard), true);
check('VSL card never offers a bare "no fit" the card cannot express', /`no fit`/.test(vslCard), false);
check('VSL card offers the merged action instead', /`lost`/.test(vslCard), true);
check('every card offers no-show, so attendance is never a separate DM', /`no show`/.test(vslCard), true);

const medCard = g.buildOutcomeCardText({ ...cardBase, rec: null, proposal: { outcome: 'no_show', confidence: 'medium', wonHint: false } });
check('a medium read is shown, not hidden behind "I can\'t tell"', /Looks like \*No show \/ Rescheduling\*/.test(medCard), true);
check('but it is never one-tap', /✅ move to/.test(medCard), false);
check('and it tells the closer the short word to type', /Reply `no show` to confirm/.test(medCard), true);

const blindCard = g.buildOutcomeCardText({ ...cardBase, rec: null, proposal: { outcome: null, confidence: 'none', wonHint: false } });
check('no recording is stated plainly', /No REVI recording found/.test(blindCard), true);
check('and Max admits it cannot tell', /I can't tell from here/.test(blindCard), true);

console.log('buildReviProspectNote — the read the whole team sees on the GHL contact');
const noteRec = { durationMin: 74, score: 61, url: 'https://fathom.video/x',
  dealStatus: 'alive', dealRecovery: 'Antes del jueves: Ron envía la propuesta.\n\nEl martes: llamada con Andrea.',
  icpFit: 'Medio-Alto — estudio creativo B2B',
  signals: { buying_signal_strength: 'high', objection_type: 'Presupuesto ($2,000 vs $3,500)', stated_timeline: 'Decisión esta semana', decision_maker_status: 'Andrea aprueba y no estuvo' } };
const note = g.buildReviProspectNote(noteRec, { callDateStr: 'Aug 4, 02:00 PM' });
check('opens with the REVI banner', /^🧠 REVI — lectura de la llamada/.test(note), true);
check('states deal status in plain words', /Estado del deal: VIVO/.test(note), true);
check('carries the blocker (decision maker)', /Decisor: Andrea aprueba y no estuvo/.test(note), true);
check('carries next steps', /Próximos pasos:/.test(note), true);
check('flattens escaped newlines from coaching_json', /\\n/.test(note), false);
check('links the recording', /🎙 Grabación: https:\/\/fathom\.video\/x/.test(note), true);
check('labels itself AI-generated so nobody quotes it as gospel', /Generado por REVI/.test(note), true);
check('never renders undefined', /undefined/.test(note), false);
check('no recording → no note at all', g.buildReviProspectNote(null), null);
const thinNote = g.buildReviProspectNote({ durationMin: 12 }, { callDateStr: 'Aug 4' });
check('a thin read still produces a valid note without empty labels', /Objeción:|Decisor:|Próximos pasos:/.test(thinNote), false);

if (failures) { console.error(`\n${failures} failure(s)`); process.exit(1); }
console.log('\nAll outcome-loop tests passed.');
