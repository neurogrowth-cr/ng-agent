// Rules test for the Setter Scorecard (Ron, 2026-11-12).  Run:  node test/setter-scorecard.test.js
// Pins the pay rules from the Playbook and the one-setter attribution in index.js.
const fs = require('fs');
const path = require('path');
const sc = require('../lib/setterScorecard');

const cases = [];
const check = (name, ok, detail) => cases.push({ name, ok, detail });
const CR = (s) => Date.parse(`${s}-06:00`); // a Costa Rica wall-clock time

// 1. Tiers 15 / 25 / 40.
check('below tier 1', JSON.stringify(sc.tierProgress(12)) === JSON.stringify({ tier: 0, next: 15, toGo: 3 }));
check('exactly 15 is tier 1', sc.tierProgress(15).tier === 1 && sc.tierProgress(15).next === 25);
check('40+ is the top tier', sc.tierProgress(41).tier === 3 && sc.tierProgress(41).next === null);

// 2. Close-rate multiplier.
check('no multiplier under 25%', sc.multiplierFor(0.2) === null);
check('x1.25 at 25%', sc.multiplierFor(0.25) === 'x1.25');
check('x1.5 at 30%', sc.multiplierFor(0.31) === 'x1.5');

// 3. Signal SLA: 15 minutes, clock only runs 08:00 to 20:00 CR.
const sig = (created, handled, extra = {}) => ({ kind: 'signal', mode: 'draft', created_at: new Date(CR(created)).toISOString(),
  status: handled ? 'handled' : 'signal', approved_at: handled ? new Date(CR(handled)).toISOString() : null, ...extra });
const now = CR('2026-11-14T12:00:00');
const sla = sc.signalSla([
  sig('2026-11-13T10:00:00', '2026-11-13T10:10:00'),   // on time
  sig('2026-11-13T10:00:00', '2026-11-13T10:40:00'),   // late
  sig('2026-11-13T11:00:00', null),                    // never answered: late
  sig('2026-11-13T21:30:00', '2026-11-14T08:05:00'),   // overnight: clock starts 08:00, on time
  sig('2026-11-14T11:50:00', null),                    // still inside its 15 min: not judged
  sig('2026-11-13T10:00:00', '2026-11-13T10:01:00', { mode: 'shadow' }), // shadow: Ron's, ignored
  { kind: 'reply', mode: 'draft', status: 'sent', created_at: new Date(CR('2026-11-13T10:00:00')).toISOString() },
], now);
check('SLA counts on-time and judged cards', sla.onTime === 2 && sla.judged === 4, JSON.stringify(sla));
check('overnight card clock starts at 08:00', sc.slaClockStart(CR('2026-11-13T21:30:00')) === CR('2026-11-14T08:00:00'));
check('early card clock starts at 08:00 same day', sc.slaClockStart(CR('2026-11-13T06:10:00')) === CR('2026-11-13T08:00:00'));

// 4. Drafts: only what Sebastián saw.
const d = sc.draftStats([
  { kind: 'reply', mode: 'draft', status: 'sent', edited: false },
  { kind: 'strike', mode: 'draft', status: 'sent', edited: true },
  { kind: 'reply', mode: 'draft', status: 'expired' },
  { kind: 'reply', mode: 'draft', status: 'rejected' },
  { kind: 'reply', mode: 'shadow', status: 'sent' },
  { kind: 'signal', mode: 'draft', status: 'handled' },
]);
check('draft counts', d.asIs === 1 && d.edited === 1 && d.expired === 1 && d.rejected === 1, JSON.stringify(d));

// 5. The text: no em dashes, no opening question/exclamation marks, no dollars.
const text = sc.formatScorecard({
  now,
  mtd: { held_with_ficha: 12, won_with_ficha: 4, attended: 18, no_shows: 10, ficha_incomplete: 2, ficha_unchecked: 0 },
  week: { leads_claimed: 22, calls_booked: 9, attended: 7, no_shows: 3 },
  prevWeek: { leads_claimed: 18, calls_booked: 7, attended: 5, no_shows: 4 },
  sla, drafts: d, backlog: { tagged: 400, replied: 37, booked: 4, struckOut: 0 },
});
check('headline names the gap to the next tier', /AQC .*\*12\*, faltan 3 para el nivel 1 \(15\)/.test(text), text);
check('show-rate gate shown', text.includes('Show rate: 64% (18 de 28). Mínimo 60%: ✅'), text);
check('multiplier shown at 33%', text.includes('4 de 12 (33%). Multiplicador x1.5'), text);
check('week vs previous week', text.includes('Leads tomados 22 (antes 18)') && text.includes('show rate 70% (antes 56%)'), text);
check('no em dash', !text.includes('—'));
check('no opening marks', !/[¿¡]/.test(text));
check('no dollar amounts', !text.includes('$'));
const gated = sc.formatScorecard({ now, mtd: { held_with_ficha: 16, attended: 5, no_shows: 5 }, week: {}, prevWeek: {}, sla: null, drafts: null, backlog: null });
check('show rate under 60% says no bonus', gated.includes('❌ sin bono por ahora') && gated.includes('No se pudo leer hoy'), gated);

// 6. index.js: soleSetter credits every call, unclaimed self-bookings included.
const SRC = fs.readFileSync(path.join(__dirname, '..', 'index.js'), 'utf8');
const slice = (from, to) => SRC.slice(SRC.indexOf(from), SRC.indexOf(to, SRC.indexOf(from)));
const block = [
  slice('const SHOWED_OUTCOMES', '// prospect_id -> latest scheduled_start'),
  slice('function isAppointmentCancelled', '\n}\n') + '\n}\n',
  slice('function isAppointmentDeleted', '\n}\n') + '\n}\n',
  slice('function _newSetterSlot', '// ─── SETTER ATTRIBUTION RECONCILER'),
].join('\n');
const { tallySetterStats } = new Function('resolveSalesMember', 'isUnresolvedSalesId', `${block}; return { tallySetterStats };`)((id) => id, () => false);
const appts = [
  { id: 'a1', prospect_id: 'p1', scheduled_start: '2026-11-05T16:00:00.000Z', setter_id: 'oscar@x.com', qualification_snapshot: {}, prospect: { email: 'l1@x.com' } },
  { id: 'a2', prospect_id: 'p2', scheduled_start: '2026-11-06T16:00:00.000Z', setter_id: null, qualification_snapshot: {}, prospect: { email: 'l2@x.com' } },
];
const outcomes = { a1: { outcome: 'won' }, a2: { outcome: 'lost' } };
const solo = tallySetterStats(appts, outcomes, [], '2026-11-01T06:00:00.000Z', '2026-11-12T06:00:00.000Z', Date.parse('2026-11-12T06:00:00.000Z'), {}, { soleSetter: 'Sebastian S' });
const s = solo.stats['Sebastian S'];
check('soleSetter takes every call', Object.keys(solo.stats).length === 1 && s.calls_booked === 2 && s.attended === 2 && s.won === 1, JSON.stringify(solo.stats));
check('won flag rides on ownedCalls', solo.ownedCalls.filter((c) => c.won).length === 1);
const normal = tallySetterStats(appts, outcomes, [], '2026-11-01T06:00:00.000Z', '2026-11-12T06:00:00.000Z', Date.parse('2026-11-12T06:00:00.000Z'));
check('default attribution unchanged', normal.stats['oscar@x.com']?.calls_booked === 1 && !normal.stats['Sebastian S'], JSON.stringify(normal.stats));
check('dynamic Setter Leaderboard retired', /RETIRED_DYNAMIC_TASKS = new Set\(\['Setter Leaderboard'\]\)/.test(SRC));
check('scorecard cron registered Tue + Sat 07:00', SRC.includes("cron.schedule('0 7 * * 2,6', wrapCronJob('runSetterScorecard'") && SRC.includes("runSetterScorecard:           '0 7 * * 2,6'"));

const failed = cases.filter((c) => !c.ok);
for (const c of cases) console.log(`${c.ok ? 'PASS' : 'FAIL'}  ${c.name}${c.ok ? '' : `\n      ${c.detail || ''}`}`);
console.log(`\n${cases.length - failed.length}/${cases.length} passed`);
process.exit(failed.length ? 1 : 0);
