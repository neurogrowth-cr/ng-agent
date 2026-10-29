// Rules test for per-ad attribution (lib/adAttribution.js).
// Run:  node test/ad-attribution.test.js
//
// gatherAdAttribution runs against a fake GHL, Meta and portal. Fixture shapes
// copy real October 2026 rows with ids and names replaced: an IG DM ad lead
// who booked and won, a form ad lead who won in the portal while the GHL deal
// stayed open, an organic WhatsApp win, a GHL-only win, an iClosed-era win
// restated with a later close_date, a deleted duplicate appointment, and a
// webinar registrant who must not count as a lead.
const assert = require('assert');
const fs = require('fs');
const path = require('path');
const lib = require('../lib/adAttribution');

let failures = 0;
const pending = [];
function check(label, fn) {
  pending.push((async () => {
    try { await fn(); console.log(`ok   ${label}`); }
    catch (e) { failures++; console.error(`FAIL ${label}: ${e.message}`); }
  })());
}

const OCT = lib.crMonthBounds('2026-10');
const adA = { sessionSource: 'Paid Social', medium: 'instagram', adId: 'A1', utmContent: 'House ad', utmCampaign: 'IG DM', utmMedium: 'Open' };
const adB = { sessionSource: 'Paid Social', medium: 'facebook', adId: 'B1', utmContent: 'Canada ad', utmCampaign: 'Form', utmMedium: 'CR' };
const contacts = {
  cA: { id: 'cA', firstName: 'Ana', dateAdded: '2026-10-05T10:00:00Z', attributionSource: adA, tags: [] },
  cB: { id: 'cB', firstName: 'Beto', dateAdded: '2026-10-06T10:00:00Z', attributionSource: adB, tags: [] },
  cO: { id: 'cO', firstName: 'Olga', dateAdded: '2026-10-07T10:00:00Z', attributionSource: { sessionSource: 'Social media', medium: 'whatsapp_coex' }, tags: [] },
  cW: { id: 'cW', firstName: 'Web', dateAdded: '2026-10-08T10:00:00Z', source: 'Webinar Optin', attributionSource: adA, tags: ['webinar 27 de octubre'] },
  cG: { id: 'cG', firstName: 'Gina', dateAdded: '2026-09-02T10:00:00Z', attributionSource: { medium: 'instagram' }, tags: [] },
  cL: { id: 'cL', firstName: 'Lalo', dateAdded: '2026-05-01T10:00:00Z', attributionSource: adB, tags: [] },
};
const appts = [
  { id: 'a1', prospect_id: 'pA', booked_at: '2026-10-06T15:00:00Z', scheduled_start: '2026-10-09T20:00:00Z', attended: null, source: 'ghl', ghl_appointment_id: 'g1', closer_id: 'ron@x', qualification_snapshot: {} },
  { id: 'a1dup', prospect_id: 'pA', booked_at: '2026-10-07T15:00:00Z', scheduled_start: '2026-10-09T20:00:00Z', attended: null, source: 'ghl', ghl_appointment_id: 'g1b', closer_id: 'ron@x', qualification_snapshot: { reason: 'deleted_in_ghl', cancelled: true } },
  { id: 'a2', prospect_id: 'pB', booked_at: '2026-10-07T15:00:00Z', scheduled_start: '2026-10-12T20:00:00Z', attended: null, source: 'ghl', ghl_appointment_id: 'g2', closer_id: 'jose@x', qualification_snapshot: {} },
  { id: 'a3', prospect_id: 'pO', booked_at: '2026-10-08T15:00:00Z', scheduled_start: '2026-10-10T20:00:00Z', attended: true, source: 'ghl', ghl_appointment_id: 'g3', closer_id: 'ron@x', qualification_snapshot: {} },
  { id: 'a4', prospect_id: 'pL', booked_at: '2026-05-01T15:00:00Z', scheduled_start: '2026-05-04T16:00:00Z', attended: null, source: 'iclosed', ghl_appointment_id: null, closer_id: 'ron@x', qualification_snapshot: {} },
  { id: 'opp1', prospect_id: 'pB', booked_at: '2026-10-07T15:00:00Z', scheduled_start: null, source: 'ghl', ghl_appointment_id: 'opp:xyz', qualification_snapshot: {} },
];
const prospects = [
  { id: 'pA', full_name: 'Ana A', email: 'a@x', ghl_contact_id: 'cA', closer_owner_id: 'ron@x' },
  { id: 'pB', full_name: 'Beto B', email: 'b@x', ghl_contact_id: 'cB', closer_owner_id: 'jose@x' },
  { id: 'pO', full_name: 'Olga O', email: 'o@x', ghl_contact_id: 'cO', closer_owner_id: 'ron@x' },
  { id: 'pL', full_name: 'Lalo L', email: 'l@x', ghl_contact_id: null, closer_owner_id: 'ron@x' },
];
const outcomes = [
  { appointment_id: 'a1', outcome: 'won', closed_revenue: 5000, close_date: '2026-10-17', created_at: '2026-10-17T20:00:00Z' },
  { appointment_id: 'a1dup', outcome: 'won', closed_revenue: 5000, close_date: '2026-10-17', created_at: '2026-10-17T20:00:00Z' },
  { appointment_id: 'a2', outcome: 'won', closed_revenue: 5000, close_date: '2026-10-22', created_at: '2026-10-22T20:00:00Z' },
  { appointment_id: 'a3', outcome: 'won', closed_revenue: 5000, close_date: '2026-10-10', created_at: '2026-10-10T20:00:00Z' },
  // iClosed-era call restated on 2026-10-02: belongs to May, not October.
  { appointment_id: 'a4', outcome: 'won', closed_revenue: 3500, close_date: '2026-10-02', created_at: '2026-05-04T20:00:00Z' },
];
const wonOpps = [
  { id: 'o1', contactId: 'cA', pipelineId: 'KH1IQuaN8aNB1lfRpvP4', status: 'won', monetaryValue: 5000, lastStatusChangeAt: '2026-10-21T05:00:00Z', name: 'Ana A' },
  { id: 'o3', contactId: 'cO', pipelineId: 'KH1IQuaN8aNB1lfRpvP4', status: 'won', monetaryValue: 5000, lastStatusChangeAt: '2026-10-10T22:00:00Z', name: 'Olga O' },
  { id: 'oG', contactId: 'cG', pipelineId: 'KH1IQuaN8aNB1lfRpvP4', status: 'won', monetaryValue: 3000, lastStatusChangeAt: '2026-10-24T05:00:00Z', name: 'Gina G',
    attributions: [{ isFirst: true, utmAdId: 'C1', utmContent: 'Old ad', utmCampaign: 'MX' }] },
  { id: 'oX', contactId: 'cX', pipelineId: 'na0mJ5F6gUtN6Qw48DNd', status: 'won', monetaryValue: 1000, lastStatusChangeAt: '2026-10-24T05:00:00Z', name: 'Webinar buyer' },
];

// Minimal PostgREST-ish fake: select().gte().lt() / .in() / .eq() / .or().limit().
function fakePortal() {
  const tables = { revops_appointments: appts, revops_prospects: prospects, revops_sales_outcomes: outcomes };
  return {
    from(t) {
      let rows = tables[t].slice();
      const q = {
        select() { return q; },
        gte(c, v) { rows = rows.filter((r) => r[c] != null && r[c] >= v); return q; },
        lt(c, v) { rows = rows.filter((r) => r[c] != null && r[c] < v); return q; },
        in(c, vs) { rows = rows.filter((r) => vs.includes(r[c])); return q; },
        eq(c, v) { rows = rows.filter((r) => r[c] === v); return q; },
        then(res) { return Promise.resolve({ data: rows, error: null }).then(res); },
      };
      return q;
    },
  };
}
const calls = { meta: [] };
const deps = {
  locationId: 'LOC', adAccountId: 'act_1', portal: fakePortal(), excludeIds: new Set(), now: new Date('2026-10-28T12:00:00Z'),
  async ghl(method, p, body) {
    if (method === 'POST' && p === '/contacts/search') {
      const gte = body.filters[0].value.gte; const lte = body.filters[0].value.lte;
      return { contacts: Object.values(contacts).filter((c) => c.dateAdded >= gte && c.dateAdded <= lte) };
    }
    if (p.startsWith('/opportunities/search') && p.includes('status=won')) return { opportunities: wonOpps, meta: {} };
    if (p.startsWith('/opportunities/search') && p.includes('contact_id=cB')) return { opportunities: [{ pipelineId: 'KH1IQuaN8aNB1lfRpvP4', status: 'open', monetaryValue: 2900 }] };
    if (p.startsWith('/opportunities/search')) return { opportunities: [] };
    if (p.startsWith('/contacts/search/duplicate')) return p.includes('l%40x') ? { contact: { id: 'cL' } } : {};
    const m = p.match(/^\/contacts\/(\w+)$/);
    if (m) return { contact: contacts[m[1]] || null };
    throw new Error(`unexpected ghl ${method} ${p}`);
  },
  async meta(p, query) {
    calls.meta.push({ p, query });
    if (p === '/act_1/insights') return { data: [
      { ad_id: 'A1', ad_name: 'House ad (Meta)', campaign_name: 'IG DM', adset_name: 'Open', spend: '400' },
      { ad_id: 'B1', ad_name: 'Canada ad', campaign_name: 'Form', adset_name: 'CR', spend: '500' },
      { ad_id: 'K1', ad_name: 'Dud ad', campaign_name: 'Form', adset_name: 'CR', spend: '220' },
    ] };
    if (p === '/') return { C1: { name: 'Old ad (Meta)', campaign: { name: 'MX' } } };
    throw new Error(`unexpected meta ${p}`);
  },
};

check('gather: per-ad counts, people not rows', async () => {
  const out = await lib.gatherAdAttribution(deps, OCT);
  const A = out.rows.find((r) => r.adKey === 'A1');
  const B = out.rows.find((r) => r.adKey === 'B1');
  const org = out.rows.find((r) => r.adKey === lib.ORGANIC);
  assert.deepStrictEqual([A.leads, A.booked, A.held, A.wins, A.revenue], [1, 1, 1, 1, 5000], 'A1 counts (dup appointment and dup win collapse)');
  assert.deepStrictEqual([B.leads, B.booked, B.held, B.wins], [1, 1, 1, 1], 'B1 counts');
  assert.deepStrictEqual([org.leads, org.booked, org.held, org.wins], [1, 1, 1, 1], 'organic counts');
  assert.strictEqual(A.adName, 'House ad (Meta)', 'Meta name wins');
  assert.strictEqual(A.cpl, 400); assert.strictEqual(A.costPerBooked, 400); assert.strictEqual(A.cac, 400);
});
check('gather: webinar registrant is not a lead', async () => {
  const out = await lib.gatherAdAttribution(deps, OCT);
  assert.strictEqual(out.totals.leads, 2);
});
check('gather: GHL-only win is credited and flagged, other pipelines ignored', async () => {
  const out = await lib.gatherAdAttribution(deps, OCT);
  const C = out.rows.find((r) => r.adKey === 'C1');
  assert.strictEqual(C.wins, 1);
  assert.strictEqual(C.adName, 'Old ad (Meta)', 'name filled from Meta ids lookup');
  assert.deepStrictEqual(out.crossCheck.ghlOnly.map((x) => x.contactId), ['cG']);
  assert.ok(!out.wins.some((w) => w.name === 'Webinar buyer'));
});
check('gather: portal win with an open GHL deal is flagged', async () => {
  const out = await lib.gatherAdAttribution(deps, OCT);
  assert.deepStrictEqual(out.crossCheck.portalOnly.map((x) => [x.contactId, x.oppStatus, x.oppValue]), [['cB', 'open', 2900]]);
});
check('gather: restated iClosed win lands in its call month, not October', async () => {
  const oct = await lib.gatherAdAttribution(deps, OCT);
  assert.strictEqual(oct.totals.wins + oct.totals.organic.wins, 4);
  assert.ok(!oct.wins.some((w) => w.name === 'Lalo L'));
  const may = await lib.gatherAdAttribution(deps, lib.crMonthBounds('2026-05'));
  assert.ok(may.wins.some((w) => w.name === 'Lalo L'), 'May has it');
});
check('gather: closes list carries ad and closer', async () => {
  const out = await lib.gatherAdAttribution(deps, OCT);
  const lines = lib.formatWins(out.wins, (id) => (id === 'ron@x' ? 'Ron' : id));
  assert.ok(lines.some((l) => l.includes('Ana A') && l.includes('Ron') && l.includes('House ad (Meta)')));
  assert.ok(lines.some((l) => l.includes('Olga O') && l.includes('organic (whatsapp_coex)')));
});

check('ratio: no spend means n/a, never $0', () => {
  const people = new Map([['x', { ad: { adId: 'Z', adName: 'Z ad' }, lead: false, booked: true, held: true, win: { revenue: 1 } }]]);
  const [r] = lib.buildAdRows(people, {});
  assert.strictEqual(r.cac, null); assert.strictEqual(r.costPerBooked, null);
  assert.ok(lib.adLine(r).includes('CAC n/a'));
});
check('scale / kill', async () => {
  const out = await lib.gatherAdAttribution(deps, OCT);
  const sk = lib.rankScaleKill(out.rows);
  assert.ok(sk.scale.some((r) => r.adKey === 'A1'));
  assert.deepStrictEqual(sk.kill.map((r) => r.adKey), [], 'Dud ad $220 is under 1.5x the $450 account $/booking');
  const sk2 = lib.rankScaleKill([{ adKey: 'K', adName: 'K', spend: 300, booked: 0, wins: 0, leads: 0 }, { adKey: 'G', adName: 'G', spend: 100, booked: 2, wins: 0, leads: 0, costPerBooked: 50 }]);
  assert.deepStrictEqual(sk2.kill.map((r) => r.adKey), ['K']);
});
check('grouped names', () => {
  assert.deepStrictEqual(lib.groupedAdNames([{ adName: 'X', spend: 10 }, { adName: 'X', spend: 5 }, { adName: 'Y', spend: 1 }], { money: true }), ['X (x2) ($15)', 'Y ($1)']);
});
check('month helpers (CR)', () => {
  assert.deepStrictEqual(lib.crMonthBounds('2026-12'), { month: '2026-12', startIso: '2026-12-01T06:00:00.000Z', endIso: '2027-01-01T06:00:00.000Z', metaSince: '2026-12-01', metaUntil: '2026-12-31' });
  assert.strictEqual(lib.prevMonth('2026-01'), '2025-12');
  assert.strictEqual(lib.crMonthOf(new Date('2026-11-01T05:00:00Z')), '2026-10');
});
check('month delta names changes, not the roster', () => {
  const cur = [{ adKey: 'A', adName: 'A ad', wins: 2, booked: 4, spend: 200, leads: 9, held: 3, revenue: 10000 }];
  const prev = [{ ad_key: 'B', ad_name: 'B ad', wins: 1, booked: 2, spend: 300 }];
  const lines = lib.monthDeltaLines(cur, prev);
  assert.ok(lines[0].startsWith('Closes from ads 1 → 2 ▲'));
  assert.ok(lines.includes('• New closer: A ad (2 won)'));
  assert.ok(lines.includes('• Stopped closing: B ad (1 won last month, 0 now)'));
  assert.deepStrictEqual(lib.monthDeltaLines(cur, []), ['No previous month in history to compare.']);
});
check('history rows hold no person data', () => {
  const [h] = lib.historyRows('2026-10', [{ adKey: 'A', adId: 'A', adName: 'A ad', campaign: 'c', adSet: 's', leads: 1, booked: 1, held: 1, wins: 1, revenue: 5, spend: 9 }]);
  assert.deepStrictEqual(Object.keys(h).sort(), ['ad_id', 'ad_key', 'ad_name', 'ad_set', 'booked', 'campaign', 'held', 'leads', 'month', 'revenue', 'spend', 'wins']);
  assert.strictEqual(h.month, '2026-10-01');
});

// Wiring in index.js.
const SRC = fs.readFileSync(path.join(__dirname, '..', 'index.js'), 'utf8');
check('get_ad_performance is Ron-only, get_lead_ad_source is not', () => {
  const ronOnly = SRC.slice(SRC.indexOf('const RON_ONLY_TOOLS'), SRC.indexOf(']);', SRC.indexOf('const RON_ONLY_TOOLS')));
  assert.ok(ronOnly.includes("'get_ad_performance'"));
  assert.ok(!ronOnly.includes("'get_lead_ad_source'"));
});
check('both tools are defined and dispatched', () => {
  for (const t of ['get_ad_performance', 'get_lead_ad_source']) {
    assert.ok(SRC.includes(`{ name: '${t}'`), `${t} defined`);
    assert.ok(SRC.includes(`toolUse.name === '${t}'`), `${t} dispatched`);
  }
});
check('weekly recap carries BY AD and the monthly block', () => {
  const recap = SRC.slice(SRC.indexOf('async function runWeeklySalesMarketingRecap'), SRC.indexOf('// MARKETING VOC'));
  assert.ok(recap.includes("'`BY AD`"));
  assert.ok(recap.includes('buildMonthlyAdBlock('));
  assert.ok(recap.includes('save: !preview'), 'preview never writes history');
  assert.ok(recap.includes('producingOnly: true'), 'weekly shows only ads with calls or closes');
  const fmt = SRC.slice(SRC.indexOf('function formatAdAttribution'), SRC.indexOf('async function loadAdHistory'));
  assert.ok(fmt.includes('!producingOnly || o.booked || o.wins'), 'organic shows in the weekly when it brought calls or closes');
});
check('team tool never prints money', () => {
  const fn = SRC.slice(SRC.indexOf('async function getLeadAdSourceTool'), SRC.indexOf('async function runWeeklySalesMarketingRecap'));
  assert.ok(!/revenue|spend|monetaryValue|closed_revenue|\$\{usd/.test(fn));
});
check('migration exists with explicit grants', () => {
  const sql = fs.readFileSync(path.join(__dirname, '..', 'migrations', '024_ad_attribution_monthly.sql'), 'utf8');
  assert.ok(/CREATE TABLE IF NOT EXISTS ad_attribution_monthly/.test(sql));
  assert.ok(/PRIMARY KEY \(month, ad_key\)/.test(sql));
  assert.ok(/GRANT SELECT, INSERT, UPDATE ON ad_attribution_monthly TO anon, authenticated/.test(sql));
});

Promise.all(pending).then(() => {
  if (failures) { console.error(`\n${failures} failing`); process.exit(1); }
  console.log('\nall ad attribution checks passed');
});
