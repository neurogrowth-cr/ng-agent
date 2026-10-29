// Ad attribution: leads, bookings, held calls and closes per Meta ad, plus the
// spend Meta reports for the same ad, so Ron can see CPL, cost per booking,
// cost per held call and CAC per ad (Ron, 2026-10-28).
//
// Every person is credited to the ad of their FIRST touch (the GHL contact's
// `attributionSource`, see lib/firstTouchAd.js); a person with no ad on the
// first touch is "organic". Counts are people, not rows: a reschedule or a
// double booking is one booking, and a deal won twice in two systems is one win.
//
// Wins come from BOTH systems and are cross-checked: the portal's
// revops_sales_outcomes (outcome won) and GHL opportunities with status won in
// the sales pipelines. Either one counts; a mismatch is listed so it can be
// fixed (Pablo Guerrero sat as an open $2,900 GHL deal while the portal had
// him won at $5,000, so the Purchase CAPI never fired).
//
// Pure helpers at the top; `gatherAdAttribution(deps, window)` does the I/O
// through injected clients so index.js and scripts/ share one path.

const { firstTouchAd } = require('./firstTouchAd');

const ORGANIC = 'organic';
const SALES_PIPELINE_IDS = new Set(['KH1IQuaN8aNB1lfRpvP4', '7KU0NBdMhhVifszmT9jo']);
// Outcomes that mean the prospect was on the call (mirrors SHOWED_OUTCOMES in index.js).
const SHOWED = new Set(['won', 'follow_up', 'disqualified', 'lost', 'nurture']);
const WEBINAR_RE = /webinar/i;
// GHL cutover. iClosed-era wins restated in bulk after it (2026-10-02: five
// April to July calls got that close_date) are dated by their call instead.
const GHL_CUTOVER_ISO = '2026-07-23T00:00:00.000Z';
const RESTATED_GAP_DAYS = 60;

// Effective close date for a portal win: close_date (else the log date), except
// an iClosed-era call whose close_date lands more than RESTATED_GAP_DAYS after
// the call and after the cutover, which is a restatement, so the call date wins.
function portalWinDate(outcome, appt) {
  const logged = outcome.close_date ? `${outcome.close_date}T12:00:00.000Z` : outcome.created_at;
  const call = appt && appt.scheduled_start;
  if (call && appt.source === 'iclosed' && Date.parse(logged) >= Date.parse(GHL_CUTOVER_ISO)
      && Date.parse(logged) - Date.parse(call) > RESTATED_GAP_DAYS * 86400000) {
    return new Date(call).toISOString();
  }
  return logged;
}

// ── Pure helpers ────────────────────────────────────────────────────────────

function num(v) { const n = Number(v); return Number.isFinite(n) ? n : 0; }
// null when either side is zero: an ad with no spend in the window has no
// cost per anything (its spend happened in another month).
function ratio(a, b) { return a > 0 && b > 0 ? +(a / b).toFixed(2) : null; }

function adKeyOf(ad) { return ad && ad.adId ? ad.adId : ORGANIC; }

// GHL opportunity first-touch attribution in the same shape as firstTouchAd.
function oppFirstTouchAd(opp) {
  const first = ((opp && opp.attributions) || []).find((a) => a.isFirst) || null;
  if (!first || !first.utmAdId) return null;
  return {
    adId: String(first.utmAdId),
    adName: first.utmContent || null,
    campaign: first.utmCampaign || null,
    adSet: first.utmMedium || null,
  };
}

function isDeletedAppt(a) {
  const s = (a && a.qualification_snapshot) || {};
  return s.reason === 'deleted_in_ghl';
}

function isSalesLeadContact(c) {
  if (!c) return false;
  if (WEBINAR_RE.test(String(c.source || ''))) return false;
  const tags = (c.tags || []).map((t) => String(t).toLowerCase());
  if (tags.includes('personal')) return false;
  if (tags.some((t) => WEBINAR_RE.test(t))) return false;
  return true;
}

// One row per ad key. `people` maps contactId -> { ad, lead, booked, held, win }.
function buildAdRows(people, spendByAd) {
  const rows = new Map();
  const row = (key, ad) => {
    if (!rows.has(key)) {
      rows.set(key, {
        adKey: key, adId: key === ORGANIC ? null : key,
        adName: key === ORGANIC ? 'Organic (no ad)' : (ad && ad.adName) || null,
        campaign: (ad && ad.campaign) || null, adSet: (ad && ad.adSet) || null,
        leads: 0, booked: 0, held: 0, wins: 0, revenue: 0, spend: 0,
      });
    }
    const r = rows.get(key);
    if (ad && !r.adName) r.adName = ad.adName;
    if (ad && !r.campaign) r.campaign = ad.campaign;
    if (ad && !r.adSet) r.adSet = ad.adSet;
    return r;
  };
  for (const p of people.values()) {
    const r = row(adKeyOf(p.ad), p.ad);
    if (p.lead) r.leads += 1;
    if (p.booked) r.booked += 1;
    if (p.held) r.held += 1;
    if (p.win) { r.wins += 1; r.revenue += num(p.win.revenue); }
  }
  for (const [adId, s] of Object.entries(spendByAd || {})) {
    const r = row(adId, { adName: s.adName, campaign: s.campaign, adSet: s.adSet });
    r.spend += num(s.spend);
    if (s.adName) r.adName = s.adName; // Meta's current name wins over the UTM copy
  }
  for (const r of rows.values()) {
    if (!r.adName) r.adName = `ad ${r.adId}`;
    r.spend = +r.spend.toFixed(2);
    r.cpl = ratio(r.spend, r.leads);
    r.costPerBooked = ratio(r.spend, r.booked);
    r.costPerHeld = ratio(r.spend, r.held);
    r.cac = ratio(r.spend, r.wins);
    r.leadToClosePct = r.leads > 0 ? Math.round((r.wins / r.leads) * 100) : null;
  }
  return [...rows.values()].sort((a, b) =>
    (b.wins - a.wins) || (b.booked - a.booked) || (b.leads - a.leads) || (b.spend - a.spend));
}

function totalsOf(rows) {
  const ads = rows.filter((r) => r.adKey !== ORGANIC);
  const sum = (k, list = ads) => list.reduce((s, r) => s + num(r[k]), 0);
  const t = {
    spend: +sum('spend').toFixed(2), leads: sum('leads'), booked: sum('booked'), held: sum('held'),
    wins: sum('wins'), revenue: sum('revenue'),
  };
  t.cpl = ratio(t.spend, t.leads);
  t.costPerBooked = ratio(t.spend, t.booked);
  t.costPerHeld = ratio(t.spend, t.held);
  t.cac = ratio(t.spend, t.wins);
  const org = rows.find((r) => r.adKey === ORGANIC);
  t.organic = org ? { leads: org.leads, booked: org.booked, held: org.held, wins: org.wins, revenue: org.revenue } : { leads: 0, booked: 0, held: 0, wins: 0, revenue: 0 };
  return t;
}

// SCALE / KILL for the monthly block. Recommendations only: Max never touches
// campaigns. Scale = it closed, or it books at or below the account's cost per
// booking with at least 2 bookings. Kill = real spend and no booking at all.
const KILL_MIN_SPEND = 50;
function rankScaleKill(rows) {
  const ads = rows.filter((r) => r.adKey !== ORGANIC);
  const t = totalsOf(rows);
  const avg = t.costPerBooked;
  const scale = ads.filter((r) => r.wins > 0
    || (r.booked >= 2 && avg != null && r.costPerBooked != null && r.costPerBooked <= avg));
  const killFloor = Math.max(KILL_MIN_SPEND, avg != null ? avg * 1.5 : 0);
  const kill = ads.filter((r) => r.booked === 0 && r.wins === 0 && r.spend >= killFloor)
    .sort((a, b) => b.spend - a.spend);
  return { scale, kill, avgCostPerBooked: avg, killFloor: +killFloor.toFixed(2) };
}

// "Canada Ad MX (x3)": one creative often runs as several ad ids across ad sets.
function groupedAdNames(rows, { money = false } = {}) {
  const by = new Map();
  for (const r of rows) {
    const g = by.get(r.adName) || { n: 0, spend: 0 };
    g.n += 1; g.spend += num(r.spend);
    by.set(r.adName, g);
  }
  return [...by.entries()].map(([name, g]) => `${name}${g.n > 1 ? ` (x${g.n})` : ''}${money ? ` ($${Math.round(g.spend)})` : ''}`);
}

// Lines for the Sunday ad sweep note (agent_knowledge intel ad_sweep:<week>,
// read by the Monday reflection brief). Same SCALE / KILL rule as the monthly
// block, grouped by ad name. `SCALE: <name> —` is the shape the next sweep
// parses to mark "2nd week running", so keep it.
function sweepLines(rows, { days = 30, prevScale = new Set(), limit = 3 } = {}) {
  const sk = rankScaleKill(rows);
  const t = totalsOf(rows);
  const group = (list) => {
    const by = new Map();
    for (const r of list) {
      const g = by.get(r.adName) || { name: r.adName, wins: 0, booked: 0, spend: 0 };
      g.wins += r.wins; g.booked += r.booked; g.spend += num(r.spend);
      by.set(r.adName, g);
    }
    return [...by.values()];
  };
  const scale = group(sk.scale).sort((a, b) => (b.wins - a.wins) || (b.booked - a.booked)).slice(0, limit);
  const kill = group(sk.kill).sort((a, b) => b.spend - a.spend).slice(0, limit);
  const lines = [`Ad ranking, last ${days} days (first-touch credit: bookings and closes, not CPL). Ads: ${t.booked} booked, ${t.wins} won, $/booking ${usd2(t.costPerBooked)}, CAC ${usd(t.cac)}.`];
  for (const g of scale) {
    const per = g.booked && g.spend ? ` at ${usd2(g.spend / g.booked)}/booking` : '';
    lines.push(`SCALE: ${g.name} — ${g.wins} won, ${g.booked} booked${per}${prevScale.has(g.name) ? ' (2nd week running)' : ''}`);
  }
  for (const g of kill) lines.push(`KILL: ${g.name} — ${usd(g.spend)} spend, 0 bookings`);
  if (!scale.length && !kill.length) lines.push(`No ad stands out for bookings or closes in the last ${days} days.`);
  return { lines, scale: scale.length, kill: kill.length };
}

const usd = (v) => (v == null ? 'n/a' : `$${Math.round(num(v)).toLocaleString('en-US')}`);
const usd2 = (v) => (v == null ? 'n/a' : `$${num(v).toFixed(2)}`);

function adLine(r) {
  const head = r.adKey === ORGANIC ? '*Organic (no ad)*' : `*${r.adName}*${r.campaign ? ` (${r.campaign})` : ''}`;
  const counts = `${r.leads} leads · ${r.booked} booked · ${r.held} held · ${r.wins} won${r.wins ? ` (${usd(r.revenue)})` : ''}`;
  if (r.adKey === ORGANIC) return `• ${head}: ${counts}`;
  return `• ${head}: ${counts} · spend ${usd(r.spend)} · CPL ${usd2(r.cpl)} · $/booking ${usd2(r.costPerBooked)} · $/held ${usd2(r.costPerHeld)} · CAC ${usd(r.cac)}`;
}

function formatTotals(t) {
  return `Ads total: spend ${usd(t.spend)} · ${t.leads} leads · ${t.booked} booked · ${t.held} held · ${t.wins} won (${usd(t.revenue)}) · CPL ${usd2(t.cpl)} · $/booking ${usd2(t.costPerBooked)} · $/held ${usd2(t.costPerHeld)} · CAC ${usd(t.cac)}`;
}

function formatWins(wins, nameOf = (x) => x) {
  if (!wins.length) return ['No closes in this window.'];
  return wins.map((w) => {
    const src = w.ad ? `${w.ad.adName || `ad ${w.ad.adId}`}${w.ad.campaign ? ` (${w.ad.campaign})` : ''}` : `organic${w.medium ? ` (${w.medium})` : ''}`;
    return `• ${w.name || 'Unknown'}: ${usd(w.revenue)}${w.closer ? ` · ${nameOf(w.closer)}` : ''} · ${src} · ${w.date || 'no date'}`;
  });
}

function formatCrossCheck(cc) {
  const out = [];
  for (const x of cc.portalOnly) out.push(`• ${x.name || x.contactId || 'Unknown'}: won in the portal, GHL deal not marked Won${x.oppStatus ? ` (${x.oppStatus}${x.oppValue != null ? `, ${usd(x.oppValue)}` : ''})` : ' (no sales deal found)'}`);
  for (const x of cc.ghlOnly) out.push(`• ${x.name || x.contactId}: Won in GHL (${usd(x.revenue)}), no won outcome in the portal`);
  for (const x of cc.unresolved) out.push(`• ${x.name || 'Unknown'}: won in the portal, no GHL contact found (no ad can be credited)`);
  return out;
}

// Month helpers, CR (UTC-6, no DST).
function crMonthBounds(month) {
  const [y, m] = month.split('-').map(Number);
  const pad = (n) => String(n).padStart(2, '0');
  const next = m === 12 ? `${y + 1}-01` : `${y}-${pad(m + 1)}`;
  const lastDay = new Date(Date.UTC(y, m, 0)).getUTCDate();
  return {
    month,
    startIso: `${month}-01T06:00:00.000Z`,
    endIso: `${next}-01T06:00:00.000Z`,
    metaSince: `${month}-01`,
    metaUntil: `${month}-${pad(lastDay)}`,
  };
}
function crMonthOf(date) {
  return new Date(date.getTime() - 6 * 3600 * 1000).toISOString().slice(0, 7);
}
function prevMonth(month) {
  const [y, m] = month.split('-').map(Number);
  return m === 1 ? `${y - 1}-12` : `${y}-${String(m - 1).padStart(2, '0')}`;
}

// History rows for ad_attribution_monthly.
function historyRows(month, rows) {
  return rows.map((r) => ({
    month: `${month}-01`, ad_key: r.adKey, ad_id: r.adId, ad_name: r.adName,
    campaign: r.campaign, ad_set: r.adSet, leads: r.leads, booked: r.booked, held: r.held,
    wins: r.wins, revenue: r.revenue, spend: r.spend,
  }));
}

// What changed vs the previous month, from history rows. Names the delta,
// never the roster.
function monthDeltaLines(curRows, prevHist) {
  if (!prevHist || !prevHist.length) return ['No previous month in history to compare.'];
  const prev = new Map(prevHist.map((h) => [h.ad_key, h]));
  const cur = new Map(curRows.map((r) => [r.adKey, r]));
  const lines = [];
  const pt = prevHist.filter((h) => h.ad_key !== ORGANIC);
  const pSpend = pt.reduce((s, h) => s + num(h.spend), 0);
  const pWins = pt.reduce((s, h) => s + num(h.wins), 0);
  const pBooked = pt.reduce((s, h) => s + num(h.booked), 0);
  const t = totalsOf(curRows);
  const arrow = (a, b) => (a > b ? '▲' : a < b ? '▼' : '=');
  lines.push(`Closes from ads ${pWins} → ${t.wins} ${arrow(t.wins, pWins)} · bookings ${pBooked} → ${t.booked} ${arrow(t.booked, pBooked)} · CAC ${usd(ratio(pSpend, pWins))} → ${usd(t.cac)} · $/booking ${usd2(ratio(pSpend, pBooked))} → ${usd2(t.costPerBooked)}`);
  for (const r of curRows) {
    if (r.adKey === ORGANIC || r.wins === 0) continue;
    const p = prev.get(r.adKey);
    if (!p || num(p.wins) === 0) lines.push(`• New closer: ${r.adName} (${r.wins} won)`);
  }
  for (const h of prevHist) {
    if (h.ad_key === ORGANIC || num(h.wins) === 0) continue;
    const c = cur.get(h.ad_key);
    if (!c || c.wins === 0) lines.push(`• Stopped closing: ${h.ad_name} (${h.wins} won last month, 0 now)`);
  }
  return lines;
}

// ── I/O ─────────────────────────────────────────────────────────────────────
// deps: { ghl(method, path, body) -> json, meta(path, query) -> json,
//         portal (supabase client), locationId, adAccountId, excludeIds (Set),
//         now (Date) }

async function ghlContactsCreatedIn(deps, startIso, endIso) {
  const out = [];
  let searchAfter;
  for (let page = 0; page < 60; page++) {
    const body = {
      locationId: deps.locationId, pageLimit: 100,
      filters: [{ field: 'dateAdded', operator: 'range', value: { gte: startIso, lte: endIso } }],
      sort: [{ field: 'dateAdded', direction: 'asc' }],
    };
    if (searchAfter) body.searchAfter = searchAfter;
    const res = await deps.ghl('POST', '/contacts/search', body);
    const list = res.contacts || [];
    out.push(...list);
    if (list.length < 100 || !list[list.length - 1].searchAfter) break;
    searchAfter = list[list.length - 1].searchAfter;
  }
  return out.filter((c) => Date.parse(c.dateAdded) < Date.parse(endIso));
}

async function ghlAllWonOpps(deps) {
  const out = [];
  let after = '';
  for (let page = 0; page < 30; page++) {
    const res = await deps.ghl('GET', `/opportunities/search?location_id=${deps.locationId}&status=won&limit=100${after}`);
    const list = res.opportunities || [];
    out.push(...list);
    const m = res.meta || {};
    if (list.length < 100 || !m.startAfterId) break;
    after = `&startAfter=${m.startAfter}&startAfterId=${m.startAfterId}`;
  }
  return out.filter((o) => SALES_PIPELINE_IDS.has(o.pipelineId));
}

async function metaSpendByAd(deps, since, until) {
  const spend = {};
  let after = null;
  for (let page = 0; page < 20; page++) {
    const query = {
      level: 'ad', fields: 'ad_id,ad_name,adset_name,campaign_name,spend',
      time_range: JSON.stringify({ since, until }), limit: '500',
    };
    if (after) query.after = after;
    const res = await deps.meta(`/${deps.adAccountId}/insights`, query);
    for (const d of res.data || []) {
      if (!d.ad_id) continue;
      spend[d.ad_id] = { spend: num(d.spend), adName: d.ad_name, campaign: d.campaign_name, adSet: d.adset_name };
    }
    after = res.paging && res.paging.next ? res.paging.cursors && res.paging.cursors.after : null;
    if (!after) break;
  }
  return spend;
}

async function gatherAdAttribution(deps, win) {
  const { startIso, endIso, metaSince, metaUntil } = win;
  const startMs = Date.parse(startIso);
  const endMs = Date.parse(endIso);
  const nowMs = (deps.now || new Date()).getTime();
  const inWin = (iso) => { const t = Date.parse(iso); return t >= startMs && t < endMs; };
  const errors = [];
  const people = new Map();       // contactId -> person
  const contactCache = new Map(); // contactId -> contact | null
  const person = (cid) => {
    if (!people.has(cid)) people.set(cid, { contactId: cid, ad: null, medium: null, lead: false, booked: false, held: false, win: null, name: null });
    return people.get(cid);
  };
  const getContact = async (cid) => {
    if (contactCache.has(cid)) return contactCache.get(cid);
    let c = null;
    try { c = (await deps.ghl('GET', `/contacts/${cid}`)).contact || null; } catch (e) { errors.push(`contact ${cid}: ${e.message}`); }
    contactCache.set(cid, c);
    return c;
  };
  const stampAd = (p, c) => {
    if (!c) return;
    p.ad = p.ad || firstTouchAd(c);
    p.medium = p.medium || (c.attributionSource && c.attributionSource.medium) || null;
    p.name = p.name || [c.firstName, c.lastName].filter(Boolean).join(' ') || c.contactName || null;
  };

  // 1. Leads: sales contacts created in the window.
  const contacts = await ghlContactsCreatedIn(deps, startIso, endIso);
  for (const c of contacts) {
    contactCache.set(c.id, c);
    if (!isSalesLeadContact(c)) continue;
    const p = person(c.id);
    p.lead = true;
    stampAd(p, c);
  }

  // 2. Appointments: booked in the window, and scheduled in the window.
  const sel = 'id, prospect_id, booked_at, scheduled_start, attended, source, iclosed_call_id, ghl_appointment_id, closer_id, qualification_snapshot';
  const [{ data: bookedRaw, error: bErr }, { data: schedRaw, error: sErr }] = await Promise.all([
    deps.portal.from('revops_appointments').select(sel).gte('booked_at', startIso).lt('booked_at', endIso),
    deps.portal.from('revops_appointments').select(sel).gte('scheduled_start', startIso).lt('scheduled_start', endIso),
  ]);
  if (bErr) throw new Error(`appointments (booked): ${bErr.message}`);
  if (sErr) throw new Error(`appointments (scheduled): ${sErr.message}`);
  const flywheel = (rows) => (rows || []).filter((a) => {
    if (typeof a.ghl_appointment_id === 'string' && a.ghl_appointment_id.startsWith('opp:')) return false;
    if (deps.excludeIds && a.iclosed_call_id && deps.excludeIds.has(a.iclosed_call_id)) return false;
    return !isDeletedAppt(a);
  });
  const booked = flywheel(bookedRaw);
  const sched = flywheel(schedRaw).filter((a) => Date.parse(a.scheduled_start) <= nowMs);

  let outcomesByAppt = {};
  if (sched.length) {
    const { data: outs, error: oErr } = await deps.portal.from('revops_sales_outcomes')
      .select('appointment_id, outcome').in('appointment_id', sched.map((a) => a.id));
    if (oErr) throw new Error(`outcomes: ${oErr.message}`);
    outcomesByAppt = Object.fromEntries((outs || []).map((o) => [o.appointment_id, o]));
  }

  // 3. Wins, portal side: won outcomes whose close date (else log date) is in the window.
  const { data: wonRaw, error: wErr } = await deps.portal.from('revops_sales_outcomes')
    .select('appointment_id, closed_revenue, close_date, created_at').eq('outcome', 'won');
  if (wErr) throw new Error(`won outcomes: ${wErr.message}`);
  const wonAll = wonRaw || [];

  // Prospects for every appointment we touch.
  const apptIds = [...new Set([...booked, ...sched].map((a) => a.id).concat(wonAll.map((o) => o.appointment_id)))];
  const apptById = new Map([...booked, ...sched].map((a) => [a.id, a]));
  const missingAppt = apptIds.filter((id) => !apptById.has(id));
  for (let i = 0; i < missingAppt.length; i += 200) {
    const { data, error } = await deps.portal.from('revops_appointments').select(sel).in('id', missingAppt.slice(i, i + 200));
    if (error) throw new Error(`appointments (wins): ${error.message}`);
    for (const a of data || []) apptById.set(a.id, a);
  }
  const prospectIds = [...new Set([...apptById.values()].map((a) => a.prospect_id).filter(Boolean))];
  const prospectById = new Map();
  for (let i = 0; i < prospectIds.length; i += 200) {
    const { data, error } = await deps.portal.from('revops_prospects')
      .select('id, full_name, email, ghl_contact_id, closer_owner_id').in('id', prospectIds.slice(i, i + 200));
    if (error) throw new Error(`prospects: ${error.message}`);
    for (const p of data || []) prospectById.set(p.id, p);
  }
  const emailCache = new Map();
  const contactIdOf = async (prospect) => {
    if (!prospect) return null;
    if (prospect.ghl_contact_id) return prospect.ghl_contact_id;
    const em = String(prospect.email || '').trim().toLowerCase();
    if (!em) return null;
    if (emailCache.has(em)) return emailCache.get(em);
    let id = null;
    try {
      const r = await deps.ghl('GET', `/contacts/search/duplicate?locationId=${deps.locationId}&email=${encodeURIComponent(em)}`);
      id = (r.contact && r.contact.id) || null;
    } catch (e) { errors.push(`email lookup: ${e.message}`); }
    emailCache.set(em, id);
    return id;
  };

  for (const a of booked) {
    const cid = await contactIdOf(prospectById.get(a.prospect_id));
    if (!cid) continue;
    const p = person(cid);
    p.booked = true;
    stampAd(p, await getContact(cid));
  }
  for (const a of sched) {
    const o = outcomesByAppt[a.id];
    const showed = a.attended === true || (o && SHOWED.has(String(o.outcome || '').toLowerCase()));
    if (!showed) continue;
    const cid = await contactIdOf(prospectById.get(a.prospect_id));
    if (!cid) continue;
    const p = person(cid);
    p.held = true;
    stampAd(p, await getContact(cid));
  }

  // 4. Wins, both systems, merged per contact.
  const wonOpps = await ghlAllWonOpps(deps);
  const ghlWonByContact = new Map();
  for (const o of wonOpps) {
    if (!o.contactId) continue;
    const prev = ghlWonByContact.get(o.contactId);
    if (!prev || Date.parse(o.lastStatusChangeAt || 0) > Date.parse(prev.lastStatusChangeAt || 0)) ghlWonByContact.set(o.contactId, o);
  }
  const portalWonContacts = new Set();
  const cc = { portalOnly: [], ghlOnly: [], unresolved: [] };
  const wins = [];
  const credited = new Set();
  for (const o of wonAll) {
    const appt = apptById.get(o.appointment_id);
    const prospect = appt ? prospectById.get(appt.prospect_id) : null;
    const cid = await contactIdOf(prospect);
    const date = portalWinDate(o, appt);
    // The portal date is the one date a win has, so a deal won in both systems
    // lands in exactly one window.
    if (cid) portalWonContacts.add(cid);
    if (!inWin(date)) continue;
    if (!cid) { cc.unresolved.push({ name: prospect && prospect.full_name }); continue; }
    if (credited.has(cid)) continue; // the same person won on two appointment rows
    credited.add(cid);
    const p = person(cid);
    stampAd(p, await getContact(cid));
    p.name = (prospect && prospect.full_name) || p.name;
    p.win = { revenue: num(o.closed_revenue), date: date.slice(0, 10), closer: (appt && appt.closer_id) || (prospect && prospect.closer_owner_id) || null, source: 'portal' };
    if (!ghlWonByContact.has(cid)) {
      let oppStatus = null; let oppValue = null;
      try {
        const r = await deps.ghl('GET', `/opportunities/search?location_id=${deps.locationId}&contact_id=${cid}&limit=20`);
        const sales = (r.opportunities || []).filter((x) => SALES_PIPELINE_IDS.has(x.pipelineId));
        if (sales[0]) { oppStatus = sales[0].status; oppValue = sales[0].monetaryValue; }
      } catch (e) { errors.push(`opps for ${cid}: ${e.message}`); }
      cc.portalOnly.push({ name: p.name, contactId: cid, oppStatus, oppValue });
    }
  }
  for (const [cid, o] of ghlWonByContact) {
    const date = o.lastStatusChangeAt || o.updatedAt;
    if (!date || !inWin(date)) continue;
    if (portalWonContacts.has(cid)) continue; // dated and credited by the portal row
    cc.ghlOnly.push({ name: o.name, contactId: cid, revenue: num(o.monetaryValue) });
    if (credited.has(cid)) continue;
    credited.add(cid);
    const p = person(cid);
    p.ad = p.ad || oppFirstTouchAd(o);
    if (!p.ad) stampAd(p, await getContact(cid));
    p.name = p.name || o.name;
    p.win = { revenue: num(o.monetaryValue), date: String(date).slice(0, 10), closer: null, source: 'ghl' };
  }

  // 5. Spend per ad from Meta. A Meta failure costs the cost columns only.
  let spendByAd = {};
  let metaOk = true;
  try { spendByAd = await metaSpendByAd(deps, metaSince, metaUntil); } catch (e) { metaOk = false; errors.push(`meta: ${e.message}`); }

  // Ads credited in the window with no spend in it (the money went out in an
  // earlier month) still need a name: ask Meta for those ids.
  const unnamed = [...new Set([...people.values()].map((p) => p.ad && !p.ad.adName && !spendByAd[p.ad.adId] ? p.ad.adId : null).filter(Boolean))];
  const named = [...new Set([...people.values()].map((p) => p.ad && p.ad.adId).filter((id) => id && !spendByAd[id]))];
  if (metaOk && named.length) {
    try {
      const res = await deps.meta('/', { ids: named.slice(0, 50).join(','), fields: 'name,campaign{name},adset{name}' });
      for (const [id, a] of Object.entries(res || {})) {
        if (!a || !a.name) continue;
        for (const p of people.values()) {
          if (p.ad && p.ad.adId === id) p.ad = { ...p.ad, adName: a.name, campaign: (a.campaign && a.campaign.name) || p.ad.campaign, adSet: (a.adset && a.adset.name) || p.ad.adSet };
        }
      }
    } catch (e) { errors.push(`meta ad names: ${e.message}${unnamed.length ? ` (${unnamed.length} unnamed)` : ''}`); }
  }

  // Same ad name as the table: Meta's current name when Meta knows the ad.
  const metaName = (ad) => (ad && spendByAd[ad.adId] && spendByAd[ad.adId].adName ? { ...ad, adName: spendByAd[ad.adId].adName, campaign: spendByAd[ad.adId].campaign || ad.campaign } : ad);
  for (const p of people.values()) if (p.win) wins.push({ name: p.name, revenue: p.win.revenue, closer: p.win.closer, date: p.win.date, ad: metaName(p.ad), medium: p.medium, source: p.win.source });
  wins.sort((a, b) => String(b.date).localeCompare(String(a.date)));

  const rows = buildAdRows(people, spendByAd);
  return { rows, totals: totalsOf(rows), wins, crossCheck: cc, metaOk, errors, contactsScanned: contacts.length };
}

module.exports = {
  ORGANIC, SALES_PIPELINE_IDS, KILL_MIN_SPEND, GHL_CUTOVER_ISO, RESTATED_GAP_DAYS,
  oppFirstTouchAd, portalWinDate, isDeletedAppt, isSalesLeadContact, buildAdRows, totalsOf, rankScaleKill,
  groupedAdNames, sweepLines, adLine, formatTotals, formatWins, formatCrossCheck,
  crMonthBounds, crMonthOf, prevMonth, historyRows, monthDeltaLines,
  gatherAdAttribution,
};
