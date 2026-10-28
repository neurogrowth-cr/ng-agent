#!/usr/bin/env node
// Builds the monthly ad-attribution history (lib/adAttribution.js) for past
// months, the same numbers the first Weekly Recap of each month stores.
//
//   node scripts/ad-attribution-history.js --from 2026-07 --to 2026-10            # dry run, prints
//   node scripts/ad-attribution-history.js --from 2026-07 --to 2026-10 --write    # upsert ad_attribution_monthly
//
// Spend for a past month is what Meta reports today for that date range, so a
// rebuilt month is a reconstruction, not a snapshot taken when it closed.
// Prints ad names and counts only; names of people appear in the closes list
// (run it locally, do not paste the output into this public repo).
//
// Needs PORTAL_SUPABASE_URL + PORTAL_SUPABASE_ANON_KEY (portal reads),
// SUPABASE_URL + SUPABASE_ANON_KEY (history writes), GHL_API_KEY +
// GHL_LOCATION_ID, META_ACCESS_TOKEN + META_AD_ACCOUNT_ID.

try { require('dotenv').config(); } catch { /* env may already be set */ }
const { createClient } = require('@supabase/supabase-js');
const lib = require('../lib/adAttribution');

const args = process.argv.slice(2);
const arg = (k) => { const i = args.indexOf(k); return i >= 0 ? args[i + 1] : null; };
const WRITE = args.includes('--write');
const FROM = arg('--from');
const TO = arg('--to') || FROM;
if (!FROM) { console.error('usage: --from YYYY-MM [--to YYYY-MM] [--write]'); process.exit(1); }

function months(from, to) {
  const out = [];
  for (let m = from; m <= to; ) {
    out.push(m);
    const [y, mo] = m.split('-').map(Number);
    m = mo === 12 ? `${y + 1}-01` : `${y}-${String(mo + 1).padStart(2, '0')}`;
  }
  return out;
}

const deps = {
  locationId: process.env.GHL_LOCATION_ID,
  adAccountId: process.env.META_AD_ACCOUNT_ID,
  portal: createClient(process.env.PORTAL_SUPABASE_URL, process.env.PORTAL_SUPABASE_ANON_KEY),
  excludeIds: new Set(),
  async ghl(method, p, body) {
    const res = await fetch(`https://services.leadconnectorhq.com${p}`, {
      method,
      headers: { Authorization: `Bearer ${process.env.GHL_API_KEY}`, Version: '2021-07-28', 'Content-Type': 'application/json' },
      body: body ? JSON.stringify(body) : undefined,
    });
    if (!res.ok) throw new Error(`${method} ${p.split('?')[0]} → ${res.status}`);
    return res.json();
  },
  async meta(p, query) {
    const qs = new URLSearchParams({ ...query, access_token: process.env.META_ACCESS_TOKEN });
    const res = await fetch(`https://graph.facebook.com/v19.0${p}?${qs}`);
    const json = await res.json();
    if (json.error) throw new Error(json.error.message);
    return json;
  },
};

async function main() {
  const max = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_ANON_KEY);
  for (const month of months(FROM, TO)) {
    const out = await lib.gatherAdAttribution(deps, lib.crMonthBounds(month));
    console.log(`\n=== ${month} (${out.contactsScanned} contacts scanned${out.metaOk ? '' : ', META FAILED'})`);
    console.log(lib.formatTotals(out.totals));
    console.log(`Organic: ${out.totals.organic.leads} leads · ${out.totals.organic.booked} booked · ${out.totals.organic.held} held · ${out.totals.organic.wins} won`);
    for (const r of out.rows.filter((x) => x.booked || x.wins || x.spend >= 20).slice(0, 12)) console.log(lib.adLine(r));
    console.log('Closes:'); for (const l of lib.formatWins(out.wins)) console.log(l);
    const cc = lib.formatCrossCheck(out.crossCheck);
    if (cc.length) { console.log('Cross-check:'); for (const l of cc) console.log(l); }
    const sk = lib.rankScaleKill(out.rows);
    console.log(`Scale: ${lib.groupedAdNames(sk.scale).join(', ') || '-'} | Kill: ${lib.groupedAdNames(sk.kill, { money: true }).join(', ') || '-'}`);
    if (out.errors.length) console.log(`Errors (${out.errors.length}): ${out.errors.slice(0, 3).join(' | ')}`);
    if (WRITE) {
      const { error } = await max.from('ad_attribution_monthly').upsert(lib.historyRows(month, out.rows), { onConflict: 'month,ad_key' });
      if (error) throw new Error(`history upsert ${month}: ${error.message}`);
      console.log(`history: ${out.rows.length} rows written`);
    }
  }
}

main().catch((e) => { console.error(e.message); process.exit(1); });
