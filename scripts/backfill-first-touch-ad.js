#!/usr/bin/env node
// One-off backfill of the "Anuncio de origen" contact field (lib/firstTouchAd.js)
// for contacts created before the intake stamp shipped.
//
//   node scripts/backfill-first-touch-ad.js                       # dry run, since 2026-10-01
//   node scripts/backfill-first-touch-ad.js --since 2026-09-01    # dry run, other start
//   node scripts/backfill-first-touch-ad.js --write               # write the field
//
// Only contacts whose FIRST touch carries a Meta ad id are touched, and a field
// that already has a value is never overwritten. The field must already exist
// in GHL; this script never creates it. Prints contact ids and counts only, no
// names (this repo is public, keep pasted output clean too).
//
// Needs GHL_API_KEY + GHL_LOCATION_ID (from .env, same as index.js).

try { require('dotenv').config(); } catch { /* env may already be set */ }
const lib = require('../lib/firstTouchAd');

const args = process.argv.slice(2);
const WRITE = args.includes('--write');
const sinceIdx = args.indexOf('--since');
const SINCE = sinceIdx >= 0 ? args[sinceIdx + 1] : '2026-10-01';
const BASE = 'https://services.leadconnectorhq.com';
const LOC = process.env.GHL_LOCATION_ID;
const HEADERS = { 'Authorization': `Bearer ${process.env.GHL_API_KEY}`, 'Version': '2021-07-28', 'Content-Type': 'application/json' };

async function ghl(method, p, body) {
  const res = await fetch(BASE + p, { method, headers: HEADERS, body: body ? JSON.stringify(body) : undefined });
  const text = await res.text();
  if (!res.ok) throw new Error(`${method} ${p} → ${res.status} ${text.slice(0, 200)}`);
  return text ? JSON.parse(text) : {};
}

async function main() {
  if (!process.env.GHL_API_KEY || !LOC) throw new Error('GHL_API_KEY and GHL_LOCATION_ID are required');
  const fields = (await ghl('GET', `/locations/${LOC}/customFields`)).customFields || [];
  const field = fields.find((f) => String(f.name || '').trim().toLowerCase() === lib.AD_FIELD_NAME.toLowerCase());
  if (!field && WRITE) throw new Error(`GHL custom field "${lib.AD_FIELD_NAME}" does not exist. Create it first (single line text).`);
  if (!field) console.log(`note: field "${lib.AD_FIELD_NAME}" does not exist yet; dry run counts every ad contact as to-write`);

  const sinceMs = Date.parse(`${SINCE}T00:00:00Z`);
  const counts = { scanned: 0, withAd: 0, alreadySet: 0, toWrite: 0, written: 0, failed: 0 };
  let searchAfter;
  for (;;) {
    const body = {
      locationId: LOC, pageLimit: 100,
      filters: [{ field: 'dateAdded', operator: 'range', value: { gte: new Date(sinceMs).toISOString() } }],
      sort: [{ field: 'dateAdded', direction: 'asc' }],
    };
    if (searchAfter) body.searchAfter = searchAfter;
    const page = await ghl('POST', '/contacts/search', body);
    const contacts = page.contacts || [];
    for (const c of contacts) {
      counts.scanned++;
      const ad = lib.firstTouchAd(c);
      if (!ad) continue;
      counts.withAd++;
      if (field && lib.hasAdField(c, field.id)) { counts.alreadySet++; continue; }
      counts.toWrite++;
      const value = lib.adFieldValue(ad);
      if (!WRITE) { console.log(`would stamp ${c.id}: ${value}`); continue; }
      try {
        await ghl('PUT', `/contacts/${c.id}`, { customFields: [{ id: field.id, field_value: value }] });
        counts.written++;
        console.log(`stamped ${c.id}: ${value}`);
      } catch (e) {
        counts.failed++;
        console.error(`FAILED ${c.id}: ${e.message}`);
      }
    }
    if (contacts.length < 100) break;
    searchAfter = contacts[contacts.length - 1].searchAfter;
    if (!searchAfter) break;
  }
  console.log(`\n${WRITE ? 'WRITE' : 'DRY RUN'} since ${SINCE}:`, JSON.stringify(counts));
}

main().catch((e) => { console.error(e.message); process.exit(1); });
