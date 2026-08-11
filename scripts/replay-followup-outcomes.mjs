#!/usr/bin/env node
/**
 * Replay the follow-up outcomes that GHL's "Outcome: Follow-up" workflow lost
 * between 2026-07-28 and 2026-08-03.
 *
 * WHY THEY WERE LOST
 *   The workflow shipped its Custom Data key as "\tng_event" (literal TAB prefix).
 *   dash's normalizer reads `customData.ng_event` with no key trimming
 *   (normalize.ts:467), so every delivery fell through to `unknown` -> skipped,
 *   while still returning 200. Ron fixed the key in the GHL UI on 2026-08-06;
 *   that fixes NEW traffic only. This script re-drives the backlog.
 *
 * HOW
 *   Reads the stored raw payloads out of ghl_webhook_deliveries, rewrites the
 *   tab-prefixed key to a clean `ng_event`, and re-POSTs them to the real
 *   ingestion endpoint. Correcting the key changes the payload hash, so the
 *   idempotency guard does not block the re-POST.
 *
 *   It deliberately goes through the webhook rather than writing revops_* rows
 *   directly: process-event.ts also calls applyOutcomeToProspect(), whose
 *   mergeProspectStatus() precedence rules are not worth hand-rolling in SQL.
 *
 * SAFETY
 *   - Dry run by default. Pass --live to actually POST.
 *   - Only replays the 8 contacts verified SAFE in the 2026-08-06 dry run:
 *     each has a prospect, exactly one appointment whose scheduled_start
 *     PREDATES the original event, and no existing outcome row to overwrite.
 *   - The 5 contacts with no prospect and no appointment are intentionally
 *     excluded: process-event would create an orphan prospect and then bail with
 *     `missing_appointment_id_for_outcome`, polluting revops_prospects for no gain.
 *   - Re-run safe: skips any contact that already has a follow_up outcome.
 *
 * USAGE
 *   node scripts/replay-followup-outcomes.mjs               # dry run
 *   GHL_WEBHOOK_SECRET=... node scripts/replay-followup-outcomes.mjs --live
 */

import 'dotenv/config';
import { createClient } from '@supabase/supabase-js';

const LIVE = process.argv.includes('--live');
const ENDPOINT = 'https://dash.neurogrowth.io/api/integrations/ghl/webhook';

// Verified SAFE on 2026-08-06 — see tasks/todo.md. Latest appointment predates
// the original event for every one of these, so the outcome cannot land on a
// call the prospect booked later.
const SAFE_CONTACT_IDS = [
  'uJG4mPeEWkC6nWZdGKYy', // Amy
  'XszebpLKYp5L3RmBojY1', // Randall Pj
  'T9uvVS9dZJmgnb2ETaBI', // Pedro Lozano
  'lezhUqFxNDGU8SpJF4Eq', // Marco Duran
  '5BjKe7N2hXY4ouihRdqq', // Luis Santillán
  'jdn4sCKCJawtKNKSfnVZ', // Josue Vargas Castro
  'UXfSmhTyrijTqTVuaa8c', // Anto
  'E876CTaXpkiR4E09qC56', // Alejandra Gonzg
];

const url = process.env.PORTAL_SUPABASE_URL;
const key = process.env.PORTAL_SUPABASE_ANON_KEY;
if (!url || !key) {
  console.error('Missing PORTAL_SUPABASE_URL / PORTAL_SUPABASE_ANON_KEY in .env');
  process.exit(1);
}
const secret = process.env.GHL_WEBHOOK_SECRET;
if (LIVE && !secret) {
  console.error('--live requires GHL_WEBHOOK_SECRET (Vercel env of dash-neurogrowth-io).');
  process.exit(1);
}

const supabase = createClient(url, key);

/** Rewrite any whitespace-padded ng_event key to the clean one. */
function repairCustomData(payload) {
  const cd = payload?.customData;
  if (!cd || typeof cd !== 'object') return { payload, repaired: false };
  const bad = Object.keys(cd).find((k) => k !== 'ng_event' && k.trim() === 'ng_event');
  if (!bad) return { payload, repaired: false };
  const fixed = {};
  for (const [k, v] of Object.entries(cd)) fixed[k === bad ? 'ng_event' : k] = v;
  return { payload: { ...payload, customData: fixed }, repaired: true };
}

const { data: rows, error } = await supabase
  .from('ghl_webhook_deliveries')
  .select('id, created_at, payload, status')
  .eq('status', 'skipped')
  .order('created_at', { ascending: true });
if (error) {
  console.error('Query failed:', error.message);
  process.exit(1);
}

const targets = [];
const seen = new Set();
for (const r of rows ?? []) {
  const p = r.payload;
  if (p?.workflow?.name !== 'Outcome: Follow-up') continue;
  const cid = p.contact_id ?? p.contact?.id;
  if (!SAFE_CONTACT_IDS.includes(cid) || seen.has(cid)) continue;
  seen.add(cid);
  const { payload, repaired } = repairCustomData(p);
  targets.push({
    cid,
    name: p.full_name || p.first_name || '(unknown)',
    label: payload.customData?.outcome_label,
    ngEvent: payload.customData?.ng_event,
    originalAt: r.created_at,
    repaired,
    payload,
  });
}

console.log(`\n${LIVE ? 'LIVE REPLAY' : 'DRY RUN'} — ${targets.length} of ${SAFE_CONTACT_IDS.length} safe contacts found\n`);
for (const t of targets) {
  console.log(
    `  ${t.name.padEnd(22)} ng_event=${t.ngEvent} outcome_label="${t.label}" ` +
      `key_repaired=${t.repaired} original=${t.originalAt.slice(0, 10)}`
  );
}

const missing = SAFE_CONTACT_IDS.filter((c) => !seen.has(c));
if (missing.length) console.log(`\n  NOT FOUND (already replayed or purged): ${missing.join(', ')}`);

if (!LIVE) {
  console.log('\nDry run only — nothing sent. Re-run with --live to POST.\n');
  process.exit(0);
}

console.log('\nPOSTing...\n');
let ok = 0;
let failed = 0;
let unauthorized = 0;
for (const t of targets) {
  try {
    const res = await fetch(ENDPOINT, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'x-api-key': secret },
      body: JSON.stringify(t.payload),
    });
    const body = await res.json().catch(() => ({}));
    const status = body?.status ?? body?.result?.status ?? '(no status)';
    const msg = body?.message ?? body?.result?.message ?? '';
    if (res.status === 401) {
      unauthorized++;
      failed++;
      console.log(`  ${t.name.padEnd(22)} HTTP 401 UNAUTHORIZED — secret rejected`);
      break; // no point hammering the endpoint with a bad secret
    }
    if (res.ok && status === 'applied') ok++;
    else failed++;
    console.log(`  ${t.name.padEnd(22)} HTTP ${res.status} -> ${status} ${msg}`);
  } catch (e) {
    failed++;
    console.log(`  ${t.name.padEnd(22)} ERROR ${e.message}`);
  }
}

if (unauthorized) {
  console.error(`
==================== REPLAY DID NOT RUN ====================
The endpoint rejected the secret (HTTP 401). NOTHING was written,
and no delivery row was even created — the route returns 401 before
it inserts.

Get the right value from:
  Vercel -> dash-neurogrowth-io -> Settings -> Environment Variables
  -> GHL_WEBHOOK_SECRET  (or the legacy GHL_REVOPS_WEBHOOK_SECRET)

Then re-run:
  GHL_WEBHOOK_SECRET='<value>' node scripts/replay-followup-outcomes.mjs --live

Note: quote the value, and beware a trailing newline if you piped it in.
============================================================`);
  process.exit(1);
}

console.log(`\nDone — ${ok} applied, ${failed} failed.\n`);
if (ok === 0) {
  console.error('WARNING: zero outcomes applied. Nothing changed. Do not treat this as success.');
  process.exit(1);
}
console.log('Verify:  select outcome, count(*) from revops_sales_outcomes');
console.log("         where outcome='follow_up' group by 1;\n");
