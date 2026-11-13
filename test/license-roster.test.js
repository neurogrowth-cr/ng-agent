// Licencia Unificada roster: the Kai reading and the post contract (recipe §4a).
//   Run:  node test/license-roster.test.js
//
// What has to hold, because each one is a silent failure if it regresses:
//   • a $140 license with the Kai switch on but no running trial is NOT active
//     (ng-automation runs it in 'license_only' mode and sends nothing);
//   • a client paying for Kai with Kai off is flagged ⚠️ and listed first;
//   • every client appears exactly once, in one of the two sections;
//   • names and prices come from the row (catalog_rungs), never from code;
//   • no 'undefined', 'null', 'NaN' or em dash ever reaches Slack.
const { SQL, summarizeLicenseRoster, formatLicenseRoster, checkLicenseRoster, kaiStatus } = require('../lib/licenseRoster');

let failures = 0;
function check(label, ok, detail = '') {
  if (!ok) { failures++; console.error(`FAIL ${label}${detail ? `\n  ${detail}` : ''}`); }
  else console.log(`ok   ${label}`);
}

const now = new Date('2026-11-16T14:00:00Z'); // Monday 08:00 CR
const d = (daysAhead) => new Date(now.getTime() + daysAhead * 24 * 3600 * 1000).toISOString();
const LU = { rung_code: 'licencia_unificada', rung_name: 'Licencia Unificada', price_amount: '140.00' };
const KLU = { rung_code: 'kai_licencia_unificada', rung_name: 'KAI + Licencia Unificada', price_amount: '200.00' };
const SN = { rung_code: 'sales_nav', rung_name: 'Sales Navigator', price_amount: '100.00' };
const ws = (over = {}) => ({ workspace_id: 'w', kai_switch: true, paused: false, auto_send: false, ...over });

const rows = [
  // $200, Kai off: paid and not working. Must be ⚠️ and first.
  { client_name: 'Selva Design & Architecture - Esteban Ossenbach', ...KLU, status: 'active', source: 'self_serve', ...ws({ kai_switch: false }) },
  // $200, Kai on, approval mode.
  { client_name: 'Mind Lift Leadership - Laura Morales', ...KLU, status: 'trialing', source: 'self_serve', ...ws() },
  // $140 with a running trial ending in 6 days: active, ⏰.
  { client_name: 'Aura Bonilla - Cacao Legal ', ...LU, status: 'active', source: 'kai_trial', trial_ends_at: d(6), ...ws() },
  // $140 with a running trial ending in 40 days, auto-send.
  { client_name: 'Johan Solis - Consorcio CSM', ...LU, status: 'active', source: 'kai_trial', trial_ends_at: d(40), ...ws({ auto_send: true }) },
  // $140, switch on, no trial: license_only, NOT active.
  { client_name: 'Flor Lizano Bolaños', ...LU, status: 'active', source: 'self_serve', ...ws({ auto_send: true }) },
  // $140, trial closed: NOT active, says when it ended.
  { client_name: 'Gabriela Hernandez', ...LU, status: 'past_due', source: 'kai_trial', trial_ends_at: d(-3), trial_closed_at: d(-3), ...ws() },
  // $140, trial running but the client paused Kai.
  { client_name: 'Sastrería Triana - Carlos Triana ', ...LU, status: 'active', source: 'kai_trial', trial_ends_at: d(13), ...ws({ paused: true }) },
  // Sales Navigator alone, no workspace: its plan has no Kai.
  { client_name: 'Kathy Arguedas - Estratega Patrimonial - DIY ', ...SN, status: 'active', source: 'self_serve', workspace_id: null, kai_switch: null, paused: null, auto_send: null },
  // Missing name and catalog row never render as undefined.
  { client_name: null, rung_code: 'kai_sales_nav', rung_name: null, price_amount: null, status: 'active', source: 'self_serve', workspace_id: null },
];

// ── 1. Kai reading ───────────────────────────────────────────────────────────
check('1a $200 + switch off = not active, pays Kai', (() => { const k = kaiStatus(rows[0], now); return !k.on && k.paysKai && /paga Kai pero Kai está apagado/.test(k.note); })());
check('1b $200 + switch on = active', kaiStatus(rows[1], now).on === true);
check('1c $140 running trial = active, ending soon', (() => { const k = kaiStatus(rows[2], now); return k.on && k.trial && k.ending; })());
check('1d $140 switch on, no trial = NOT active', (() => { const k = kaiStatus(rows[4], now); return !k.on && /nunca activó la prueba/.test(k.note); })(), JSON.stringify(kaiStatus(rows[4], now)));
check('1e $140 trial closed = NOT active, says when', (() => { const k = kaiStatus(rows[5], now); return !k.on && /terminó el 13 nov/.test(k.note); })(), kaiStatus(rows[5], now).note);
check('1f paused by client = NOT active', (() => { const k = kaiStatus(rows[6], now); return !k.on && /el cliente lo pausó/.test(k.note); })());
check('1g sales_nav = no Kai in plan', kaiStatus(rows[7], now).note === 'su plan no incluye Kai');
check('1h kai_sales_nav without workspace = not configured', /no está configurado/.test(kaiStatus(rows[8], now).note));

// ── 2. Summary ───────────────────────────────────────────────────────────────
const s = summarizeLicenseRoster({ now, rows });
check('2a totals', s.totals.clients === 9 && s.totals.on === 3 && s.totals.off === 6, JSON.stringify(s.totals));
check('2b paid-Kai-off listed first', s.off[0].paysKai && s.off[1].paysKai && !s.off[2].paysKai, s.off.map((c) => c.client_name).join(' > '));
check('2c ending trial leads the active list', s.on[0].client_name === 'AURA BONILLA - CACAO LEGAL', s.on.map((c) => c.client_name).join(' > '));
check('2d sales_nav last', s.off[s.off.length - 1].client_name.startsWith('KATHY'), s.off.map((c) => c.client_name).join(' > '));

// ── 3. Post contract ─────────────────────────────────────────────────────────
const text = formatLicenseRoster(s);
console.log('\n' + text + '\n');
check('3a header', /^`LICENCIA UNIFICADA: LISTA AL DÍA \(lunes, 16 de noviembre\)`/.test(text), text.split('\n')[0]);
check('3b totals line', /9 clientes con licencia · 3 con Kai activo · 6 sin Kai activo/.test(text));
check('3c price from the row', /• SELVA DESIGN & ARCHITECTURE - ESTEBAN OSSENBACH · KAI \+ Licencia Unificada \$200 · ⚠️ paga Kai pero Kai está apagado/.test(text), text);
check('3d trial line', /• AURA BONILLA - CACAO LEGAL · Licencia Unificada \$140 · prueba gratis hasta el 22 nov ⏰ · con aprobación/.test(text), text);
check('3e past due flagged', /GABRIELA HERNANDEZ .* · pago atrasado/.test(text));
check('3f unknown name and plan fall back', /• UNKNOWN CLIENT · kai_sales_nav · ⚠️ paga Kai pero Kai no está configurado/.test(text), text);
check('3g what-to-do covers each case', /`QUÉ HACER`/.test(text) && /⚠️ Paga Kai/.test(text) && /Prueba registrada/.test(text) && /Licencia \$140 sin Kai/.test(text) && /⏰ Prueba que vence/.test(text));
check('3h contract passes', checkLicenseRoster(text, s).length === 0, checkLicenseRoster(text, s).join('; '));

// ── 4. The contract can fail (a test that cannot go red is not a criterion) ──
const broken = text.replace(/^• FLOR.*\n/m, '');
check('4a a dropped client line is caught', checkLicenseRoster(broken, s).some((p) => /client lines/.test(p)), checkLicenseRoster(broken, s).join('; '));
check('4b undefined is caught', checkLicenseRoster(text + '\nundefined', s).length > 0);
check('4c em dash is caught', checkLicenseRoster(text + ' —', s).length > 0);
check('4d no licenses = no post', formatLicenseRoster(summarizeLicenseRoster({ now, rows: [] })) === null);

// ── 5. Query guards ──────────────────────────────────────────────────────────
check('5a demo excluded', /COALESCE\(d\.is_demo, false\) = false/.test(SQL.roster) && /ws\.is_demo/.test(SQL.roster));
check('5b read-only', !/\b(INSERT|UPDATE|DELETE)\b/i.test(SQL.roster));

if (failures) { console.error(`\n${failures} failure(s)`); process.exit(1); }
console.log('\nall license roster checks passed');
