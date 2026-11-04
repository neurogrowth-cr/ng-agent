// Kai escalations digest: the formatter's contract (recipe §4a).
//   Run:  node test/kai-escalations.test.js
//
// What has to hold, because each one is a silent failure if it regresses:
//   • a quiet day posts nothing (null), never an empty header;
//   • a client with stale items is named with both counts and the oldest age;
//   • clients are ordered by stale escalations first, so the DM leads with the
//     item a human must answer;
//   • the pre-cutoff backlog is one number, never a per-client list;
//   • no 'undefined', 'null' or 'NaN' ever reaches Slack.
const { summarizeKaiEscalations, formatKaiEscalationsDigest, daysAgo } = require('../lib/kaiEscalations');

let failures = 0;
function check(label, ok, detail = '') {
  if (!ok) { failures++; console.error(`FAIL ${label}${detail ? `\n  ${detail}` : ''}`); }
  else console.log(`ok   ${label}`);
}

const now = new Date('2026-11-04T14:30:00Z'); // 08:30 CR, a Wednesday
const d = (daysBack) => new Date(now.getTime() - daysBack * 24 * 3600 * 1000).toISOString();

const rows = [
  { workspace_id: 'w-flor', client_name: 'Flor Lizano Bolaños', paused: false, auto_send: false, status: 'drafted',   n: 71, stale: 59, oldest: d(14) },
  { workspace_id: 'w-flor', client_name: 'Flor Lizano Bolaños', paused: false, auto_send: false, status: 'escalated', n: 33, stale: 31, oldest: d(34) },
  { workspace_id: 'w-cosiba', client_name: 'Cosiba Inversiones', paused: true, auto_send: false, status: 'escalated', n: 20, stale: 20, oldest: d(34) },
  { workspace_id: 'w-mind', client_name: 'Mind Lift Leadership - Laura Morales', paused: false, auto_send: true, status: 'drafted', n: 6, stale: 5, oldest: d(11) },
  // Fresh only: created yesterday, nothing stale. Must not be listed.
  { workspace_id: 'w-licita', client_name: 'Licita CR - Kenneth Ramirez ', paused: false, auto_send: false, status: 'drafted', n: 4, stale: 0, oldest: d(1) },
  // Unknown workspace name never renders as undefined.
  { workspace_id: 'w-ghost', client_name: null, paused: null, auto_send: null, status: 'escalated', n: 1, stale: 1, oldest: d(3) },
];

// ── 1. Summary shape ─────────────────────────────────────────────────────────
const s = summarizeKaiEscalations({ now, since: '2026-10-01', staleHours: 48, rows, legacy: { n: 252, clients: 9 } });
check('1a clients with stale items only', s.clients.every((c) => c.escalated.stale > 0 || c.drafted.stale > 0),
  s.clients.map((c) => c.client_name).join('|'));
check('1b four clients listed (fresh-only client dropped)', s.clients.length === 4, `got ${s.clients.length}`);
check('1c ordered by stale escalations first', s.clients[0].client_name === 'FLOR LIZANO BOLAÑOS' && s.clients[1].client_name === 'COSIBA INVERSIONES',
  s.clients.map((c) => c.client_name).join(' > '));
check('1d totals add up', s.totals.staleEscalated === 52 && s.totals.staleDrafted === 64, JSON.stringify(s.totals));
check('1e legacy carried as one number', s.legacy.n === 252 && s.legacy.clients === 9);
check('1f null client name becomes UNKNOWN CLIENT', s.clients.some((c) => c.client_name === 'UNKNOWN CLIENT'));
check('1g oldest age in days', s.clients[0].escalated.oldestDays === 34 && s.clients[0].drafted.oldestDays === 14,
  JSON.stringify(s.clients[0]));

// ── 2. Formatter contract ────────────────────────────────────────────────────
const text = formatKaiEscalationsDigest(s);
check('2a header present', /^`KAI: ESCALADOS SIN ATENDER \(/.test(text), text.split('\n')[0]);
check('2b totals line names counts and clients', /⚠️ 52 escalados y 64 borradores con más de 48 h en 4 clientes\./.test(text), text);
check('2c Flor line carries both buckets and ages', /• FLOR LIZANO BOLAÑOS: 31 escalados sin responder \(el más viejo 34 d\) · 59 borradores sin revisar \(14 d\)/.test(text), text);
check('2d paused flag shown', /• COSIBA INVERSIONES: 20 escalados sin responder \(el más viejo 34 d\) · ⏸️ cliente en pausa/.test(text), text);
check('2e auto-send flag shown', /MIND LIFT LEADERSHIP - LAURA MORALES: 5 borradores sin revisar \(11 d\) · envío automático activo/.test(text), text);
check('2f fresh-only client not listed', !/LICITA/.test(text));
check('2g legacy backlog is one line', /• Pendiente anterior al 2026-10-01: 252 en 9 clientes/.test(text), text);
check('2h what-to-do block present', /`QUÉ HACER`/.test(text) && /Escalado = Kai no pudo responder/.test(text));
check('2i no undefined/null/NaN', !/undefined|null|NaN/.test(text), text);
check('2j no em dash', !/—/.test(text));

// ── 3. Quiet day posts nothing ───────────────────────────────────────────────
const quiet = summarizeKaiEscalations({ now, rows: [rows[4]], legacy: { n: 0, clients: 0 } });
check('3a nothing stale, no post', formatKaiEscalationsDigest(quiet) === null);
const quietButLegacy = summarizeKaiEscalations({ now, rows: [], legacy: { n: 252, clients: 9 } });
check('3b legacy alone does not wake the owner every day', formatKaiEscalationsDigest(quietButLegacy) === null);

// ── 4. Line cap ──────────────────────────────────────────────────────────────
const many = Array.from({ length: 14 }, (_, i) => ({
  workspace_id: `w${i}`, client_name: `Cliente ${String(i).padStart(2, '0')}`, paused: false, auto_send: false,
  status: 'escalated', n: 2, stale: 2, oldest: d(5),
}));
const capped = formatKaiEscalationsDigest(summarizeKaiEscalations({ now, rows: many, legacy: { n: 0, clients: 0 } }), { maxLines: 10 });
check('4a capped at 10 client lines plus a remainder line', (capped.match(/^• CLIENTE /gm) || []).length === 10 && /• y 4 clientes más/.test(capped), capped);

// ── 5. daysAgo guards ────────────────────────────────────────────────────────
check('5a bad timestamp is null', daysAgo('not a date', now) === null && daysAgo(null, now) === null);
check('5b future timestamp clamps to 0', daysAgo(new Date(now.getTime() + 3600e3).toISOString(), now) === 0);

if (failures) { console.error(`\n${failures} check(s) failed`); process.exit(1); }
console.log('\nall checks passed');
