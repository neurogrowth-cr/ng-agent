'use strict';
// Triage ficha detection for the Setter Leaderboard (Ron, 2026-10-11, script
// audit decision 4): the headline setter metric is HELD CALLS WITH A COMPLETE
// TRIAGE FICHA. Bookings and show rate stay as diagnostics (Persuasion Playbook
// team rule 3: reward bookings and you breed bookings).
//
// The ficha is the "FICHA DE TRIAGE" note the setter pastes on the GHL contact
// (template NG_Triage_Ficha_GHL_Note_v1.2.txt, v1.0 before it) and fills during
// the triage call. Its own rule: "un claro en blanco = no se agenda". The six
// claros are the numbered sections 1. NEGOCIO, 2. NÚMEROS, 3. DOLOR,
// 4. DECISOR, 5. INVERSIÓN, 6. COMPROMISOS.
//
// A claro counts as FILLED when at least one of its lines is answered: a box is
// ticked ([x], [X], [✓]) or the line differs from the pasted template skeleton
// once blanks (______) and empty boxes are removed. Free text under the claro
// counts too. Lenient on purpose: this is a v1 signal read from notes typed by
// hand in the GHL UI, where "Otro: ______" or "Con: ______" stay blank for a
// good reason, so "every blank filled" would fail nearly every real ficha.
//
// Pure: no GHL, no DB. index.js fetches the notes (fetchContactNotesForFicha)
// and credits the call's OWNER (claim owner, same as every other setter stat),
// never the note's author. Tested in test/triage-ficha.test.js.

const HEADER_RE = /ficha\s+de\s+triage(?:\s+v?(\d+(?:\.\d+)?))?/i;
const CLARO_RE = /^\s*([1-6])\s*[.)-]\s*(negocio|numeros|dolor|decisor|inversion|compromisos)\b/;
const END_RE = /^\s*(caso a enviar|veredicto)\b/;
const TICK_RE = /\[\s*[xX✓✔]\s*\]/;
const CLARO_NAMES = ['NEGOCIO', 'NÚMEROS', 'DOLOR', 'DECISOR', 'INVERSIÓN', 'COMPROMISOS'];

// Template lines inside the six claros, v1.0 and v1.2. A pasted line that
// normalises to one of these is still blank; anything else is an answer.
const SKELETON_LINES = [
  // 1. NEGOCIO
  'Vende: ______ a: ______',
  '[ ] Empresas  [ ] Personas',
  // 2. NÚMEROS
  'Cobra por cliente: $______   [ ] $1,500 o más',
  'Factura al mes: $______   [ ] $5,000 o más  [ ] Tiene caja para invertir',
  'Clientes nuevos al mes: ______',
  'Clientes nuevos al mes: mes normal ____ / mes malo ____',
  'Canal hoy: [ ] Referidos  [ ] Publicidad  [ ] A mano  [ ] Otro: ______',
  // 3. DOLOR
  'Frase exacta: "______"',
  'Tres frases textuales:',
  '"______"',
  'Ya intentó: ______   No funcionó porque: ______',
  'Patrón previo: cuánto tiempo le dio ______ · quién lo contrató ______ · quién lo cortó ______',
  'Costo del problema: $______ al mes',
  'Por qué ahora: ______',
  'Urgencia: __ / 10   [ ] 7 o más   Qué le falta para 10: ______',
  // 4. DECISOR
  '[ ] Solo  [ ] Con: ______  [ ] Confirmado dentro de la llamada',
  // 5. INVERSIÓN
  '[ ] Rango $5,000 a $7,500 dicho (por voz)',
  'Respuesta: ______',
  // 6. COMPROMISOS
  '[ ] Llamada dentro de 48 horas',
  '[ ] Computadora y cámara',
  '[ ] Material visto',
  '[ ] Verá el material y responde "listo"',
  'Meta escrita (su línea, tal cual): "______"',
  'Posible obstáculo para llegar: ______',
];

// Accent-insensitive, alphanumerics only: survives the whitespace, &nbsp; and
// punctuation drift of a note typed in the GHL editor.
function normalize(s) {
  return String(s || '')
    .normalize('NFD').replace(/[̀-ͯ]/g, '')
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '');
}
const SKELETON = new Set(SKELETON_LINES.map(normalize));

// GHL notes typed in the UI carry HTML in `body` and plain text in `bodyText`;
// API-posted notes carry plain text in both. Keep line breaks: a <p> or <br>
// becomes a newline so the claro headers stay on their own line.
function noteText(note) {
  if (note && typeof note === 'object') {
    if (note.bodyText) return String(note.bodyText);
    return String(note.body || '')
      .replace(/<br\s*\/?>/gi, '\n').replace(/<\/p>/gi, '\n').replace(/<[^>]*>/g, ' ')
      .replace(/&nbsp;/gi, ' ').replace(/&amp;/gi, '&').replace(/&quot;/gi, '"');
  }
  return String(note || '');
}

function lineIsAnswered(line) {
  if (TICK_RE.test(line)) return true;
  const n = normalize(line);
  if (!n) return false;
  return !SKELETON.has(n);
}

// Parse one note. Returns null when the note is not a ficha, else
// { version, filled: [names of filled claros], missing: [names], complete }.
function parseFicha(noteOrText) {
  const text = noteText(noteOrText);
  const header = text.match(HEADER_RE);
  if (!header) return null;
  const filled = new Set();
  let current = null;
  for (const rawLine of text.split(/\r?\n/)) {
    const line = rawLine.trim();
    if (!line) continue;
    const folded = line.normalize('NFD').replace(/[̀-ͯ]/g, '').toLowerCase();
    const claro = folded.match(CLARO_RE);
    if (claro) { current = Number(claro[1]); continue; }
    if (END_RE.test(folded)) { current = null; continue; }
    if (current && lineIsAnswered(line)) filled.add(current);
  }
  const filledNames = CLARO_NAMES.filter((_, i) => filled.has(i + 1));
  const missing = CLARO_NAMES.filter((_, i) => !filled.has(i + 1));
  return { version: header[1] || null, filled: filledNames, missing, complete: missing.length === 0 };
}

// Best ficha across a contact's notes (a setter may paste twice; the most
// complete one is the one that counts). status: 'complete' | 'incomplete' |
// 'none'.
function fichaStatus(notes) {
  let best = null;
  for (const n of (notes || [])) {
    const f = parseFicha(n);
    if (f && (!best || f.filled.length > best.filled.length)) best = f;
  }
  if (!best) return { status: 'none', filled: 0, missing: CLARO_NAMES.slice(), version: null };
  return { status: best.complete ? 'complete' : 'incomplete', filled: best.filled.length, missing: best.missing, version: best.version };
}

module.exports = { parseFicha, fichaStatus, noteText, normalize, CLARO_NAMES, SKELETON_LINES };
