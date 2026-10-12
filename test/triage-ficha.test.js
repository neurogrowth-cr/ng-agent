// Triage ficha detection for the Setter Leaderboard.  Run:  node test/triage-ficha.test.js
//
// Shapes come from real notes on GHL contacts (2026-09-22 and 2026-09-25, v1.0
// fichas Sebastian typed in the GHL editor: ticks arrive as "[ X]" / "[X ]",
// blanks stay "______", bodyText keeps the line breaks, body is <p> soup) plus
// the v1.2 template, blank and filled. Names and figures are synthetic.
const { parseFicha, fichaStatus, noteText, CLARO_NAMES } = require('../lib/triageFicha');

const cases = [];
const check = (name, ok, detail) => cases.push({ name, ok, detail });

const TEMPLATE_V12 = `FICHA DE TRIAGE v1.2 (pegar en la nota del contacto en GHL, llenar durante la llamada)

Setter: ______   Fecha: ______   Duración: __ min

Nombre / empresa / país: ______ / ______ / ______
LinkedIn o sitio: ______
Dato del perfil usado en la apertura: ______

1. NEGOCIO
Vende: ______ a: ______
[ ] Empresas  [ ] Personas

2. NÚMEROS
Cobra por cliente: $______   [ ] $1,500 o más
Factura al mes: $______   [ ] $5,000 o más  [ ] Tiene caja para invertir
Clientes nuevos al mes: mes normal ____ / mes malo ____
Canal hoy: [ ] Referidos  [ ] Publicidad  [ ] A mano  [ ] Otro: ______

3. DOLOR
Tres frases textuales:
"______"
"______"
"______"
Ya intentó: ______   No funcionó porque: ______
Patrón previo: cuánto tiempo le dio ______ · quién lo contrató ______ · quién lo cortó ______
Costo del problema: $______ al mes
Por qué ahora: ______
Urgencia: __ / 10   [ ] 7 o más   Qué le falta para 10: ______

4. DECISOR
[ ] Solo  [ ] Con: ______  [ ] Confirmado dentro de la llamada

5. INVERSIÓN
[ ] Rango $5,000 a $7,500 dicho (por voz)
Respuesta: ______

6. COMPROMISOS
[ ] Llamada dentro de 48 horas
[ ] Computadora y cámara
[ ] Material visto
Meta escrita (su línea, tal cual): "______"
Posible obstáculo para llegar: ______

CASO A ENVIAR: ______

VEREDICTO
[ ] AGENDAR A (avisar a Ron en #ng-sales-goats)
[ ] AGENDAR B
[ ] NURTURE
[ ] DQ
Si agenda: fecha y hora ______   Closer: ______
Si nurture o DQ: razón ______   Regreso día 30: ______

REGLA: un claro en blanco = no se agenda. La meta escrita puede llegar después de colgar, pero antes de la sesión.
A = cobra $3,000 o más, o factura $10,000 o más al mes, urgencia 9 a 10 y decide solo.`;

// 1. The blank template is a ficha with nothing filled.
{
  const f = parseFicha(TEMPLATE_V12);
  check('blank v1.2 template is a ficha', !!f && f.version === '1.2', f);
  check('blank template has 0 claros filled', f.filled.length === 0 && f.missing.length === 6 && !f.complete, f);
  check('the rule line and the veredicto never count as answers', f.missing.join(',') === CLARO_NAMES.join(','), f);
}

// 2. A real v1.0 shape: five claros answered, COMPROMISOS untouched.
const REAL_V10_INCOMPLETE = `FICHA DE TRIAGE v1.0

Setter: Sebas  Fecha: 25/9/26   Duración: 9 min

Nombre / empresa / país: ______ / ______ / ______

LinkedIn o sitio: ______

Baterias industriales, encargados de la region y buscan implementar Linkedln


1. NEGOCIO

Vende: Baterías industriales a industria alimenticia, farmaceutica entre otros

[ X] Empresas  [ ] Personas


2. NÚMEROS

Cobra por cliente: $3000-$10000  [ ] $1,500 o más

Factura al mes: $______   [ ] $5,000 o más  [ ] Tiene caja para invertir

Clientes nuevos al mes: 1-2

Canal hoy: [ ] Referidos  [ ] Publicidad  [ ] A mano  [ ] Otro: ______

No encuentra ese tipo de cliente en instagram


3. DOLOR

Frase exacta: "______"

Ya intentó: ______   No funcionó porque: ______

Costo del problema: $______ al mes

Por qué ahora: ______

Urgencia: 10 / 10   [ ] 7 o más   Qué le falta para 10: ______

No tienen dolor. Están en un momento de crecimiento


4. DECISOR

[ ] Solo  [ ] Con: socios que estarán en la llamada  [ ] Confirmado dentro de la llamada


5. INVERSIÓN

[ ] Rango $5,000 a $7,500 dicho (por voz)

Respuesta: Perfecto


6. COMPROMISOS

[ ] Llamada dentro de 48 horas

[ ] Computadora y cámara

[ ] Verá el material y responde "listo"

Posible obstáculo para llegar: ______


CASO A ENVIAR: Un Caso


VEREDICTO

[X ] AGENDAR A (avisar a Ron en #ng-sales-goats)

[ ] AGENDAR B

Si agenda: fecha y hora ______   Closer: ______
`;
{
  const f = parseFicha(REAL_V10_INCOMPLETE);
  check('v1.0 header is recognised', !!f && f.version === '1.0', f);
  check('typed ticks "[ X]" and free text count as answers', f.filled.join(',') === 'NEGOCIO,NÚMEROS,DOLOR,DECISOR,INVERSIÓN', f);
  check('COMPROMISOS left as pasted is the one blank claro', f.missing.join(',') === 'COMPROMISOS' && !f.complete, f);
  check('a ticked VEREDICTO does not fill a claro', !f.complete, f);
}

// 3. A complete v1.2 ficha, with blanks a setter legitimately leaves ("Otro: ______").
const FILLED_V12 = TEMPLATE_V12
  .replace('Vende: ______ a: ______', 'Vende: consultoría de procesos a: plantas de alimentos')
  .replace('[ ] Empresas  [ ] Personas', '[x] Empresas  [ ] Personas')
  .replace('Cobra por cliente: $______', 'Cobra por cliente: $4,000')
  .replace('"______"\n"______"\n"______"', '"Necesito más clientes"\n"______"\n"______"')
  .replace('[ ] Solo  [ ] Con: ______', '[✓] Solo  [ ] Con: ______')
  .replace('Respuesta: ______', 'Respuesta: me queda claro')
  .replace('[ ] Llamada dentro de 48 horas', '[X ] Llamada dentro de 48 horas');
{
  const f = parseFicha(FILLED_V12);
  check('one answer per claro makes the ficha complete', f.complete && f.missing.length === 0, f);
  check('a ✓ tick counts', f.filled.includes('DECISOR'), f);
}

// 4. GHL note objects: bodyText first, HTML body as a fallback with line breaks kept.
{
  const html = FILLED_V12.split('\n').map(l => `<p style="margin:0px">${l.replace(/"/g, '&quot;').replace(/ {2}/g, '&nbsp; ')}</p>`).join('');
  const fromBody = parseFicha({ body: html });
  check('HTML-only body is parsed with <p> as line breaks', !!fromBody && fromBody.complete, fromBody);
  const fromBodyText = parseFicha({ body: '<p>irrelevant</p>', bodyText: REAL_V10_INCOMPLETE });
  check('bodyText wins over body', !!fromBodyText && fromBodyText.missing.join(',') === 'COMPROMISOS', fromBodyText);
  check('noteText strips tags when only body exists', noteText({ body: '<p>a</p><p>b</p>' }).split('\n').map(l => l.trim()).filter(Boolean).join('|') === 'a|b');
}

// 5. Not a ficha.
{
  check('an ordinary setter note is not a ficha', parseFicha('Agencia de marketing, cobra 300 mil por proyecto, quiere crecer') === null);
  check('a REVI note is not a ficha', parseFicha({ bodyText: '🧠 REVI call read: strong buying signals' }) === null);
  check('empty input is not a ficha', parseFicha('') === null && parseFicha(null) === null);
}

// 6. fichaStatus picks the most complete ficha across a contact's notes.
{
  check('no notes → none', fichaStatus([]).status === 'none' && fichaStatus(undefined).status === 'none');
  check('only ordinary notes → none', fichaStatus([{ bodyText: 'hola' }]).status === 'none');
  const inc = fichaStatus([{ bodyText: 'hola' }, { bodyText: REAL_V10_INCOMPLETE }]);
  check('one incomplete ficha → incomplete with 5 filled', inc.status === 'incomplete' && inc.filled === 5 && inc.missing.join(',') === 'COMPROMISOS', inc);
  const best = fichaStatus([{ bodyText: REAL_V10_INCOMPLETE }, { bodyText: FILLED_V12 }, { bodyText: TEMPLATE_V12 }]);
  check('the most complete ficha wins', best.status === 'complete' && best.filled === 6 && best.version === '1.2', best);
}

const failed = cases.filter(c => !c.ok);
for (const c of cases) console.log(`${c.ok ? '✓' : '✗'} ${c.name}${c.ok ? '' : ' ' + JSON.stringify(c.detail || {})}`);
console.log(`${cases.length - failed.length}/${cases.length} passed`);
if (failed.length) process.exit(1);
