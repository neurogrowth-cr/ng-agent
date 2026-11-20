#!/usr/bin/env node
// Dry run of the inbox watch liveness check (lib/axonHealth.js). Reads the
// real row AXON stamps (axon.engine_heartbeat, id 'cron:inbox_watch') with
// Max's own read key and prints what Max would post. It posts nothing and
// writes nothing.
//
//   node scripts/axon-inbox-watch-dry.js                      what Max sees now
//   node scripts/axon-inbox-watch-dry.js --plus-min 120       as if 120 minutes had passed with no new run
//   node scripts/axon-inbox-watch-dry.js --at 2026-11-21T07:30:00-06:00
//
// The second form is how to see the alert on purpose without touching AXON.
require('dotenv').config();
const { createClient } = require('@supabase/supabase-js');
const ax = require('../lib/axonHealth');

function arg(name) {
  const i = process.argv.indexOf(name);
  return i === -1 ? null : process.argv[i + 1];
}

(async () => {
  const plus = arg('--plus-min');
  const at = arg('--at');
  const now = at ? new Date(at).getTime() : Date.now() + (plus ? Number(plus) * 60 * 1000 : 0);
  if (!Number.isFinite(now)) { console.error('Fecha u offset no válido.'); process.exit(2); }

  const db = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_ANON_KEY, { db: { schema: 'axon' } });
  let result;
  try {
    const { data, error } = await db.from('engine_heartbeat').select('beat_at').eq('id', ax.INBOX_WATCH_BEAT_ID).limit(1);
    result = ax.classifyInboxWatch({ data, error, now });
  } catch (err) {
    result = ax.classifyInboxWatch({ thrown: err, now });
  }

  console.log(`Reloj usado: ${new Date(now).toISOString()}${at || plus ? ' (simulado)' : ''}`);
  console.log(`Última lectura completa registrada por AXON: ${result.beatAt || (result.kind === 'blind' ? 'no se pudo leer' : 'ninguna')}`);
  console.log(`Veredicto: ${result.kind}${result.reason ? ` (${result.reason})` : ''}`);
  const { posts } = ax.stepInboxWatch(ax.newInboxWatchState(), result);
  if (!posts.length) { console.log('Max no publicaría nada.'); return; }
  console.log('\nMax publicaría en #ng-fulfillment-axon y por DM a Ron (DRY RUN, no se envió nada):\n');
  for (const text of posts) console.log(text);
})();
