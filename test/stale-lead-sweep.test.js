// Rules test for the stale-unclaimed-lead sweep's terminal-tag filter.
//   Run:  node test/stale-lead-sweep.test.js
//
// Extracts the real TERMINAL_LEAD_TAGS / dropDispositionedLeads / getUnclaimedLeads
// block straight out of index.js rather than copying it, so the test cannot drift
// from shipped behaviour (same approach as make-watchdog.test.js — index.js boots
// the Slack app on require).
//
// Fixtures use the live GHL tag vocabulary for location iUkpsgZqYJ1ftVxGsoXE
// captured 2026-08-19, and the real SANTIAGO LONDONO U row that motivated the fix.
const fs   = require('fs');
const path = require('path');

const SRC = fs.readFileSync(process.argv[2] || path.join(__dirname, '..', 'index.js'), 'utf8');
const block = SRC.slice(
  SRC.indexOf('const TERMINAL_LEAD_TAGS'),
  SRC.indexOf('// Resolves the @setters Slack user group'),
);

let failures = 0;
function check(label, actual, expected) {
  const ok = JSON.stringify(actual) === JSON.stringify(expected);
  if (!ok) { failures++; console.error(`FAIL ${label}\n  expected: ${JSON.stringify(expected)}\n  actual:   ${JSON.stringify(actual)}`); }
  else console.log(`ok   ${label}`);
}

// Minimal thenable stand-in for the supabase-js query builder: every filter
// method returns `this`, and awaiting it yields { data, error }.
function table(rows, error = null) {
  const b = {
    select: () => b, gte: () => b, order: () => b, in: () => b,
    then: (res) => res({ data: rows, error }),
  };
  return b;
}

// Builds the extracted block with stubbed I/O.
// contacts: contactId → GHL contact object | 'throw' | null
function build({ leads = [], claims = [], contacts = {}, env = {} } = {}) {
  const lookups = [];
  const supabaseStub = {
    from: (name) => (name === 'lead_posts' ? table(leads) : table(claims)),
  };
  const ghlGetContactStub = async (id) => {
    lookups.push(id);
    const c = contacts[id];
    if (c === 'throw') throw new Error('GHL 429');
    return c === undefined ? null : c;
  };
  const factory = new Function(
    'process', 'supabase', 'ghlGetContact', 'console',
    `${block}; return getUnclaimedLeads;`,
  );
  const fn = factory(
    { env }, supabaseStub, ghlGetContactStub,
    { log: () => {}, warn: () => {}, error: () => {} },
  );
  return { fn, lookups };
}

const DAY = 24 * 60 * 60 * 1000;
const lead = (ts, contactId, fullName, over = {}) => ({
  slack_message_ts: ts, slack_channel_id: 'C0AJANQBYUE', contact_id: contactId,
  full_name: fullName, source: 'LinkedIn Flywheel - Appointment',
  posted_at: new Date(Date.now() - 2 * DAY).toISOString(), ...over,
});
const contact = (tags) => ({ id: 'x', tags });
const names = (rows) => rows.map(r => r.fullName);

(async () => {
  // 1. The live case: no-fit + cancelled is dispositioned, so it drops out.
  {
    const { fn } = build({
      leads: [lead('1785213177.277389', '6eHHbKPtzKrT56z7luyy', 'SANTIAGO LONDONO U')],
      contacts: { '6eHHbKPtzKrT56z7luyy': contact(['fb lead form', 'no-fit', 'cancelled']) },
    });
    check('1a no-fit lead is filtered out of the sweep', names(await fn(Date.now() - 30 * DAY)), []);
  }

  // 2. An untouched lead — the whole reason the sweep exists — still reports.
  {
    const { fn } = build({
      leads: [lead('1.1', 'c1', 'Adrian RM')],
      contacts: { c1: contact(['fb lead form', 'new contact']) },
    });
    check('2a untagged lead is still reported', names(await fn(Date.now() - 30 * DAY)), ['Adrian RM']);
  }

  // 3. `cancelled` alone is NOT terminal — a cancelled call still needs a human.
  {
    const { fn } = build({
      leads: [lead('1.1', 'c1', 'Cancelled Only')],
      contacts: { c1: contact(['cancelled', 'appt-booked']) },
    });
    check('3a cancelled alone stays in the sweep', names(await fn(Date.now() - 30 * DAY)), ['Cancelled Only']);
  }

  // 4. Every terminal tag in the default set drops its lead.
  for (const t of ['won-deal', 'no-fit', 'generic-lost', 'activation-done', 'call-showed']) {
    const { fn } = build({ leads: [lead('1.1', 'c1', 'X')], contacts: { c1: contact([t]) } });
    check(`4:${t} is terminal`, names(await fn(Date.now() - 30 * DAY)), []);
  }

  // 5. Fail SAFE: a GHL outage must never hide a lead.
  {
    const { fn } = build({ leads: [lead('1.1', 'c1', 'Throws')], contacts: { c1: 'throw' } });
    check('5a GHL throw keeps the lead', names(await fn(Date.now() - 30 * DAY)), ['Throws']);
  }
  {
    const { fn } = build({ leads: [lead('1.1', 'c1', 'Missing')], contacts: {} }); // → null contact
    check('5b unresolvable contact keeps the lead', names(await fn(Date.now() - 30 * DAY)), ['Missing']);
  }

  // 6. A claimed lead never costs a GHL lookup.
  {
    const { fn, lookups } = build({
      leads: [lead('1.1', 'c1', 'Claimed')],
      claims: [{ ghl_contact_id: 'c1' }],
      contacts: { c1: contact(['new contact']) },
    });
    check('6a claimed lead is excluded', names(await fn(Date.now() - 30 * DAY)), []);
    check('6b claimed lead costs no GHL call', lookups, []);
  }

  // 7. Dedup: a dup row reusing the original ts is claimed via the ORIGINAL id.
  {
    const { fn } = build({
      leads: [lead('1.1', 'orig', 'Dup Lead'), lead('1.1', 'dup', 'Dup Lead')],
      claims: [{ ghl_contact_id: 'orig' }],
      contacts: { orig: contact([]), dup: contact([]) },
    });
    check('7a dup row does not resurrect a claimed lead', names(await fn(Date.now() - 30 * DAY)), []);
  }

  // 8. Tag matching survives casing and stray whitespace from the CRM.
  {
    const { fn } = build({
      leads: [lead('1.1', 'c1', 'Messy Tag')],
      contacts: { c1: contact(['  No-Fit ']) },
    });
    check('8a tag match is case/space-insensitive', names(await fn(Date.now() - 30 * DAY)), []);
  }

  // 9. The lookup cap: 60 unclaimed leads cost exactly 50 GHL calls, and the
  //    10 unchecked ones are still reported rather than silently dropped.
  {
    const leads = [], contacts = {};
    for (let i = 0; i < 60; i++) {
      const id = `c${i}`;
      leads.push(lead(`ts${i}`, id, `L${i}`, { posted_at: new Date(Date.now() - (60 - i) * DAY).toISOString() }));
      contacts[id] = contact(['no-fit']); // all dispositioned — only the checked ones can drop
    }
    const { fn, lookups } = build({ leads, contacts });
    const out = await fn(Date.now() - 90 * DAY);
    check('9a cap holds GHL calls at 50', lookups.length, 50);
    check('9b past-cap leads are still reported', out.length, 10);
    check('9c the cap spends its budget oldest-first', lookups[0], 'c0');
  }

  // 10. The tag list is env-overridable without a code change.
  {
    const { fn } = build({
      leads: [lead('1.1', 'c1', 'Custom')],
      contacts: { c1: contact(['follow-up']) },
      env: { STALE_LEAD_TERMINAL_TAGS: 'follow-up' },
    });
    check('10a STALE_LEAD_TERMINAL_TAGS overrides the default set', names(await fn(Date.now() - 30 * DAY)), []);
  }

  // 11. The live 2026-08-21 case: QA test leads are suppressed everywhere the
  //     shared query feeds (hourly nag + daily sweep), and cost zero GHL
  //     lookups — they drop before the tag-check budget is spent.
  {
    const { fn, lookups } = build({
      leads: [
        lead('1755781200.000100', 'qaRace01', 'QA RaceTest BORRAR'),
        lead('1755781200.000200', 'qaNoEm02', 'QA NoEmail BORRAR'),
        lead('1755781200.000300', 'real0001', 'Francisco Reyes'),
      ],
    });
    const out = await fn(Date.now() - 30 * DAY);
    check('11a QA test leads are suppressed, real lead survives', names(out), ['Francisco Reyes']);
    check('11b suppressed test leads cost no GHL lookups', lookups, ['real0001']);
  }

  // 12. Whole-word matching only: real names that merely CONTAIN qa/test as a
  //     substring must never be hidden — this filter fails toward hiding leads.
  {
    const { fn } = build({
      leads: [
        lead('1.2', 'c1', 'Ahmed Baqai'),
        lead('1.3', 'c2', 'Teresa Testa'),
        lead('1.4', 'c3', 'Prueba Uno'),
      ],
    });
    check('12a substring lookalikes survive, whole-word "Prueba" drops',
      names(await fn(Date.now() - 30 * DAY)), ['Ahmed Baqai', 'Teresa Testa']);
  }

  // 13. The word list is env-overridable without a code change.
  {
    const { fn } = build({
      leads: [lead('1.5', 'c1', 'Demo Lead'), lead('1.6', 'c2', 'QA Check')],
      env: { STALE_LEAD_TEST_NAME_WORDS: 'demo' },
    });
    check('13a STALE_LEAD_TEST_NAME_WORDS overrides the default word set',
      names(await fn(Date.now() - 30 * DAY)), ['QA Check']);
  }

  console.log(failures ? `\n${failures} FAILURE(S)` : '\nAll stale-lead sweep checks passed.');
  process.exit(failures ? 1 : 0);
})();
