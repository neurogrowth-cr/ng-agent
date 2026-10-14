'use strict';
// Lead-to-close block for the Friday delivery delta (Ron, 2026-10-14).
//
// Month to date against the previous full month: leads -> booked -> held -> won,
// split by channel and by country. Definitions match closer_month_scorecard and
// the JS boards (classifyUnheldAppt + classifyOutcome in index.js):
//   leads   lead_posts rows not marked personal, by Costa Rica month of posted_at
//   booked  strategy-call appointments by Costa Rica month of scheduled_start;
//           deleted bookings and superseded rows (a reschedule) are left out,
//           so a rescheduled call counts once, at its new time
//   held    the logged outcome says the call happened (won, lost, follow_up,
//           nurture, disqualified)
//   no-show outcome no_show, or cancelled and never rebooked
//   won     outcome won on that appointment
//   show    held / (held + no-show); calls with no outcome yet are left out
// index.js owns the I/O; everything here is pure (test/lead-to-close.test.js).

const { table } = require('./slackTable');
const { leadChannel } = require('./leadVolume');

const CR_OFFSET_MS = 6 * 60 * 60 * 1000; // Costa Rica is UTC-6, no DST

const CHANNELS = [
  ['form', 'FB form'],
  ['dm', 'Setter/DM'],
  ['self', 'Self-serve'],
];
const COUNTRIES = [
  ['CR', 'Costa Rica'],
  ['MX', 'Mexico'],
  ['Other', 'Other'],
  ['Unknown', 'No phone'],
];
const MONTHS = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
const SELF_SERVE_RE = /self[\s._-]?serv|linkedin flywheel/i;

/** 'YYYY-MM' of a timestamp in Costa Rica time. */
function crMonthKey(ts) {
  const ms = new Date(ts).getTime();
  if (!Number.isFinite(ms)) return null;
  return new Date(ms - CR_OFFSET_MS).toISOString().slice(0, 7);
}

/** Month-to-date and previous-month windows (UTC ISO bounds of Costa Rica months). */
function monthWindows(now = new Date()) {
  const cr = new Date(new Date(now).getTime() - CR_OFFSET_MS);
  const y = cr.getUTCFullYear();
  const m = cr.getUTCMonth();
  const start = (yy, mm) => new Date(Date.UTC(yy, mm, 1) + CR_OFFSET_MS);
  const mk = (yy, mm) => {
    const s = start(yy, mm);
    const e = start(yy, mm + 1);
    const key = new Date(s.getTime() - CR_OFFSET_MS).toISOString().slice(0, 7);
    return { key, startIso: s.toISOString(), endIso: e.toISOString(), name: MONTHS[Number(key.slice(5)) - 1] };
  };
  const mtd = mk(y, m);
  mtd.day = cr.getUTCDate();
  return { mtd, prev: mk(y, m - 1) };
}

/** Lead channel bucket from a lead_posts row: 'form' | 'dm' | 'self'. */
// lead_posts.channel already holds the channel key (fb_form, instagram,
// whatsapp, messenger, other; set at intake since #254). Older rows have no
// channel, so the source label decides: "Facebook" is the instant form.
function leadBucket({ source, channel } = {}) {
  if (SELF_SERVE_RE.test(String(source || ''))) return 'self';
  const ch = String(channel || '').trim().toLowerCase() || leadChannel({ source });
  return ch === 'fb_form' ? 'form' : 'dm';
}

/**
 * Channel of a booked call. The lead card wins when the contact has one; a
 * booking with no lead card and no setter booked itself (self-serve).
 */
function apptBucket({ setterId, leadSource, leadRowBucket } = {}) {
  if (leadRowBucket) return leadRowBucket;
  if (!setterId) return 'self';
  if (SELF_SERVE_RE.test(String(leadSource || ''))) return 'self';
  return String(leadSource || '').trim().toLowerCase() === 'facebook' ? 'form' : 'dm';
}

/** Country from the phone on the booking: 'CR' | 'MX' | 'Other' | 'Unknown'. */
function countryFromPhone(phone) {
  const raw = String(phone || '').trim();
  const digits = raw.replace(/\D/g, '');
  if (!digits) return 'Unknown';
  const intl = raw.startsWith('+') || raw.startsWith('00');
  const d = raw.startsWith('00') ? digits.slice(2) : digits;
  if (intl) {
    if (d.startsWith('506')) return 'CR';
    if (d.startsWith('52')) return 'MX';
    return 'Other';
  }
  // No plus sign: only trust a full country-code length.
  if (d.length === 11 && d.startsWith('506')) return 'CR';
  if (d.length === 12 && d.startsWith('52')) return 'MX';
  if (d.length === 13 && d.startsWith('521')) return 'MX';
  return 'Other';
}

const empty = () => ({ leads: 0, booked: 0, held: 0, noShow: 0, pending: 0, won: 0 });

/**
 * leads: [{ month, bucket }]
 * appts: [{ month, bucket, country, cls: 'showed'|'no_show'|'pending'|'rescheduled', won }]
 * Returns { [month]: { total, channel: {form,dm,self}, country: {CR,MX,Other,Unknown} } }.
 */
function tallyFunnel({ leads = [], appts = [], months = [] }) {
  const out = {};
  for (const m of months) {
    out[m] = {
      total: empty(),
      channel: Object.fromEntries(CHANNELS.map(([k]) => [k, empty()])),
      country: Object.fromEntries(COUNTRIES.map(([k]) => [k, empty()])),
    };
  }
  for (const l of leads) {
    const t = out[l.month];
    if (!t) continue;
    t.total.leads++;
    (t.channel[l.bucket] || t.channel.dm).leads++;
  }
  for (const a of appts) {
    const t = out[a.month];
    if (!t) continue;
    const rows = [t.total, t.channel[a.bucket] || t.channel.dm, t.country[a.country] || t.country.Other];
    for (const r of rows) {
      r.booked++;
      if (a.cls === 'showed') r.held++;
      else if (a.cls === 'no_show') r.noShow++;
      else if (a.cls === 'pending') r.pending++;
      if (a.won) r.won++;
    }
  }
  return out;
}

/** Ratio as a percentage string, '-' when the denominator is 0. */
function pct(n, d) {
  if (!d) return '-';
  const v = (100 * n) / d;
  if (v === 0) return '0%';
  if (v < 1) return `${v.toFixed(2)}%`;
  return v < 10 ? `${v.toFixed(1)}%` : `${Math.round(v)}%`;
}

function rates(r) {
  // A self-serve call skips the lead feed, so booked can outrun leads in that
  // row; a lead-based rate over 100% would be noise, so it reads '-'.
  const leadOk = r.leads > 0 && r.booked <= r.leads;
  return {
    leadToBook: leadOk ? pct(r.booked, r.leads) : '-',
    show: pct(r.held, r.held + r.noShow),
    close: pct(r.won, r.held),
    leadToClose: leadOk ? pct(r.won, r.leads) : '-',
  };
}

/** Delta sentence: lead-to-close this month against last month. */
function headline(cur, prev, mtd, prevW) {
  const c = rates(cur);
  const p = rates(prev);
  const wins = (n) => `${n} ${n === 1 ? 'win' : 'wins'}`;
  return `Lead to close is ${c.leadToClose} so far in ${mtd.name} (${wins(cur.won)} from ${cur.leads} leads) `
    + `against ${p.leadToClose} in ${prevW.name} (${wins(prev.won)} from ${prev.leads}).`;
}

/**
 * tally: tallyFunnel output; windows: monthWindows output.
 * Returns the Slack block (no title emoji, tables in code blocks).
 */
function formatLeadToClose(tally, windows) {
  const { mtd, prev } = windows;
  const cur = tally[mtd.key];
  const old = tally[prev.key];
  if (!cur || !old) return '';
  const mLabel = `${mtd.name} 1-${mtd.day}`;
  const funnelRows = [];
  const push = (label, r, period) => {
    const x = rates(r);
    funnelRows.push([label, period, r.leads, r.booked, r.held, r.won, x.leadToBook, x.show, x.close, x.leadToClose]);
  };
  push('All', cur.total, mLabel);
  push('', old.total, prev.name);
  for (const [k, label] of CHANNELS) {
    push(label, cur.channel[k], mLabel);
    push('', old.channel[k], prev.name);
  }
  const countryRows = [];
  for (const [k, label] of COUNTRIES) {
    const a = cur.country[k];
    const b = old.country[k];
    if (k === 'Unknown' && !a.booked && !b.booked) continue;
    for (const [r, period, name] of [[a, mLabel, label], [b, prev.name, '']]) {
      const x = rates(r);
      countryRows.push([name, period, r.booked, r.held, r.noShow, r.won, x.show, x.close]);
    }
  }
  const pending = cur.total.pending;
  const notes = [];
  if (pending) notes.push(`${pending} ${mtd.name} ${pending === 1 ? 'call has' : 'calls have'} no outcome yet, so ${pending === 1 ? 'it is' : 'they are'} left out of show and close rates.`);
  const selfGap = [cur, old].some(t => t.channel.self.booked > t.channel.self.leads);
  if (selfGap) notes.push('Self-serve calls often book without a lead card, so that row has no lead rates.');
  notes.push('Booked, held and won count calls scheduled in the month; country comes from the phone on the booking.');
  return [
    '*Lead to close*',
    headline(cur.total, old.total, mtd, prev),
    table(['', '', 'Leads', 'Booked', 'Held', 'Won', 'L>B', 'Show', 'Close', 'L>C'], funnelRows),
    table(['', '', 'Booked', 'Held', 'No-show', 'Won', 'Show', 'Close'], countryRows),
    notes.join(' '),
  ].join('\n');
}

module.exports = {
  CHANNELS, COUNTRIES,
  crMonthKey, monthWindows, leadBucket, apptBucket, countryFromPhone,
  tallyFunnel, rates, pct, formatLeadToClose,
};
