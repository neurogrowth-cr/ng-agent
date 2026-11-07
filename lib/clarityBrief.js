// Clarity Brief: pure helpers (no Slack, no network, no database).
// Spec: tasks/specs/clarity-brief.md (Ron, 2026-11-03). index.js owns the
// collectors, the model calls, the Slack posting and the reaction handler;
// everything here can run in a test with fixtures.
//
// Vocabulary
//   candidate   something a collector found: { source, actor, title, url, dedupeKey,
//               createdAt, updatedAt?, kind?, tier?, whyRon?, action?, snippet? }
//   item        a candidate that survived suppression and ranking (same shape)
//   signature   source|actor|topic, what a ❌ teaches ("never again this")
//   actorSig    source|actor|*, what three ❌ in 30 days mute ("never this sender")

const crypto = require('crypto');

const TZ = 'America/Costa_Rica';
const MAX_ITEMS = 7;
const MAX_PROJECTS = 7;
const RUBRIC_LABEL = 'provisional (revenue now > operation health > hygiene)';
const TIERS = { 1: 'Revenue now', 2: 'Operation health', 3: 'Hygiene' };
const SOURCES = ['slack', 'max', 'gmail_ron', 'gmail_contact', 'calendar', 'github', 'wiki'];
const WINDOW_DEFAULT_H = 24;
const WINDOW_MAX_DAYS = 7;
// Classifier batch: 10 candidates fit well inside the token budget even when
// every one is needs_ron (about 110 output tokens each). 20 at 1800 tokens ran
// at the edge on 2026-11-05.
const CLASSIFY_BATCH = 10;
const CLASSIFY_MAX_TOKENS = 3000;
const MUTE_THRESHOLD = 3;
const MUTE_WINDOW_DAYS = 30;
const FOLLOWUP_MIN_AGE_DAYS = 14;
const FOLLOWUP_PER_BRIEF = 2;
const FOLLOWUP_RESHOW_DAYS = 7;
const ACCEPTED_STALE_DAYS = 7;
const EXPIRE_UNREACTED_H = 20;
const SNOOZE_H = 20;

const ACCEPT_EMOJIS = new Set(['white_check_mark', 'heavy_check_mark', 'check', 'ballot_box_with_check']);
const DISMISS_EMOJIS = new Set(['x', 'negative_squared_cross_mark', 'no_entry_sign', 'no_entry']);
const SNOOZE_EMOJIS = new Set(['next_track_button', 'fast_forward', 'arrow_right', 'black_right_pointing_double_triangle_with_vertical_bar']);

// Kinds a collector may set when it already knows what a thing is; the
// classifier fills the rest. Tier by kind is the rubric in section 3.1.
const KIND_TIER = {
  client_reply: 1, prospect_reply: 1, payment: 1, continuation: 1, deal_approval: 1,
  check_broken: 2, watchdog: 2, flag_flip: 2, team_ask: 2, security: 2, mention: 2, approval: 2,
  pr_review: 3, followup: 3, admin: 3, calendar: 3,
};

// ── Dates ────────────────────────────────────────────────────────────────────
function crParts(date) {
  const d = date instanceof Date ? date : new Date(date);
  const long = new Intl.DateTimeFormat('en-US', { timeZone: TZ, weekday: 'short', month: 'short', day: 'numeric' }).formatToParts(d);
  const get = (t) => (long.find(p => p.type === t) || {}).value;
  const ymd = new Intl.DateTimeFormat('en-CA', { timeZone: TZ, year: 'numeric', month: '2-digit', day: '2-digit' }).format(d);
  const hm = new Intl.DateTimeFormat('en-GB', { timeZone: TZ, hour: '2-digit', minute: '2-digit', hour12: false }).format(d);
  return { weekday: get('weekday'), month: get('month'), day: Number(get('day')), ymd, hm };
}

// The window is "since the last posted brief": Friday's brief makes Monday
// cover the weekend. No last brief (first run, or the table was emptied) means
// 24 hours. A gap longer than a week is clamped so a long outage does not
// dump a month of email into one DM; the clamp is reported, never silent.
function briefWindow({ lastPostedAt, now = new Date() } = {}) {
  const to = new Date(now);
  let from = lastPostedAt ? new Date(lastPostedAt) : new Date(to.getTime() - WINDOW_DEFAULT_H * 3600e3);
  let clamped = false;
  const max = new Date(to.getTime() - WINDOW_MAX_DAYS * 86400e3);
  if (!(from instanceof Date) || Number.isNaN(from.getTime()) || from >= to) { from = new Date(to.getTime() - WINDOW_DEFAULT_H * 3600e3); }
  if (from < max) { from = max; clamped = true; }
  return { from, to, clamped, hours: Math.round((to - from) / 3600e3) };
}

function daysBetween(a, b) { return Math.floor(Math.abs(new Date(b) - new Date(a)) / 86400e3); }

// ── Keys ─────────────────────────────────────────────────────────────────────
const STOP = new Set(['the', 'a', 'an', 're', 'fw', 'fwd', 'your', 'you', 'for', 'of', 'to', 'in', 'on', 'and', 'is', 'at', 'from', 'with', 'new', 'de', 'la', 'el', 'los', 'las', 'un', 'una', 'para', 'por', 'con', 'y', 'su', 'tu', 'del']);

function hashText(s) { return crypto.createHash('sha1').update(String(s || '')).digest('hex').slice(0, 10); }

function topicKey(title) {
  const toks = String(title || '').toLowerCase().normalize('NFD').replace(/[̀-ͯ]/g, '')
    .replace(/[^a-z0-9\s]/g, ' ').split(/\s+/)
    .filter(t => t && !STOP.has(t) && !/\d/.test(t) && t.length > 1);
  return toks.slice(0, 3).join('-') || 'general';
}

function signatureFor(c) { return `${c.source}|${String(c.actor || 'unknown').toLowerCase()}|${topicKey(c.title)}`; }
function actorSignatureFor(c) { return `${c.source}|${String(c.actor || 'unknown').toLowerCase()}|*`; }

function tierFor(c) {
  if ([1, 2, 3].includes(Number(c.tier))) return Number(c.tier);
  if (c.kind && KIND_TIER[c.kind]) return KIND_TIER[c.kind];
  return 3;
}

// ── Suppression (what a ❌ or a previous showing taught us) ──────────────────
// dismissals: rows of clarity_dismissals { signature, muted }
// shownItems: rows of clarity_items (any status) with dedupe_key, status, shown_at, snoozed_until
function applySuppression({ candidates = [], dismissals = [], shownItems = [], now = new Date() } = {}) {
  const dismissed = new Set(dismissals.map(d => d.signature));
  const mutedActors = new Set(dismissals.filter(d => d.muted).map(d => d.signature));
  const latest = new Map();
  for (const row of shownItems) {
    const prev = latest.get(row.dedupe_key);
    if (!prev || new Date(row.shown_at) > new Date(prev.shown_at)) latest.set(row.dedupe_key, row);
  }
  const kept = [], dropped = [];
  for (const c of candidates) {
    const sig = c.signature || signatureFor(c);
    const actorSig = actorSignatureFor(c);
    let reason = null;
    if (dismissed.has(sig)) reason = 'dismissed';
    else if (mutedActors.has(actorSig)) reason = 'muted';
    else {
      const row = latest.get(c.dedupeKey);
      if (row) {
        if (row.status === 'dismissed' || row.status === 'done') reason = `already_${row.status}`;
        else if (row.status === 'accepted') reason = 'accepted';
        else if (row.status === 'snoozed') { if (row.snoozed_until && new Date(row.snoozed_until) > now) reason = 'snoozed'; }
        else if (row.status === 'shown' || row.status === 'expired') {
          const escalated = c.updatedAt && new Date(c.updatedAt) > new Date(row.shown_at);
          if (!escalated) reason = 'repeat';
        }
      }
    }
    if (reason) dropped.push({ candidate: c, reason });
    else kept.push({ ...c, signature: sig, tier: tierFor(c) });
  }
  return { kept, dropped };
}

// Rank by tier, then older first. Items without a URL cannot meet the
// structural contract (Source: must be a link) so they go to overflow.
function rankAndCap(items = [], { max = MAX_ITEMS, now = new Date() } = {}) {
  const withUrl = [], noUrl = [];
  for (const it of items) (it.url && /^https?:\/\//.test(it.url) ? withUrl : noUrl).push(it);
  const sorted = withUrl.slice().sort((a, b) => {
    const t = tierFor(a) - tierFor(b);
    if (t) return t;
    return new Date(a.createdAt || now) - new Date(b.createdAt || now);
  });
  return {
    shown: sorted.slice(0, max).map(it => ({ ...it, tier: tierFor(it) })),
    overflow: [...sorted.slice(max).map(it => ({ item: it, reason: 'cap' })), ...noUrl.map(it => ({ item: it, reason: 'no_url' }))],
  };
}

// ── Projects in flight (block 2) ─────────────────────────────────────────────
// accepted: clarity_items rows with status accepted (Ron said "I take it")
// prs: { title, url, repo, updatedAt, author?, draft? } open PRs and branches of Ron's
// followupProjects: wiki follow-ups flagged as projects by the collector
function projectsInFlight({ accepted = [], prs = [], followupProjects = [], now = new Date(), max = MAX_PROJECTS } = {}) {
  const rows = [];
  for (const a of accepted) {
    const stale = daysBetween(a.accepted_at || a.shown_at, now) >= ACCEPTED_STALE_DAYS;
    rows.push({ kind: 'accepted', title: a.title, url: a.url, tier: Number(a.tier) || 3, lastActivityAt: a.accepted_at || a.shown_at, stale, itemId: a.id, ts: a.ts });
  }
  for (const p of prs) rows.push({ kind: 'pr', title: p.title, url: p.url, tier: Number(p.tier) || 3, lastActivityAt: p.updatedAt, stale: false });
  for (const f of followupProjects) rows.push({ kind: 'followup', title: f.title, url: f.url, tier: Number(f.tier) || 3, lastActivityAt: f.noted, stale: false });
  rows.sort((a, b) => (a.tier - b.tier) || (new Date(a.lastActivityAt || 0) - new Date(b.lastActivityAt || 0)));
  return rows.slice(0, max).map(r => ({ ...r, quietDays: r.lastActivityAt ? daysBetween(r.lastActivityAt, now) : null }));
}

// ── Wiki follow-ups ──────────────────────────────────────────────────────────
// Parses the "## Ron" table of ops/open-followups.md: | Item | Noted | Source |
function parseFollowupTable(markdown, { section = 'Ron' } = {}) {
  const lines = String(markdown || '').split('\n');
  let inSection = false;
  const out = [];
  for (const raw of lines) {
    const line = raw.trim();
    if (/^## /.test(line)) { inSection = new RegExp(`^## .*\\b${section}\\b`).test(line); continue; }
    if (!inSection || !line.startsWith('|')) continue;
    const cells = line.split('|').slice(1, -1).map(c => c.trim());
    if (cells.length < 2) continue;
    if (/^-+$/.test(cells[0].replace(/\s/g, '')) || /^item$/i.test(cells[0])) continue;
    const noted = (cells[1] || '').match(/\d{4}-\d{2}-\d{2}/);
    out.push({ item: cells[0], noted: noted ? noted[0] : null, source: cells[2] || '' });
  }
  return out;
}

// Up to two follow-ups per brief: at least 14 days old, oldest first, not
// shown in the last 7 days. A ❌ is handled by applySuppression through the
// wiki:<hash> dedupe key, so this only has to rotate.
function eligibleFollowups({ rows = [], shownItems = [], now = new Date(), pageUrl = '', minAgeDays = FOLLOWUP_MIN_AGE_DAYS, perBrief = FOLLOWUP_PER_BRIEF, reshowDays = FOLLOWUP_RESHOW_DAYS } = {}) {
  const recent = new Set(shownItems
    .filter(r => r.source === 'wiki' && daysBetween(r.shown_at, now) < reshowDays)
    .map(r => r.dedupe_key));
  const blocked = new Set(shownItems.filter(r => r.source === 'wiki' && ['dismissed', 'done', 'accepted'].includes(r.status)).map(r => r.dedupe_key));
  return rows
    .map(r => ({ ...r, dedupeKey: `wiki:${hashText(r.item)}`, ageDays: r.noted ? daysBetween(r.noted, now) : null }))
    .filter(r => r.ageDays !== null && r.ageDays >= minAgeDays && !recent.has(r.dedupeKey) && !blocked.has(r.dedupeKey))
    .sort((a, b) => b.ageDays - a.ageDays)
    .slice(0, perBrief)
    .map(r => ({
      source: 'wiki', actor: 'wiki', kind: 'followup', tier: 3,
      title: clip(r.item, 110), url: pageUrl, dedupeKey: r.dedupeKey, createdAt: r.noted,
      whyRon: `Open ${r.ageDays} days in the wiki follow-ups, owner Ron.`,
      action: 'Do it, delegate it, or ❌ to drop it from the brief (the wiki row stays).',
      snippet: r.source,
    }));
}

// ── Reactions ────────────────────────────────────────────────────────────────
// Returns what the handler should write. Never writes itself.
function applyReaction({ item, reaction, userId, ronId, now = new Date() } = {}) {
  if (!item) return { ignore: true, reason: 'no_item' };
  if (userId !== ronId) return { ignore: true, reason: 'not_ron' };
  const status = item.status;
  if (ACCEPT_EMOJIS.has(reaction)) {
    if (status === 'shown' || status === 'snoozed' || status === 'expired') return { status: 'accepted', set: { status: 'accepted', accepted_at: now.toISOString(), reacted_by: userId } };
    if (status === 'accepted') return { status: 'done', set: { status: 'done', done_at: now.toISOString(), reacted_by: userId } };
    return { ignore: true, reason: `accept_from_${status}` };
  }
  if (DISMISS_EMOJIS.has(reaction)) {
    if (['shown', 'accepted', 'snoozed', 'expired'].includes(status)) {
      return {
        status: 'dismissed',
        set: { status: 'dismissed', reacted_by: userId },
        dismissal: { signature: item.signature, source: item.source, sample_title: item.title },
      };
    }
    return { ignore: true, reason: `dismiss_from_${status}` };
  }
  if (SNOOZE_EMOJIS.has(reaction)) {
    if (status === 'shown' || status === 'expired') {
      const until = new Date(now.getTime() + SNOOZE_H * 3600e3).toISOString();
      return { status: 'snoozed', set: { status: 'snoozed', snoozed_until: until, reacted_by: userId } };
    }
    return { ignore: true, reason: `snooze_from_${status}` };
  }
  return { ignore: true, reason: 'other_emoji' };
}

// Three ❌ on the same sender or channel within 30 days mute the actor.
// dismissalsForActor: rows whose signature starts with "source|actor|" (any topic).
function muteDecision({ dismissalsForActor = [], actorSignature, now = new Date() } = {}) {
  const recent = dismissalsForActor.filter(d => !d.signature.endsWith('|*') && daysBetween(d.last_at || d.first_at, now) <= MUTE_WINDOW_DAYS);
  const count = recent.reduce((n, d) => n + Math.max(1, Number(d.count) || 1), 0);
  return { mute: count >= MUTE_THRESHOLD, count, actorSignature };
}

function expireUnreacted(rows = [], now = new Date(), hours = EXPIRE_UNREACTED_H) {
  return rows.filter(r => r.status === 'shown' && (now - new Date(r.shown_at)) >= hours * 3600e3).map(r => r.id);
}

// ── Formatting ───────────────────────────────────────────────────────────────
function clip(s, n) { s = String(s || '').replace(/\s+/g, ' ').trim(); return s.length <= n ? s : s.slice(0, n - 1).trimEnd() + '…'; }
function noDash(s) { return String(s || '').replace(/\s*\u2014\s*/g, ', ').replace(/\s+\u2013\s+/g, ', '); }

function headerLine({ date, items, projects }) {
  const p = crParts(date);
  return `CLARITY BRIEF · ${p.weekday} ${p.month} ${p.day} · ${items} for you · ${projects} projects`;
}
const HEADER_RE = /^CLARITY BRIEF · \w+ \w+ \d+ · (\d+) for you · (\d+) projects$/;

// counts: { slack: { scanned: 31, failed: false }, gmail_contact: { scanned: 0, failed: 'auth' }, ... }
function scannedLine(counts = {}, { dropped = 0, expired = 0, muted = 0, enabledSources = SOURCES } = {}) {
  const parts = [];
  for (const src of enabledSources) {
    const c = counts[src] || {};
    if (c.failed) parts.push(`⚠️ ${src} unavailable`);
    else parts.push(`${src} ${Number(c.scanned) || 0}`);
  }
  parts.push(`dropped ${dropped}`);
  if (expired) parts.push(`expired ${expired}`);
  if (muted) parts.push(`muted ${muted}`);
  return `Scanned: ${parts.join(' · ')}`;
}

function todayLine(events = []) {
  if (!events.length) return null;
  const bits = events.slice(0, 6).map(e => `${e.hm} ${clip(noDash(e.label), 40)}`);
  return `Today: ${bits.join(' · ')}${events.length > 6 ? ` · +${events.length - 6} more` : ''}`;
}

function formatParent({ date, items = [], projects = [], rubricLabel = RUBRIC_LABEL, scanned, today = [], staleNote = '' } = {}) {
  const lines = [headerLine({ date, items: items.length, projects: projects.length }), `Ranked against: ${rubricLabel}`];
  const t = todayLine(today);
  if (t) lines.push(t);
  lines.push('');
  if (items.length) lines.push(`For you: ${items.length} item${items.length === 1 ? '' : 's'} in this thread. React ✅ take · ❌ never again · ⏭ tomorrow.`);
  else lines.push('Nothing needs you today.');
  lines.push('');
  if (projects.length) {
    lines.push('Projects in flight');
    projects.forEach((p, i) => {
      const quiet = p.quietDays === null || p.quietDays === undefined ? '' : ` · ${p.quietDays}d quiet`;
      const flag = p.stale ? ' · 7+ days: ✅ done or ❌ drop' : '';
      const why = p.why ? `: ${clip(noDash(p.why), 140)}` : '';
      lines.push(`${i + 1}. ${clip(noDash(p.title), 90)} (T${p.tier}${quiet}${flag})${why}`);
    });
  } else lines.push('Projects in flight: none tracked.');
  if (staleNote) lines.push(staleNote);
  lines.push('');
  lines.push(scanned);
  return lines.join('\n');
}

function formatItemReply(item, index) {
  const tier = tierFor(item);
  return [
    `*${index}. ${clip(noDash(item.title), 100)}*  [T${tier} ${TIERS[tier]} · ${item.source}]`,
    `Source: ${item.url}`,
    `Why you: ${clip(noDash(item.whyRon || item.why_ron || 'Only you can decide this.'), 200)}`,
    `Do: ${clip(noDash(item.action || 'Decide and react.'), 180)}`,
  ].join('\n');
}

// ── Structural contract (section 4a) ─────────────────────────────────────────
const FORBIDDEN = [
  [/\bundefined\b/, 'undefined'], [/\bnull\b/, 'null'], [/\[object Object\]/, 'object'],
  [/⚠️ PARSE FAILED/, 'parse_failed'], [/APPROVAL_NEEDED/, 'approval_needed'], [/\u2014/, 'em_dash'],
];
const EMPTY_LABEL = /^[A-Z][A-Za-z ]{1,24}:\s*$/m;

function validateClarityBrief({ parent = '', replies = [], enabledSources = SOURCES } = {}) {
  const problems = [];
  const lines = parent.split('\n');
  const h = lines[0].match(HEADER_RE);
  if (!h) problems.push('header');
  else if (Number(h[1]) !== replies.length) problems.push(`header_count:${h[1]}!=${replies.length}`);
  if (!lines.some(l => /^Ranked against: \S/.test(l))) problems.push('ranked_against');
  const scanned = lines.find(l => l.startsWith('Scanned: '));
  if (!scanned) problems.push('scanned_line');
  else for (const src of enabledSources) {
    if (!new RegExp(`(^Scanned: |· )${src} \\d+`).test(scanned) && !scanned.includes(`⚠️ ${src} unavailable`)) problems.push(`scanned_missing:${src}`);
  }
  if (replies.length === 0 && !parent.includes('Nothing needs you today.')) problems.push('zero_items_line');
  replies.forEach((r, i) => {
    if (!/^Source: https?:\/\/\S+/m.test(r)) problems.push(`reply${i + 1}:source`);
    if (!/^Why you: \S/m.test(r)) problems.push(`reply${i + 1}:why`);
    if (!/^Do: \S/m.test(r)) problems.push(`reply${i + 1}:do`);
  });
  const all = [parent, ...replies].join('\n');
  for (const [re, name] of FORBIDDEN) if (re.test(all)) problems.push(`forbidden:${name}`);
  if (EMPTY_LABEL.test(all)) problems.push('empty_label');
  return { ok: problems.length === 0, problems };
}

// Numbers-only fallback (section 4a, second failure): header, Scanned line,
// raw titles with their URLs, no prose.
function fallbackReplies(items = []) {
  return items.map((it, i) => [
    `*${i + 1}. ${clip(noDash(it.title), 100)}*  [T${tierFor(it)} ${TIERS[tierFor(it)]} · ${it.source}]`,
    `Source: ${it.url}`,
    `Why you: ${clip(noDash(it.whyRon || 'Flagged for you by the collector.'), 200)}`,
    `Do: ${clip(noDash(it.action || 'Open the source and decide.'), 180)}`,
  ].join('\n'));
}

// ── Verdict (section 7) ──────────────────────────────────────────────────────
function verdictLine({ ok, postedAt, items = 0, projects = 0, sourcesOk = 0, sourcesTotal = 0, costUsd = 0, issues = [] } = {}) {
  if (ok) return `✅ ALL GREEN: Clarity Brief posted ${crParts(postedAt || new Date()).hm}, ${items} for you, ${projects} projects, sources ${sourcesOk}/${sourcesTotal}, cost $${Number(costUsd || 0).toFixed(2)}`;
  return `⚠️ ISSUES: ${issues.length ? issues.join(' · ') : 'unspecified'}`;
}

// ── Model prompts and parsing ────────────────────────────────────────────────
const TOOL_ALERT_ALLOWLIST = 'payment failed or declined, invoice or payment overdue, account suspended or disconnected, security alert or new sign-in, quota or limit exceeded, domain, DNS or certificate expiring, legal or compliance notice, a client or prospect replying';

function buildClassifyPrompt(batch = []) {
  const rows = batch.map(c => JSON.stringify({ idx: c.idx, source: c.source, actor: c.actor, kind: c.kind || null, title: clip(c.title, 140), snippet: clip(c.snippet || '', 400), ageHours: c.ageHours ?? null }));
  return [
    "You triage the CEO's morning brief. For each candidate decide whether ONLY Ron (the CEO) can act on it.",
    'Return a JSON array only, one object per candidate, in the same order as the candidates.',
    'Every object MUST start with "idx", copied exactly from its candidate. Fields:',
    '  idx: the candidate idx (required)',
    '  verdict: needs_ron | team | fyi | noise',
    '  tier: 1 (money or a deal waits on Ron: a client or prospect reply, a payment failed or overdue, a day-91 continuation decision, a close blocked on him), 2 (the machine or the team is blocked on him: a broken check, a watchdog alarm, a flag waiting for a flip, a teammate asking him, security), 3 (nothing stops without him but it rots: reviews, follow-ups, admin)',
    '  title: at most 80 characters, English, names the sender or system and the ask',
    '  why_ron: at most 140 characters, why a teammate cannot do this',
    '  action: at most 120 characters, imperative, the next physical step',
    'Only needs_ron objects carry tier, title, why_ron and action. For team, fyi and noise return just {"idx": n, "verdict": "..."}.',
    `Tool and system notifications are needs_ron only when they match: ${TOOL_ALERT_ALLOWLIST}. Newsletters, marketing, receipts for normal payments, calendar auto-notifications, social notifications, digests Max himself posted are noise.`,
    'A message a setter, closer or fulfillment teammate can answer is team. Information with no ask is fyi.',
    'Never invent facts not in the candidate. Never use an em dash. Output English.',
    'Candidates:',
    ...rows,
  ].join('\n');
}

function extractJson(raw, open, close) {
  let s = String(raw || '').replace(/```(?:json)?/gi, '').trim();
  const a = s.indexOf(open), b = s.lastIndexOf(close);
  if (a < 0 || b < 0 || b <= a) return null;
  try { return JSON.parse(s.slice(a, b + 1)); } catch { return null; }
}

// Returns null when nothing in the answer can be matched to a candidate, so a
// batch the model answered without idx is reported as junk, never dropped in
// silence. A model that leaves idx out but answers every candidate in order is
// matched by position (seen on 2026-11-05: every object lacked idx).
function parseClassification(raw, batchSize) {
  const arr = extractJson(raw, '[', ']');
  if (!Array.isArray(arr)) return null;
  const byPosition = batchSize !== undefined && arr.length === batchSize && arr.every(o => o && typeof o === 'object' && o.idx === undefined);
  const out = new Map();
  for (const [pos, o] of arr.entries()) {
    if (!o || typeof o !== 'object') continue;
    const idx = byPosition ? pos : (o.idx === undefined || o.idx === null || o.idx === '' ? NaN : Number(o.idx));
    if (!Number.isInteger(idx) || idx < 0 || (batchSize !== undefined && idx >= batchSize)) continue;
    const verdict = ['needs_ron', 'team', 'fyi', 'noise'].includes(o.verdict) ? o.verdict : 'noise';
    const tier = [1, 2, 3].includes(Number(o.tier)) ? Number(o.tier) : 3;
    out.set(idx, {
      verdict, tier,
      title: clip(noDash(o.title || ''), 80) || null,
      whyRon: clip(noDash(o.why_ron || o.whyRon || ''), 140) || null,
      action: clip(noDash(o.action || ''), 120) || null,
    });
  }
  return out.size ? out : null;
}

function buildDraftPrompt({ items = [], projects = [], rubricLabel = RUBRIC_LABEL } = {}) {
  return [
    "You write the CEO's morning brief. Ron reads it on his phone at 06:50. Tight, factual, English, no em dash, no emoji, no filler.",
    `Ranking frame: ${rubricLabel}.`,
    'Return a JSON object only: { "items": [ { "idx", "title", "why_ron", "action" } ], "projects": [ { "idx", "why" } ] }.',
    'items: keep every idx given; title at most 80 characters naming who and what; why_ron at most 140 characters, why only Ron can do it; action at most 120 characters, imperative, one step. Do not change facts, names, amounts or dates; if the input lacks a detail, leave it out.',
    'projects: for every idx, why: at most 110 characters, why this ranks here today (what it unblocks or what it costs to leave quiet).',
    'Items:',
    ...items.map(it => JSON.stringify({ idx: it.idx, tier: tierFor(it), source: it.source, title: it.title, why_ron: it.whyRon || null, action: it.action || null, snippet: clip(it.snippet || '', 300) })),
    'Projects:',
    ...projects.map(p => JSON.stringify({ idx: p.idx, tier: p.tier, title: p.title, kind: p.kind, quietDays: p.quietDays, stale: !!p.stale })),
  ].join('\n');
}

function parseDraftJson(raw, { nItems = 0, nProjects = 0 } = {}) {
  const obj = extractJson(raw, '{', '}');
  if (!obj || typeof obj !== 'object') return null;
  const items = new Map(), projects = new Map();
  for (const o of (Array.isArray(obj.items) ? obj.items : [])) {
    const idx = Number(o && o.idx);
    if (!Number.isInteger(idx) || idx < 0 || idx >= nItems) continue;
    items.set(idx, { title: clip(noDash(o.title || ''), 80) || null, whyRon: clip(noDash(o.why_ron || o.whyRon || ''), 140) || null, action: clip(noDash(o.action || ''), 120) || null });
  }
  for (const o of (Array.isArray(obj.projects) ? obj.projects : [])) {
    const idx = Number(o && o.idx);
    if (!Number.isInteger(idx) || idx < 0 || idx >= nProjects) continue;
    projects.set(idx, { why: clip(noDash(o.why || ''), 110) || null });
  }
  if (items.size !== nItems) return null;
  return { items, projects };
}

function mergeDraft({ items = [], projects = [], draft } = {}) {
  const mergedItems = items.map((it, i) => {
    const d = draft && draft.items.get(i);
    return d ? { ...it, title: d.title || it.title, whyRon: d.whyRon || it.whyRon, action: d.action || it.action } : it;
  });
  const mergedProjects = projects.map((p, i) => {
    const d = draft && draft.projects.get(i);
    return d && d.why ? { ...p, why: d.why } : p;
  });
  return { items: mergedItems, projects: mergedProjects };
}

// Names the mailbox in the link. /u/0/ is whichever account the browser signed in
// first, so a contact@ thread would open in Ron's inbox and show nothing.
function gmailThreadUrl(mailbox, threadId) {
  return `https://mail.google.com/mail/u/${encodeURIComponent(String(mailbox || '0')).replace(/%40/g, '@')}/#inbox/${threadId}`;
}

module.exports = {
  gmailThreadUrl,
  TZ, MAX_ITEMS, MAX_PROJECTS, RUBRIC_LABEL, TIERS, SOURCES, KIND_TIER,
  ACCEPT_EMOJIS, DISMISS_EMOJIS, SNOOZE_EMOJIS, MUTE_THRESHOLD, MUTE_WINDOW_DAYS,
  crParts, briefWindow, daysBetween, hashText, topicKey, signatureFor, actorSignatureFor, tierFor,
  applySuppression, rankAndCap, projectsInFlight, parseFollowupTable, eligibleFollowups,
  applyReaction, muteDecision, expireUnreacted,
  clip, noDash, headerLine, scannedLine, todayLine, formatParent, formatItemReply, validateClarityBrief, fallbackReplies, verdictLine,
  buildClassifyPrompt, parseClassification, buildDraftPrompt, parseDraftJson, mergeDraft,
  CLASSIFY_BATCH, CLASSIFY_MAX_TOKENS,
};
