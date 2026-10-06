// Kai health watch. Recipe: ~/automations/ops/recipes/kai-health-watch.md
//
// Kai (ng-automation, Railway service ng-automation-KAI) is the LinkedIn reply
// agent. Until 2026-09-23 nothing watched it: an Anthropic spending-limit
// outage (2026-09-16), 98 replies misreported as failed on a dead Slack
// channel, and an auto-reply on top of a client's own reply all went
// unnoticed. This module holds the pure logic (SQL, evaluation, streaks,
// digest formatting) so it is testable without booting the Slack app;
// index.js owns the I/O and the crons.
//
// Read-only: every query runs on portalPg (role max_readonly, BYPASSRLS,
// SELECT on prosp_reply_queue / prosp_reply_log / workspaces). Nothing here
// writes to Kai's tables, pauses a workspace or resends anything.

const CR_TZ = 'America/Costa_Rica';
const REALERT_MS = 6 * 60 * 60 * 1000;

// Consecutive failing polls before the first alert. Facts (an error row, a
// double send) fire on the first poll; conditions that a deploy or a quiet
// hour can fake need confirmation.
const NEEDED = {
  health: 3,
  stuck: 2,
  intake: 2,
  thread_check: 2,
  thread_check_account: 2,
  thread_check_key: 2,
  skip_share: 2,
  double_send: 1,
  watcher_read: 2,
  errors: 1,
};
const FIXED_IDS = ['health', 'stuck', 'intake', 'thread_check', 'thread_check_account', 'thread_check_key', 'skip_share', 'double_send'];

// Backtested on 2026-08-24..09-23: a 3 h window would have fired falsely on 12
// weekdays (replies arrive in bursts); 6 h fires on one day (09-16, a real
// low-volume day), 8 h on none.
const INTAKE_WINDOW_HOURS = 6;

const SQL = {
  errors: `
    SELECT l.id, l.event, l.created_at,
           COALESCE(l.detail->>'error', q.error, '') AS error,
           COALESCE(l.detail->>'context', '') AS context,
           COALESCE(w.client_name, '') AS client_name
      FROM prosp_reply_log l
      LEFT JOIN prosp_reply_queue q ON q.id = l.queue_id
      LEFT JOIN workspaces w ON w.id = l.workspace_id
     WHERE l.event IN ('error', 'send_failed', 'slack_notify_failed')
       AND l.created_at > $1::timestamptz - interval '60 minutes'
       AND l.created_at <= $1::timestamptz
     ORDER BY l.created_at DESC
     LIMIT 200`,
  stuck: `
    SELECT count(*)::int AS n, min(created_at) AS oldest
      FROM prosp_reply_queue
     WHERE status = 'processing'
       AND created_at < $1::timestamptz - interval '10 minutes'
       AND created_at > $1::timestamptz - interval '7 days'`,
  received: `
    SELECT count(*)::int AS n
      FROM prosp_reply_log
     WHERE event = 'received'
       AND created_at > $1::timestamptz - make_interval(hours => $2::int)
       AND created_at <= $1::timestamptz`,
  // Every failed pre-send thread read in 24 h, with the workspace's latest
  // read that worked (any event that only happens after a successful read).
  // A failure followed by a good read is resolved. Cosiba 2026-10-05: an
  // expired Prosp login, fixed by the client in 6 minutes, alerted 39 minutes
  // later as "Prosp may be down" because the old rule only counted rows.
  threadCheck: `
    SELECT l.workspace_id, l.created_at,
           COALESCE(w.client_name, '') AS client_name,
           COALESCE(l.detail->>'errorClass', '') AS error_class,
           COALESCE(l.detail->>'error', '') AS error,
           (SELECT max(s.created_at)
              FROM prosp_reply_log s
             WHERE s.workspace_id = l.workspace_id
               AND s.event IN ('sent', 'auto_sent', 'skipped_client_replied', 'skipped_prospect_replied')
               AND s.created_at > $1::timestamptz - interval '24 hours'
               AND s.created_at <= $1::timestamptz) AS last_ok_at
      FROM prosp_reply_log l
      LEFT JOIN workspaces w ON w.id = l.workspace_id
     WHERE l.created_at > $1::timestamptz - interval '24 hours'
       AND l.created_at <= $1::timestamptz
       AND (l.event = 'thread_check_failed'
            OR (l.event = 'auto_send_fallback' AND l.detail->>'reason' = 'thread_check_failed'))
     ORDER BY l.created_at DESC
     LIMIT 500`,
  autoSend24h: `
    SELECT count(*) FILTER (WHERE event = 'auto_sent')::int AS sent,
           count(*) FILTER (WHERE event = 'skipped_client_replied' AND actor = 'agent')::int AS skipped
      FROM prosp_reply_log
     WHERE created_at > $1::timestamptz - interval '24 hours'
       AND created_at <= $1::timestamptz`,
  // Two replies to the same prospect within 30 minutes. Normal back-and-forth
  // is hours apart; this is the signature of answering one message twice.
  // Real case: 2026-09-23 09:52 UTC, Prosp delivered one Palantier prospect
  // message twice (1 ms apart, two message ids) and Kai sent two different
  // replies 3 s apart. Looks back 60 min only, so each event alerts once.
  doubleSends: `
    SELECT w.client_name, b.prospect_name, a.sent_at AS first_at, b.sent_at AS second_at
      FROM prosp_reply_queue a
      JOIN prosp_reply_queue b
        ON b.workspace_id = a.workspace_id
       AND b.prospect_linkedin_url = a.prospect_linkedin_url
       AND b.id <> a.id
       AND b.sent_at >= a.sent_at
       AND b.sent_at - a.sent_at <= interval '30 minutes'
      JOIN workspaces w ON w.id = a.workspace_id
     WHERE a.status = 'sent' AND b.status = 'sent'
       AND a.id < b.id
       AND b.sent_at > $1::timestamptz - interval '60 minutes'
       AND b.sent_at <= $1::timestamptz
     LIMIT 20`,
};

function crParts(date) {
  const parts = new Intl.DateTimeFormat('en-US', {
    timeZone: CR_TZ, weekday: 'short', hour: 'numeric', hour12: false,
  }).formatToParts(date);
  const map = Object.fromEntries(parts.map((p) => [p.type, p.value]));
  return { weekday: map.weekday, hour: Number(map.hour) % 24 };
}

// Intake silence only means something while replies normally flow.
function isIntakeWindow(date) {
  const { weekday, hour } = crParts(date);
  return !['Sat', 'Sun'].includes(weekday) && hour >= 8 && hour < 20;
}

function classifyKaiError(row) {
  const text = String(row.error || '');
  if (/usage limit/i.test(text)) return 'usage_limit';
  if (row.event === 'slack_notify_failed' || /channel_not_found|not_in_channel|slack/i.test(text)) return 'slack';
  if (row.event === 'send_failed') return 'prosp_send';
  return 'pipeline';
}

const ERROR_LABELS = {
  usage_limit: 'Anthropic API spending limit reached. Kai cannot draft ANY reply until the limit is raised in the Anthropic console.',
  slack: 'Kai could not post to Slack. Replies still process, but nobody sees them. Check the workspace Slack channel IDs.',
  prosp_send: 'Prosp rejected a reply Kai tried to send. Check the Prosp API key and sender account for that client.',
  pipeline: 'Kai hit an error while processing a reply. The reply was not answered.',
};

// Why a thread read failed. Kai stamps detail.errorClass since 2026-10-05
// (ng-automation lib/thread-freshness.js classifyThreadCheckError); this copy
// classifies older rows from the error text. Keep the two in step.
function classifyThreadCheckError(error) {
  const text = String(error || '');
  if (/^missing_/.test(text)) return 'config';
  if (/payment/i.test(text)) return 'payment_failed';
  const http = /^conversation_http_(\d{3})/.exec(text);
  if (http) {
    const status = Number(http[1]);
    if (status === 401 && /expired|reauth/i.test(text)) return 'login_expired';
    if (status === 401 || status === 403) return 'api_key_rejected';
    if (status === 404 && /account/i.test(text)) return 'account_not_found';
    if (status === 429) return 'rate_limited';
    if (status >= 500) return 'prosp_unreachable';
    return 'thread_unreadable';
  }
  if (/^conversation_(fetch_error|non_json)/.test(text)) return 'prosp_unreachable';
  if (/^conversation_(empty|unrecognized_shape)/.test(text)) return 'thread_unreadable';
  return 'unknown';
}

// Who has to act on a thread-read failure.
const THREAD_CLASS_GROUP = {
  login_expired: 'account',
  account_not_found: 'account',
  payment_failed: 'account',
  api_key_rejected: 'key',
  config: 'key',
};
const THREAD_CLASS_LABELS = {
  login_expired: 'LinkedIn login expired in Prosp',
  account_not_found: 'LinkedIn account disconnected in Prosp',
  payment_failed: 'Prosp billing problem',
  api_key_rejected: 'Prosp rejected our API key',
  config: 'Prosp API key or LinkedIn URL missing in Kai',
};

// Failures still open: after the workspace's last good read, newest first.
function unresolvedThreadFailures(rows) {
  return (rows || [])
    .filter((r) => !r.last_ok_at || new Date(r.last_ok_at) < new Date(r.created_at))
    .map((r) => ({ ...r, cls: r.error_class || classifyThreadCheckError(r.error) }));
}

function clientLabel(name) {
  return String(name || '').trim().toUpperCase() || 'UNKNOWN CLIENT';
}

// One line per workspace: "COSIBA: LinkedIn login expired in Prosp (4 tries, last 17:00 CR)".
function threadFailureLines(rows) {
  const byWs = new Map();
  for (const r of rows) {
    const key = r.workspace_id || r.client_name;
    if (!byWs.has(key)) byWs.set(key, []);
    byWs.get(key).push(r);
  }
  return [...byWs.values()].map((list) => {
    const latest = list[0];
    const at = new Date(latest.created_at).toLocaleTimeString('en-US', { timeZone: CR_TZ, hour: '2-digit', minute: '2-digit', hour12: false });
    const label = THREAD_CLASS_LABELS[latest.cls] || latest.cls;
    return `${clientLabel(latest.client_name)}: ${label} (${list.length} tr${list.length === 1 ? 'y' : 'ies'}, last ${at} CR)`;
  });
}

async function readKaiSnapshot(pg, now) {
  const ts = now.toISOString();
  const [errors, stuck, received, threadCheck, autoSend, doubleSends] = await Promise.all([
    pg.query(SQL.errors, [ts]),
    pg.query(SQL.stuck, [ts]),
    pg.query(SQL.received, [ts, INTAKE_WINDOW_HOURS]),
    pg.query(SQL.threadCheck, [ts]),
    pg.query(SQL.autoSend24h, [ts]),
    pg.query(SQL.doubleSends, [ts]),
  ]);
  return {
    errors: errors.rows,
    stuck: stuck.rows[0] || { n: 0, oldest: null },
    received: received.rows[0]?.n ?? 0,
    threadCheckFails: threadCheck.rows,
    autoSend: autoSend.rows[0] || { sent: 0, skipped: 0 },
    doubleSends: doubleSends.rows,
  };
}

// Turn one poll into per-check results: { id, failing, reason, needed, skip }.
// `snapshot` is null when the DB read failed; `snapshotError` says why.
function evaluateKaiChecks({ health, snapshot, snapshotError, now }) {
  const results = [];

  results.push(health.ok
    ? { id: 'health', failing: false, needed: NEEDED.health }
    : { id: 'health', failing: true, needed: NEEDED.health,
        reason: `Kai is not answering its health check (${health.error || 'no response'}). Nothing is being received, drafted or sent. Check the ng-automation-KAI Railway service logs.` });

  if (!snapshot) {
    results.push({ id: 'watcher_read', failing: true, needed: NEEDED.watcher_read,
      reason: `Max cannot read Kai's tables (${snapshotError || 'unknown error'}). The Kai watch is blind until this is fixed. Check PORTAL_READONLY_DATABASE_URL on ng-pm-MAX.` });
    for (const id of FIXED_IDS.filter((i) => i !== 'health')) results.push({ id, skip: true });
    return results;
  }
  results.push({ id: 'watcher_read', failing: false, needed: NEEDED.watcher_read });

  const byClass = new Map();
  for (const row of snapshot.errors) {
    const cls = classifyKaiError(row);
    if (!byClass.has(cls)) byClass.set(cls, []);
    byClass.get(cls).push(row);
  }
  for (const [cls, rows] of byClass) {
    const latest = rows[0];
    const client = String(latest.client_name || '').trim().toUpperCase() || 'UNKNOWN CLIENT';
    results.push({
      id: `errors:${cls}`, failing: true, needed: NEEDED.errors,
      reason: `${ERROR_LABELS[cls]} ${rows.length} in the last hour. Latest: ${client}: ${String(latest.error || latest.event).slice(0, 160)}`,
    });
  }

  results.push(snapshot.stuck.n > 0
    ? { id: 'stuck', failing: true, needed: NEEDED.stuck,
        reason: `${snapshot.stuck.n} repl${snapshot.stuck.n === 1 ? 'y has' : 'ies have'} been stuck mid-processing for over 10 minutes. Kai likely crashed during a reply. Check the Railway logs.` }
    : { id: 'stuck', failing: false, needed: NEEDED.stuck });

  if (!isIntakeWindow(now)) {
    results.push({ id: 'intake', skip: true });
  } else {
    results.push(snapshot.received === 0
      ? { id: 'intake', failing: true, needed: NEEDED.intake,
          reason: `No prospect replies received in ${INTAKE_WINDOW_HOURS} hours during business hours (usual weekday volume is 17 to 50 a day). Prosp may have stopped sending webhooks.` }
      : { id: 'intake', failing: false, needed: NEEDED.intake });
  }

  // Thread reads: split by who has to act, and drop any workspace whose
  // later read worked. Kai already refused to send and told the approver, so
  // these alerts are about follow-through, not damage.
  const open = unresolvedThreadFailures(snapshot.threadCheckFails);
  const account = open.filter((r) => THREAD_CLASS_GROUP[r.cls] === 'account');
  const key = open.filter((r) => THREAD_CLASS_GROUP[r.cls] === 'key');
  const hourAgo = now.getTime() - 60 * 60 * 1000;
  const prosp = open.filter((r) => !THREAD_CLASS_GROUP[r.cls] && new Date(r.created_at).getTime() > hourAgo);
  const prospWorkspaces = new Set(prosp.map((r) => r.workspace_id || r.client_name)).size;

  results.push(account.length
    ? { id: 'thread_check_account', failing: true, needed: NEEDED.thread_check_account,
        reason: `Kai cannot read or send for ${threadFailureLines(account).join('; ')}. The client has to fix this in Prosp; Kai told the approver in Slack. Nothing has gone through since, so check they saw it.` }
    : { id: 'thread_check_account', failing: false, needed: NEEDED.thread_check_account });

  results.push(key.length
    ? { id: 'thread_check_key', failing: true, needed: NEEDED.thread_check_key,
        reason: `Kai cannot read or send for ${threadFailureLines(key).join('; ')}. This is on our side: update the client's Prosp key in the dash Admin Prosp tab.` }
    : { id: 'thread_check_key', failing: false, needed: NEEDED.thread_check_key });

  results.push(prosp.length >= 3 || prospWorkspaces >= 2
    ? { id: 'thread_check', failing: true, needed: NEEDED.thread_check,
        reason: `Kai could not read the LinkedIn thread ${prosp.length} time${prosp.length === 1 ? '' : 's'} in the last hour across ${prospWorkspaces} client${prospWorkspaces === 1 ? '' : 's'} (${threadFailureLines(prosp).join('; ')}). Nothing was sent: approvals were refused and auto-send fell back to review. Prosp's conversation endpoint may be down.` }
    : { id: 'thread_check', failing: false, needed: NEEDED.thread_check });

  const { sent, skipped } = snapshot.autoSend;
  const attempts = sent + skipped;
  results.push(attempts >= 5 && skipped / attempts >= 0.5
    ? { id: 'skip_share', failing: true, needed: NEEDED.skip_share,
        reason: `Kai held back ${skipped} of ${attempts} auto-sends in 24 hours because the client "already replied". That is too many to be real; auto-send has effectively stopped. Check that Prosp's conversation order has not changed.` }
    : { id: 'skip_share', failing: false, needed: NEEDED.skip_share });

  if (snapshot.doubleSends.length) {
    const lines = snapshot.doubleSends.slice(0, 5).map((d) =>
      `${String(d.client_name || '').trim().toUpperCase()} to ${d.prospect_name || 'a prospect'}`);
    results.push({ id: 'double_send', failing: true, needed: NEEDED.double_send,
      reason: `Kai replied twice within 30 minutes to the same prospect: ${lines.join('; ')}.` });
  } else {
    results.push({ id: 'double_send', failing: false, needed: NEEDED.double_send });
  }

  return results;
}

const CHECK_TITLES = {
  health: 'Kai is down',
  watcher_read: 'Kai watch is blind',
  stuck: 'Replies stuck',
  intake: 'No replies arriving',
  thread_check: 'Thread check failing',
  thread_check_account: 'Client Prosp account disconnected',
  thread_check_key: 'Prosp key rejected',
  skip_share: 'Auto-send holding back too much',
  double_send: 'Double reply',
  'errors:usage_limit': 'Anthropic spending limit',
  'errors:slack': 'Slack posts failing',
  'errors:prosp_send': 'Prosp sends failing',
  'errors:pipeline': 'Reply processing errors',
};

// Pure streak machine. Returns the next state plus the messages to post.
// A check id missing from `results` counts as healthy (an error class that
// stopped occurring), unless the poll was blind (snapshot read failed), in
// which case absent dynamic ids are left untouched.
function stepKaiWatch(prevState, results, now, { blind = false } = {}) {
  const state = {};
  for (const [k, v] of Object.entries(prevState || {})) state[k] = { ...v };
  const alerts = [];
  const recoveries = [];
  const t = now.getTime();
  const seen = new Set();

  for (const r of results) {
    seen.add(r.id);
    if (r.skip) continue;
    const s = state[r.id] || { streak: 0, alerted: false, lastAlertAt: 0 };
    if (r.failing) {
      s.streak += 1;
      s.reason = r.reason;
      if (s.streak >= r.needed && (!s.alerted || t - s.lastAlertAt >= REALERT_MS)) {
        alerts.push({ id: r.id, title: CHECK_TITLES[r.id] || r.id, reason: r.reason, repeat: s.alerted });
        s.alerted = true;
        s.lastAlertAt = t;
      }
      state[r.id] = s;
    } else {
      if (s.alerted) recoveries.push({ id: r.id, title: CHECK_TITLES[r.id] || r.id });
      delete state[r.id];
    }
  }

  if (!blind) {
    for (const id of Object.keys(state)) {
      if (seen.has(id)) continue;
      if (state[id].alerted) recoveries.push({ id, title: CHECK_TITLES[id] || id });
      delete state[id];
    }
  }

  return { state, alerts, recoveries };
}

function formatKaiAlert(a, ronId) {
  const mention = ronId ? `<@${ronId}> ` : '';
  const again = a.repeat ? ' (still happening, repeats every 6h)' : '';
  return `🔴 ${mention}\`KAI PROBLEM: ${a.title.toUpperCase()}\`${again}\n${a.reason}`;
}

// The account and key checks look back 24 h, so "recovered" can also mean
// nobody tried again; say so instead of claiming it is fixed.
const RECOVERY_NOTES = {
  thread_check_account: 'A later read worked, or nobody has tried in 24 hours.',
  thread_check_key: 'A later read worked, or nobody has tried in 24 hours.',
};

function formatKaiRecovery(r) {
  return `✅ \`KAI RECOVERED: ${r.title.toUpperCase()}\`\n${RECOVERY_NOTES[r.id] || 'Back to normal.'}`;
}

// ─── Daily digest ───────────────────────────────────────────────────────────

const DIGEST_SQL = {
  // Per CR calendar day, the current status of replies received that day.
  daily: `
    SELECT (created_at AT TIME ZONE '${CR_TZ}')::date::text AS day,
           count(*)::int AS received,
           count(*) FILTER (WHERE status = 'sent')::int AS sent,
           count(*) FILTER (WHERE status = 'escalated')::int AS escalated,
           count(*) FILTER (WHERE status = 'failed')::int AS failed,
           count(*) FILTER (WHERE status = 'skipped')::int AS skipped,
           count(*) FILTER (WHERE status = 'drafted')::int AS drafted
      FROM prosp_reply_queue
     WHERE created_at >= $1::timestamptz AND created_at < $2::timestamptz
     GROUP BY 1`,
  backlog: `
    SELECT count(*)::int AS n
      FROM prosp_reply_queue
     WHERE status = 'drafted' AND created_at < $1::timestamptz - interval '24 hours'`,
  // Clients whose replies stopped: >= 10 in the 14 days before the last 5
  // days, none in the last 5. Backtested 2026-09-07..09-23: flags 0 to 2
  // clients a day, all genuinely quiet.
  silence: `
    SELECT w.client_name,
           max(q.created_at) AS last_reply,
           count(*) FILTER (WHERE q.created_at < $1::timestamptz - interval '5 days')::int AS prior,
           count(*) FILTER (WHERE q.created_at >= $1::timestamptz - interval '5 days')::int AS recent
      FROM prosp_reply_queue q
      JOIN workspaces w ON w.id = q.workspace_id
     WHERE q.created_at >= $1::timestamptz - interval '19 days' AND q.created_at < $1::timestamptz
     GROUP BY w.client_name
    HAVING count(*) FILTER (WHERE q.created_at < $1::timestamptz - interval '5 days') >= 10
       AND count(*) FILTER (WHERE q.created_at >= $1::timestamptz - interval '5 days') = 0`,
  // Replies that were drafted or sent but never reached Slack: a human was
  // never shown them. Zero on every day of the 30-day backtest.
  divergence: `
    SELECT count(*)::int AS n
      FROM prosp_reply_queue q
     WHERE q.created_at >= $1::timestamptz AND q.created_at < $2::timestamptz
       AND q.status IN ('drafted', 'sent')
       AND NOT EXISTS (SELECT 1 FROM prosp_reply_log l WHERE l.queue_id = q.id AND l.event = 'slack_sent')`,
  autoSend: `
    SELECT w.client_name,
           count(*) FILTER (WHERE l.event = 'auto_sent')::int AS sent,
           count(*) FILTER (WHERE l.event = 'skipped_client_replied' AND l.actor = 'agent')::int AS held,
           count(*) FILTER (WHERE l.event = 'auto_send_fallback')::int AS fallback
      FROM workspaces w
      LEFT JOIN prosp_reply_log l
        ON l.workspace_id = w.id AND l.created_at >= $1::timestamptz AND l.created_at < $2::timestamptz
     WHERE w.auto_send_enabled = true
     GROUP BY w.client_name
     ORDER BY w.client_name`,
};

// CR has no DST: midnight CR is 06:00 UTC.
function crDayStartUtc(date) {
  const crNow = new Date(date.getTime() - 6 * 3600 * 1000);
  return new Date(Date.UTC(crNow.getUTCFullYear(), crNow.getUTCMonth(), crNow.getUTCDate(), 6));
}

async function readKaiDigest(pg, now) {
  const todayStart = crDayStartUtc(now);
  const yStart = new Date(todayStart.getTime() - 24 * 3600 * 1000);
  const histStart = new Date(todayStart.getTime() - 29 * 24 * 3600 * 1000);
  const [daily, backlog, silence, divergence, autoSend] = await Promise.all([
    pg.query(DIGEST_SQL.daily, [histStart.toISOString(), todayStart.toISOString()]),
    pg.query(DIGEST_SQL.backlog, [now.toISOString()]),
    pg.query(DIGEST_SQL.silence, [now.toISOString()]),
    pg.query(DIGEST_SQL.divergence, [yStart.toISOString(), todayStart.toISOString()]),
    pg.query(DIGEST_SQL.autoSend, [yStart.toISOString(), todayStart.toISOString()]),
  ]);
  return {
    now, todayStart, yStart,
    daily: daily.rows, backlog: backlog.rows[0]?.n ?? 0,
    silence: silence.rows, divergence: divergence.rows[0]?.n ?? 0, autoSend: autoSend.rows,
  };
}

const METRICS = ['received', 'sent', 'escalated', 'failed', 'skipped'];
// Only the direction that means trouble is flagged, and only when the gap is
// at least 3 replies: with 5 metrics at 1.5 sigma, flagging every wobble
// would make "ALL GREEN" rare and teach everyone to ignore the digest.
const BAD_DIRECTION = { received: 'low', escalated: 'high', failed: 'high', skipped: 'high' };

function isWeekendDay(isoDay) {
  const d = new Date(`${isoDay}T12:00:00Z`).getUTCDay();
  return d === 0 || d === 6;
}

function summarizeDaily(dailyRows, yStart, todayStart) {
  const byDay = new Map(dailyRows.map((r) => [r.day, r]));
  const yDay = new Date(yStart.getTime() - 6 * 3600 * 1000).toISOString().slice(0, 10);
  const zero = { received: 0, sent: 0, escalated: 0, failed: 0, skipped: 0, drafted: 0 };
  const yesterday = { ...zero, ...(byDay.get(yDay) || {}) };
  const weekend = isWeekendDay(yDay);

  const baselineDays = [];
  for (let i = 2; i <= 29; i += 1) {
    const d = new Date(todayStart.getTime() - i * 24 * 3600 * 1000 - 6 * 3600 * 1000).toISOString().slice(0, 10);
    if (isWeekendDay(d) === weekend) baselineDays.push({ ...zero, ...(byDay.get(d) || {}) });
  }

  const stats = {};
  const flags = [];
  for (const m of METRICS) {
    const vals = baselineDays.map((r) => Number(r[m]) || 0);
    const n = vals.length;
    const mean = n ? vals.reduce((a, b) => a + b, 0) / n : 0;
    const sd = n ? Math.sqrt(vals.reduce((a, b) => a + (b - mean) ** 2, 0) / n) : 0;
    stats[m] = { n, mean, sd, lo: Math.min(...vals, Infinity), hi: Math.max(...vals, -Infinity) };
    const x = Number(yesterday[m]) || 0;
    const dir = BAD_DIRECTION[m];
    // A day with no rows is a real zero (Kai received nothing), so the
    // 28-day window always yields 20 weekday or 8 weekend samples; the n >= 7
    // guard only protects against a shortened history window.
    if (!dir || n < 7) continue;
    const gap = x - mean;
    const beyond = Math.abs(gap) > 1.5 * sd && Math.abs(gap) >= 3;
    if (beyond && ((dir === 'high' && gap > 0) || (dir === 'low' && gap < 0))) {
      flags.push({ metric: m, value: x, mean });
    }
  }
  return { yDay, weekend, yesterday, stats, flags };
}

function formatKaiDigest(data, openAlerts = []) {
  const s = summarizeDaily(data.daily, data.yStart, data.todayStart);
  // "Went quiet" is flagged once, on the first digest after the 5-day mark.
  // Monday's digest covers the weekend too, so its window is 3 days wide.
  const gapDays = crParts(data.now).weekday === 'Mon' ? 3 : 1;
  const newlySilent = data.silence.filter((r) => data.now - new Date(r.last_reply) < (5 + gapDays) * 24 * 3600 * 1000);
  const stillSilent = data.silence.filter((r) => !newlySilent.includes(r));
  const issues = [];
  for (const f of s.flags) issues.push(`${f.metric} ${f.value} (usual ${Math.round(f.mean)})`);
  if (newlySilent.length) issues.push(`${newlySilent.length} client${newlySilent.length === 1 ? '' : 's'} went quiet`);
  if (data.divergence > 0) issues.push(`${data.divergence} never reached Slack`);
  if (openAlerts.length) issues.push(`${openAlerts.length} open alert${openAlerts.length === 1 ? '' : 's'}`);

  const y = s.yesterday;
  const range = (m) => (s.stats[m].n ? ` (usual ${s.stats[m].lo} to ${s.stats[m].hi})` : '');
  const flagged = new Set(s.flags.map((f) => f.metric));
  const mark = (m) => (flagged.has(m) ? ' ⚠️' : '');
  const lines = [];
  lines.push(`\`KAI DAILY HEALTH: ${s.yDay}\``);
  lines.push('');
  lines.push(issues.length ? `⚠️ ISSUES: ${issues.join(' · ')}` : '✅ ALL GREEN');
  lines.push('');
  lines.push(`\`YESTERDAY (${s.weekend ? 'WEEKEND' : 'WEEKDAY'})\``);
  lines.push(`• Prospect replies received: ${y.received}${range('received')}${mark('received')}`);
  lines.push('');
  lines.push(`• Sent: ${y.sent} · Escalated: ${y.escalated}${mark('escalated')} · Failed: ${y.failed}${mark('failed')} · Held back: ${y.skipped}${mark('skipped')} · Waiting for review: ${y.drafted}`);
  lines.push('');
  lines.push('`AUTO-SEND`');
  if (!data.autoSend.length) lines.push('• No client has auto-send on.');
  for (const r of data.autoSend) {
    lines.push(`• ${String(r.client_name || '').trim().toUpperCase()}: ${r.sent} sent · ${r.held} held back (client replied first) · ${r.fallback} sent to review instead`);
  }
  lines.push('');
  lines.push('`WATCH`');
  lines.push(`• Open alerts: ${openAlerts.length ? openAlerts.map((a) => CHECK_TITLES[a] || a).join(', ') : 'none'}`);
  lines.push('');
  lines.push(`• Replies that never reached Slack: ${data.divergence}${data.divergence ? ' ⚠️' : ''}`);
  if (newlySilent.length) {
    lines.push('');
    lines.push(`• Went quiet (no replies in 5 days, busy before): ${newlySilent.map((r) => String(r.client_name).trim().toUpperCase()).join(', ')} ⚠️`);
  }
  if (stillSilent.length) {
    lines.push('');
    lines.push(`• Still quiet: ${stillSilent.map((r) => String(r.client_name).trim().toUpperCase()).join(', ')}`);
  }
  lines.push('');
  lines.push(`• Drafts waiting over 24h: ${data.backlog} (reported only, not a problem)`);
  return lines.join('\n');
}

module.exports = {
  SQL, DIGEST_SQL, NEEDED, INTAKE_WINDOW_HOURS, REALERT_MS,
  crParts, isIntakeWindow, classifyKaiError, classifyThreadCheckError, unresolvedThreadFailures,
  readKaiSnapshot, evaluateKaiChecks,
  stepKaiWatch, formatKaiAlert, formatKaiRecovery,
  crDayStartUtc, readKaiDigest, summarizeDaily, formatKaiDigest,
};
