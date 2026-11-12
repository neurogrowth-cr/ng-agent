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

const { notDemoWorkspace } = require('./demoClient');
const CR_TZ = 'America/Costa_Rica';
const REALERT_MS = 6 * 60 * 60 * 1000;

// Consecutive failing polls before the first alert. Facts (an error row, a
// double send) fire on the first poll; conditions that a deploy or a quiet
// hour can fake need confirmation.
const NEEDED = {
  health: 3,
  stuck: 2,
  prosp_quiet: 2,
  kai_intake: 2,
  thread_check: 2,
  thread_check_account: 2,
  thread_check_key: 2,
  skip_share: 2,
  double_send: 1,
  watcher_read: 2,
  errors: 1,
  thread_watch: 2,
  thread_watch_reads: 3,
  thread_watch_blocked: 2,
  spool_waiting: 2,
  kai_hold: 2,
};

// Kai's thread watch (ng-automation lib/thread-watch.js) runs hourly and
// takes up to ~8 minutes; /health reports its last finished run. Stale after
// two missed runs. Red when over 30% of thread reads fail (recipe
// automations/ops/recipes/kai-thread-watch.md, 4b).
const THREAD_WATCH_STALE_MS = 2 * 60 * 60 * 1000 + 15 * 60 * 1000;
const THREAD_WATCH_READ_FAIL_RED = 0.3;
const THREAD_WATCH_MIN_CANDIDATES = 20;
// Prosp's firewall limits by IP, and Railway's outbound address is shared with
// other customers, so it can cut a run short on traffic that is not Kai's
// (2026-11-10: three cut runs at 12 to 15 reads a minute). One cut run only
// delays those leads an hour; red when the cut lasts this many runs in a row.
const THREAD_WATCH_BLOCKED_RUNS_RED = 3;

/**
 * Liveness of Kai's later-messages reader, from Kai's /health body
 * (`threadWatch`). Absent (older Kai) or mode off: nothing to check.
 */
const THREAD_WATCH_IDS = ['thread_watch', 'thread_watch_reads', 'thread_watch_blocked'];

function evaluateThreadWatch(threadWatch, now) {
  if (!threadWatch || !threadWatch.mode || threadWatch.mode === 'off') {
    return THREAD_WATCH_IDS.map((id) => ({ id, skip: true }));
  }
  const t = now.getTime();
  const lastOk = Date.parse(threadWatch.lastOkAt || '');
  const booted = Date.parse(threadWatch.bootedAt || '');
  const since = Number.isNaN(lastOk) ? booted : lastOk;
  const out = [];
  if (!Number.isNaN(since) && t - since > THREAD_WATCH_STALE_MS) {
    const hours = Math.round((t - since) / 3600000);
    const what = Number.isNaN(lastOk) ? `has not finished a run since Kai started ${hours} h ago` : `has not finished a run in ${hours} h`;
    const err = threadWatch.lastError ? ` Last error: ${String(threadWatch.lastError).slice(0, 160)}.` : '';
    out.push({ id: 'thread_watch', failing: true, needed: NEEDED.thread_watch,
      reason: `Kai's later-messages reader ${what} (it runs hourly).${err} Prospects' second messages are not being read. Check the ng-automation-KAI Railway logs for [thread-watch].` });
  } else {
    out.push({ id: 'thread_watch', failing: false, needed: NEEDED.thread_watch });
  }
  const s = threadWatch.lastSummary || {};
  // Leads a firewall block left unread were never tried: they are not failed
  // reads and say nothing about client accounts. The block has its own check.
  const cut = s.blockedStopped || 0;
  const unreadable = Math.max(0, (s.unreadable || 0) - cut);
  const tried = (s.read || 0) + unreadable;
  const share = tried >= THREAD_WATCH_MIN_CANDIDATES ? unreadable / tried : 0;
  out.push(share > THREAD_WATCH_READ_FAIL_RED
    ? { id: 'thread_watch_reads', failing: true, needed: NEEDED.thread_watch_reads,
        reason: `Kai's later-messages reader could not read ${unreadable} of ${tried} conversations (${Math.round(share * 100)}%, red over 30%). Usually client Prosp accounts (disconnected, unpaid, logged out); a jump across many clients means a Prosp or key problem.` }
    : { id: 'thread_watch_reads', failing: false, needed: NEEDED.thread_watch_reads });
  const streak = threadWatch.blockedRunsInARow || 0;
  out.push(streak >= THREAD_WATCH_BLOCKED_RUNS_RED
    ? { id: 'thread_watch_blocked', failing: true, needed: NEEDED.thread_watch_blocked,
        reason: `Prosp's firewall has cut Kai's later-messages reader short ${streak} hourly runs in a row (last run left ${cut} conversations unread), so prospects' later messages are hours late. Railway's outbound address is shared with other customers, so the traffic is usually not ours. If it keeps up, give ng-automation-KAI a static outbound IP on Railway.` }
    : { id: 'thread_watch_blocked', failing: false, needed: NEEDED.thread_watch_blocked });
  return out;
}
// ─── Prosp webhook watch (Ron, 2026-11-12: Prosp webhooks are never off) ────
// On 2026-11-12 the portal database was overloaded for five hours. The dash
// answered Prosp 400 and 504, Kai answered 404, and Prosp switched our
// webhooks off without telling anyone (two error answers were enough for
// Kai's). Ten workspaces went dark; the global prosp_quiet check fired 6.5
// hours late because a trickle from one small workspace kept resetting it and
// Max, reading the same database, was blind. Since then the dash parks an
// event it cannot store in prosp_webhook_spool (migration 027, this project)
// and Kai holds a reply it cannot store in memory. The checks below watch
// both, and each workspace on its own.

function crClock(date) {
  return new Date(date).toLocaleTimeString('en-US', { timeZone: CR_TZ, hour: '2-digit', minute: '2-digit', hour12: false });
}

// Kai's /health reports webhookHold: { held, oldestHeldAt, replayed,
// postedToSlack, gaveUp }. Absent on an older Kai: nothing to check.
const KAI_HOLD_MS = 30 * 60 * 1000;

function evaluateKaiHold(webhookHold, now) {
  if (!webhookHold || typeof webhookHold !== 'object') return { id: 'kai_hold', skip: true };
  const held = Number(webhookHold.held) || 0;
  const oldest = Date.parse(webhookHold.oldestHeldAt || '');
  if (held > 0 && !Number.isNaN(oldest) && now.getTime() - oldest > KAI_HOLD_MS) {
    return { id: 'kai_hold', failing: true, needed: NEEDED.kai_hold,
      reason: `Kai has been holding ${held} repl${held === 1 ? 'y' : 'ies'} in memory since ${crClock(oldest)} CR because it cannot store ${held === 1 ? 'it' : 'them'} (portal database down or slow). Prosp was told OK, so the webhook stays on, but a held reply is lost if Kai restarts: do not deploy ng-automation until this clears. Kai posts each one in #ng-automation after 30 minutes so a person can answer.` };
  }
  return { id: 'kai_hold', failing: false, needed: NEEDED.kai_hold };
}

// prosp_webhook_spool rows with stored_at null, oldest first. The dash replay
// (/api/scheduled/prosp-spool-drain) stores them and stamps stored_at.
const SPOOL_WAIT_MS = 30 * 60 * 1000;
const SPOOL_READ_LIMIT = 1000;
// One slow insert parks one event and the replay clears it in minutes. The
// "events are being held" notice waits for more than that.
const SPOOL_NOTICE_MIN = 5;
const SPOOL_NOTICE_AGE_MS = 10 * 60 * 1000;

function spoolTimes(rows) {
  return (rows || []).map((r) => Date.parse(r && r.received_at)).filter((t) => !Number.isNaN(t));
}

function evaluateSpool(rows, now) {
  const times = spoolTimes(rows);
  const t = now.getTime();
  const late = times.filter((x) => t - x > SPOOL_WAIT_MS).length;
  if (!late) return { id: 'spool_waiting', failing: false, needed: NEEDED.spool_waiting };
  const oldest = Math.min(...times);
  const count = times.length >= SPOOL_READ_LIMIT ? `${SPOOL_READ_LIMIT} or more` : String(late);
  return { id: 'spool_waiting', failing: true, needed: NEEDED.spool_waiting,
    reason: `${count} Prosp event${late === 1 ? ' has' : 's have'} been waiting over 30 minutes to be stored (oldest ${crClock(oldest)} CR). The portal database is still refusing them or the dash replay job is not running. Nothing is lost while they wait; check /api/scheduled/prosp-spool-drain on Vercel and the portal database.` };
}

// One plain notice when the dash starts parking events and one when the
// spool is empty again. `holding` is the previous answer.
function spoolNotice(holding, rows, now) {
  const times = spoolTimes(rows);
  if (!holding) {
    const aged = times.some((x) => now.getTime() - x > SPOOL_NOTICE_AGE_MS);
    if (times.length >= SPOOL_NOTICE_MIN || aged) {
      const n = times.length >= SPOOL_READ_LIMIT ? `${SPOOL_READ_LIMIT} or more` : times.length;
      return { holding: true,
        message: `🟡 \`PROSP EVENTS HELD\`\nThe portal database is not storing Prosp events right now. ${n} held so far, nothing lost; they are stored when it recovers.` };
    }
    return { holding: false, message: null };
  }
  if (!times.length) {
    return { holding: false, message: '✅ `PROSP EVENTS STORED`\nThe portal database is storing Prosp events again. Nothing is waiting.' };
  }
  return { holding: true, message: null };
}

const FIXED_IDS = ['health', 'stuck', 'prosp_quiet', 'kai_intake', 'thread_check', 'thread_check_account', 'thread_check_key', 'skip_share', 'double_send'];

// Intake (Ron, 2026-11-08: compare Prosp with Kai). Counting Kai's own
// replies fired on quiet afternoons (2026-10-20, 10-23, 10-30, 11-03, each
// 18:45 to 19:45 with Prosp webhooks flowing). Two checks instead:
//   prosp_quiet  no Prosp webhook event of any kind (dash prosp_webhook_logs)
//                for PROSP_QUIET_HOURS on a weekday 10:00 to 19:00 CR. The
//                longest weekday gap 07:00 to 19:00 in the 30 days to
//                2026-11-08 was 1.6 h; 2026-10-27 (almost no events all day)
//                would have fired.
//   kai_intake   a prospect reply Prosp reported for a Kai client (activated
//                before the reply) that has no Kai row within 30 minutes.
//                0 such replies in the 14 days to 2026-11-08 (158 of 158).
const PROSP_QUIET_HOURS = 3;
const KAI_INTAKE_LOOKBACK_HOURS = 6;
const INTAKE_WINDOW_HOURS = PROSP_QUIET_HOURS;

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
       AND ${notDemoWorkspace('l.workspace_id')}
     ORDER BY l.created_at DESC
     LIMIT 200`,
  stuck: `
    SELECT count(*)::int AS n, min(created_at) AS oldest
      FROM prosp_reply_queue
     WHERE status = 'processing'
       AND created_at < $1::timestamptz - interval '10 minutes'
       AND created_at > $1::timestamptz - interval '7 days'
       AND ${notDemoWorkspace('workspace_id')}`,
  // Any Prosp webhook event at all (dash stores every one). Index-only scan.
  prospEvents: `
    SELECT EXISTS (
      SELECT 1 FROM prosp_webhook_logs
       WHERE created_at > $1::timestamptz - make_interval(hours => $2::int)
         AND created_at <= $1::timestamptz
    ) AS any`,
  // Replies Prosp reported for a Kai client that never became a Kai row.
  // Matched on the lead (raw payload first, then the normalized URL), any
  // workspace (a shared-workspace reply lands in its owner's). Replies before
  // the workspace was activated do not count: dash does not forward them.
  missedReplies: `
    SELECT w.client_name, count(*)::int AS n, min(l.created_at) AS oldest
      FROM prosp_webhook_logs l
      JOIN workspaces w ON w.dashboard_id::text = l.customer_id::text
     WHERE l.event_type IN ('LinkedIn Message Replied', 'LinkedIn InMail Replied')
       AND l.created_at > $1::timestamptz - make_interval(hours => $2::int)
       AND l.created_at <= $1::timestamptz - interval '30 minutes'
       AND w.prosp_reply_agent_enabled
       AND w.is_demo = false
       AND (w.activated_at IS NULL OR w.activated_at < l.created_at)
       AND NOT EXISTS (
         SELECT 1 FROM prosp_reply_queue q
          WHERE q.created_at BETWEEN l.created_at - interval '10 minutes' AND l.created_at + interval '30 minutes'
            AND (q.raw_payload->'eventData'->>'lead' = l.prospect_id
                 OR rtrim(lower(q.prospect_linkedin_url), '/') = rtrim(lower(l.prospect_id), '/')))
     GROUP BY w.client_name
     ORDER BY n DESC
     LIMIT 20`,
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
       AND ${notDemoWorkspace('l.workspace_id')}
     ORDER BY l.created_at DESC
     LIMIT 500`,
  autoSend24h: `
    SELECT count(*) FILTER (WHERE event = 'auto_sent')::int AS sent,
           -- A delayed auto-reply the client answered by hand during the wait
           -- (ng-automation lib/reply-send-timing.js, detail.scheduled) is the
           -- client being quick, not auto-send holding back.
           count(*) FILTER (WHERE event = 'skipped_client_replied' AND actor = 'agent'
                              AND COALESCE(detail->>'scheduled', '') <> 'true')::int AS skipped
      FROM prosp_reply_log
     WHERE created_at > $1::timestamptz - interval '24 hours'
       AND created_at <= $1::timestamptz
       AND ${notDemoWorkspace('workspace_id')}`,
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
       AND w.is_demo = false
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

// Prosp silence only means something while campaigns normally run.
function isIntakeWindow(date) {
  const { weekday, hour } = crParts(date);
  return !['Sat', 'Sun'].includes(weekday) && hour >= 10 && hour < 19;
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
  const [errors, stuck, prospEvents, missed, threadCheck, autoSend, doubleSends] = await Promise.all([
    pg.query(SQL.errors, [ts]),
    pg.query(SQL.stuck, [ts]),
    pg.query(SQL.prospEvents, [ts, PROSP_QUIET_HOURS]),
    pg.query(SQL.missedReplies, [ts, KAI_INTAKE_LOOKBACK_HOURS]),
    pg.query(SQL.threadCheck, [ts]),
    pg.query(SQL.autoSend24h, [ts]),
    pg.query(SQL.doubleSends, [ts]),
  ]);
  return {
    errors: errors.rows,
    stuck: stuck.rows[0] || { n: 0, oldest: null },
    prospEvents: prospEvents.rows[0]?.any !== false,
    missedReplies: missed.rows || [],
    threadCheckFails: threadCheck.rows,
    autoSend: autoSend.rows[0] || { sent: 0, skipped: 0 },
    doubleSends: doubleSends.rows,
  };
}

// Turn one poll into per-check results: { id, failing, reason, needed, skip }.
// `snapshot` is null when the DB read failed; `snapshotError` says why.
function evaluateKaiChecks({ health, snapshot, snapshotError, now, spool = null }) {
  const results = [];

  results.push(health.ok
    ? { id: 'health', failing: false, needed: NEEDED.health }
    : { id: 'health', failing: true, needed: NEEDED.health,
        reason: `Kai is not answering its health check (${health.error || 'no response'}). Nothing is being received, drafted or sent. Check the ng-automation-KAI Railway service logs.` });
  // Only judged while Kai answers: a down Kai is already the health alert.
  if (health.ok) results.push(...evaluateThreadWatch(health.threadWatch, now));
  else results.push(...THREAD_WATCH_IDS.map((id) => ({ id, skip: true })));
  // Replies Kai could not store and is holding in memory (ng-automation lib/webhook-hold.js).
  results.push(health.ok ? evaluateKaiHold(health.webhookHold, now) : { id: 'kai_hold', skip: true });
  // Prosp events the dash parked in Max's own database. Read apart from the
  // portal snapshot, so a portal outage (when the spool matters) does not hide it.
  results.push(spool && Array.isArray(spool.rows) ? evaluateSpool(spool.rows, now) : { id: 'spool_waiting', skip: true });

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
    results.push({ id: 'prosp_quiet', skip: true });
  } else {
    results.push(snapshot.prospEvents === false
      ? { id: 'prosp_quiet', failing: true, needed: NEEDED.prosp_quiet,
          reason: `Prosp has sent no webhook of any kind (visits, invitations, messages, replies) for ${PROSP_QUIET_HOURS} hours on a weekday. Prosp is down or, more likely, switched our webhooks off after failed deliveries, so no replies reach Kai or the funnel. Fix: in Prosp open Settings, Webhooks on each workspace and switch any Error row back on (Test Webhook, then Update); it needs a real click. Playbook: wiki ops/playbooks.md. If every row is Active, check Prosp's status and the dash /api/prosp/webhook logs on Vercel.` }
      : { id: 'prosp_quiet', failing: false, needed: NEEDED.prosp_quiet });
  }

  const missed = snapshot.missedReplies || [];
  const missedTotal = missed.reduce((n, r) => n + (r.n || 0), 0);
  results.push(missedTotal > 0
    ? { id: 'kai_intake', failing: true, needed: NEEDED.kai_intake,
        reason: `${missedTotal} prospect repl${missedTotal === 1 ? 'y' : 'ies'} reached the dashboard but not Kai in the last ${KAI_INTAKE_LOOKBACK_HOURS} hours: ${missed.slice(0, 5).map((r) => `${String(r.client_name || 'unknown').trim().toUpperCase()} ${r.n}`).join(', ')}. Kai never drafted these. Check the ng-automation-KAI logs for [prosp-webhook] and the dash forward to Kai.` }
    : { id: 'kai_intake', failing: false, needed: NEEDED.kai_intake });

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
  prosp_quiet: 'Prosp stopped sending',
  kai_intake: 'Replies not reaching Kai',
  thread_check: 'Thread check failing',
  thread_check_account: 'Client Prosp account disconnected',
  thread_check_key: 'Prosp key rejected',
  skip_share: 'Auto-send holding back too much',
  double_send: 'Double reply',
  thread_watch: 'Later-messages reader stopped',
  thread_watch_reads: 'Later-messages reader cannot read threads',
  thread_watch_blocked: 'Later-messages reader blocked by Prosp',
  spool_waiting: 'Prosp events not being stored',
  kai_hold: 'Kai is holding replies in memory',
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
           -- A prospect's no closed by Kai (ng-automation lib/decline-close.js) is the normal path, not a skip to watch.
           -- A pleasantry Kai closed (ng-automation lib/pleasantry.js, Ron 2026-11-07) is normal too.
           count(*) FILTER (WHERE status = 'skipped' AND COALESCE(status_reason, '') NOT IN ('prospect_declined', 'pleasantry'))::int AS skipped,
           -- An auto-reply waiting for its send time (drafted + send_after) is not waiting for review.
           count(*) FILTER (WHERE status = 'drafted' AND NOT (COALESCE(kind, 'reply') = 'reply' AND send_after IS NOT NULL))::int AS drafted
      FROM prosp_reply_queue
     WHERE created_at >= $1::timestamptz AND created_at < $2::timestamptz
       AND ${notDemoWorkspace('workspace_id')}
     GROUP BY 1`,
  backlog: `
    SELECT count(*)::int AS n
      FROM prosp_reply_queue
     WHERE status = 'drafted' AND created_at < $1::timestamptz - interval '24 hours'
       AND NOT (COALESCE(kind, 'reply') = 'reply' AND send_after IS NOT NULL)
       AND ${notDemoWorkspace('workspace_id')}`,
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
       AND w.is_demo = false
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
       AND COALESCE(q.status_reason, '') <> 'decline_close' -- never goes to Slack on purpose
       -- An auto-reply still waiting for its send time has no Slack post yet, on purpose.
       AND NOT (q.status = 'drafted' AND COALESCE(q.kind, 'reply') = 'reply' AND q.send_after IS NOT NULL)
       AND NOT EXISTS (SELECT 1 FROM prosp_reply_log l WHERE l.queue_id = q.id AND l.event = 'slack_sent')
       AND ${notDemoWorkspace('q.workspace_id')}`,
  autoSend: `
    SELECT w.client_name,
           count(*) FILTER (WHERE l.event = 'auto_sent')::int AS sent,
           count(*) FILTER (WHERE l.event = 'skipped_client_replied' AND l.actor = 'agent')::int AS held,
           count(*) FILTER (WHERE l.event = 'auto_send_fallback')::int AS fallback
      FROM workspaces w
      LEFT JOIN prosp_reply_log l
        ON l.workspace_id = w.id AND l.created_at >= $1::timestamptz AND l.created_at < $2::timestamptz
     WHERE w.auto_send_enabled = true
       AND w.is_demo = false
     GROUP BY w.client_name
     ORDER BY w.client_name`,
  // Weekly (Mondays): how each client handles the drafts Kai leaves for their
  // approval, last 30 days (Ron, 2026-11-08: an internal check, not a client
  // KPI; fulfillment calls the slow ones). Leaves out Kai's own closes (a no,
  // a pleasantry), auto-replies waiting for their send time, and drafts the
  // client already answered by hand or marked handled.
  approvals: `
    SELECT w.client_name,
           count(*)::int AS drafts,
           count(*) FILTER (WHERE q.status = 'drafted' AND q.send_after IS NULL
                              AND COALESCE(q.thread_state, '') <> 'answered' AND q.customer_handled_at IS NULL)::int AS waiting,
           max(EXTRACT(EPOCH FROM ($1::timestamptz - q.created_at)) / 3600)
             FILTER (WHERE q.status = 'drafted' AND q.send_after IS NULL
                       AND COALESCE(q.thread_state, '') <> 'answered' AND q.customer_handled_at IS NULL)::float AS oldest_hours,
           count(*) FILTER (WHERE q.approved_by LIKE 'customer:%' AND q.approved_at IS NOT NULL)::int AS approved,
           (percentile_cont(0.5) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (q.approved_at - q.created_at)) / 3600)
             FILTER (WHERE q.approved_by LIKE 'customer:%' AND q.approved_at IS NOT NULL))::float AS median_approve_hours,
           count(*) FILTER (WHERE q.status_reason = 'expired')::int AS expired
      FROM prosp_reply_queue q
      JOIN workspaces w ON w.id = q.workspace_id
     WHERE COALESCE(q.kind, 'reply') = 'reply'
       AND q.created_at >= $1::timestamptz - interval '30 days' AND q.created_at < $1::timestamptz
       AND q.draft_reply IS NOT NULL
       AND COALESCE(q.status_reason, '') NOT IN ('decline_close', 'prospect_declined', 'pleasantry')
       AND w.prosp_reply_agent_enabled = true
       AND w.is_demo = false
     GROUP BY w.client_name`,
};

// A client is slow to approve when 3+ drafts sit over 2 days, approvals
// usually take over a day, or 5+ drafts expired unsent (draft expiry closes
// them at 14 days). On 2026-11-08 this flags 8 of 16 Kai clients.
const APPROVAL_SLOW_HOURS = 24;
const APPROVAL_WAIT_HOURS = 48;
const APPROVAL_MAX_LINES = 8;

function slowApprovers(rows) {
  return (rows || [])
    .map((r) => ({
      client: String(r.client_name || '').trim().toUpperCase(),
      drafts: Number(r.drafts) || 0,
      waiting: Number(r.waiting) || 0,
      oldestHours: r.oldest_hours == null ? null : Number(r.oldest_hours),
      approved: Number(r.approved) || 0,
      medianHours: r.median_approve_hours == null ? null : Number(r.median_approve_hours),
      expired: Number(r.expired) || 0,
    }))
    .filter((r) =>
      (r.waiting >= 3 && r.oldestHours != null && r.oldestHours > APPROVAL_WAIT_HOURS) ||
      (r.approved >= 3 && r.medianHours != null && r.medianHours > APPROVAL_SLOW_HOURS) ||
      r.expired >= 5)
    .sort((a, b) => (b.waiting + b.expired) - (a.waiting + a.expired));
}

function hoursOrDays(h) {
  if (h == null) return '-';
  return h < 48 ? `${Math.round(h)} h` : `${Math.round(h / 24)} d`;
}

function approvalLine(r) {
  const parts = [];
  parts.push(r.waiting ? `${r.waiting} waiting, oldest ${hoursOrDays(r.oldestHours)}` : 'none waiting');
  parts.push(r.approved
    ? `${r.approved} of ${r.drafts} approved, usually after ${hoursOrDays(r.medianHours)}`
    : `0 of ${r.drafts} approved`);
  if (r.expired) parts.push(`${r.expired} expired unsent`);
  return `• ${r.client}: ${parts.join(' · ')}`;
}

// CR has no DST: midnight CR is 06:00 UTC.
function crDayStartUtc(date) {
  const crNow = new Date(date.getTime() - 6 * 3600 * 1000);
  return new Date(Date.UTC(crNow.getUTCFullYear(), crNow.getUTCMonth(), crNow.getUTCDate(), 6));
}

async function readKaiDigest(pg, now) {
  const todayStart = crDayStartUtc(now);
  const yStart = new Date(todayStart.getTime() - 24 * 3600 * 1000);
  const histStart = new Date(todayStart.getTime() - 29 * 24 * 3600 * 1000);
  const weekly = crParts(now).weekday === 'Mon';
  const [daily, backlog, silence, divergence, autoSend, approvals] = await Promise.all([
    pg.query(DIGEST_SQL.daily, [histStart.toISOString(), todayStart.toISOString()]),
    pg.query(DIGEST_SQL.backlog, [now.toISOString()]),
    pg.query(DIGEST_SQL.silence, [now.toISOString()]),
    pg.query(DIGEST_SQL.divergence, [yStart.toISOString(), todayStart.toISOString()]),
    pg.query(DIGEST_SQL.autoSend, [yStart.toISOString(), todayStart.toISOString()]),
    weekly ? pg.query(DIGEST_SQL.approvals, [now.toISOString()]) : Promise.resolve({ rows: null }),
  ]);
  return {
    now, todayStart, yStart,
    daily: daily.rows, backlog: backlog.rows[0]?.n ?? 0,
    silence: silence.rows, divergence: divergence.rows[0]?.n ?? 0, autoSend: autoSend.rows,
    approvals: approvals.rows,
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

function formatKaiDigest(data, openAlerts = [], extras = {}) {
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
  // Mondays only (readKaiDigest reads it then; null on other days).
  const slow = data.approvals ? slowApprovers(data.approvals) : null;
  if (slow && slow.length) issues.push(`${slow.length} client${slow.length === 1 ? '' : 's'} slow to approve`);
  const quiet = extras.workspaceQuiet || null;
  const quietOpen = quiet && quiet.mode !== 'off' ? (quiet.open || []) : [];
  if (quietOpen.length) issues.push(`${quietOpen.length} Prosp workspace${quietOpen.length === 1 ? '' : 's'} gone quiet`);

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
  if (quiet) lines.push(...workspaceQuietDigestLines(quiet));
  if (slow) {
    lines.push('');
    lines.push('`CLIENT APPROVALS (WEEKLY, LAST 30 DAYS)`');
    if (!slow.length) lines.push('• No client is leaving Kai drafts waiting. ✅');
    for (const r of slow.slice(0, APPROVAL_MAX_LINES)) lines.push(approvalLine(r));
    if (slow.length > APPROVAL_MAX_LINES) lines.push(`• and ${slow.length - APPROVAL_MAX_LINES} more`);
    if (slow.length) lines.push('→ Fulfillment: call these clients, or offer auto-send (Kai waits a human-looking delay).');
  }
  return lines.join('\n');
}

// ─── Per-workspace silence ──────────────────────────────────────────────────
// A workspace that normally sends in these hours and has gone quiet. The
// workspace's own last five weekdays say when it is normally active, so no
// clock window is hard-coded (Prosp's busy hours are 03:00 to 06:00 CR, which
// prosp_quiet never looks at). Windows are aligned to the UTC clock hour H:
//   regular   10 or more events in (H-3h, H] on 4 of its last 5 weekdays
//   quiet     none in (H-3h, now]
//   stopped   regular, quiet, and 10 or more today in (H-9h, H-3h]: it was
//             sending and stopped. Flagged at once.
//   not started  regular and quiet for 3 clock hours in a row with nothing
//             earlier today. Flagged then (a late start is common).
// List imports (Contact Added, Contact Extracted) never count: they come in
// bursts at odd hours. A flagged workspace is dropped unless Prosp still
// reports an active campaign for it (dash prosp_campaign_telemetry, checked
// in the last 3 hours): most silences are a paused, unpaid or finished
// campaign. When the telemetry itself is stale the workspace is flagged
// anyway and the text says so.
// Backtest on the portal, weekdays 2026-10-29 to 2026-11-12, hourly: on the
// ten normal days 8 silences flag before the campaign filter and 1 after (a
// workspace whose day ended early); on 2026-11-12 it flags 5 of the 6
// workspaces Prosp cut off, at 14:00 and 15:00 UTC, three to four hours after
// each one's last event. The telemetry keeps no history, so the "after"
// number uses each campaign's status on 2026-11-12. It ships in digest mode
// until the number holds in production.
const WQ_WINDOW_HOURS = 3;
const WQ_EARLIER_HOURS = 6;
const WQ_MIN_EVENTS = 10;
const WQ_BASELINE_DAYS = 5;
const WQ_MIN_ACTIVE_DAYS = 4;
const WQ_NOT_STARTED_HOURS = 3;
const WQ_NEEDED_POLLS = 2;
const WQ_TELEMETRY_FRESH_MS = 3 * 60 * 60 * 1000;
const WQ_OPEN_MAX_MS = 7 * 24 * 60 * 60 * 1000;
const HOUR_MS = 60 * 60 * 1000;
const WQ_NOT_IMPORTS = "event_type NOT IN ('Contact Added', 'LinkedIn Contact Extracted')";
const WQ_WORKSPACE = "event_data->'eventData'->>'workspaceId'";

// Every read is a created_at range scan over a few hours of prosp_webhook_logs
// (27 ms and 11k buffers for a 9.5 hour peak slice on 2026-11-12). That table
// is 4 GB and a full walk of it starved the portal on 2026-11-12: never widen
// these to days.
SQL.workspaceNow = `
    SELECT ${WQ_WORKSPACE} AS workspace_id,
           count(*) FILTER (WHERE created_at > $2::timestamptz)::int AS recent,
           count(*) FILTER (WHERE created_at <= $2::timestamptz)::int AS earlier,
           max(created_at) AS last_at
      FROM prosp_webhook_logs
     WHERE created_at > $1::timestamptz AND created_at <= $3::timestamptz
       AND ${WQ_NOT_IMPORTS}
     GROUP BY 1`;
SQL.workspaceBaseline = `
    SELECT ${WQ_WORKSPACE} AS workspace_id,
           count(*)::int AS n,
           array_agg(DISTINCT campaign_id) FILTER (WHERE campaign_id IS NOT NULL) AS campaigns
      FROM prosp_webhook_logs
     WHERE created_at > $1::timestamptz AND created_at <= $2::timestamptz
       AND ${WQ_NOT_IMPORTS}
     GROUP BY 1`;
SQL.workspaceTelemetry = `
    SELECT prosp_campaign_id, is_active, status_checked_at
      FROM prosp_campaign_telemetry
     WHERE prosp_campaign_id = ANY($1::text[])`;
SQL.telemetryNewest = 'SELECT max(status_checked_at) AS newest FROM prosp_campaign_telemetry';
// Client names for the alert text. A workspace that maps only to the demo
// client is never reported (only_demo).
SQL.workspaceNames = `
    SELECT wc.workspace_id,
           bool_or(COALESCE(wc.is_shared, false)) AS is_shared,
           array_agg(DISTINCT d.client_name) FILTER (WHERE d.client_name IS NOT NULL AND COALESCE(d.is_demo, false) = false) AS clients,
           COALESCE(bool_and(COALESCE(d.is_demo, false)), false) AS only_demo
      FROM prosp_workspace_customers wc
      LEFT JOIN client_dashboards d ON d.id = wc.customer_id
     WHERE wc.workspace_id = ANY($1::text[])
     GROUP BY wc.workspace_id`;

// KAI_WORKSPACE_QUIET: digest (default: no red alert, a section in the daily
// digest), alert (red alerts), off.
function workspaceQuietMode(value) {
  const v = String(value || '').trim().toLowerCase();
  return v === 'alert' || v === 'off' ? v : 'digest';
}

function utcHourStart(date) {
  return new Date(Math.floor(date.getTime() / HOUR_MS) * HOUR_MS);
}

function isUtcWeekday(date) {
  const d = date.getUTCDay();
  return d !== 0 && d !== 6;
}

// (H-3h, H] on each of the last five weekdays before today.
function baselineWindows(now) {
  const h = utcHourStart(now).getTime();
  const out = [];
  for (let k = 1; out.length < WQ_BASELINE_DAYS && k <= 14; k += 1) {
    const to = new Date(h - k * 24 * HOUR_MS);
    if (!isUtcWeekday(to)) continue;
    out.push({ from: new Date(to.getTime() - WQ_WINDOW_HOURS * HOUR_MS), to });
  }
  return out;
}

// Workspaces that are regular in this window and quiet now.
function quietCandidates({ current, baseline }) {
  const now = new Map((current || []).filter((r) => r.workspace_id).map((r) => [r.workspace_id, r]));
  const byWs = new Map();
  for (const day of baseline || []) {
    for (const r of day || []) {
      if (!r.workspace_id) continue;
      if (!byWs.has(r.workspace_id)) byWs.set(r.workspace_id, { counts: [], campaigns: new Set() });
      const w = byWs.get(r.workspace_id);
      w.counts.push(Number(r.n) || 0);
      for (const c of r.campaigns || []) w.campaigns.add(c);
    }
  }
  const out = [];
  for (const [workspaceId, w] of byWs) {
    const activeDays = w.counts.filter((n) => n >= WQ_MIN_EVENTS).length;
    if (activeDays < WQ_MIN_ACTIVE_DAYS) continue;
    const cur = now.get(workspaceId);
    if (cur && Number(cur.recent) > 0) continue;
    const sorted = [...w.counts].sort((a, b) => a - b);
    out.push({
      workspaceId,
      activeDays,
      usual: sorted[Math.floor(sorted.length / 2)],
      earlier: cur ? Number(cur.earlier) || 0 : 0,
      lastAt: cur && cur.last_at ? new Date(cur.last_at).toISOString() : null,
      campaigns: [...w.campaigns],
    });
  }
  return out;
}

// Baseline slices are read once per clock hour (cache.key) and one at a
// time; most polls read only the last nine hours.
async function readWorkspaceQuiet(pg, now, cache = {}) {
  const h = utcHourStart(now);
  const key = h.toISOString();
  if (cache.key !== key) {
    const days = [];
    for (const w of baselineWindows(now)) {
      const { rows } = await pg.query(SQL.workspaceBaseline, [w.from.toISOString(), w.to.toISOString()]);
      days.push(rows);
    }
    cache.key = key;
    cache.days = days;
  }
  const split = new Date(h.getTime() - WQ_WINDOW_HOURS * HOUR_MS);
  const from = new Date(split.getTime() - WQ_EARLIER_HOURS * HOUR_MS);
  const current = (await pg.query(SQL.workspaceNow, [from.toISOString(), split.toISOString(), now.toISOString()])).rows;
  const data = { current, baseline: cache.days, telemetry: [], telemetryNewest: null, names: [] };
  const candidates = quietCandidates(data);
  if (candidates.length) {
    const campaigns = [...new Set(candidates.flatMap((c) => c.campaigns))];
    const [telemetry, newest, names] = await Promise.all([
      campaigns.length ? pg.query(SQL.workspaceTelemetry, [campaigns]) : Promise.resolve({ rows: [] }),
      pg.query(SQL.telemetryNewest),
      pg.query(SQL.workspaceNames, [candidates.map((c) => c.workspaceId)]),
    ]);
    data.telemetry = telemetry.rows;
    data.telemetryNewest = newest.rows[0]?.newest || null;
    data.names = names.rows;
  }
  return data;
}

function workspaceLabel(workspaceId, nameRow) {
  if (nameRow && nameRow.is_shared) return 'The shared agency workspace';
  const clients = ((nameRow && nameRow.clients) || []).map(clientLabel);
  if (!clients.length) return `Prosp workspace ${workspaceId}`;
  const shown = clients.slice(0, 3).join(', ');
  return clients.length > 3 ? `${shown} and ${clients.length - 3} more` : shown;
}

// One poll. `prev` and the returned `state` map workspace id to its entry;
// an entry is `open` once flagged and stays until the workspace sends again
// (so one alert per silence, however long). `started` and `ended` are the
// entries to announce. Nothing is judged on a weekend, or while the dash
// spool holds events (they are parked, not missing).
function stepWorkspaceQuiet(prev, data, now, { spoolPending = 0 } = {}) {
  const state = {};
  for (const [k, v] of Object.entries(prev || {})) state[k] = { ...v };
  const started = [];
  const ended = [];
  if (!data || !isUtcWeekday(now) || spoolPending > 0) return { state, started, ended, skipped: true };

  const t = now.getTime();
  const hour = utcHourStart(now).getTime();
  const sending = new Set((data.current || []).filter((r) => r.workspace_id && Number(r.recent) > 0).map((r) => r.workspace_id));
  const candidates = new Map(quietCandidates(data).map((c) => [c.workspaceId, c]));
  const names = new Map((data.names || []).map((r) => [r.workspace_id, r]));
  const newest = Date.parse(data.telemetryNewest || '');
  const telemetryFresh = !Number.isNaN(newest) && t - newest <= WQ_TELEMETRY_FRESH_MS;
  const activeCampaigns = new Set((data.telemetry || [])
    .filter((r) => r.is_active === true && t - Date.parse(r.status_checked_at || '') <= WQ_TELEMETRY_FRESH_MS)
    .map((r) => r.prosp_campaign_id));

  for (const [id, s] of Object.entries(state)) {
    if (s.open) {
      if (sending.has(id)) { ended.push({ ...s, endedAt: now.toISOString() }); delete state[id]; }
      else if (t - Date.parse(s.openedAt) > WQ_OPEN_MAX_MS) delete state[id];
    } else if (!candidates.has(id)) {
      delete state[id];
    }
  }

  for (const [id, c] of candidates) {
    const s = state[id] || { workspaceId: id, hours: 0, polls: 0, lastHour: null, open: false };
    if (s.open) continue;
    if (s.lastHour !== hour) {
      s.hours = s.lastHour === hour - HOUR_MS ? s.hours + 1 : 1;
      s.lastHour = hour;
    }
    s.polls += 1;
    state[id] = s;
    const tier = c.earlier >= WQ_MIN_EVENTS ? 'stopped' : (s.hours >= WQ_NOT_STARTED_HOURS ? 'not_started' : null);
    if (!tier || s.polls < WQ_NEEDED_POLLS) continue;
    const nameRow = names.get(id);
    if (nameRow && nameRow.only_demo) continue;
    const campaign = !telemetryFresh ? 'unknown' : (c.campaigns.some((x) => activeCampaigns.has(x)) ? 'active' : 'none');
    if (campaign === 'none') continue;
    Object.assign(s, {
      open: true, openedAt: now.toISOString(), tier, campaign,
      since: c.lastAt, usual: c.usual, activeDays: c.activeDays,
      label: workspaceLabel(id, nameRow),
    });
    started.push({ ...s });
  }
  return { state, started, ended, skipped: false };
}

function workspaceQuietLink(workspaceId) {
  return `https://prosp.ai/settings?workspace=${encodeURIComponent(workspaceId)}&tab=Webhooks`;
}

function workspaceQuietSince(entry) {
  return entry.since
    ? `since ${crClock(entry.since)} CR`
    : `for over ${WQ_WINDOW_HOURS + WQ_EARLIER_HOURS} hours`;
}

function formatWorkspaceQuietAlert(entry) {
  const pattern = `in hours where it sent about ${entry.usual} on ${entry.activeDays} of its last ${WQ_BASELINE_DAYS} weekdays`;
  const today = entry.tier === 'stopped' ? ' It was sending earlier today and stopped.' : '';
  const prosp = entry.campaign === 'active'
    ? 'Prosp still reports an active campaign there, so Prosp has most likely switched our webhook off.'
    : 'Max could not check whether its campaigns are still running (the dash telemetry is stale), so this may be a paused campaign or a webhook Prosp switched off.';
  return {
    id: `prosp_workspace_quiet:${entry.workspaceId}`,
    title: 'Prosp workspace went quiet',
    reason: `${entry.label} has sent no Prosp event ${workspaceQuietSince(entry)}, ${pattern}.${today} ${prosp} Fix: open ${workspaceQuietLink(entry.workspaceId)} and switch every Error row back on (Test Webhook, then Update). It needs a real click. The events of the gap are gone.`,
  };
}

function formatWorkspaceQuietRecovery(entry) {
  return `✅ \`KAI RECOVERED: PROSP WORKSPACE WENT QUIET\`\n${entry.label} is sending Prosp events again.`;
}

// Digest section. `open` are entries still quiet, `recent` the ones that
// started sending again since the last digest (so a false alarm is visible
// while the check runs in digest mode).
function workspaceQuietDigestLines({ mode, open = [], recent = [] } = {}) {
  if (!mode || mode === 'off') return [];
  const lines = ['', `\`PROSP WEBHOOKS${mode === 'digest' ? ' (TRIAL, NO ALERTS YET)' : ''}\``];
  if (!open.length && !recent.length) {
    lines.push('• No workspace went quiet in its usual hours. ✅');
    return lines;
  }
  for (const e of open) {
    lines.push(`• ${e.label}: no Prosp event ${workspaceQuietSince(e)} (usually about ${e.usual} in those hours) ⚠️ Check its Webhooks tab: ${workspaceQuietLink(e.workspaceId)}`);
  }
  for (const e of recent) {
    lines.push(`• ${e.label}: went quiet ${workspaceQuietSince(e)}, sending again at ${crClock(e.endedAt)} CR`);
  }
  return lines;
}

module.exports = {
  SQL, DIGEST_SQL, NEEDED, INTAKE_WINDOW_HOURS, PROSP_QUIET_HOURS, KAI_INTAKE_LOOKBACK_HOURS, REALERT_MS,
  crParts, isIntakeWindow, classifyKaiError, classifyThreadCheckError, unresolvedThreadFailures,
  readKaiSnapshot, evaluateKaiChecks, evaluateThreadWatch, THREAD_WATCH_STALE_MS,
  stepKaiWatch, formatKaiAlert, formatKaiRecovery,
  crDayStartUtc, readKaiDigest, summarizeDaily, formatKaiDigest,
  slowApprovers, approvalLine, APPROVAL_SLOW_HOURS,
  evaluateKaiHold, evaluateSpool, spoolNotice, SPOOL_READ_LIMIT,
  workspaceQuietMode, utcHourStart, baselineWindows, quietCandidates, readWorkspaceQuiet,
  stepWorkspaceQuiet, formatWorkspaceQuietAlert, formatWorkspaceQuietRecovery, workspaceQuietDigestLines,
};
