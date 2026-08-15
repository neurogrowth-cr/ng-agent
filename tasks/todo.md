# TODO — Max data-map reliability + Portal launch KPIs — session 2026-08-07

Plan: ~/.claude/plans/how-can-max-never-sorted-parnas.md (approved by Ron: full Max package + admin Client Progress KPIs)
Trigger: Max claimed activation call dates "aren't stored" — they live in customer_activities.completed_at.

## Part A — ng-agent (Max)
- [x] A1. `resolveDayAnchor()` helper + replaced 4 anchor sites (getClientStatus, getPortalAlerts, runMondayGapDetection, runFulfillmentStandup) — all now print the raw date, e.g. `Day 44 since activation call (2026-06-24)`. Standup getDayCount was silently anchoring on created_at (latent bug) — now SLA-correct; Josue's day numbers will shift.
- [x] A2. `SYSTEM_PROMPT_DATA_MAP` (~30 lines) wired into both prompt branches (Ron + team tier)
- [x] A3. Rule #6 rewritten — hard rule: never claim "doesn't exist" without data map + schema search + one query_portal_db attempt; applies to every user
- [x] A4a. `getGlobalLessons()` (≤10 shared process lessons, 90d) injected into every interactive callClaude prompt
- [x] A4b. `detectAndSaveCorrection()` (EN+ES regex gate → Haiku confirm) at DM handler + non-Max-rooted mention threads; saves `correction:*` keys, source='correction'
- [x] A4c. MAX_TOOL_ROUNDS 5 → 7
- [x] node --check clean; booking-gates 25/25, make-watchdog 18/18, strike-rules 11/11 pass; resolveDayAnchor branch-tested inline (4 scenarios incl. CR-tz date rollover)

## Part B — dash.neurogrowth.io (admin Client Progress tab)
- [x] B1–B5 shipped on worktree branch `feat/launch-sla-kpis` (commit 21b4c11, worktree /Users/ronduarte/dash-wt-launch-kpis)
- [x] `npm run build` green (132/132 pages; env pulled via vercel env pull)
- [x] **Launch definition corrected mid-build:** plan assumed `go_live_at` = launch. Real data: go_live_at PREDATES the activation call for all 7 spot-checked clients (it's a provisioning timestamp). Actual launch = `Campaign QA check`/`Campaign Validation` activity completed_at — matches Ron's sheet ±2 days. Config: `launchActivityMatchers`.
- [x] Ground-truth SQL vs sheet: Brújula 8d within SLA; UMC 7d; Licita 5d; CR Fishing 5d; Palantier 11d; Synapse 29d OVER; Ubuntu Tech launched 2026-08-03 (TTL 20d OVER — sheet was stale). NEW: Selva Design + CONAUDITA activation 7/17, Day 21+, never launched → will trip the over-SLA alert.

## Rollout (2026-08-11, Ron approved "full commit")
- [x] Local main was 15 commits behind origin — fast-forwarded, stash-pop conflicts in index.js (1: opts.systemAppend × lessonBlock — combined), lessons.md and project-state.md (both sides kept, date-ordered; restored the 2026-08-04 Make-lesson header the auto-merge ate)
- [x] ng-agent commit `f8931ac` (Max reliability package + Make watchdog + replay script) pushed to main → Railway deploy `99969be9` SUCCESS 20:03 UTC, clean boot, 10 dynamic crons; watchdog logs "NOT registered — MAKE_API_TOKEN is not set" as designed
- [x] getGlobalLessons query verified live against agent_knowledge (0 rows yet — block empty until first correction is captured)
- [x] dash: branch rebased on origin/main, pushed, [PR #28](https://github.com/neurogrowth-cr/dash.neurogrowth.io/pull/28) open; worktree removed
- [x] dash PR #28 MERGED (Ron's go, 2026-08-11 20:10 UTC) — Vercel production deploy `dpl_F4ZL5scJ` READY, aliased to dash.neurogrowth.io
- [ ] Ron: tell Josue standup Day-7/Day-14 numbers are now activation-call-anchored (will shift for some clients)
- [x] Ron set MAKE_API_TOKEN → Railway deploy `5549a68b` SUCCESS 20:33 UTC; watchdog registered (`*/10 * * * *` CR). **First sweep verified live 20:40:00 UTC: 705ms, status ok, us2 token authenticated (no blind-watchdog alert), Auto Strike Mover correctly ignored.** Fired one true-positive: `make_scenario_down / dlq` on 5148796 (2 incomplete executions) — Slack alert confirmed in #ng-fullfillment-ops at 14:40 CST. First real proof the dlq branch works: Make reports isActive:true while runs pile up failing.
- [ ] Ron: clear the 2 incomplete executions on Make scenario 5148796 (Incomplete executions tab → inspect/retry; they are replayable). Watchdog posts its own ✅ all-clear on the next sweep once the queue is empty. Each queued item = a booking with no CAPI event and no setter Slack alert.
- [ ] Watch: tomorrow 8:30 AM CR roster + 9 AM standup DMs — day lines should carry (YYYY-MM-DD) anchors
- [ ] Follow-up (separate PR): activation-call-activity-gate.ts:14 exact-equality title match would miss "Activation Call Completed" — gates a write path, verify prod template titles first

## Review
Part A closes the incident class three ways: the date is now IN the tool output (no lookup needed), the data map + rule #6 make ad-hoc lookups deterministic, and corrections now persist into every future interactive chat. Part B puts the same truth on the portal with a data-verified launch definition. Biggest catch of the session: `go_live_at` is not a launch date — both the plan and Max's first data-map draft had it wrong; SQL spot-check caught it before ship.

---

# TODO — Roster stale outcomes + MTD setter leaderboard + calendar dates — session 2026-08-04

Plan: ~/.claude/plans/1-i-think-we-nifty-wand.md (approved by Ron: keep Tue+Sat cadence; clear stale rows)

- [x] 1. Roster stale-outcome guard + "rebooked after earlier no-show" wording (index.js:2891/2922)
- [x] 2. Same staleness guard in getCloserWeeklyStats / getSetterWeeklyStats
- [x] 3. Setter Leaderboard → CR-anchored month-to-date window (index.js:1134 + claims lookback + REVI limit + format block)
- [x] 4. getCalendarEvents CR-anchored 24h windows + weekday labels; dated calendar block labels in registerDynamicCron
- [x] 5. DB: cleared 2 stale no_show outcome rows + reset attended (Daniela b1ed5819, José 68db5c6a)
- [x] 6. DB: scheduled_tasks prompts updated (roster 7873fe73, leaderboard 0fec5e2f)
- [x] 7. DB: agent_knowledge checked — no stale "Win Da Week tomorrow" rows existed (confusion came from calendar block only)
- [x] 8. dash PR #22 MERGED (bfda6fd) — Vercel prod deploy dpl_C5Du4Efa building at handoff
- [x] 9. ng-agent PR #37 MERGED (b8d1e5e) — Railway auto-deployed 194e9aec SUCCESS, clean boot 04:55 UTC, all 10 dynamic crons registered (new prompts loaded at boot)
- [x] 10. Verify: SQL confirms 0 outcomes on future calls; simulated Fri-7PM-CR calendar bounds correct; MTD = Aug 1 06:00Z
- [x] 11. tasks/project-state.md + tasks/lessons.md updated (shipped in PR #37)
- [ ] 12. Watch next roster (8:30 AM CR Aug 5 — José's Aug 5 call should read "scheduled — rebooked after earlier no-show" if GHL re-fires, else "scheduled") + Sat leaderboard post (should read "August MTD"); confirm Vercel deploy READY

---

# TODO — Strike mover: quiet per-run posts → nightly summary — session 2026-08-04

Plan approved by Ron (chose "daily summary inside the nightly learning report").
Shipped as [PR #35](https://github.com/neurogrowth-cr/ng-agent/pull/35) (branch `strike-quiet-reporting`, commit `da32af9`).

- [x] `runAutoStrikeMover`: persist each sweep to `agent_activity` as `strike_sweep` (metadata: mode/scanned/moved/capped/skips/moves/failures); Slack post now dry-run-only
- [x] `migrations/012_strike_sweep_read.sql` — narrow anon-select RLS policy, APPLIED to portal project `xqzfhofxtmqjozowrwdo`; anon read verified (no RLS error)
- [x] `strikeBuildDailyDigest(24)` helper; zero sweeps → cron-dead alert
- [x] `runNightlyLearning`: `=== AUTO STRIKE MOVER ===` digest section, prompt bullet 10 (anomalies only), deterministic strike line in the nightly Slack post
- [x] `node --check` clean; `test/strike-rules.test.js` 11/11 pass
- [x] `tasks/project-state.md` entry (in the PR)
- [x] Ron merged PR #35 (merge commit `d5b8d32`, 2026-08-04 22:45 UTC)
- [x] Railway deploy `10171dee` SUCCESS, clean boot; 6 PM CR sweep on new code wrote `strike_sweep` row (scanned 1410, moved 0, 1 transient 401) and posted NOTHING to #ng-pm-agent — last old-style post was 4:04 PM CR, pre-deploy
- [ ] Morning of 2026-08-05 ~5:30 AM CR: confirm nightly learning post carries `Strike mover (last 24h): …` line

---

# TODO — GHL workflow error emails: two broken customData mappings — session 2026-08-02

Trigger: two "Workflow Error Detected" emails (2026-07-27 17:06:20Z, 2026-07-29 01:58:04Z).
GHL never names the workflow in those emails, so this was traced from delivery data.

## Cleared (not the cause)
- [x] dash `/api/integrations/ghl/webhook` — 200 on every delivery in both windows
- [x] ng-pm-MAX Railway — nothing arrived at 01:58; no 401/500; `ghl-claim` mirroring fine
- [x] dash normalizer is CORRECT — it rejects genuinely malformed payloads. No code fix needed.

## Root causes (both are GHL workflow config, not code)
1. **`Outcome: Follow-up`** (`73f4f0c0-a89e-4bfd-a927-c9e5d127ede7`)
   customData key is `"\tng_event"` — a literal TAB prefix. `normalize.ts:467` reads
   `customData.ng_event` with no key trimming, so it never matches → `unknown` → skipped.
   **Never worked once.** 11 deliveries since 2026-07-28 03:33, 0 applied.
   Proof of loss: only one `follow_up` row in `revops_sales_outcomes` in 12 days, dated
   2026-07-22 — before this workflow's first delivery. Nothing else emits this signal.
2. **`Neurogrowth Dashboard Webhook`** (`3abff40c-7133-48b9-a7aa-4bafb0cf37c3`)
   customData collapsed to a single key literally named `undefined` with an empty value —
   GHL's signature for a field referencing a deleted custom value / unresolvable merge tag.
   Broke between 2026-07-27 16:17:30 (last good) and 17:02:09 (first bad); first error
   email landed 17:06:20. 33 skipped since.
   Working mapping was: `appointment_id, email, end_time, full_name, host_email, location,
   ng_event, notes, phone, start_time`.

## Impact (measured, not estimated)
- Follow-up: 11 skipped, only 2 contacts covered nearby by another event, 5 contacts have
  no GHL event on record at all. **Real, unrecoverable-from-elsewhere loss.**
- Dashboard Webhook: 33 skipped, but 19 covered within ±15min by the parallel `call_booked`
  sender and 32/33 contacts landed eventually. **Mostly noise, not a booking hole.**

## Verified safe / no action needed
- [x] `outcome_label: "follow up"` (with a space) maps correctly — explicit alias in
      `revops-ingest/outcome-map.ts:15`, and `mapOutcomeLabel` trims+lowercases.
      Fixing the tab key alone is sufficient; no dash change required.
- [x] Fixing #2 will NOT break the live cancel/reschedule Slack alert. `ng_appt_change_notify()`
      branches on `v_ng IS NULL OR v_ng = 'call_booked'`, so restoring `ng_event: call_booked`
      still matches the reschedule path. (Supersedes the "separate fix in dash repo" note in
      the 2026-07-31 section below — there is no dash bug here.)
- [x] **SUPERSEDED 2026-08-04 (post dash PR #20):** do NOT restore `ng_event: call_booked` on
      the Dashboard Webhook after all. The workflow's single webhook serves FOUR triggers
      (Form Submitted no-filter, 2× Appointment-status event-type-Any, Opportunity Won/Lost),
      so a forced call_booked label mislabels form submissions (prospect wrongly marked booked,
      hydrate may attach an appointment) and cancellations (explicit ng_event beats status in
      the normalizer). Since PR #20 the normalizer classifies these payloads from the calendar
      shape correctly with NO ng_event. Ron rebuilt the 10 fields 2026-08-04, then deleted the
      ng_event row per this note — the other 9 rows stay.

## GHL UI fixes — DONE (Ron, verified 2026-08-06)
- [x] `Outcome: Follow-up` — clean `ng_event` key since 2026-08-06 01:03. Last bad
      delivery 2026-08-03 14:45:20.
- [x] `Neurogrowth Dashboard Webhook` — full 10-key customData restored since
      2026-08-05 17:26. Last `undefined` delivery 2026-08-05 01:01.
- [x] Both now normalize correctly: 7 `call_booked` + 1 `call_rescheduled` (Dashboard),
      10 `ghl.outcome` (Follow-up), all `applied`.

## STILL OPEN — the 13-contact follow-up backlog was never replayed
The fix only affects NEW traffic. 9 of the 10 post-fix follow-ups are people who were
never in the skipped set, i.e. fresh organic events — good proof the fix works, but not
a recovery. Tally: 14 distinct contacts skipped, 10 applied, **only 1 overlaps**.

Script: `scripts/replay-followup-outcomes.mjs` (dry run by default, `--live` to POST).
Dry run 2026-08-06: all 8 safe contacts resolved, key repaired, `ng_event=outcome` +
`outcome_label="follow up"` → maps to `follow_up`. Nothing sent.

- [ ] **Run the live replay** (needs `GHL_WEBHOOK_SECRET` from dash-neurogrowth-io Vercel env):
      `GHL_WEBHOOK_SECRET=... node scripts/replay-followup-outcomes.mjs --live`
      Replays 8 of 13 — Amy · Randall Pj · Pedro Lozano · Marco Duran · Luis Santillán ·
      Josue Vargas Castro · Anto · Alejandra Gonzg.
      Goes through the real webhook, not direct SQL: `process-event.ts` also calls
      `applyOutcomeToProspect()`, whose `mergeProspectStatus()` precedence rules aren't
      worth hand-rolling. NOT the admin backfill route either — it pulls appointments /
      opportunities from the GHL API by date and cannot reconstruct a follow-up outcome,
      which only ever existed in the webhook's customData.

### The 5 deliberately excluded — separate problem, do NOT replay
Jake Vargas · Edgar Serrano · Frank Prado · Alonso Víquez · Carina Borges
have **no prospect row and no appointment** in `revops_*`. Replaying would make
`resolveOrCreateProspect` create an orphan prospect, then bail with
`missing_appointment_id_for_outcome` — pollution, zero gain.
- [ ] Investigate why these 5 have a logged call outcome but no appointment ever ingested.
      Likely a booking path that never reached `revops_appointments` (pre-cutover, or a
      calendar outside the allowlist — cf. the 2026-07-28 "Intro calendar" lesson).

### Misattribution check (why only 8 are safe)
All 13 payloads carry **no `appointment_id`** and no calendar block, so
`process-event.ts:289` falls back to `findLatestAppointmentForProspect` — the prospect's
latest appointment *as of replay time*. Verified per contact that the latest appointment
PREDATES the original event, so no outcome can land on a call booked later. Re-verify if
this replay is deferred and any of the 8 rebooks in the meantime.

## Unrelated, noticed in passing
- [ ] `runAutoStrikeMover` logged `failures 1` on its 2026-07-29 02:06 sweep (now mode=live).

## Review (2026-08-06)
Both GHL workflows confirmed fixed from live delivery data — new follow-up outcomes now
land as `follow_up` in `revops_sales_outcomes`, and the `"follow up"` → `follow_up` alias
mapping worked as predicted from `revops-ingest/outcome-map.ts:15` (no dash change needed).
Dashboard Webhook reschedules still classify correctly, confirming the `ng_appt_change_notify()`
compatibility analysis was right.

Correction worth recording: a 10-row burst of `follow_up` outcomes on 2026-08-06 01:03–01:09
initially read as a successful replay of the backlog. It was not — cross-checking contact IDs
showed only 1 of the 14 lost contacts overlapped. Lesson: verify recovery by ENTITY identity
(contact_id), never by row count or timing coincidence.

---

# TODO — Post-cutover follow-ups (dedupe, retry, iclosed_* migration) — session 2026-08-04 — DONE

## 1. Booked-alert dedupe (kill double alert on reschedule) — SHIPPED
- [x] RPC `ng_register_booked_alert(appt_id)` (website Supabase, migration `ng_booked_alert_dedupe_rpc`) — atomic "first time?" insert into ng_appt_slack_alerts kind='booked'; fail-open on empty key; granted to anon (Make calls it via PostgREST)
- [x] Backfilled 'booked' rows for every known GHL appointment (deliveries + revops)
- [x] Make scenario 5148796: HTTP module (id 9) → RPC before Slack; Slack filter posts unless RPC returned literal `false` (fail-open on HTTP error via builtin:Ignore). Bonus: CAPI event_ids now fall back to `1.calendar.appointmentId` when `1.appointment_id` merge tag is empty.
- [x] Verified live: replayed booked payload for already-alerted appt → 7 ops, Slack suppressed; RPC true/false/fail-open unit-tested via SQL + PostgREST
- Incident during rollout: first blueprint update was invalid (missing mandatory `rejectUnauthorized`) → scenario bricked+deactivated ~7 min, reverted, validated via validate_module_configuration, reapplied. One stray Ricardo booked alert posted 22:01 CST from the queued test payload. See tasks/lessons.md 2026-08-04.

## 2. Alert delivery retry (Make outage resilience) — SHIPPED
- [x] pg_cron enabled (website project); migration `ng_alert_outbox_retry`
- [x] `ng_alert_outbox` (body, request_id, attempts, delivered) + `ng_outbox_send(id)`
- [x] `ng_appt_change_notify()` now writes outbox + sends; `ng_alert_outbox_sweep()` marks 2xx delivered, retries failures/timeouts/missing responses (≤6 attempts, 48h window, 5-min backoff), purges >14d
- [x] cron job `ng-alert-outbox-sweep` every 5 min; verified: synthetic cancel → outbox row → 200 → sweep marked delivered

## 3. ng-agent iclosed_* migration debt — ALREADY CLOSED (verified, no work needed)
Parallel session's "post-cutover truth pass" (commits 94ed36b/6ee753b/fdf41d4 + dash PRs #20/#21) closed all four audit gaps. Verified in code: setter/leaderboard reads native `revops_appointments.setter_id`; `setter_attributions` frozen as history; scrapers read revops (CR-anchored); `getNonFlywheelCallIds` frozen memo, deletable ~2026-11-20; GHL rows sales-only by construction (dash GHL_SALES_CALENDAR_IDS allowlist). Remaining `iclosed_*` refs are legacy-row reads + frozen filter only.

## Still Ron-only (GHL UI, no API — from the 2026-08-02 investigation above)
- [ ] Fix `Outcome: Follow-up` webhook customData (tab-prefixed `ng_event` key — retype by hand)
- [ ] Rebuild `Neurogrowth Dashboard Webhook` customData (collapsed to `undefined`) — both alert trigger and dash normalizer already handle the restored `ng_event: call_booked` shape

---

# TODO — Call Cancelled / Rescheduled Slack Alerts (post-iClosed) — session 2026-07-31

Context: iClosed cutover 2026-07-23 killed native cancel/reschedule notifications.
Booked alerts live in Make scenario 5148796 ([PROD] GHL Appt Booked → CAPI + Slack Alert → #ng-sales-goats C0AJANQBYUE).
Cancel + reschedule events ALREADY land in Supabase `ghl_webhook_deliveries` (project xqzfhofxtmqjozowrwdo / neurogrowth-website, fed by dash.neurogrowth.io webhook):
- cancels: `ghl.call_cancelled` (GHL wf "Outcome: Cancelled", customData.ng_event=call_cancelled)
- reschedules: arrive as `unknown` rows (GHL wf "Neurogrowth Dashboard Webhook", no ng_event) — same appointmentId, new startTime

## Plan
- [x] Make webhook 2642038 + scenario 5822303 "[PROD] GHL Appt Cancelled/Rescheduled → Slack Alert" (webhook → Slack #ng-sales-goats; message lines prebuilt in SQL)
- [x] Supabase (website project) migration `ng_appt_cancel_reschedule_slack_alerts`: appointmentId expression index; dedupe table `ng_appt_slack_alerts`; trigger fn `ng_appt_change_notify()` — cancelled → 🔴 (by client/team), reschedule (prior row, different startTime) → 🟠 old → new; pg_net POST; catch-all exception so ingestion never blocks
- [x] End-to-end test with 🧪 TEST-labeled synthetic rows → verified in Slack → synthetic rows deleted
- [x] Activate scenario — LIVE
- [x] Update tasks/project-state.md + memory

## Known quirks (accepted v1)
- A reschedule also re-fires the "booked" alert (GHL re-triggers the booked wf) — new 🟠 alert adds old→new context; deduping the booked alert = future work.
- Alert path rides dash webhook ingestion (single path); pg_net is fire-and-forget (no retry if Make down).
- dash normalizer marks reschedules "unknown_event_shape" — separate fix in dash repo; trigger reads raw payload so not blocking.
  **CORRECTED 2026-08-02:** not a dash bug. Those rows are `unknown` because the
  "Neurogrowth Dashboard Webhook" GHL workflow's customData broke on 2026-07-27 17:02
  (collapsed to a key named `undefined`). Fix is in the GHL UI — see the 2026-08-02
  section at the top. The alert trigger keeps working either way.

## Review (2026-08-02)
Verified end-to-end: test cancel + test reschedule both rendered correctly in #ng-sales-goats
(🔴 with "was scheduled for" + by-team attribution; 🟠 with old → new times). Bonus proof:
two REAL reschedules (Darwing Carvajal, Manuel Velazco) that occurred between trigger deploy
and scenario activation were queued by the Make webhook and delivered on activation — nothing
lost. pg_net responses 200 "Accepted". Test rows + dedupe entries cleaned up.

---

# TODO — Wire ng-agent into WhatsApp fulfillment groups (Phase 1: listen-only)

## Goal
Give ng-agent full context of every deployment/fulfillment effort that happens in our
WhatsApp groups (onboarding → all fulfillment phases), and the ability to surface
stalls / unanswered client questions / phase blockers. **Phase 1 = listen only.**
Outbound follow-ups into groups are a separate, later phase.

## Decisions locked (Ron, 2026-06-23)
- **Bridge:** self-hosted **Baileys** (free, open-source, data stays in our stack, runs on Railway).
  Not the official WhatsApp Cloud API — it cannot read or post in groups at all.
- **Scope:** **listen-only** first. ng-agent is a silent group member; sends nothing.
- **Number:** a dedicated NG WhatsApp number (NOT Ron's personal), added to each group.

## Architecture
Separate `wa-bridge/` service (mirrors `ghl-mcp/` being its own process) so a dropped
WhatsApp Web session can never take down the Slack bot. The two share only Supabase:
bridge writes, ng-agent reads.

```
WhatsApp groups → [wa-bridge: Baileys] → Supabase(whatsapp_messages)
                                              ↓
                          ng-agent reads ← (new tool + daily digest cron)
```

## Three things to get right
1. **Persistent auth across redeploys** — Railway wipes disk on deploy, which would force
   constant re-pairing. Mount a **Railway volume** at the Baileys auth dir so we pair once.
   (Alt if volume is a pain: persist creds to a `wa_auth_state` Supabase table.)
2. **Pairing** — use Baileys **pairing-code** flow (bridge prints code → NG phone:
   Linked Devices → Link with phone number → enter code). Avoids reading a QR from logs.
3. **Group → client mapping** — bridge auto-discovers groups (`@g.us` JID + subject) into
   `whatsapp_groups`; fuzzy-match subject → Portal `client_dashboards`; Ron confirms/fixes
   the unresolved few. This is what turns chatter into deployment context.

## Tasks
- [ ] `migrations/012_whatsapp.sql`
  - `whatsapp_groups` (`group_jid` PK, `subject`, `client_id`, `is_active`,
    `first_seen_at`, `last_message_at`) — the group→client map.
  - `whatsapp_messages` (`id`, `group_jid` FK, `wa_message_id` UNIQUE for idempotency,
    `sender_jid`, `sender_name`, `body`, `has_media`, `media_type`, `sent_at`,
    `ingested_at`, `correlation_id`).
  - anon INSERT/SELECT RLS, mirroring the proven `setter_claims` policy pattern.
- [ ] `wa-bridge/` new Railway service
  - Baileys connect + pairing-code, auth persisted to Railway volume.
  - On `messages.upsert`: keep only group JIDs (`@g.us`); upsert into `whatsapp_messages`
    (idempotent on `wa_message_id`); auto-register unseen groups into `whatsapp_groups`.
  - Ignore 1:1, status, newsletters. Media → store caption + `has_media=true` only
    (downloading media is Phase 2).
  - Fire-and-forget logging + correlation IDs, per existing convention.
- [ ] ng-agent (`index.js`)
  - New read tool `get_whatsapp_activity(client|group, since)` so Ron can ask
    "what's happening in <client>'s group?" — fuzzy-resolves group via `whatsapp_groups`.
  - New `scheduled_tasks` cron → daily digest to `#ng-fullfillment-ops`: per active group,
    flag stalls, unanswered client questions, phase blockers (cross-ref Portal phase).
- [ ] `.env.example` — `WA_BRIDGE_NUMBER`, `WA_AUTH_DIR`, Supabase creds reuse.

## Out of scope (Phase 2+)
- Any outbound into groups (drafted follow-ups via the existing approval sentinel).
- Media download/storage.
- Per-client digest channels.

## Data-handling note
Group messages include our customers' messages, which will live in Supabase. It's our
business data / our groups, so defensible — flagged here as an explicit decision, not silent.

## Review
_(fill in after implementation: verification steps, what shipped, what broke)_

---

# TODO — Make [PROD] scenario watchdog — session 2026-08-04

Trigger: scenario 5148796 auto-deactivated for ~7 min on 2026-08-04 after an invalid
blueprint push. The only signal was a Make email to Ron. Webhooks queue (nothing lost),
but CAPI events go stale and setters see no booking alerts while a scenario is dark.

## Done
- [x] `checkMakeScenarioHealth()` — hourly poll (`0 * * * *` CR) of Make API v2 for team 432699
- [x] Watches every scenario whose name starts with `[PROD]` (auto-covers new ones)
- [x] Alerts on `isActive === false` (deactivated) and `isinvalid === true` (bad blueprint)
- [x] Posts to #ng-fullfillment-ops tagging Ron, with a direct edit link to the scenario
- [x] Debounced: one alert per scenario per outage + an all-clear when it flips back
      (same shape as the Socket Mode watchdog; in-memory, so a Railway restart re-alerts
      on a still-open outage rather than swallowing it)
- [x] Paginates — Make does not document the default `pg[limit]`, and a silent truncation
      would drop scenarios from the watch list with no visible symptom
- [x] A dead/absent API token raises its own "the watchdog is blind" alert instead of
      failing silently — the exact failure mode this feature exists to kill
- [x] `MAKE_WATCHDOG_IGNORE` (default `5776020`) — `[PROD] Auto Strike Mover` is
      deactivated ON PURPOSE (superseded by Max's own `runAutoStrikeMover` cron), so
      without this the watchdog false-alarms on its very first run
- [x] Gated on `MAKE_API_TOKEN` — cron does not register if the var is absent
- [x] `test/make-watchdog.test.js` 18/18 pass (extracts the real function from index.js,
      stubs fetch/Slack); `node --check` clean; strike-rules 11/11 + booking-gates 25/25 green
- [x] Response contract verified against the live team-432699 payload: 29 scenarios, all
      carry `isActive`/`isinvalid`, 4 match `[PROD]`

## Blocker (Ron's action — ships inert until done)
Max has no Make credential. Ron creates a Make API token (Profile → API access → Add token,
scope `scenarios:read`) and sets `MAKE_API_TOKEN` in Railway.

## Recommended follow-up
Rename `[PROD] Auto Strike Mover` to `[RETIRED] …` in Make (matching the existing
`[DISABLED …]` / `[ARCHIVED …]` convention) and drop the `MAKE_WATCHDOG_IGNORE` default —
naming beats an ID allowlist.

---

# TODO — Alert triage bridge (Make DLQ first consumer) — session 2026-08-14/15

Plan: ~/.claude/plans/how-can-max-never-sorted-parnas.md (approved: generic bridge + Make DLQ consumer; channel-visible proposals, roster ✅; retry + reactivate)

- [x] PR #47 merged + deployed (`c07950c`) — generic bridge, emoji approval, executors, 30 tests
- [x] PR #49 (parallel session) rebased onto #47, conflict resolved, merged + deployed (`9e64db3`) — DLQ damping
- [x] Interaction tests 13d–13f added (damping × triage); fixed a pagination bug in the triage test's own stub
- [x] Ron set `MAKE_TRIAGE_ENABLED=true` + `REMEDIATION_DRY_RUN=true` on ng-pm-MAX
- [x] Rollout step 1 verified live: Max listed the real DLQ entries via `make_list_dlqs` (proves `dlqs:read` scope + envelope + timestamp parsing → the 44h guard is functional, not a silent no-op)
- [x] First real triage 04:10 UTC — `no_action`, correct reasoning, declined the offered action. Smoke scenario deleted.
- [ ] **Ron: discard (not retry) the 2 Aug-5 entries** — [scenario 5148796](https://us2.make.com/432699/scenarios/5148796/edit) → Incomplete executions → Delete. Diagnosed as test artifacts from that day's blueprint edits; ✅ all-clear posts ~30 min after the queue empties (damping applies to the recovery edge too).
- [ ] **Watch the first REAL proposal** — this is the untested path (Ron chose to let a real incident prove it). Check: (a) the message carries `metadata.event_payload`; (b) ✅ produces a threaded reply. Silence on ✅ = missing `reactions:write`/`channels:history` in #ng-fullfillment-ops.
- [ ] Then unset `REMEDIATION_DRY_RUN` to arm for real (rollout step 3)
- [ ] Optional, still unverified: `POST /scenarios/{id}/start` (reactivate executor) — a 404 surfaces as a ❌ thread reply, not a crash
- [ ] Separate PR: `runMondayGapDetection` bare `correlationId` ReferenceError — the Monday gap report has never posted when gaps existed
