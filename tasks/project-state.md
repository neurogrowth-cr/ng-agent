# ng-agent — Project State

Last updated: 2026-05-03

## Recent changes

- **2026-05-03 — Setter roster transition: Oscar M + William B onboarded, Debbanny offboarded.** New active setters: Oscar M (Slack `U0B1S1UMH9P`, GHL `ZcmdIz2EEraPd80W2zop`, `oscar.neurogrowth@gmail.com`) and William B (Slack `U0B16P6DQ2F`, GHL `N8mvtuHbbbY7QppqNMr7`, `william.neurogrowth@gmail.com`). Updated 13 places in [index.js](../index.js) split between **active routing** (claim handler, sales standup, leaderboard, stalled-followups, role/permissions — Debbanny removed, Oscar/William added) and **historical resolution** (display name maps, GHL ID lookups, iClosed email map — Debbanny preserved so old EOD data still renders her name). Portal Supabase `admin_users` table updated: Oscar/William inserted with `revops_can_set: true`, Debbanny flipped to `revops_can_set: false`. Closers Jonathan + Jose also removed from leaderboard `SETTER_GHL_IDS` array (they don't submit setter EOD; were always rendering zeros). Commit `edb9d3c`.
- **2026-05-03 — Initiative 1 hardening: 5 bug fixes after live deploy.** (a) Skin-tone reaction modifier (`hand::skin-tone-3`) was bypassing the allow-list — strip suffix before checking, commit `903d1b6`. (b) Hardcoded Appointment Setting Pipeline ID had a confusable character (`KH1l...` lowercase L → `KH1I...` uppercase I) — every opp was being silently filtered out of reassignment, commit `3ec4565`. (c) Threaded confirmation reply had nested parens and jargon — rewrote to multi-line bullets with plain English, commit `d32cdd5`. (d) Ron's GHL user ID added to `GHL_USER_NAMES` so `claimed_by_setter_name` populates on his test claims, commit `d32cdd5`. (e) Leaderboard joined on GHL user ID but `revops_setter_eod_daily.setter_id` actually stores email — Saturday's first leaderboard would have rendered all zeros despite real activity. Refactored to a single `ACTIVE_SETTERS` roster carrying name + email + Slack ID; EOD joins by email, claims by Slack ID, no conversion needed, commit `c0a6866`.
- **2026-05-03 — Initiative 1 GHL scope unblock.** Initial reaction-driven claims hit `401: token not authorized for this scope` on `PUT /contacts/{id}`. Root cause: the `ng-pm-agent` Private Integration Token was missing `opportunities.write` (and any scope changes don't propagate to existing tokens — they're baked in at issue time). Fixed by enabling the missing scope + rotating the PIT + updating `GHL_API_KEY` in Railway. Active scopes on the PIT now: `contacts.readonly`, `contacts.write`, `opportunities.readonly`, `opportunities.write`, `users.readonly`, `conversations.readonly`. End-to-end loop verified: Maria Fernanda Troconis lead claimed successfully, row landed in `setter_claims`, GHL contact + Appointment Setting Pipeline opportunity both reassigned to claimer.
- **2026-05-03 — Build Cycle 2: setter_claims audit trail + twice-weekly leaderboard.** New table `public.setter_claims` in primary Supabase (migration `003_setter_claims.sql`) records every Slack reaction-driven lead claim with `seconds_to_claim` (computed at insert time from Slack `ts` vs `claimed_at`). Reaction handler in [index.js](../index.js) inserts into `setter_claims` after successful GHL writes — non-fatal on insert failure. Threaded reply unchanged (no time delta shown to setters; metric is for offline SLA mining only). New cron `0 18 * * 3,6 America/Costa_Rica` runs `runSetterLeaderboard` and posts MTD per-setter performance to `#ng-sales-goats`. Data sources: portal Supabase `revops_setter_eod_daily` (calls_placed, qualified_leads, calls_show, calls_no_show, new_conversations, follow_ups) + primary Supabase `setter_claims` (leads_claimed). Metrics: leads_claimed, calls_placed, engagement_actions (new_conversations + follow_ups — proxy for "chats sent"), qualified_leads, show_rate. Ranked by qualified_leads desc, tiebreak calls_placed. Static cron (not dynamic) so it bypasses the `#ng-sales-goats` approval gate. No LLM call.
- **2026-05-02 — Initiative 1: Reaction-driven lead claim → GHL `assignedTo` write.** GHL leads come in unassigned by design (race-for-leads culture). The `#ng-sales-goats` post in `handleGHLWebhook` now carries Slack `metadata` with `{contact_id, location_id, full_name, correlation_id}`. New `slack.event('reaction_added')` handler in [index.js](../index.js): when a setter reacts ✋/✅ on a lead post, Max resolves their Slack ID → GHL user via new `SLACK_TO_GHL_USER` reverse map, calls `PUT /contacts/{id}` with `assignedTo`, adds a ✅ reaction (idempotency anchor), and posts a threaded confirmation. Race-safe: second reactor sees the ✅ already there and is ignored. Non-setters get a friendly "ping Ron to add you" thread reply. **Slack app scopes required:** `reactions:read`, `reactions:write` + `reaction_added` event subscription — verify in Slack app config before deploy. Channel constant `LEAD_CHANNEL_ID = 'C0AJANQBYUE'`. Activity log: `event_type: 'ghl_lead_claimed'`.
- **2026-05-02 — Initiative 2: Stalled prospect detection (DRY-RUN mode).** New cron `0 11 * * 1-5` runs `runStalledProspectFollowups`. Finds WhatsApp conversations where last message is inbound + ≥ 2 business days old + assigned to a known setter. Applies 8 skip gates (no-setter, thread-too-short, voice-note-or-empty, emoji-only, opt-out phrase scan EN+ES, no-phone, DNC tag, future iClosed booking, attended call). Eligible prospects DM'd to the **assigned setter** (each setter sees only their own list); summary DM to Ron with skip breakdown. **Live auto-send is gated by `STALLED_FOLLOWUPS_LIVE='true'` env var (default off) and is intentionally not yet wired** — falls back to dry-run DM with a warning log. Live path will land after 2 business days of clean dry-run output. Helpers: `businessDaysBetween`, `hasOptOutSignal`, `ghlGetConversationMessages`, `ghlGetContact`, `hasFutureBooking`, `hasAttendedCall`, `evaluateStalledCandidate`. Reads (no writes): GHL `/conversations/search`, `/conversations/{id}/messages`, `/contacts/{id}`; Supabase `revops_iclosed_bookings`.
- **2026-04-28 — VSL · WhatsApp Help Click intake workflow + `context` field.** New GHL workflow "VSL · WhatsApp Help Click — Intake & Setter Handoff" fires on WhatsApp helper click → setter chat → opportunity on setter pipeline → existing `/webhook/ghl-lead`. Webhook payload now includes a `context` custom field (e.g. `"VSL · WhatsApp Help Click — booking friction"`) explaining why the lead is showing up. `handleGHLWebhook` in [index.js](../index.js) now reads `cd.context`, threads it into Max's setter-DM briefing prompt with adjusted action guidance, and adds a `📝` line to the `#ng-sales-goats` channel post. Commit `97a3bdd`.
- **2026-04-25 — Phase 1 anomaly detection layer.** New tables `metric_observations` + `metric_baselines` in primary Supabase. Daily 6am Costa Rica cron scrapes 8 business metrics (Meta CPL, close rate, setter calls, Phase 0→1 conversion, Phase 1/2 cycle days, Day 7 at-risk count, GHL response time), recomputes 28-day rolling baselines, and DMs domain-routed roles when any metric drifts ≥1.5σ. Anomalies persist to `agent_knowledge` (category `alert`, visibility `shared`). Two new tools: `detect_anomalies` (ad-hoc dry-run) and `query_metric_history`. Bootstraps with 7-day warmup window — silently skips metrics with sample_size < 7. Routing constant `ANOMALY_ROUTING` in [index.js](../index.js) at the top of the anomaly block.
- **2026-04-24 — Team-wide pilot rollout.** Max opened to Ron + Tania + Josue + David. See git log `0700aac` and `6ebc8dc`.

## Outstanding — Two Initiatives effort

### Initiative 1 — Reaction-driven lead claim → ✅ shipped, live, verified

End-to-end loop working: ✋/✅ on a `#ng-sales-goats` lead post → GHL contact + Appointment Setting Pipeline opportunity reassigned to claimer → ✅ from Max + threaded confirmation → row written to `setter_claims` with `seconds_to_claim`. No pending work on this initiative.

### Initiative 2 — Stalled prospect auto-follow-up → 🟡 dry-run only, live send NOT yet wired

Currently shipped: WhatsApp stalled-prospect detection cron (Mon–Fri 11 AM CR) running in dry-run mode, DMs the assigned setter their stalled list, all 7 hard skip gates evaluated (no-setter, thread-too-short, voice-note-or-empty, emoji-only, opt-out EN+ES phrase scan, no-phone, DNC tag, future iClosed booking, attended call). Summary DM to Ron with skip breakdown.

**Not yet shipped (gated behind dry-run validation):**

- [ ] **Live auto-send path.** When `STALLED_FOLLOWUPS_LIVE='true'`, currently falls back to dry-run with a warning log — the actual send call is not wired.
- [ ] **New GHL MCP tool `send_conversation_message`** in [ghl-mcp/index.js](../ghl-mcp/index.js). POST `/conversations/messages` with `{ type: 'WhatsApp', contactId, message }`. Required scope on the GHL PIT: `conversations.write` (verify before adding).
- [ ] **`prospect_followups` Supabase migration** — new table tracking auto-sent followups. Schema: `(id, contact_id, sent_at, message, channel, setter_slack_id, attempt_n)` + indexes on `contact_id, sent_at desc`. Used for 14-day cooldown and 2-lifetime cap.
- [ ] **Claude follow-up generator** — short single-sentence message in conversation language, references last setter message, ≤25 words, ends with question, no emoji/markdown. Inputs: last 8 messages, lead `context` field, setter first name, NeuroGrowth offer brief.
- [ ] **Send-time guardrails** — business hours only (10:00–18:00 CR), 14-day per-contact cooldown, 2-lifetime cap, daily volume cap (start at 3, raise to 10 after 2 weeks of clean reply quality).
- [ ] **Setter pause command** — DM Max `pause <contactId>` to add to `agent_knowledge` category `setter_pref` and skip future auto-followups for that contact.
- [ ] **Cross-post summary to `#ng-sales-goats`** — `🔔 Auto-followup sent — N prospects nudged today.`

**Decision criteria to flip live (per the original rollout plan):**
1. ≥ 2 business days of clean dry-run output (no spurious candidates, no skip-gate misfires)
2. Spot-check the dry-run setter DMs to verify the prospects flagged are actually worth nudging
3. Confirm none of the candidates are in a state Max shouldn't message (closed-lost, manual setter pause, etc.)

**Trigger to plan the build:** the scheduled remote agent firing Mon 2026-05-04 09:00 CR will review the first dry-run + Saturday's leaderboard and recommend whether to flip live.

### Items deferred from the original spec audit (v1.1 / not in scope for the two-initiatives effort)

These were called out in the spec but explicitly de-scoped or never targeted for v1:

- [ ] **Form-response block in lead post** (revenue, country, decision-maker, offer-interest) — needs GHL custom-field IDs from Ron before it can be wired.
- [ ] **10-min unclaimed safety alert** — `chat.scheduleMessage` queued at lead-post time, cancelled on claim. Useful safety net for after-hours leads.
- [ ] **5-min auto-release rule** — if no GHL activity within 5 min of claim, clear `assignedTo` and re-open the lead. Spec marked v1.1.
- [ ] **True AQC attribution** — currently leaderboard shows setter-self-reported `qualified_leads` from EOD daily. True AQC requires `revops_appointments.setter_id ↔ revops_sales_outcomes.outcome` join with confirmed outcome enum value (`AQC` literal? `Qualified`? Need to confirm with closer workflow).
- [ ] **Migrating "engagement actions" from EOD proxy to true GHL outbound message aggregation** — would require a daily sync table walking every conversation's messages.
- [ ] **End-of-month archive post** — on the 1st of each month, post the previous month's final standings before MTD numbers reset.
- [ ] **/dispute Slack command** — lightweight command for setters to flag a `Not Qualified` disposition for Ron's review.

## What this is

**Max** — an AI Slack teammate for NeuroGrowth, a single-file Node app (`index.js`, ~3000 lines) + `ghl-mcp/` sidecar. Runs as a long-lived Node process on **Railway** (auto-deploys from GitHub `main`). Slack Socket Mode.

- Repo: `neurogrowth-cr/ng-agent` on GitHub
- Runtime: Node ≥18, Slack Bolt, Claude Sonnet 4.6 via `@anthropic-ai/sdk`
- Entrypoint: `node index.js`

## Integrations

- **Slack** (Bolt + Socket Mode) — messages, @mentions, DMs, file uploads, scheduled messages
- **Claude API** — Sonnet 4.6, tool use, multi-round reasoning, vision on images/PDFs
- **OpenAI** — Whisper for audio transcription
- **Supabase** (2 projects):
  - **ng-agent main** (`zbuqpdwjpxgsetduhjeo`) — `conversations`, `agent_knowledge`, `scheduled_tasks`
  - **neurogrowth-proposals** (`xqzfhofxtmqjozowrwdo`) — portal: `client_dashboards`, `customer_activities`, `customer_activity_templates`, `flywheel_ai_onboarding`, view `v_phase0_fulfillment`
- **Google** — Gmail, Drive, Calendar (read + write), Sheets, Docs
  - OAuth token stored as `GOOGLE_TOKEN` env var in Railway
  - Scope: `gmail.readonly`, `gmail.send`, `calendar.events`, `drive.readonly`, `drive.metadata.readonly`
  - Re-auth script: `node reauth_google.js` → paste new token to Railway
- **GoHighLevel** — MCP server (`ghl-mcp/`) for pipelines, contacts, opportunities, conversations
- **Meta Ads** — account/campaign/ad-set performance
- **Notion** — search + task creation (Operations Tracking + Project Sprint Tracking)

## Domain model

- **Client lifecycle:** Phase 0 (pre-portal onboarding) → Phase 1 (optimization) → Phase 2 (campaign launch) → Phase 3 (stabilization, 20-day checkpoint) → Live → blocked-if-stuck
- **Phase 0 steps** (via `v_phase0_fulfillment.phase0_step`) — Tania owns steps 1–4, Josue takes over at step 5:
  1. `1_awaiting_signup` — no clerk_user_id, no T&C
  2. `2_awaiting_terms` — signed up, T&C not accepted
  3. `3_awaiting_form` — T&C signed, form not completed
  4. `4_awaiting_activation_call` — form done, no booking
  5. `5_ready_for_handoff` — all done, go_live_at still null → Josue kicks off Phase 1
  - Filter: `go_live_at IS NULL`
- **SLA:** 14-day activation window (Day 1 = activation call completed_at)
- **Stabilization anchor:** `stabilization_started_at` (Phase 3 Day 1)
- **Build & Release** — 14-day delivery model
- **Retention tiers** — OMEGA / ROLEX / PATEK
- **Customer types** — `flywheel-ai` / `full-service`

## Team roles

| Person | Slack ID | Owns |
|--------|----------|------|
| Ron | RON_SLACK_ID | CEO — approves all team-channel posts |
| Tania | U07SMMDMSLQ | Phase 0 (steps 1–4), SLA enforcement, Phase 3 client success + 1:1s, blocked client outreach |
| Josue | U08ABBFNGUW | Phase 0 step 5 handoff, Phase 1–2 ops, activation calls, campaign sequencing |
| Valeria | U09Q3BXJ18B | Delivery docs, Claude Projects, Phase 1 |
| Felipe | U09TNMVML3F | Campaign launches, Prosp, Phase 2 |
| Joseph | — | Setter |
| Debbanny | — | Setter |
| Jose Carranza | — | Closer |
| Jonathan Madriz | — | Closer |

## Tools (33 total)

**Portal/fulfillment:**
- `get_client_status` — all active clients phases 1–live with activity details
- `get_phase0_clients` — pre-portal pipeline from `v_phase0_fulfillment`, grouped by step
- `get_portal_alerts` — Phase 0 (OVERDUE/AT RISK/HANDOFF READY) + Phase 1/2 (blocked/overdue/at-risk), split into two sections

**Calendar (NEW — requires `calendar.events` scope):**
- `add_calendar_attendees` — add guests to existing event, Google sends invites (`chat.scheduleMessage`)
- `create_calendar_event` — create new event with attendees

**Slack:**
- `create_slack_reminder` — one-off scheduled message via `chat.scheduleMessage` (channel name, channel ID, or user ID for DM); max 120 days out

**Sales/CRM:**
- `get_sales_intelligence` — iClosed + RevOps Supabase tables (closer/setter EOD daily)
- `get_ghl_conversations` — GHL pipeline conversations with setter assignment

**Knowledge:**
- `search_knowledge`, `save_knowledge`, `get_knowledge_category`

**Google:**
- `search_gmail`, `send_email`, `get_calendar_events` (now returns event IDs in brackets), `get_recent_emails`, `search_drive`, `read_google_sheet`, `read_google_doc`

**Notion:** `search_notion`, `get_notion_page`, `create_notion_task`

**Meta Ads:** `get_meta_ads_summary`, `get_meta_campaigns`, `get_meta_adsets`, `get_meta_ads`

**Scheduled tasks admin:** `create_scheduled_task`, `list_scheduled_tasks`, `clean_duplicate_tasks`, `delete_scheduled_task`

**Slack:** `read_slack_channel`, `draft_channel_post`

## Scheduled jobs (cron — all CR timezone)

| Time | Days | Job |
|------|------|-----|
| 5:30 AM | Mon–Fri | Nightly Learning — scan 5 channels + Gmail + calendar → `agent_knowledge` |
| 8:00 AM | Mon–Fri | Proactive Team DMs (`runProactiveDMs`) — event-triggered alerts: Josue (Day 7/14 today, stalled phases), Tania (blocked, Phase 0 stuck ≥7d, Phase 3 Day 20), Valeria (Phase 1 stalled), Felipe (Phase 2 stalled) |
| 9:00 AM | Mon–Fri | Fulfillment Standup (`runFulfillmentStandup`) — personalized daily brief DMs to Josue, Valeria, Felipe, Tania |
| 2:00 PM | Monday | Gap Detection — blocked/overdue/stale clients + Phase 0 gaps; Ron approves before posting to ops channel |
| 3:00 PM + 8:00 PM | Daily | Proactive Alerts — unresolved `alert` knowledge entries >24h |
| 6:00 PM | Mon–Fri | Fulfillment EOD Pulse → `#ng-fullfillment-ops` |
| 10:30 PM | Friday | Weekly Portal Trends + Friday Delivery Wrap-Up |
| 9:00 AM | Daily | Phase 3 Day 20 Check-in Alert |

**Dynamic crons** stored in `scheduled_tasks` — validated section headers, reject + notify Ron if malformed.

## Standup DM contents (9 AM)

**Josue:** Blocked clients (with what's blocking) → Day 14 today (must launch) → Day 7 today (at risk) → Phase 0 activation calls needed + handoff-ready → pipeline snapshot.

**Valeria:** Phase 1 clients with urgency flags (Day 10+ ⚠️, Day 7+ 👀) + next pending doc activities → blocked clients to check.

**Felipe:** Phase 2 clients with urgency flags + next pending campaign activities → Phase 3 stabilization day counts.

**Tania:** Phase 0 full pipeline (all steps, Day 7+ flagged, Day 14+ overdue, step 5 = Josue handoff) → SLA watch (Day 14 due today + past-SLA clients) → Phase 3 all clients (Day 18+ = 1:1 soon, Day 20+ = 🔴 schedule now) → blocked clients for outreach.

## Fulfillment report scheduled tasks (in Supabase `scheduled_tasks`)

Both `Fulfillment EOD Pulse` and `Friday Delivery Wrap-Up` now call:
- `get_phase0_clients` in STEP 1
- Output includes `PHASE 0 — PRE-PORTAL` section (grouped by step, tags Josue `<@U07SMMDMSLQ>` if stuck >7 days)

## Architecture notes

- **Channel cache** — 10-min TTL on Slack channel list (rate limit guard)
- **Approval flow** — team-channel posts require Ron's DM approval; 30-min TTL on pending approvals
- **Thread context** — @mentions in threads fetch full thread history before responding
- **Rate limit** — per-user 10 req / 60s
- **Conversation pruning** — 40 rows per user
- **Knowledge categories** — client, team, process, decision, alert, intel
- **Deployment** — `git push origin main` → Railway auto-redeploys in ~1–2 min

## What was built today (2026-04-23)

1. **Phase 0 tool** — `getPhase0Clients()` + `get_phase0_clients` tool reading `v_phase0_fulfillment`
2. **Phase 0 fully wired** — `getPortalAlerts()` now includes Phase 0 tiers (OVERDUE/AT RISK/HANDOFF READY); `runProactiveDMs()` DMs Tania on Phase 0 stuck ≥7 days; `runMondayGapDetection()` includes Phase 0 in gap report
3. **EOD/Friday reports updated** — both scheduled task prompts now call `get_phase0_clients` and render a PHASE 0 section
4. **Slack reminders** — `createSlackReminder()` + `create_slack_reminder` tool (one-off scheduled messages, channel or DM)
5. **Google Calendar write** — `addCalendarAttendees()` + `createCalendarEvent()` tools; scope upgraded to `calendar.events`; token updated in Railway
6. **Morning standup cron** — `runFulfillmentStandup()` at 9 AM Mon–Fri, personalized DMs to Josue, Valeria, Felipe, Tania
7. **ProactiveDMs fixed** — moved from 2 AM Tue–Sat → 8 AM Mon–Fri; Josue language updated from "tomorrow" to "today"
8. **Tania standup** — comprehensive 9 AM brief: Phase 0 pipeline, SLA watch, Phase 3 with 1:1 flags, blocked clients
