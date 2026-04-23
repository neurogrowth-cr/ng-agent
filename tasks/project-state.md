# ng-agent — Project State

Last updated: 2026-04-23

## What this is

**Max** — an AI Slack teammate for NeuroGrowth, a single-file Node app (`index.js`, ~2700 lines) + `ghl-mcp/` sidecar. Runs as a long-lived Node process on **Railway** (auto-deploys from GitHub `main`). Slack Socket Mode.

- Repo: `neurogrowth-cr/ng-agent` on GitHub
- Runtime: Node ≥18, Slack Bolt, Claude Sonnet 4.6 via `@anthropic-ai/sdk`
- Entrypoint: `node index.js`

## Integrations

- **Slack** (Bolt + Socket Mode) — messages, @mentions, DMs, file uploads
- **Claude API** — Sonnet 4.6, tool use, multi-round reasoning, vision on images/PDFs
- **OpenAI** — Whisper for audio transcription
- **Supabase** (2 projects):
  - **ng-agent main** — `conversations`, `agent_knowledge`, `scheduled_tasks`
  - **neurogrowth-proposals** (`xqzfhofxtmqjozowrwdo`) — portal: `client_dashboards`, `customer_activities`, `customer_activity_templates`, `flywheel_ai_onboarding`, view `v_phase0_fulfillment`
- **Google** — Gmail, Drive, Calendar, Sheets, Docs
- **GoHighLevel** — MCP server (`ghl-mcp/`) for pipelines, contacts, opportunities, conversations
- **Meta Ads** — account/campaign/ad-set performance
- **Notion** — search + task creation (Operations Tracking + Project Sprint Tracking)

## Domain model

- **Client lifecycle:** Phase 0 (onboarding) → Phase 1 → Phase 2 → Phase 3 → live → (stabilization 20-day checkpoint) → blocked-if-stuck
- **Phase 0 steps** (via `v_phase0_fulfillment.phase0_step`):
  1. `1_awaiting_signup` — no clerk_user_id, no T&C
  2. `2_awaiting_terms` — signed up, T&C not accepted
  3. `3_awaiting_form` — T&C signed, form not completed
  4. `4_awaiting_activation_call` — form done, no booking
  5. `5_ready_for_handoff` — all done, go_live_at still null
  - Filter: `go_live_at IS NULL`
- **Build & Release** — 14-day delivery model
- **Retention tiers** — OMEGA / ROLEX / PATEK
- **Customer types** — `flywheel-ai` / `full-service`

## Team roles (role-based permissions + per-role system prompts)

- **Ron** — CEO, only person who can approve team-channel posts
- **Setters** — Joseph, Debbanny
- **Closers** — Jose Carranza, Jonathan Madriz (routed by user ID)
- **Fulfillment** — Josue (ops), Valeria (docs), Felipe (campaigns), Tania

## Tools (28 total, defined in `index.js` tools array ~line 2080)

Read: `search_notion`, `search_gmail`, `search_drive`, `read_google_sheet`, `read_google_doc`, `read_slack_channel`, `search_ghl_conversations`, `search_knowledge`, `get_client_status`, `get_portal_alerts`, `get_sales_intelligence`, `get_meta_ads_performance`
Write: `send_email`, `create_notion_task`, `save_knowledge`, `draft_slack_message` (team channels require Ron approval)
Admin: `create_scheduled_task`, `list_scheduled_tasks`, `update_scheduled_task`, `delete_scheduled_task`

## Scheduled jobs

- **5:30 AM CR, Mon–Fri** — Nightly Learning: scan 5 channels + Gmail + calendar → Claude extracts to `agent_knowledge`
- **10:30 PM Fri** — Weekly Portal Trends
- **2:00 PM Mon** — Gap Detection (overdue clients, stale activities, blocked)
- **3:00 PM & 8:00 PM daily** — Proactive Alerts (unresolved alert-category knowledge >24h)
- **2:00 AM Tue–Sat** — Proactive Team DMs (Day 7/14 checkpoints, Day 20 stabilization)
- **Dynamic crons** stored in `scheduled_tasks` with validated section headers (reject + re-prompt if malformed; notify Ron instead of posting garbage)

## Architecture notes

- **Channel cache** — 10-min TTL on Slack channel list (rate limit guard)
- **Approval flow** — team-channel posts require Ron's DM approval; 30-min TTL on pending approvals
- **Thread context** — @mentions in threads fetch full thread history before responding
- **Rate limit** — per-user 10 req / 60s
- **Conversation pruning** — 40 rows per user
- **Knowledge categories** — client, team, process, decision, alert, intel

## Current open work

- **Phase 0 fulfillment tool** — `v_phase0_fulfillment` view created in neurogrowth-proposals; grants identical to `client_dashboards`. Need to add `getPhase0Clients()` in `index.js` (~line 1264), register `get_phase0_clients` tool (~line 2080), add dispatcher branch (~line 2111). Then update `Fulfillment EOD Pulse` and `Friday Delivery Wrap-Up` prompts in `scheduled_tasks` to add a PHASE 0 — PRE-PORTAL section. Deploy = push to `main`, Railway auto-deploys.
