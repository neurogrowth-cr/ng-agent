# Lessons

> **Convention (since 2026-08-12): APPEND new lessons at the BOTTOM of this file
> (newest LAST). Never insert at the top — parallel sessions collide when everyone
> writes to the same first lines.**

## 2026-08-07 — Derived metrics that discard their source date create "data doesn't exist" hallucinations

**Symptom.** Max told Ron the portal "doesn't store" activation call dates. It does — `customer_activities.completed_at` joined to a template title containing 'activation call'. Max's own `get_client_status` reads that exact field, computes `daysSince`, then throws the date away and prints only "Day N since activation call".

**Root causes (all four mattered).**
- Tool outputs surfaced only the derived integer, never the source timestamp — so Max's honest reading of its own output was "no date available".
- `search_portal_schema` matches table/column NAMES only; "activation call" is a row VALUE (template title), so schema search structurally can't find it. An empty schema search proves nothing.
- Zero data dictionary in the system prompt — no table map, joins, or recipes.
- The feedback learning loop injected lessons only into scheduled reports; interactive chats got none, so corrections never stuck.

**Takeaways.**
- When a tool computes a derived value (day count, rate, delta), print the source anchor next to it. Costs nothing, kills a whole class of "not stored" claims.
- Keep a PORTAL DATA MAP in the system prompt with canonical recipes for business concepts that live as row values.
- Never let an agent claim data "doesn't exist" without: data map → schema search → at least one actual SELECT.

## 2026-08-07 — `go_live_at` is a provisioning date, not the launch date

**Symptom.** Planned launch-SLA KPI as "activation call → flywheel_ai_onboarding.go_live_at". SQL spot-check: go_live_at PRECEDES the activation call for all 7 checked clients (it's set when the account is provisioned).

**Real launch signal (verified vs Ron's tracking sheet, matches ±2 days):** latest `customer_activities.completed_at` where template title contains 'campaign qa check' or 'campaign validation'.

**Takeaway.** Column names lie. Before anchoring any KPI on a timestamp column, spot-check its ordering against a known ground truth (here: sheet launch dates). One SELECT caught what two independent plans (dash KPI + Max data map) both got wrong.

## 2026-08-04 — Make blueprint edits via API can brick a live scenario; validate modules first

**Symptom.** After adding an HTTP module to `[PROD] GHL Appt Booked → CAPI + Slack Alert` (5148796) via `scenarios_update`, the scenario failed at *initialization* ("Cannot read properties of undefined (reading 'account')"), ran 0 operations, and Make **auto-deactivated** it — live booking webhooks started queuing.

**Root cause.** The `http:ActionSendData` mapper was missing the mandatory `rejectUnauthorized` field (and module-level `parameters: {handleErrors, useNewZLibDeCompress}`). `scenarios_update` accepts an invalid blueprint without complaint; the failure only surfaces on the next incoming event.

**Fixes/takeaways.**
- ALWAYS run `validate_module_configuration` (org 2866980 / team 432699) on any new/edited module BEFORE `scenarios_update` on a live scenario. It caught the missing field precisely.
- After any `scenarios_update`, check `isinvalid`/`isActive` in the response and call `scenarios_activate` if needed — an init error deactivates the scenario and a later update does NOT reactivate it.
- Webhook payloads queue while a scenario is broken/inactive and replay on activation — nothing is lost, but they replay under whatever blueprint is live at that moment (our queued test replayed under the reverted blueprint and posted a stray booked alert).
- Recovery order that worked: revert to last-known-good blueprint → reactivate → validate the new module offline → reapply → re-test with a synthetic webhook POST and confirm via operations count (filtered module = one fewer op).

## 2026-05-13 — Slack listeners on private channels need `message.groups`, not `message.channels`

**Symptom.** The iClosed "Strategy call booked" relay (a `slack.event('message')` listener watching `#ng-sales-goats`) never fired once since it shipped — zero `iclosed-setter-reveal-*` rows in `agent_knowledge`, no threaded setter reveals, despite many qualifying iClosed posts.

**Misleading signals that wasted time.**
- Max clearly *posts* in `#ng-sales-goats` (lead alerts, reports) → assumed it could also receive messages there. It can't: `chat:write` lets a bot post to channels it isn't subscribed to receive from.
- `reaction_added` events from that channel *were* being delivered → assumed message events would be too. Different event/scope path.
- Slack app config showed `message.channels` subscribed + `channels:history` granted + bot was a channel member (`/invite` said "already in this channel"). All true, all irrelevant.

**Root cause.** `#ng-sales-goats` is a **private** channel. Private-channel messages dispatch under `message.groups` (scope `groups:history`), **not** `message.channels` (scope `channels:history`). The app was only subscribed to `message.channels`, so every message in any private channel was silently never delivered.

**How we proved it.** Added a temp trace logging *every* message event the listener received (channel + channel_type). Railway logs showed 100% `channel_type: "im"`, zero for the private channel — definitively "listener never invoked for that channel" vs. a downstream filter bug.

**Fix.** Add the `message.groups` bot event in Slack app config. If `groups:history` is already an approved scope, no reinstall is needed (Socket Mode re-handshakes).

**Takeaways.**
- When a Slack listener targets a channel, check whether it's public or private and subscribe to the matching event (`message.channels` *and/or* `message.groups`). The public/private distinction is invisible in code.
- "Bot posts there fine" and "bot gets reactions there" are NOT evidence the bot receives messages there.
- To distinguish "listener never fires" from "listener fires but filters drop it," trace *before* the first early-return with the broadest possible scope, then narrow.

## 2026-05-08 — Duplicate "New Lead" Slack posts come from GHL, not our code

**Symptom.** Two `🆕 *New Lead*` posts in `#ng-sales-goats` for what looked like the same person, seconds apart. Reported case: Roberto Javier Gomez Avalos / "Rob" at 09:29 CDMX.

**Investigation.** Queried `lead_posts` (ng-agent Supabase) for any pairs <2 min apart over 30 days; cross-joined with `agent_activity` (neurogrowth-proposals) to read message bodies and correlation IDs.

**Findings.**
- 5 duplicate pairs in ~16 hours (table only has 2 days of history — `lead_posts` was added in commit `960792a`, so older data is absent).
- Every pair follows the same shape:
  - First post: full name + email + `Source: Facebook`, often assigned to a setter.
  - Second post (5–36 s later): nickname or partial name, no email, **`Source: Paid Social`**, often unassigned.
- Each pair has **two distinct correlation IDs**, meaning two separate GHL webhook payloads — not one webhook firing twice in our code.

**Root cause.** GHL has (at least) two workflows that both create a contact + fire the lead webhook off the same Meta Lead Ads event:
1. The clean "Facebook Lead Form" workflow (Source=Facebook, full intake).
2. A "Paid Social" workflow that captures only the phone (Source=Paid Social, nickname-only).

So GHL produces two contact records per real lead, each with its own contact_id, and our webhook handler ([index.js:5743](../index.js:5743)) faithfully posts both because dedup is keyed only on `contact_id`.

**Fix locus.**
- **Primary:** GHL workflow config. Either disable the "Paid Social" duplicate creator or make it update the existing Facebook contact instead of creating a new one. This is where the data corruption actually lives — the second contact is junk and pollutes setter_claims, leaderboards, and any per-contact reporting.
- **Secondary (safety net in our code):** add a dedup pass in `handleGHLWebhook` before posting — look up `lead_posts` rows from the last ~30 min, match on normalized phone (last 10 digits) OR email. If a match exists, skip the Slack post and instead drop a threaded reply on the original ("⚠️ GHL created a second contact <id>"). Catches the case even when the GHL config drifts again.

**Pattern to remember.** When you see two "New Lead" posts close in time and the second one has `Source: Paid Social` (or "Social media") + no email + nickname, it's the FB Lead Form → WhatsApp dup. The first post is the real contact; the second is the auto-spawned WA contact.

**Confirmed root cause (added later).** It's not a GHL workflow misconfig — it's GHL's *native* "auto-create contact when an unknown WhatsApp number messages in" behavior. Funnel sequence:
1. Lead submits FB Lead Form → GHL creates contact A with their typed phone + email + Source=Facebook.
2. Thank-you page has a "Start WhatsApp chat" CTA. Lead clicks → opens WhatsApp on their device → they message us.
3. The phone tied to their WhatsApp account differs from what they typed in the form (one's a landline / typed mobile, the other's their actual WA number). GHL doesn't recognize it, auto-creates contact B with Source=Paid Social, no email, and whatever display name WhatsApp surfaces (often a nickname).
4. Both contacts enroll in "New Lead Intake and Assignment (NON-VSL PIPELINES)" workflow → both fire our `/webhook/ghl` → both post to Slack.

**Only happens on FB Lead Form funnels**, not the VSL self-book funnel (no WA click-to-chat there).

**Fixes shipped on our side:**
1. Migration `008_lead_posts_dedup` — phone_last10 + email_lower columns and indexes.
2. Migration `009_lead_posts_source_name` — source + full_name + name_prefix3 columns and indexes.
3. `handleGHLWebhook` ([index.js:5806](../index.js:5806)) — two-tier dedup. (a) Look back 30 min on normalized phone OR email; (b) If no hit AND incoming has no email AND incoming source is non-FB social, fuzzy-match on `name_prefix3` (first 3 chars of first name) against a Facebook-source post within the last 5 min. On match: skip the top-level Slack post + setter DM, threaded-reply on the original ("⚠️ GHL spawned a second contact"), and still record the dup contact_id → original ts so reverse-mirror claims work.

**Cases the fuzzy match still misses.** When the WA display name has a totally different first name from the form (e.g., form: "Warner Zuñiga", WA: "WZM" — initials only). Those will still produce two top-level Slack posts. Acceptable until the GHL-side fix lands.

**Real fix (GHL side, not yet built).** Pre-fill the WhatsApp click-to-chat URL with `?text=…Ref: {{contact.id}}`, then add a GHL workflow on `Inbound Message` that parses the ID and merges/deletes the auto-created dup. See chat 2026-05-08 for full step-by-step. Until that lands, the safety net carries the load.

---

## 2026-07-22 — Financial signals leaked to a team channel (confidentiality boundary)

**What happened.** An evening scheduled report posted to a team channel included the iClosed $360 payment failure AND the Tech-Stack account balance ($185.97). Source: `registerDynamicCron` injected Ron's unread Gmail (`getRecentEmails()`) into EVERY scheduled report prompt regardless of destination channel — a bank/billing notice in the inbox became team-visible output.

**Rule.** Company financials (bank balances, payment failures, billing status, invoices, card/account info) are **Ron-only** until he says otherwise. Ron's Gmail is a confidential source: it must never feed a prompt whose output posts to a team-visible surface.

**Fix shipped.** (1) Email context only injected when `task.channel === RON_SLACK_ID` (DM). (2) Team-channel report prompts carry an explicit CONFIDENTIALITY rule as defense-in-depth. (3) Nightly learning: new `confidential` extraction category → saved as `visibility='private'` knowledge under Ron's user_id, DM'd to Ron, excluded from the public "What I learned tonight" post (proactive-alerts cron only re-broadcasts `shared` alerts, so private entries can't resurface).

**Pattern to remember.** Whenever wiring a new data source into Max, ask: *who can see the output surface this feeds?* A source's sensitivity must be ≤ the audience of every surface it reaches. Gmail/Calendar/GHL-financials → Ron-only surfaces. Slack channels/portal → team surfaces OK.

## 2026-07-28 — A filter that hides data is invisible; verify allowlists against raw counts, not code review

**Symptom.** The Jul 27 Weekly Closer Leaderboard reported Ron at "$0 closed, 0 sold" for a week in which he had closed a $3,500 deal that was correctly logged in `revops_sales_outcomes` the same day.

**Root cause — three independent allowlists, each silently dropping rows at a different layer.**
1. `SALES_FLYWHEEL_SLUGS` (ng-agent) omitted Ron's personal iClosed event slug → `getNonFlywheelCallIds()` classified his calls as partner-consulting and excluded them from every sales report. Also hid a $3,895 win from June.
2. dash's GHL sales-calendar allowlist omitted the "Intro" calendar → those bookings never reached `revops_appointments`.
3. The GHL workflow trigger itself filtered to one calendar → GHL never *sent* the webhook at all.

**Why it stayed hidden for weeks.** Every one of these fails *closed and silently*. An excluded call looks exactly like a call that never happened — there is no error, no log line, no gap in a sequence. Reports rendered confidently with missing rows.

**How we proved each one.** Never by reading the code — always by counting raw rows:
- Ran the leaderboard's exact query against Supabase and diffed the result against the Slack post.
- Counted webhook deliveries by calendar id: **0 for Intro vs 19 for the main calendar** — that single count proved the gap was upstream in GHL, not in dash, and saved fixing the wrong layer.
- Cross-checked "pending" calls against Fathom recordings: 12 of 14 had recordings, proving `pending` meant "outcome not logged", not "call didn't happen".

**Takeaways.**
- **An allowlist is a silent data filter. Any time one exists, get a raw count of what it excludes** — `count(*) where <excluded>` — and confirm every exclusion is intentional by name. Don't trust that the list looks reasonable.
- **When data is missing across a multi-hop pipeline, count at each hop before editing any hop.** The delivery-count check located the true failure point in one query; without it, the dash fix alone would have shipped and changed nothing.
- **Distinguish "no signal" from "negative signal" in every metric.** `pending` (no outcome row) is not `no_show`. Anything derived from a missing row needs an independent source of truth — Fathom recordings are the reliable one for "did this call happen".
- GHL specifics worth remembering: its "In calendar" trigger filter is **single-select** — cover N calendars with N duplicate trigger cards (they OR together into the same action). Its public API is **list-only for workflows** (`GET /workflows/{id}` → 404), and the builder SPA won't render in an automated browser tab, so every workflow edit is manual UI work.
- Vercel blocks deploys whose GitHub commit author has no linked Vercel account, even when that person's *email* is already on the team — it matches on GitHub login. Fix once via Account → Authentication → Login Connections rather than redeploying by hand each time.

## 2026-08-06 — Verify a data recovery by entity identity, never by row count or timing

**Symptom.** Two GHL "Workflow Error Detected" emails. Root cause turned out to be malformed
`customData` keys in two GHL workflows: `Outcome: Follow-up` shipped a literal TAB in the key
(`"\tng_event"`), and `Neurogrowth Dashboard Webhook` collapsed its whole mapping to one key
literally named `undefined`. Both made dash normalize the payload to `unknown` → skipped, while
still returning **200**, so nothing ever alarmed.

**The near-miss.** After the fix, `revops_sales_outcomes` showed 10 fresh `follow_up` rows in a
6-minute burst. Row count roughly matched the 14 lost deliveries, and the timing looked exactly
like a replay. I reported it as "recovered". Cross-checking `contact_id` showed only **1** of the
14 lost contacts overlapped — 9 of the 10 were new people whose outcomes had simply flowed in
correctly post-fix. The backlog of 13 was still missing.

**Takeaways.**
- **A recovery is proven by matching entity IDs, not by counting rows or noticing a plausible
  burst.** "N rows appeared and N were lost" is a coincidence until the identifiers line up.
  The query that settles it is a set difference on the entity key, not a `count(*)`.
- **A fix working for new traffic says nothing about the backlog.** Separate the two claims
  explicitly: "new events flow correctly" and "old events were recovered" need separate proof.
- **200 OK is not success.** Dash returned 200 on every malformed delivery and recorded
  `status='skipped'` in the row. Any ingestion path with a skip/quarantine state needs a
  standing count of skipped-by-reason; otherwise a broken producer is indistinguishable from
  a quiet one. This is the same silent-failure shape as the 2026-07-28 allowlist lesson.
- **Store the raw payload.** `ghl_webhook_deliveries.payload` is the only reason root-causing
  was possible at all, and the only reason replay is still on the table — the follow-up outcome
  existed nowhere else (the admin backfill route pulls appointments/opportunities from the GHL
  API by date and cannot reconstruct it).
- **Check the malformed key at the byte level.** `\tng_event` renders identically to `ng_event`
  in most UIs. Compare `jsonb_object_keys` output, not the rendered label.

## 2026-08-06 — Make blueprints are a shared checkout: fetch-modify-write races clobber parallel edits silently

**Symptom.** The Closer/Setter relabel + setter line pushed to Make scenario 5148796 at 20:01 UTC
was gone by the next booking alert (22:59 UTC): the message still said `Host:` with no setter.
Execution history showed a second `modify` event at 20:11 UTC — a parallel session (working on
CAPI match quality) had fetched the blueprint BEFORE 20:01, made its own edits (`country` field,
`action_source: system_generated`), and pushed the whole thing back, silently reverting the Slack
text. The exact same race ate the 2026-08-05 "Assigned to" edit (project-state recorded it shipped;
the 09:05 UTC blueprint edit that day wiped it).

**Why it happens.** `scenarios_update` wholesale-REPLACES the blueprint — there is no merge, no
version check, no conflict error. Two sessions doing fetch→edit→push always end with last-writer-wins,
and the loser gets zero signal. This is the Make equivalent of the shared-git-checkout rule in
global CLAUDE.md; the remote state can change between your fetch and your push.

**Takeaways.**
- **Fetch immediately before update, never edit a blueprint fetched minutes earlier.** Minimize
  the fetch→push window.
- **After pushing, re-fetch and diff the field you changed** — the update response echoing success
  only proves YOUR write landed, not that it survived. Also scan `executions_list` for `modify`
  events after your own; any later `modify` you didn't make means your edit may be gone.
- **When re-applying over a clobber, merge — don't counter-clobber.** The 20:11 edit contained real
  CAPI improvements; the re-push had to keep them (edit the CURRENT blueprint, not your old copy).
- **When Ron reports "the fix didn't take", check `lastEdit`/modify history FIRST** before assuming
  the original edit was wrong — on this team, concurrent-session clobber is the leading cause.

## 2026-08-04 — An outcome attached to a call that hasn't happened yet is always stale

**Symptom.** Daily Call Roster printed `outcome: no-show` on Daniela Bruno's fresh 2 PM call (and José Manuel's Aug 5 call).

**Root cause chain.** GHL reschedules reuse the appointment (same appointmentId) and keep `appointmentStatus=noshow`; the Outcome workflow re-fires on the update carrying the NEW startTime + the STALE status; dash upserts in place and `revops_sales_outcomes` is unique per appointment — so the old booking's no-show becomes "the outcome" of the upcoming call.

**Rule.** Any report joining `revops_appointments` × `revops_sales_outcomes` must gate on `scheduled_start <= now()` before treating an outcome (or the `attended` boolean) as describing that call. Fixed in `getSalesIntelligence` today-branch + both weekly-stats fns; upstream guard in dash PR #22 (`clearStaleNoShowState`). Also: `reschedule_count` is 0 on rows that demonstrably rescheduled 2-3×  — never use it as a signal.

## 2026-08-04 — Date math on the server clock is wrong after 6 PM CR; label calendar blocks with resolved dates

**Symptom.** On Fridays Max said "tomorrow is the Win Da Week meeting" (it's Mondays).

**Root cause.** `getCalendarEvents` built day windows with `new Date()`+`setDate`/`setHours` — server-local (Railway = UTC), so after 6 PM CR "today" was already tomorrow — and `setHours(29,…)` on the end bound made every window 48h wide. The block was injected as `TOMORROW'S CALENDAR:` with raw ISO event lines (no weekday, organizer's -05:00 offset), so the model had nothing to cross-check the label against and trusted it.

**Rules.** (1) Never compute a "day" from the process clock — derive the CR date string first (`toLocaleDateString('en-CA', { timeZone: 'America/Costa_Rica' })`), then build UTC instants (pattern: `_crMidnightUtc` / `_crDayBoundsUtc`). (2) Any context block making a temporal claim ("today", "tomorrow") must carry the resolved date + weekday in the label AND in each line, so the LLM can catch a mislabel instead of amplifying it. (3) `setHours(29,…)` rolls the date forward a day — it's a window-width bug wearing a convenience-trick costume.

## 2026-08-12 — Parallel sessions clobber each other via stale checkouts and top-of-file docs

**Symptom.** Three confirmed incidents: (1) 2026-05-26 — a docs-only session committed a stale copy of `index.js` (stale stash pop), reverting a just-merged PR's +88 lines; hand-repaired 2 min later (`ad44677`). (2) 2026-08-11 — `f8931ac` (direct push to main, no PR) hand-copied the Make-watchdog code from unmerged branch `make-scenario-watchdog` but rewrote its `project-state.md` entry from stale knowledge — main's docs said hourly polling while the shipped code ran `*/10`, and `dlqCount` + `checkBookingAlertDivergence` went undocumented until 2026-08-12. (3) Same class outside git: Make blueprint fetch-modify-write races (see the 2026-08-06 lesson above).

**Root causes.**
- Sessions editing the shared checkout, where another session's `stash`/`checkout` changes files under them.
- Both `project-state.md` and `lessons.md` prepended new entries at the top — every concurrent session wrote to the same first lines, guaranteeing conflicts or silent overwrites.
- 28 of the 40 commits before this fix went straight to main with no PR, so half the work never got a merge base; hand-copying a branch's code instead of merging it orphans the branch AND forks the docs.

**Fixes shipped 2026-08-12.**
- Docs are now append-at-bottom (newest LAST) with `merge=union` in `.gitattributes`; old entries archived to `tasks/archive/`.
- Repo `CLAUDE.md` + CI + permission deny on `git push origin main`: all changes via PR from an isolated worktree off fresh `origin/main`.

**Takeaways.**
- Never hand-copy code from another session's branch — merge or cherry-pick the actual commit, or the docs and git history fork.
- A "docs-only" commit that touches `index.js` is always a bug; CI now rejects it.
- When two writers share one mutable surface (checkout, Make blueprint, prepend-at-top file), the fix is always the same: isolate copies (worktrees), make writes commutative (append + union), or serialize through a queue (PRs).

## 2026-08-13 — In-memory approval state is unusable for anything a cron triggers

**Symptom.** Designing an "alert fires → Max proposes a fix → human approves → execute" flow, the obvious move was to reuse the existing `APPROVAL_NEEDED` sentinel + "reply yes" machinery. It would have silently failed in production most of the time.

**Why it doesn't work for alerts.** `pendingApprovals` is a plain module-level object: 30-minute TTL, wiped by every Railway deploy, keyed by approver ID only (a second draft silently overwrites the first), no thread awareness, and its stage-2 phrase list is an exact-match English array — "sí", "dale", or even "yes do it" match nothing. That's all fine for its actual use case (a human asks for a draft and approves it seconds later in the same DM). It is completely wrong when the two events are separated by hours and a redeploy, which is exactly the shape of every cron-fired alert.

**The pattern that does work, already in the codebase.** The campaign-draft approval stores the entire payload in the Slack message's own `metadata.event_payload` and rehydrates it via `conversations.history` when someone reacts. No TTL, no server state, survives restarts, works days later. Idempotency comes from whether the bot already reacted to that message.

**Takeaways.**
- **Match approval-state durability to the gap between propose and approve.** Seconds → memory is fine. Hours, or anything a cron starts → the state must live somewhere that survives a deploy. The message itself is the cheapest such place and needs no migration.
- When two approval patterns already exist in a codebase, the newer/more-used one isn't automatically the right one — check what each assumes about elapsed time.
- Post-hoc "has the bot already reacted?" idempotency leaves a real double-execute window when two people react at once. An atomic claim (win `reactions.add` or bail on `already_reacted`) closes it.

## 2026-08-13 — Fire-and-forget code makes negative assertions pass vacuously

**Symptom.** A new test asserted "a `no_action` triage verdict posts no proposal" and "a failed triage doesn't post" — both passed on the first run. They were meaningless: the triage call is deliberately fire-and-forget (`fn(...).catch(...)`, not awaited, so it can never block or break the alert loop it rides on), so the assertions ran before the promise chain had done anything at all. The one test that caught it was the *positive* case — "a proposal IS posted" — which failed with 0.

**Takeaway.** Any test asserting that something did NOT happen, against code that starts unawaited async work, must drain the microtask queue first (`await new Promise(r => setImmediate(r))`). Otherwise every negative assertion in that file is green for the wrong reason. Write at least one positive assertion in the same block — it's the only thing that reveals the problem.

## A scraper that returns 0 on error is indistinguishable from a real zero (2026-08-14)

**Symptom.** The weekly recap reported "New contacts this week: 0" for months while GHL was delivering ~100 leads/week. `_scrapeGhlNewContactsToday` had no `res.ok` check: any non-200 body has no `meta.total` and no `contacts`, so the function fell through to `(data.contacts || []).length` → `0`, and `recordObservation` persisted it because `Number(0)` is finite. A dead credential and a genuine zero-lead day produced byte-identical rows. The fake zeros also poisoned the 28-day anomaly baseline, so nothing ever fired to flag it. Compounding it, the call passed GHL **v1** `startDate`/`endDate` params to the **v2** `/contacts/` endpoint, which does not accept them.

**Takeaway.** A metric scraper must **throw** on failure, never return a plausible value. `null`/throw gets recorded as a skip or error; `0` gets recorded as data and silently becomes the baseline. When a metric reads flat-zero for more than a few days, verify it against a raw count from the source before believing it — this is the same failure shape as the `iclosed_calls_held_yest` zombie (audit INS-09), which summed 0 while 31 real appointments existed. Prefer an internal deduped table (`lead_posts`) over a raw third-party count when one exists: it is already error-checked, already deduped, and cannot drift from the rest of the reporting.

## `merge=union` does not apply on GitHub — only to local merges (2026-08-15)

**Symptom.** Two docs PRs that each only *appended* to the bottom of `tasks/project-state.md` still collided: PR #52 flipped to `mergeStateStatus: DIRTY / mergeable: CONFLICTING` the moment PR #53 merged, despite `.gitattributes` declaring `tasks/project-state.md merge=union`.

**Takeaway.** Custom merge drivers live in the local git config and run on the machine doing the merge. GitHub's server-side merge ignores them, so the union guardrail protects `git pull`/`git rebase` on a workstation but does **not** stop a PR from conflicting. When a docs PR goes CONFLICTING: rebase it locally (where union resolves cleanly), verify no entry got duplicated — union keeps *both* sides, so a restructured file can silently gain a second copy of an entry — then force-push. Also merge docs PRs promptly; in this repo `main` moved four times during one PR's life and dirtied it twice.

## 2026-08-15 — The unused-underscore convention can silently kill a whole report

**Symptom.** The Monday delivery gap report had never posted whenever it actually had gaps. No alert, no error surfaced, months of silence that looked exactly like "no gaps this week."

**Root cause, two faults stacked.**
1. `runMondayGapDetection(_correlationId)` used the leading-underscore "this param is unused" convention — but the param WAS used, four lines from the end: `executeChannelPost(OPS_CHANNEL, message, null, correlationId)`. That threw a `ReferenceError` while evaluating the call arguments, so the post function was never entered.
2. Even with that fixed, `executeChannelPost` called `say()` unguarded. On cron paths `say` is `null`, so the channel post SUCCEEDED and then the confirmation threw a `TypeError` — and the `catch` called `say()` again, throwing a second time.

Both were swallowed by `catch (err) { console.error(...) }`.

**Why it hid for so long.** The function returns early when `gaps.length === 0`. The broken line only ran on the weeks that had something to report — so the failure was perfectly anti-correlated with anyone noticing. A quiet week and a broken week produced identical output: nothing.

**Takeaways.**
- **`_name` is a claim, not a decoration.** Prefixing a parameter asserts "nothing in this body reads it." If anything does, you get a ReferenceError that only fires on the path that reads it. Grep the body before adding the underscore — a sweep of the other nine `_correlationId` crons found no second instance, but only because they genuinely never use it.
- **An optional callback must be guarded at every call site, including inside the catch.** A `catch` that calls the same possibly-null function it is catching for turns one fault into two and propagates out.
- **`catch { console.error }` on a scheduled job is indistinguishable from success.** Nothing reads Railway logs on a Monday. Any cron whose failure mode is "posts nothing" needs either a heartbeat row or an alert on the empty case — the same silent-failure shape as the 2026-07-28 allowlist and the 2026-08-06 skipped-webhook lessons.
- **A regression test for an invisible bug must be proven to fail against the old code.** Running the new test against `git show origin/main:index.js` failed 7 of 12 checks; the one that passed (`the message actually reaches the channel`) is what confirmed the post itself was fine and only the confirmation threw.
- **Measure the coverage of a signal before designing a workflow around it.** "REVI knows who showed up" was true but only ~39% of the time. The plan's fallback — DM the closer when REVI is silent — would therefore have fired on ~60% of calls, i.e. the manual SOP with extra steps. The dry run caught it before a line of it shipped. A verification step phrased as a *number with a bar* ("if the human-ask count isn't small, the cascade is wrong") is what made the failure visible; "check it looks right" would not have.
- **Positive and negative evidence are not symmetric, and the asymmetry is usually the design.** A REVI recording proves a call happened (16/16 held, 0 no-shows ever had one). Its absence proves nothing (25 of 27 recording-less calls were held). Writing `showed` on proof while refusing to write `noshow` on absence is not timidity — it is the only direction the data supports.
- **Derive the fact from the judgment instead of asking for both.** Attendance is implied by every outcome except `no_show`, so asking "did they show?" *and* "what happened?" asks the same person the same thing twice. Collapsing them removed an entire DM surface that was already built.
- **A guard that has never matched a row is not a guard.** `qualification_snapshot.ghl.cancelled` had been in the cancelled-call filter for weeks and never excluded anything — dash writes the flag at the top level. Nothing failed loudly; cancelled calls were simply nagged. Any predicate over a JSON blob written by another system should be verified against real rows once, or it is decoration.
- **When a test slices source out of a monolith, a new `const` can break a DIFFERENT test.** Adding `GHL_TERMINAL_STAGE_IDS` inside the strike-mover region put a reference to an out-of-slice constant into `strike-rules.test.js`'s extracted block. Constants belong in the block that owns their meaning, which is also the block whose test covers them.
- **A reused row carries its old answers forward — always ask whether a status predates the thing it describes.** GHL reuses the appointment row on reschedule, so a `noshow` logged for a Jul 24 call rode onto the Aug 4 rebooking and sat there while the prospect attended a 74-minute recorded call. The tell is purely temporal: `dateUpdated` four days *before* `startTime`. Attendance written before a call cannot be about that call. 3 of 16 post-cutover terminal statuses were stale this way — and the first version of this feature read all three as settled truth.
- **"Flag it to a human" can be a way of not solving the problem.** The conflict DM felt safe and was the wrong answer: the discrepancy was fully explainable from timestamps already in the payload. Ron asking "how would that be a no-show?" is what forced the actual explanation. Before escalating a contradiction, check whether the data already says which side is stale.
- **A surface that says "reply here" must actually read "here."** The outcome card's footer invited a threaded reply, but the DM handler routed thread replies to the LLM with no parent context — so Max asked "which prospect?" directly under a card that named the prospect. Any message handler that co-exists with metadata-carrying bot messages needs a thread-parent check before the LLM sees the text; the parent's metadata usually answers the question the LLM is about to ask.
- **Read the whole call before diagnosing it — options objects live at the closing paren.** A grep that caught `cron.schedule(task.cron_expression, async () => {` but not the `}, { timezone: 'America/Costa_Rica' })` 400 lines down produced a confident, tabulated, completely false "every dynamic cron fires 6h early" diagnosis — nearly a pointless "fix" that would have ACTUALLY shifted 10 production reports. For any long callback, grep the region between the call's open and close (or the function's tail) before claiming an option is absent.
- **If the reply IS the artifact, "return only the artifact" is a wish, not a guarantee.** The nightly closer nudge's prompt already said "Return ONLY the final reminder text" — the model complied *and then framed it anyway* ("Here is the final reminder text: ---"), and the whole sales team saw the scaffolding. Any cron whose output posts verbatim needs a machine-checked cut, not a politely-worded instruction. The cheap form is a whitelist anchor: declare the deliverable's known FIRST token and slice everything before it (`TASK_LEAD_ANCHOR` / `trimToLeadAnchor`). Note the guard must pass through a reply with no anchor at all — that is a *different* failure (the model produced no deliverable) and belongs to the re-prompt path, not to a silent blanking.
- **Renaming a scheduled task silently unhooks every map keyed by its name.** Renaming "Daily Closer Attendance & Outcome Reminder" → "Daily Closer Outcome Reminder" left a stale `TASK_HEADERS` key, so the task stopped matching its own (short-nudge) validation rule and fell through to the unknown-task 300-char branch. Nothing errored; the rule just quietly stopped applying. Grep the old name across `index.js` before renaming anything in `scheduled_tasks`.

## 2026-08-17 — Max rate-limited himself and dropped a setter's lead claim

**Symptom.** Oscar reacted ✅ on a lead in `#ng-sales-goats`; Max replied
"tried to claim, but GHL update failed: GHL PUT /contacts/faew9KFlhcL4TC0ECiAq →
429: Too Many Requests. Reach out to Ron." The lead stayed unassigned in GHL.

**Root cause.** Not a GHL outage — Max starving himself. The Railway logs line up
exactly: `runAutoStrikeMover starting (mode=LIVE)` at 20:00:02Z, the failed claim at
20:00:24Z, and a *different* claim succeeding at 20:01:04Z once the sweep had moved
on. The sweep pages every card across four stages back-to-back with no delay, then
reads a conversation per card; a human's claim landing inside that window loses the
race for the per-location burst budget. Every GHL call in `index.js` was a bare
`fetch` that threw on the first non-ok status, so one transient 429 killed the write
permanently.

**Takeaways.**
- **A third-party client with no retry is a bug, not a simplification.** 429 and 5xx
  are normal operating conditions, not exceptions. Retry those (backoff, honour
  `Retry-After`).
- ~~never retry 401/404 — they will not come good~~ — **half of this was wrong, see
  the 2026-08-19 entry below.** 404 stands. For GHL, 401 does come good: 18 of 19
  contacts that 401'd did so exactly once in 95 sweeps. It is now retried once.
- **Background sweeps must yield to interactive paths.** A cron that bursts an API
  is invisible until it collides with a human action, and then the human eats the
  failure. Throttle the sweep's *paging* loop, not just its per-item loop.
- **When an action fails recoverably, say the recovery, not "reach out to Ron."**
  The claim emoji is the idempotency anchor and it was never added, so re-reacting
  was always a clean retry — nobody knew.
- **A failure that only `console.error`s cannot be measured.** The dropped-claim path
  now writes a `ghl_lead_claim_failed` row to `agent_activity`, so "how often does
  this happen?" is a query instead of a log scrape.

## 2026-08-19 — GitHub ignores `merge=union`, so a fine PR reads as conflicted and silently skips CI

**Symptom.** PR #59 sat a full day showing `CONFLICTING` / `DIRTY`, and
`gh pr checks` said **"no checks reported"** — CI had never run on it at all.

**Root cause.** `.gitattributes` marks `tasks/lessons.md`, `tasks/todo.md` and
`tasks/project-state.md` as `merge=union` so parallel sessions can both append.
Git honours that; **GitHub's merge engine does not honour gitattributes merge
drivers**. Another session appended to `lessons.md`, GitHub saw two edits to the same
region, called it a conflict — and a conflicted PR does not run checks. `git merge-tree`
reported **0 conflict hunks** for the same merge, which is the tell: when git and
GitHub disagree about a union-merged file, git is right.

**Takeaways.**
- **Trust `git merge-tree`, not the GitHub badge, for repos using `merge=union`.**
  `git merge-tree $(git merge-base A B) A B | grep -c '^<<<<<<<'` gives the real answer
  in one command.
- **`CONFLICTING` silently implies `no CI`.** The dangerous part is not the red label,
  it is that nothing ran. Always check `gh pr checks` separately — a PR can look merely
  stale while being completely untested.
- **Keep `tasks/*.md` appends out of code PRs.** The repo already forbids the inverse
  (a `docs/*` branch must not touch `index.js`); the same separation protects code PRs
  from a docs append making them un-mergeable. Land the docs in a follow-up.
- **Rebasing is a treadmill in an active repo.** `main` took 6 merges from parallel
  sessions during one fix; the first rebase went stale mid-flight. Rebase, verify, push
  and merge in one tight pass, or enable auto-merge and stop chasing.
- **Check `gh pr list` before building anything.** A parallel session had already
  shipped the retry fix; the near-duplicate was caught only because the PR list was
  read first. Four separate collisions happened this session, and one PR merged
  cleanly *because paths differed* — GitHub reported it MERGEABLE while it would have
  created a second, duplicate route rather than a conflict. **Git will not protect you
  from duplicated parallel work.**

## 2026-08-19 — A correct general rule can be wrong for one vendor; measure before adopting it

**Symptom.** The GHL retry wrapper deliberately excluded 401 on the reasoning that
"a 401 will never come good." Sound advice in general — and wrong here, leaving ~20
recoverable failures per month unretried.

**Root cause.** The rule was adopted from first principles rather than from this
system's data. 30 days of `strike_sweep` metadata says the opposite: 401s hit 19
distinct contacts and **18 of them 401'd exactly once** across ~95 sweeps. A genuine
permission or token failure would have failed that same contact every sweep. They were
also load-independent — **0 of 95 sweeps** saw a 401 and a 429 together, and `scanned`
averaged 1528 on sweeps with a 401 vs 1549 without — so it was not the burst limit
wearing an auth mask either. It is GHL's auth layer blinking, ~1 per sweep.

**Takeaways.**
- **Distinguish "this error class is permanent" from "this error class is permanent
  *here*."** The generic rule is a prior, not evidence. One query against stored failure
  metadata settled it.
- **The shape of a failure identifies its cause better than its count.** 20 × 429 and
  20 × 401 looked identical as totals. Bursty-vs-spread told them apart: the 429s landed
  in 2 sweeps, the 401s trickled across 17. That single distinction drove both the retry
  design and the per-status alert thresholds.
- **Retrying a "permanent" error is safe only when bounded.** One retry absorbs a blink;
  a dead token still fails in 2 calls instead of 1, and is caught by a failure-spike
  threshold instead — a real auth outage produces hundreds of 401s, not one.
- **This only became answerable because failures were stored as structured metadata.**
  The same investigation against `console.error` output would have been guesswork. Store
  the failure, not just the log line.

## 2026-08-19 — Slack returns the message source, not the render; and fixtures copied off the screen agree with the bug

**Symptom.** The customer alert fan-out quality check reported a new customer as
having no alert card while the card sat in `#ng-new-client-alerts`, posted in the
same minute. It repeated hourly, to `#ng-fullfillment-ops`, in front of the whole
fulfillment team.

**Root cause.** `GMAIL_ALERT_HEADERS` held the rendered glyphs — `🔔🔔 NEW FLYWHEEL
AI CUSTOMER 🔔🔔`. `conversations.history` returns the message **source**, where
Slack stores emoji as `:bell::bell:`. The filter matched nothing, in every route.
So the content contract evaluated **zero cards** while reporting `0 bad card(s)`,
and no card ever reached the coverage set, so **every** new customer read as
uncovered. The first customer created after the check shipped was the false alarm.

The first fix attempt (#78) read the same symptom as a name-ordering problem —
`client_dashboards` said "Aura Bonilla - Cacao Legal", the card said "Cacao Legal -
Aura Bonilla" — and switched to matching on email. True, necessary, and not the
cause. The next hourly run on the deployed fix still reported the same customer
missing. That second data point is the only reason the real cause was found.

**Takeaways.**
- **Read the wire, not the screen.** Anything a rendering layer touches — emoji,
  `<mailto:a@b|a@b>` link wrapping, `&amp;`, user mentions — differs between what
  a human sees and what the API returns. Pull one real message and look at it
  before writing a matcher against it.
- **Test fixtures transcribed from a screenshot encode the same misreading as the
  code.** 47 checks were green against hand-written glyph fixtures. Copy fixtures
  from an API read, and say in the file where they came from.
- **A filter that matches nothing looks exactly like a clean system.** `0 bad
  cards` was true and meaningless. When a check can report "nothing wrong," it
  needs a path that proves it actually examined something — a positive-control
  fixture in the test is the cheap version.
- **Verify a fix against the next live run, not against the test suite.** #78's
  tests were green and its reasoning was correct; production said otherwise one
  hour later.
- **A standing condition is a daily note, not an hourly alarm.** The same check
  posted every hour because a channel it could not read counted as something to
  say — and the remediation was a Slack invite, not a deploy. Identical hourly
  alerts with nothing actionable are how a channel gets muted, and a muted channel
  hides the real gap too. Suppress on the clock (stateless), not in a memo that
  resets on every deploy.

## 2026-08-19 — Won-handoff notes: reusing a join contract for the wrong question
- **The same two records can be joined for different questions, and the right
  rule differs per question.** The 36h appointment↔recording join exists to
  PROVE a call happened (attendance). Reusing it to FETCH that call's context
  looked like reuse and was a bug: dash attaches a won outcome to the prospect's
  LATEST appointment, so the appointment date is not when the closing call
  happened. Fernando Corella won Aug 6 against an Aug 12 appointment with the
  real call on Aug 4 — missed by 8 days. Ron's framing is the rule: "sometimes
  the prospect does not close the deal right there in the call, but later on
  follow up — however, we need the context of the call anyways." Before reusing
  a matcher, restate the question it was built to answer.
- **A dry run must never write state.** The no-recording path wrote its
  `gave_up`/`no_recording` markers regardless of mode, and `gave_up` is
  terminal — so merely PREVIEWING sealed 3 deals out of the first live sweep
  (confirmed: 3 markers written 15:00Z, deleted by hand afterwards). If a mode
  is called dry-run, gate every write behind it, not just the obvious one.
- **"Ambiguity skips" was the wrong default for multi-call sales.** The opp-row
  fallback refused to pick when a prospect had >1 recording. Multi-call sales
  are normal; the LAST call before the win is the answer, not a reason to bail.
- **A handler that can receive a threaded message must read the thread, or it is answering a different question.** Max's DM handler ignored `thread_ts` entirely: Ron replied inside a report thread and got an answer about the unrelated conversation in the main DM, posted at channel level. The tell is that the reply was *coherent* — it was a good answer to the last thing in the flat history. This is the general form of the 2026-08-18 outcome-card lesson ("a surface that says 'reply here' must actually read 'here'"), and the general form is the one that matters: any handler that feeds an LLM a flat per-user history will confidently answer the wrong conversation the moment Slack gives it a second one. Thread context has to *outrank* the history, not sit beside it.
- **"Everywhere" for a Slack bot means every thread surface, not every message.** Ron asked for thread awareness in "DMs, private, public channels, mentions — all places where a thread can take place". Read literally as "answer everything everywhere", that turns the bot into a firehose in every channel it is a member of. The line that actually matches the intent is *threads he is already in*: he carries on his own conversations and stays out of other people's. Worth stating the narrowing explicitly rather than shipping either extreme silently.
- **Widening a handler's entry condition moves every side effect that sits above the new gate.** Letting the channel handler see thread replies workspace-wide put `checkApproval` and the "you're not on Max's roster" rejection in front of messages in channels he had never spoken in — non-roster people would have been told off in threads that were none of his business. When a guard clause is relaxed, re-read everything between the old guard and the first reply, not just the reply.
- **Two Slack listeners can match the same event.** `app_mention` and `slack.message` both fire for a tagged message in a channel the bot is in; the moment the message listener stops filtering that case out, every tag gets answered twice. A tiny TTL claim keyed on `channel:ts`, taken before the first thing that can reply, is order-independent and cheaper than reasoning about which listener Slack dispatches first — which is not something the docs promise.
- **When extracting shared code, ask which side effects were paying for the old trigger.** Lifting the thread block out of `app_mention` would have run two LLM calls (report-lesson + client-context extraction) on every thread reply everywhere instead of on explicit tags only — quietly multiplying cost and inventing "lessons" out of follow-up questions. A refactor that changes *how often* code runs is a behaviour change wearing a refactor's clothes; gate the side effects on the original trigger unless the wider firing is the point.
- **A shared retry wrapper only helps the call sites that actually use it.** `ghlFetch` — with its evidence-backed single-retry for GHL's transient 401 "auth blink" — already existed when the outcome loop was written, but every new GHL call in it used raw `fetch`. The first live appointment-status sweep then wrote **0 of 6**, all 401, and the identical PUT with the identical token succeeded by hand an hour later. When adding calls to a service that already has a hardened client, grep for the wrapper before writing `fetch(` — the protection is opt-in and silence is the failure mode.
- **"Permissions problem" is a satisfying diagnosis and a lazy one.** A uniform 401 across six writes looked exactly like a missing token scope, and that is what I reported. It was wrong: same token, same endpoint, same appointment returned 200 an hour later. The cheap disproof — re-run the exact failing call — costs one request and should come *before* telling anyone to go change account settings.
- **A retraction is a claim too, and needs evidence that reproduces.** The entry above ("Permissions problem is a satisfying diagnosis and a lazy one") retracted a correct diagnosis. On 2026-08-20 the same PUT, with the same token, on the same appointment, returned `401 "not authorized for this scope"` again — 4 of 4 writes, each already retried once by `ghlFetch`, 8 refusals. Ron ticked `calendars/events.write` in GHL and the identical request came back `422 "appointmentStatus must be a valid enum value"`. The scope really was missing, for a day and a half, while the sweep wrote nothing. Whatever produced that by-hand 200 on 2026-08-19, it was not this endpoint with this token — most likely it was never actually executed. Retracting a diagnosis on a single unreproduced observation is the same error as making one, in the more expensive direction: it also rewrote the alert copy to send the next reader hunting a blink.
- **Read-vs-write asymmetry inside a single run beats "transient" as an explanation.** The evidence was in the run all along: the calendar READS succeeded (40 considered, 2 skipped as already-terminal — only reachable by reading a real status back) while every WRITE was refused on scope. A transient auth blink does not sort itself by HTTP verb across two days. Before accepting "transient", ask what the same token did successfully in the same window; a permission gap is verb- and resource-shaped, an outage is not.
- **Test a write scope without writing anything: send a deliberately invalid value.** `PUT …/appointments/{id} {"appointmentStatus":"ng_probe_invalid_status"}` returns `401` if the scope is missing (auth is checked before validation) and `422` if it is present — the probe answers the question, mutates nothing, and needs no cleanup. This is the cheap disproof the entry above asked for, in a form that is safe to run against production data.
- **A GHL scope change takes a few minutes to reach an existing token — and does not regenerate it.** The first probe after Ron added the scope still returned 401; five minutes later, 422. The token string was unchanged, so nothing needed updating in Railway or `.env` and no redeploy was involved. Do not conclude "the checkbox did nothing" from one immediate retry, and do not go recreate the integration before waiting.
