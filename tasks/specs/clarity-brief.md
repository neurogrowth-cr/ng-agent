# Recipe: Clarity Brief (Ron's morning signal)

**Owner:** Ron   **Status:** live since 2026-11-04 (`CLARITY_BRIEF_MODE=live`); no brief has run on real data yet; three sources still partial, see section 8
**Written:** 2026-11-03   **Last verified:** n/a
**Decision:** brainstorm with Ron on 2026-11-03 (chat). Rulings folded in: runs inside Max on Railway (not a Claude task on the Mac, which ran 8 hours late on 2026-11-03); English; Slack scope is mentions, #ng-pm-agent and alerts that tag Ron, not every channel; the Monday brief covers Saturday and Sunday; contact@ is a separate mailbox that must be read.
**Watched by:** the cron liveness audit (07:30 daily, Ron DM) plus its own divergence check `checkClarityBriefPosted` (section 3, step 14). Until step 14 exists the recipe stays `off`.
**Why it exists:** Max already sends Ron about twelve separate DMs and posts (Friday recap, Monday reflection brief, zombie digest, provisioning lag, stale lead sweep, nightly confidential learning, IG drafts, dry runs, CHECK BROKEN). The wiki lists 37 open follow-ups owned by Ron. Nothing says "this is yours today". The brief is the one place that does.

## 1. Job

Every weekday morning, read everything that could need Ron (Slack mentions, Max's own items waiting on Ron, Ron's inbox, contact@, today's calendar, open PRs, wiki follow-ups) and hand him one DM: at most 7 items he must decide or do, his projects in flight ranked against the current plan, and one line saying what was scanned and dropped. Learn from his ✅ ❌ ⏭ so the brief gets shorter, not longer. What Ron does today by opening six apps before coffee.

## 2. Schedule

| Field | Value |
|---|---|
| Scheduler | node-cron (ng-agent), declared in `STATIC_CRON_SCHEDULES` |
| Expression | `50 6 * * 1-5` |
| Timezone | America/Costa_Rica |
| Window | since the last posted brief (`clarity_runs.posted_at`), so Monday covers Saturday and Sunday; first run 24 h |
| Mode | `CLARITY_BRIEF_MODE=off` (default) · `live` (Ron DM). No shadow mode: Ron is the only audience, so the first live run is the review |
| Kill switch | `CLARITY_BRIEF_DISABLED=true` stops the cron, the reaction handler and the divergence check in one deploy |
| Expiry | none. Retirement is Ron's call, in chat. |

## 3. Skills (steps, in order)

| # | Skill | Implemented at | Reads | Writes |
|---|---|---|---|---|
| 1 | Window: last successful `clarity_runs.posted_at`, default now minus 24 h | new `lib/clarityBrief.js` `briefWindow()` (pure) | `clarity_runs` | nothing |
| 2 | Collect Slack: messages in the window that mention `<@U05HXGX18H3>` in the channels listed in `CLARITY_SLACK_CHANNELS`, threads where Ron was mentioned and the last reply is not his, and ⚠️ / CHECK BROKEN posts in #ng-pm-agent | `collectSlack()`; `conversations.history` + `conversations.replies` per channel (bot token, same calls `runSequenceReplyAlerts` uses) | Slack | nothing |
| 3 | Collect Max's own items waiting on Ron: `pendingApprovals[RON_SLACK_ID]`, recipes still in a dry-run or shadow mode (`CLIENT_ATTENTION_ALERTS_MODE`, `GUARDIAN_MODE`, `OPEN_DEAL_SWEEP_MODE` and any future `*_MODE` env read at boot), `ig_guide_drafts` awaiting ✅, posts in Max's own DM with Ron that carry no reaction after 24 h (zombie digest, provisioning lag, strike digest), backlog PRs opened by `max-backlog-monthly` awaiting merge | `collectMaxPending()`; the bot can read its own DM with Ron via `conversations.history` | in-memory state, Supabase, Slack DM | nothing |
| 4 | Collect Gmail, Ron: `users.messages.list` with `q = in:inbox newer_than:<window> -category:promotions -category:social`, metadata headers plus snippet, cap 200 | `collectGmail(account='ron')` using `getGoogleAuth()` (`index.js:2521`, `GOOGLE_TOKEN`) | Gmail | nothing |
| 5 | Collect Gmail, contact@: same query with a second refresh token `GOOGLE_TOKEN_CONTACT` (one-time OAuth consent signed in as contact@ with Max's existing client id; section 8) | `collectGmail(account='contact')`; `getGoogleAuth(account)` gains an account argument | Gmail | nothing |
| 6 | Collect Calendar: Ron's events today with attendees; mark sales calls (match `revops_appointments` by time and email) and client calls (`loadActiveClientEmails`) | `collectCalendar()` via the existing `google.calendar` helper (`index.js:2880`) | Calendar, portal DB | nothing |
| 7 | Collect GitHub: open PRs across `neurogrowth-cr` repos (ng-agent, dash, ng-revi, ng-axon, ng-automation-KAI, roi-rm-okr-reporting, ng-wiki) where review is requested from Ron, the PR is a backlog PR awaiting his merge, or CI is red on a branch of his | `collectGitHub()` via REST with `GITHUB_TOKEN` widened (section 8) | GitHub | nothing |
| 8 | Collect wiki follow-ups: parse the `## Ron` table of `ops/open-followups.md` in `neurogrowth-cr/ng-wiki` (contents API); eligible when noted ≥ 14 days ago, not dismissed, not shown in the last 7 days; at most 2 per brief, oldest first | `collectWikiFollowups()` + `parseFollowupTable()` (pure) | GitHub contents API | nothing |
| 9 | Classify every candidate from steps 2, 4, 5: `needs_ron / team / fyi / noise`, a one-line reason, a tier (section 3.1) and a suggested action. Emails: sender type from headers (human, client, prospect, tool); tool notifications pass only when they match the alert allowlist (payment failed, overdue, disconnected, suspended, security, quota) | `classifyCandidates()` with the Haiku model `hotClassify` uses, batched 20 per call | step outputs, `loadActiveClientEmails`, `lead_posts` for prospect match | `agent_activity` row with counts |
| 10 | Suppress: drop candidates whose signature matches `clarity_dismissals` (source + sender or channel + topic key), drop `dedupe_key`s already shown and not re-escalated, re-enter snoozed items | `applySuppression()` (pure) | `clarity_items`, `clarity_dismissals` | nothing |
| 11 | Rank and cap: tier, then age; block 1 holds at most 7 items; block 2 lists projects in flight (Ron's open branches and PRs, wiki follow-ups that are projects, items he ✅'d and has not closed) ranked by section 3.1 with a one-line why; everything else collapses into the noise line with counts per source | `rankAndCap()` (pure) | steps 2 to 10 | nothing |
| 12 | Draft the DM in English: parent message = header, block 2, noise line; one thread reply per block-1 item with `Source:` (URL), `Why you:`, `Do:`; Max seeds ✅ ❌ ⏭ on each item reply | `draftBrief()` with `MODEL_AGENT` and `THINKING_OFF`, same em-dash and narration guards as `igGuideReplies`; poster hardcodes `RON_SLACK_ID` | step 11 | Slack DM; `clarity_runs` row; one `clarity_items` row per item with its `ts` |
| 13 | Reactions (Ron only, DM only, the item found by its own `ts` in `clarity_items`): ✅ accepted, shows in block 2 as in progress until a second ✅ (done) or 7 days (brief asks keep or drop); ❌ dismissed and a dismissal signature is stored; three ❌ on the same sender or channel within 30 days mute that signature and the noise line says so once; ⏭ snoozed until the next brief | `reaction_added` handler following `runIgGuideCheck` (atomic claim on the row, a double tap never double-writes) | `clarity_items` | `clarity_items.status`, `clarity_dismissals` |
| 14 | Divergence check 07:15 Mon to Fri: a `clarity_runs` row with today's `posted_at` must exist; otherwise DM Ron `⚠️ CHECK BROKEN: no Clarity Brief today (<reason from the last error row>)` | `checkClarityBriefPosted()`, shape of `checkBookingAlertDivergence` | `clarity_runs` | Ron DM |

### 3.1 Ranking rubric (provisional until the November frame exists)

The 90-day frame in the strategy repo closed 2026-10-31 and no November frame is written yet (Ron, 2026-11-03: "necesitamos claridad en esto"). Until it lands in `plan-of-record.md`, the brief ranks with this table and says so in its header: `Ranked against: provisional (revenue now > operation health > hygiene)`. When the frame lands, the table in `lib/clarityBrief.js` is replaced and the header names the frame.

| Tier | What belongs here | Examples |
|---|---|---|
| 1 Revenue now | anything where money or a deal waits on Ron | prospect or client reply waiting on him, payment failed or overdue, day-91 continuation decisions (November is the first continuation read), a close blocked on his approval |
| 2 Operation health | the machine or the team is blocked on him | CHECK BROKEN, watchdog alarms that tag him, a flag waiting for a flip out of dry run, a teammate asking him in Slack, security |
| 3 Hygiene | nothing stops without him, but it rots | PR reviews, wiki follow-ups, admin, cleanup |

Within a tier, older first. Block 2 ranks projects by the tier they serve, then by days since last activity.

### 3.2 What runs where (Ron's ruling, 2026-11-03)

Everything in this recipe runs on Railway inside Max. The Mac is not a dependency: if it is off, asleep or mid-reinstall the brief still posts at 06:50 with every source above.

Two inputs cannot be reached from Railway and are phase 2, not this spec:

| Input | Why Railway cannot see it | Phase 2 shape |
|---|---|---|
| Ron's DMs with teammates, Slack search | a bot token reads only channels it is a member of and its own DMs | a Ron user token (`SLACK_RON_USER_TOKEN`, scopes `search:read`, `im:history`) stored on ng-pm-MAX; no Mac involved |
| The Claude sessions board (`~/.claude/handoffs/recover.sh board`) | it lives on the Mac | a nightly push from the Mac between 20:00 and 23:00 CR (a `launchd` job, while Ron is still working) writes the board to a `clarity_sources` row; the 06:50 brief reads the row and, when it is older than 36 h, says `sessions: last push <date>` instead of failing. Ron accepted an evening cutoff for Mac-sourced data; nothing Mac-sourced is read in the morning |

## 4. CRITERIA

### 4a. Structural contract (machine-checkable)

Parent message MUST contain: the header line matching `^CLARITY BRIEF · \w+ \w+ \d+ · \d+ for you · \d+ projects$`, a line starting `Ranked against:`, a line starting `Scanned:` with one `<source> <n>` pair per enabled source (and `⚠️ <source> unavailable` for a failed one).
Each item reply MUST contain: `Source:` followed by a URL, `Why you:` with a non-empty value, `Do:` with a non-empty value.
Zero items: parent MUST contain `Nothing needs you today.` plus the `Scanned:` line. Never silent.
MUST NOT contain: `undefined`, `null`, `[object Object]`, `⚠️ PARSE FAILED`, `APPROVAL_NEEDED`, an em dash, an empty value after any `Label:`.
Checked by: `validateClarityBrief()` in `lib/clarityBrief.js` (pure). Fails once: reprompt once. Fails twice: post the numbers-only fallback (header, `Scanned:` line, raw item titles with their URLs, no prose) and append `⚠️ ISSUES: draft failed validation twice`.

### 4b. Numeric thresholds (machine-checkable)

| Metric | Green | Yellow | Red | Window | Warmup |
|---|---|---|---|---|---|
| Items in block 1 | 0 to 7 | - | > 7 (bug) | per run | none |
| Sources that returned | all enabled | one failed (brief posts, verdict ⚠️) | two or more failed, or the same source two days running | per run | none |
| ✅ rate (items accepted / items shown) | ≥ 30% | 20 to 30% | < 20%: signal quality, propose a rubric change to Ron | trailing 10 briefs | n ≥ 10 briefs |
| ❌ rate | ≤ 40% | 40 to 60% | > 60% | trailing 10 briefs | n ≥ 10 briefs |
| Repeats (same `dedupe_key` shown twice without re-escalation) | 0 | - | ≥ 1 (bug) | per run | none |
| Emails read per run (both inboxes) | ≤ 200 | - | cap hit: verdict ⚠️ partial | per run | none |
| Cost per run (Haiku batches + one Sonnet draft) | ≤ $0.15 | $0.15 to $0.30 | > $0.30 | trailing 10 | n ≥ 5 |

### 4c. Rubric (LLM-judged)

n/a in v1. Prose quality is judged by Ron's reactions, which 4b measures.

### 4d. Human gate

The brief writes to Ron's DM and its own three tables, nothing else. A ✅ only changes status in v1; it never sends, assigns, merges or emails. Never auto-anything: no outbound email, no post to a team channel, no GHL write, no PR merge, no reply on Ron's behalf. Gmail-derived content never leaves Ron's DM (gated in code: the poster hardcodes `RON_SLACK_ID`).

## 5. On failure (fail closed)

- One source throws: skip it, keep going, name it on the `Scanned:` line as `⚠️ <source> unavailable`, verdict ⚠️. Same source fails two days running: the brief adds a `CHECK BROKEN` line naming it.
- Draft fails `validateClarityBrief()` twice: numbers-only fallback (4a), never a padded or invented item.
- Whole run throws: `wrapCronJob` logs it and writes a `clarity_runs` row with `error`; the 07:15 divergence check DMs Ron.
- Never: post a partial brief as if complete, post to any channel, retry a post (a double DM counts as a failure), act on a reaction from anyone but Ron.

## 6. Proof it works

- [ ] `test/clarityBrief.test.js` (filename added to `.github/workflows/ci.yml`): `briefWindow()` makes Monday cover the weekend; `parseFollowupTable()` on a fixture copied from the wiki page; `applySuppression()` drops a dismissed signature and re-enters a snoozed item; `rankAndCap()` never exceeds 7 and orders by tier then age; `validateClarityBrief()` passes a good fixture and FAILS a fixture with an empty `Do:` and one with an em dash; the reaction state machine (✅ ✅ = done, three ❌ = mute).
- [ ] First live run read by Ron in chat on the first weekday after merge.
- [ ] The 4a check was made to fail on purpose once and the fallback posted.
- [ ] `clarity_runs` deleted for one day on purpose and the 07:15 check fired.

## 7. Verdict line (exact strings)

`✅ ALL GREEN: Clarity Brief posted <HH:MM>, <n> for you, <m> projects, sources <k>/<k>, cost $<x>`
`⚠️ ISSUES: <source> unavailable · fallback used · needs Ron: <what>`

(Verdict lines go to the `clarity_runs` row and to the cron liveness audit, not to the brief itself.)

## 8. Go-live state (production changes, ask first)

Done 2026-11-04: migration 025 applied and probed, `CLARITY_BRIEF_MODE=live` on ng-pm-MAX, default `CLARITY_SLACK_CHANNELS` in use.

Still open, all Ron's hands. The brief runs without them and names what it could not read:

1. **contact@ consent with its own OAuth client.** contact@ lives in the `neurogrowthconsulting.com` Google organization and Max's client is Internal to `neurogrowth.io`, so Google answers `403 org_internal`. Do not switch Max's client to External: every Max Google feature shares it, and Gmail read would need Google's restricted-scope verification. Instead, signed in as contact@: a Google Cloud project with the Gmail API on, consent screen Audience Internal, a Desktop app client. Then `GOOGLE_CREDENTIALS_CONTACT` (that client's JSON) and `GOOGLE_TOKEN_CONTACT` (its refresh token, `gmail.readonly`) on ng-pm-MAX. Ron does the sign-in; nobody pastes either value in chat. A refresh token only works with the client that issued it, so the two variables always change together.
2. **`GITHUB_TOKEN` widened.** Fine-grained PAT, contents and pull requests read on ng-agent, dash, ng-revi, ng-axon, ng-automation-KAI, ng-wiki and roi-rm-okr-reporting, keeping Actions write on `roi-rm-okr-reporting` for the VoC PDF. Editing an existing token keeps its value, so Railway does not change. Checked 2026-11-04: only ng-agent reads pull requests; the wiki follow-ups file is unreachable.
3. **Max in `#ng-automation`** (`C0BDFHGF9DZ`, private). The other 7 channels on the default list are confirmed.

## 9. Lessons folded in

- `runProactiveAlerts` (disabled 2026-05-07) recycled the same stale list every day with no fresh signal. Hence the since-last-brief window, `dedupe_key`, dismissals and muting.
- The Setter Hot Replies DM task was junk on quiet days. Hence a zero-item brief is one line, never a padded list.
- Report grooming bar: name the delta, collapse known holds to a count. Hence the `Scanned:` line.
- Financial confidentiality: Gmail content is Ron-only; source sensitivity must not exceed surface audience. Hence the hardcoded DM target and no channel fallback.
- Run-once token burn on double deploy: the cron writes its `clarity_runs` row before posting so a second boot in the same minute sees it and skips.
