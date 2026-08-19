# Fix: a GHL 429 silently dropped a setter's lead claim (2026-08-17)

## The incident
Oscar reacted ✅ on Alonso Quesada Sancho (`faew9KFlhcL4TC0ECiAq`) in `#ng-sales-goats`.
Max replied `GHL PUT /contacts/… → 429: Too Many Requests. Reach out to Ron.` and the
lead stayed unassigned in GHL.

Railway logs, `ng-pm-MAX`:

```
20:00:02Z  runAutoStrikeMover starting (mode=LIVE)
20:00:24Z  Lead claim GHL write failed: GHL PUT /contacts/faew9KFlhcL4TC0ECiAq → 429
20:01:04Z  Lead yneMHFOJ0XDb0bQyd8Z4 (Jose Cuellar) claimed … opps 1/1   ← 40s later, fine
```

The hourly strike sweep pages every card in four stages with no gap between pages,
then reads a conversation per card. Oscar's claim landed 22s into that burst and lost
the race for GHL's per-location budget. Purely transient — and there was no retry
anywhere in Max's GHL client, so one 429 killed the write for good.

## Done
- [x] `ghlFetch(url, init, { retries, label })` — one shared wrapper, placed above
      `ghlGetConversationMessages` so it stays outside the source slice
      `test/strike-rules.test.js` extracts. Returns the `Response` unchanged (callers
      keep their own error handling), retries 429/5xx only, backs off 1s→3s→8s,
      honours `Retry-After` capped at 30s, and `console.warn`s each retry.
- [x] Routed through it: the claim's contact PUT, opportunity search and opportunity
      PUTs; `ghlFetchJson` (all sweep reads); `ghlGetConversationMessages`;
      `ghlMoveOpportunityStage`; `ghlGetContact`. Left the ~20 low-volume GHL call
      sites alone — these are the burst path and the claim path.
- [x] `ghlSearchOppsByStage` now sleeps `STRIKE_MOVER_THROTTLE_MS` (existing env var,
      default 120ms) between pages, matching the per-card loop. Adds a few hundred ms
      to a sweep that runs for minutes; removes the tight loop that opens the burst.
- [x] Claim failure message: a 429/5xx that survives the retries now tells the setter
      to remove their ✅ and re-add it (the claim emoji is the idempotency anchor, so
      re-reacting is a clean retry). Other errors keep the "reach out to Ron" wording.
- [x] Dropped claims write a `ghl_lead_claim_failed` row to `agent_activity` — the
      path previously left nothing but a console line.
- [x] `test/ghl-retry.test.js` — 16 checks, extracts the real `ghlFetch` from index.js
      and injects fake `fetch`/`setTimeout`, so it runs offline and instantly.
      Registered in `.github/workflows/ci.yml`; CLAUDE.md's stale "three test files"
      line now just says every file in `test/`.

## Verification
- `node --check index.js` clean; all seven test files green (ghl-retry 16/16,
  strike-rules 11/11, booking-gates 25/25, make-watchdog, make-triage, outcome-loop,
  channel-post).
- `test/strike-rules.test.js` is the canary for the helper's placement — it extracts
  and strips `ghl*` functions by name, so a helper in the wrong block breaks it.

## Follow-up (needs Ron / post-deploy)
- Alonso Quesada Sancho (`faew9KFlhcL4TC0ECiAq`) never got assigned. Have Oscar
  re-react on the original message, or assign him in GHL by hand.
- After the deploy, watch for `GHL 429 … retry` lines around the top of the hour.
  That line appearing *without* a following "Lead claim GHL write failed" is the fix
  working. If 429s become constant rather than incidental, the next step is a shared
  token-bucket limiter in front of every GHL call rather than per-call retries.

---

# Feature: Max follows Slack threads everywhere (2026-08-19)

## The incident
In Max's app DM, Ron replied inside the thread on Max's "Outcome-logging escalation"
post (15 prospects, Aug 5–14) asking *"is this still true? meaning are these still
pending?"*. Max answered **at channel level**, about the unrelated 4-calls
conversation happening in the main DM, then added *"I see you tagged me but didn't
include a message."*

## Root cause
The DM handler never looked at `message.thread_ts`:
- it never fetched the thread, so the escalation list was invisible — Max answered
  from `loadHistory()`, the flat last-10 DM history, which held the 4-calls chat;
- it replied with bare `say(reply)`, which posts to the DM root, not the thread.

The channel handler replied *in*-thread but was equally blind to thread content.
Only `app_mention` read a thread, and only when Max was explicitly tagged.

## Scope (Ron: "all conversations, DMs, threads on private, public channels, mentions")
All *thread* surfaces. Max stays silent on ordinary top-level channel chatter
outside `#ng-pm-agent` — widening that would make him answer every message in
every channel he is a member of. Outside `#ng-pm-agent` an untagged thread reply
is answered only when **Max is already in the thread** (he posted the root, or
replied earlier). A tag still works anywhere, as before.

## Done
- [x] One shared thread path: `buildThreadContext()` + the pure helpers
      `isMaxMessage` / `isMaxThread` / `formatThreadTranscript` /
      `shouldAnswerChannelMessage` / `claimEvent`. The 65-line inline block in
      `app_mention` is gone; all three handlers call the same code.
- [x] **DM handler is thread-aware** — reads the thread it was asked in, and answers
      **in that thread**. Top-level DMs stay top-level (no thread is started).
      Roster-reject, rate-limit and error replies land in the thread too, and a
      voice note/file dropped in a thread is answered there.
- [x] **Channel handler widened** — `#ng-pm-agent` unchanged and now thread-aware;
      every other channel (public, private, group DM) continues threads Max is in.
      The gate runs **before** `checkApproval` and the roster rejection, so neither
      fires in a channel Max should stay out of.
- [x] **Roster names in transcripts.** The old renderer printed `user:U0AM…` for
      everyone except the tagger (and labelled that person "Ron" regardless of who
      it was). Senders now resolve through `getMemberContext()`, and `<@Uxxx>`
      markup inside messages resolves too.
- [x] **Root gets 1200 chars, replies 300.** The thread root is normally the report
      being asked about; cutting it at 300 is part of why Max could not answer
      "are these still pending".
- [x] **`claimEvent` dedupe** — an @mention inside a thread now matches both
      `app_mention` and `slack.message`; whichever lands first answers, 60s TTL.
- [x] **Learning side effects stay tied to an explicit tag.** `extractAndSaveReportLesson`
      and `extractClientContext` are LLM calls; firing them on every thread reply
      everywhere would multiply cost and manufacture "lessons" out of follow-up
      questions. `tagged: true` (app_mention only) keeps today's behaviour exactly.
- [x] System prompt rule 6 — THE THREAD IS THE QUESTION: a `THREAD CONTEXT` block
      outranks the flat conversation history; say so rather than substituting the
      other conversation.
- [x] `test/thread-context.test.js` — 35 checks, slice-compiled from index.js.
      No CI edit needed: PR #76 landed a `test/*.test.js` glob while this branch
      was open, so a new file is picked up automatically (and the suite fails
      closed if the glob matches nothing).

## Verification
- `node --check index.js` clean; **all 13 test files green** (35/35 new checks).
- Run against `git show origin/main:index.js` the new test cannot even compile —
  none of these helpers exist on main, and the behaviour they encode had no code
  path there at all. That is the shape of the bug, not a subtle regression.
- Not verifiable offline (listed as follow-up below): the live Slack round-trip and
  whether `message.mpim` is subscribed.

## Follow-up (needs Ron / post-deploy)
- **Confirm `message.mpim` is subscribed** in the Slack app config
  (api.slack.com/apps/A0AMJFZDY2X → Event Subscriptions). `message.groups` was added
  2026-05-13 after the same silent gap; group DMs stay dark without `message.mpim`.
- After deploy, walk the failing case: reply in the escalation thread in the DM →
  the answer must land **in that thread** and be about those 15 prospects.
- Tag Max in a channel thread → exactly **one** reply (dedupe working).
- Watch Railway `ng-pm-MAX` for `Thread context fetch error` lines. One
  `conversations.replies` call now fires per thread reply in every channel Max is
  in; if that ever hits Slack rate limits the gate needs a cheaper pre-check.
