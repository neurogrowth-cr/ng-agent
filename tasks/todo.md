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
