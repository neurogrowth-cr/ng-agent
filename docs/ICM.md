# ICM — Intelligent Context Management (fleet standard)

The token-efficiency architecture adopted fleet-wide (Max, AXON, REVI, dash)
after the 2026-09-02 Document Factory account-drain incident. Every Claude call
site in every NeuroGrowth repo complies with these seven invariants or
documents why not. Shipped via ng-agent #173/#174/#175, ng-axon #32,
ng-revi #90, dash #47.

Pricing basis: cache write = 1.25× input (5-min TTL) or 2× (1-hour TTL),
cache READ = 0.1×. Max 4 `cache_control` breakpoints per request. Minimum
cacheable prefix: 1024 tokens on Sonnet-class models, 4096 on Haiku 4.5 —
below the minimum a marker silently no-ops, so never cache small Haiku
one-shots.

## The seven invariants

1. **ICM-1 — Cache-ordered requests.** Tools array is a static literal with
   `cache_control` on the last tool definition. `system` is a blocks array,
   never a plain string: block 1 = everything byte-stable across calls
   (cached), block 2 (unmarked) = semi-stable + volatile content (lessons,
   timestamps, per-call appends). **Timestamps never appear above a
   breakpoint** — a minute-granular timestamp mid-prefix is the classic
   silent cache-killer (it shipped twice in this fleet before ICM).
2. **ICM-2 — Rolling message breakpoint** in multi-round tool loops: mark the
   last block of the final user message on a **request-time shallow copy** —
   never persist markers into the retained history array (accumulated markers
   blow the 4-breakpoint limit → API 400). Rounds 2..N then re-read all prior
   rounds at 0.1×. Breakpoint budget: tools + static system + rolling = 3 of 4.
3. **ICM-3 — Full usage capture at every call site.** Every response logs:
   model, input/output tokens, `cache_creation_input_tokens`,
   `cache_read_input_tokens`, duration, correlation id, and a `call_site`
   tag. Zero unlogged sites — an unlogged site makes every cost baseline a
   lie. Total context volume = input + cache_creation + cache_read
   (`input_tokens` alone EXCLUDES cached tokens).
4. **ICM-4 — Model tier constants, env-overridable.** Exactly two per repo:
   `<REPO>_MODEL` (the agent's current model) and `<REPO>_MODEL_LIGHT`
   (Haiku). No other model literals anywhere. Mechanical calls (format
   transforms, one-line narrations, extractions) use LIGHT; anything a human
   reads as the agent's voice stays on the agent model. Rollback for any
   tiering decision = env flip on Railway, no redeploy.
5. **ICM-5 — Context hygiene.** Every tool result and transcript entering
   model context passes a head/tail cap with an elision marker. No unbounded
   `JSON.stringify` into messages, ever — an uncapped result rides every
   subsequent round of the loop at full price.
6. **ICM-6 — Budget guards count total context** (input + cache_creation +
   cache_read), checked before committing the next spend. Counting
   `input_tokens` alone silently loosens the guard the day caching ships —
   this happened to `AXON_FACTORY_INPUT_BUDGET`.
7. **ICM-7 — Narrow tool sets, standardized variants.** Scheduled jobs that
   need 3–6 tools use an allow-list; all report crons share ONE list (one
   cache identity), never bespoke per-cron sets, and never tool sets generated
   dynamically per call — the tools array is part of the cache key.

## TTL choice per surface

1h TTL costs an extra 0.75× of the prefix vs 5-min; one avoided cold re-bill
saves 0.9×. Choose 1h whenever real call cadence falls between 5 min and 1 h
(Max main loop: hourly crons + all-day DMs; factory legs; REVI scoring), 5-min
for conversational bursts (AXON chat, REVI chat, rolling message breakpoints).

## Where things live

- **Max**: `MODEL_AGENT`/`MODEL_LIGHT` + `ALL_TOOLS`/`REPORT_TOOLS` +
  `markCacheTail`/`capText` in `index.js`; usage → portal `agent_activity`
  (`tokens_cache_write/read`, `call_site`). Escape hatches:
  `ICM_CRON_ALL_TOOLS=1`, `ICM_TOOL_RESULT_CAP`, `NG_AGENT_MODEL[_LIGHT]`.
- **AXON**: chat + factory in `index.js`/`lib/factory.js`; usage →
  `axon.llm_usage` + cache columns on `axon.factory_runs`.
- **REVI**: `engine/lib/llmlog.js` → `revi.llm_usage`; transcript cap in
  `fathom.buildTranscriptText` (`REVI_TRANSCRIPT_CAP`); `REVI_MODEL[_LIGHT]`.
- **dash**: real pricing incl. cache rates in `src/lib/llm/pricing.ts`
  (prefix-match model resolution); admin UI reads the cache columns.

## Verifying health / spotting regressions

- Cache hit ratio = read / (input + write + read). Expect ≥60% on multi-round
  agent calls once warm.
- `tokens_cache_write` recurring every call with `tokens_cache_read ≈ 0` =
  **byte instability above a breakpoint** — diff two consecutively rendered
  static blocks to find the volatile byte.
- Write ≈ 2× old input cost with few reads = 1h TTL on a surface too sparse
  for it — drop that surface to 5-min.
- Cron errors / degraded reports after tool narrowing → `ICM_CRON_ALL_TOOLS=1`.
- Haiku quality complaints → flip that repo's `*_MODEL_LIGHT` env to Sonnet.

Weekly sanity check: dash admin cost total should reconcile within ~10–15% of
the Anthropic Console usage page.
