# Spec: Agent event bus (siblings report to Max)

**Owner:** Ron   **Status:** PROPOSAL, nothing built. Needs Ron's go on section 2 and the decisions in section 9.
**Written:** 2026-11-08   **Checked against:** ng-agent `origin/main` at `52cd9e8` (#460), the wiki (`systems/supabase-max.md`, `repos/ng-axon.md`, `repos/ng-automation-KAI.md`, `repos/ng-revi.md`, `decisions/log.md`) and the strategy repo (`docs/NORTH_STAR.md`, `docs/context/ng-agent.md`).
**Not read this session:** the ng-revi, ng-axon and ng-automation code. Facts about them come from the wiki pages above and must be rechecked in each repo before its phase starts.

## 1. Job

Today Max learns about REVI, AXON and Kai only by reaching into their tables on a timer. Each sibling has no way to tell Max "this needs a human", "this job finished" or "this failed". The bus gives every sibling one cheap way to report, and gives Max one place to read.

Shape: hub and spoke, not a mesh. Four agents fully meshed is twelve one-way links, each with its own secret and contract. One table gives the same reach with one write per agent. No sibling ever needs another sibling's address.

## 2. Alignment (flag for Ron before any code)

- **Covered by a decision:** `decisions/log.md`, 2026-07-21: "Max is the HUB over Kai and REVI; shared knowledge and chained insight loops."
- **Not covered by the 90-day plan:** `docs/context/ng-agent.md` lists three required changes for Max (tier context, ROAS, hygiene). An agent bus is not one of them. The North Star rule is "if a change doesn't serve this, it waits."
- **So:** this is internal plumbing that Ron has to rule in. If the answer is yes, a line goes in `plan-of-record.md` first. If the answer is "later", this file is the parked design.

## 3. What is true today (verified in ng-agent main)

| Piece | State |
|---|---|
| `POST /agent/consult` (index.js:18422) | Live. Secret in header `x-agent-secret`, 100 KB body cap. Two purposes already: `initiative_check` (REVI's stalled-initiative verdicts) and a generic `question`. Mutating tools and REVI's own data are banned in this mode (`CONSULT_DROP_TOOLS`). |
| REVI watch | `runReviCrossChecks` reads `revi.engine_heartbeat` (id `poller`). |
| AXON watch | `checkAxonHealth`, every 10 min, reads `axon.engine_heartbeat` (id `service`), three failing polls before it alerts, kill switch `AXON_HEALTH_WATCH=off`. |
| Kai watch | `checkKaiHealth` plus a weekday digest, logic in `lib/kaiHealth.js`. Pings Kai's `/health` and reads Kai's queue through the read-only portal connection. Kill switch `KAI_HEALTH_WATCH=off`. |
| Correlation ids | Every cron run and consult call already carries one. |
| Clarity Brief | Ron's weekday 06:50 DM already collects "what needs Ron" from seven sources. It is the natural reader for sibling `needs_human` events. |

## 4. Corrections to the first draft of this idea

1. **Kai is not in Max's database.** Max, REVI and AXON share Supabase project `zbuqpdwjpxgsetduhjeo`. Kai lives in the portal project (`xqzfhofxtmqjozowrwdo`), whose schema dash owns. Kai cannot insert into a Max table without new credentials. That makes Kai the last phase, not part of the first PR.
2. **Not every agent has a service-role key.** Max and REVI run on the anon key. Only AXON holds a service-role key. The table has to work with anon insert under RLS, the same pattern as `max_backlog`.
3. **Porting the AXON heartbeat first is the wrong proof.** That watch works. Replacing it changes nothing Ron can see and puts a working alarm at risk. The first proof should be an event Max cannot see today.
4. **Unverified claim dropped.** The draft said the AXON check "silently stopped when it was tied to the Make token". Nothing in the code or the wiki supports that. The code comment says the watch was added after the 2026-09-08 crash loop.
5. **`/agent/ask` mostly exists.** The `question` purpose on `/agent/consult` is already the generic ask into Max. No rename needed.

## 5. Design

### 5.1 One table, in Max's project

`public.agent_events`, migration `026_agent_events.sql`, applied before the code that writes to it merges.

| Column | Notes |
|---|---|
| `id` bigserial, `created_at` timestamptz | |
| `source` text | `max`, `revi`, `axon`, `kai` (check constraint) |
| `kind` text | `needs_human`, `error`, `job_done`, `handoff` (check constraint) |
| `severity` text | `info`, `warn`, `red` |
| `audience` text | `ron`, `management`, `team`. Sensitivity of the source can never exceed the surface it lands on (confidentiality rule). Defaults to `ron`. |
| `subject_key` text | What it is about, as a stable key: `client:<portal id>`, `call:<recording id>`, `run:<id>`. Lets Max collapse repeats. |
| `summary` text, 300 chars max | One plain sentence a human can read as is. |
| `payload` jsonb, 4 KB max (check constraint) | Ids and counts only. Never a transcript, never a draft, never lead or client personal data. |
| `correlation_id` text | Carried from the job that produced it. |
| `idempotency_key` text, unique with `source` | A retry never creates a second event. |
| `consumed_at`, `acked_at`, `acked_by` | Set by Max only. |

Access: RLS on, anon select and insert, update for Max's bookkeeping columns, no DELETE grant, explicit `GRANT ... TO anon, authenticated` plus sequence USAGE (this project revokes default grants). Honest limit: Max and REVI share one anon key, so `source` is self-declared. That is acceptable for an internal table that only produces notifications, and it is the reason section 6 bans events from triggering writes.

### 5.2 Heartbeats stay where they are

`revi.engine_heartbeat` and `axon.engine_heartbeat` already work and already page. A heartbeat every five minutes as an event row is about 290 rows a day per agent for no new information. The bus carries things that happened, not "still alive".

### 5.3 Max reads by polling, not by webhook

One cron, every minute, through `lib/cronSchedule.js`: read up to 50 rows where `consumed_at is null`, oldest first, route each, stamp `consumed_at`. A database webhook would lose every event fired while Max is mid-deploy. A poll loses nothing and needs no new production configuration. One minute of delay does not matter for any event listed here.

### 5.4 Routing is plain code, zero tokens

New `lib/agentEvents.js`, pure and tested, same split as `lib/kaiHealth.js` (logic in lib, only I/O in `index.js`).

| Kind | What Max does |
|---|---|
| `needs_human` | Becomes a Clarity Brief candidate (new collector). `red` also DMs Ron at once. |
| `error` | Posts to `#ng-pm-agent`, collapsed by `source` + `subject_key`, at most one post per key per 6 h. |
| `job_done` | No post. Stored so reports and the cost report can read it. |
| `handoff` | Runs a named handler from an allowlist in code. Unknown names are logged and dropped. |

No LLM call anywhere on this path. Events are rows. The only thing that spends tokens stays `/agent/consult`, on demand.

### 5.5 The emitter

One small module per repo (about 40 lines): `emit({ kind, severity, subjectKey, summary, payload, correlationId, idempotencyKey })`. It never throws and never blocks the agent's real work. If the insert fails it logs one line and moves on. Off switch per sibling: `AGENT_EVENTS_EMIT=off`.

The four copies are a duplicated contract. It gets a row in the wiki's `rules/data-ownership.md` list so a column change is made in all four the same day.

### 5.6 Agent list

A constant in `lib/agentEvents.js` (name, repo, health URL, heartbeat source). Four rows do not need a table. If a fifth agent appears, revisit.

### 5.7 Max asking a sibling

Not in v1. Kai already has `/internal/*` endpoints behind `x-internal-key`. REVI and AXON are Socket Mode workers and would need an HTTP server added. Build an ask endpoint in a sibling only when there is a specific question Max needs answered that it cannot read from the sibling's tables.

## 6. Guardrails

- **Sibling output is data, never an instruction.** No event can reach a mutating tool. `handoff` handlers are an allowlist in code, and none of them writes outside Max's own tables in v1.
- **No transcripts, no financials, no personal data in events.** 4 KB cap enforced by the database. REVI teardowns and initiative data stay Ron-only whatever channel carries them.
- **Kill switches.** Max: `AGENT_EVENTS_MODE=off | shadow | live`. Shadow consumes and writes one daily DM to Ron listing what it would have done. Siblings: `AGENT_EVENTS_EMIT=off`.
- **Noise budget.** Ron's rule from the report grooming work applies: name the delta, not the roster. A sibling that emits more than 20 non-`job_done` events in a day gets one summary line, not 20 posts.
- **Public repo.** ng-agent is public. Tests use synthetic events only.

## 7. Phases

| Phase | Repo | What ships | Proof |
|---|---|---|---|
| 0 | none | Ron rules on section 2 and section 9 | Decision recorded |
| 1 | ng-agent + ng-axon | Migration 026, Max poller and router in `shadow`, AXON emitter with two events (section 9, decision 2) | A real AXON event shows in Ron's shadow DM within two minutes, twice, with no duplicate on retry |
| 2 | ng-agent | Clarity Brief collector for `needs_human`, flip to `live` | The event appears as a brief item with a working ✅ |
| 3 | ng-revi | REVI emitter | Same proof as phase 1 |
| 4 | ng-automation | Kai emitter. Needs Max's project URL and anon key added to Kai's Railway service (new cross-project credential, Ron approves) | Same proof |
| 5 | as needed | Sibling ask endpoints, only for a named question | n/a |

Each phase is its own PR. Phase 1 is roughly one migration, one lib file with tests, about 60 lines of I/O in `index.js`, and the emitter in ng-axon.

## 8. How it can fail quietly, and the check

- Poller stops: covered by the existing cron liveness audit (07:30 daily, Ron DM).
- A sibling stops emitting: in `live`, Max's weekly check counts events per source for the last 7 days and names any source at zero that was above zero the week before.
- Events pile up unconsumed: the poller alerts when the oldest unconsumed row is older than 15 minutes.

## 9. Decisions for Ron

1. **Build now or park?** See section 2. Recommendation: phase 1 only, in shadow, because it is small and reversible. Phases 3 and 4 wait until phase 2 has shown one event that changed what Ron did that morning.
2. **Which two AXON events prove it?** Candidates: a Document Factory run finished or was rejected; a launch QA check failed. Recommendation: launch QA failed (`needs_human`) and factory run failed (`error`), because both are things a person must act on and both sit only in AXON's channel today.
3. **Kai's credential (phase 4).** Give Kai the Max project anon key, or leave Kai on today's pull model for good. Recommendation: decide at phase 4, not now.
4. **Where `error` events post.** Recommendation: `#ng-pm-agent`, same as the Kai watch.
