# Recipe: Pipeline guardian, phase A (draft and approve)

**Owner:** Ron   **Status:** draft (spec only, nothing built)
**Written:** 2026-10-24   **Last verified:** n/a
**Decision:** strategy repo `docs/plan-of-record.md`, UPDATE 2026-10-24 "the pipeline guardian replaces the second setter seat". This spec does not change the decision; where it needs a number the decision does not have, it says `[confirm]`.
**Watched by:** `guardian_watchdog` (section 4b) and the hot-reply broken-check pattern (3 failed runs in a row post to #ng-pm-agent). Until that check exists the recipe may run in `shadow` only.

## 1. Job

Read every inbound message and every silence on the Appointment Setting pipeline, draft the next reply or strike in usted, and hand Sebastián a one-tap approval; pass him only the leads that need a human (call request, tangible question, hot verdict, objection outside the playbook). What two setters did by hand between calls.

## 2. Schedule

| Field | Value |
|---|---|
| Scheduler | node-cron (ng-agent), declared in `STATIC_CRON_SCHEDULES` |
| Expression | `*/15 8-20 * * *` (replies and strikes), `0 18 * * 1-6` (digest of what is still waiting) |
| Timezone | America/Costa_Rica |
| Mode | `GUARDIAN_MODE=off` (default) · `shadow` (drafts to Ron's DM only, nothing to Sebastián, nothing to leads) · `draft` (phase A: drafts to Sebastián, sends only on his ✅) · `live` (phase B, not this spec) |
| Per channel | `GUARDIAN_CHANNELS=whatsapp,instagram,facebook,email` (a channel left out is read but never written to) |
| Kill switch | `GUARDIAN_DISABLED=true` stops drafting, sending and assigning in one deploy; GHL tag `guardian-off` on a contact stops it for that lead; the `personal` tag stops it as everywhere else |
| Expiry | none. Retirement is Ron's call, in chat. |

## 3. Skills (steps, in order)

| # | Skill | Implemented at | Reads | Writes |
|---|---|---|---|---|
| 1 | Population: open Appointment Setting cards from New Lead to Strike 3 plus Hot Strike, with their GHL conversation | new `lib/pipelineGuardian.js` `selectPopulation()`; data via `hotSearchConversations` (both directions) and `/opportunities/search` by pipeline `KH1IQuaN8aNB1lfRpvP4` | GHL conversations, opportunities, contact tags, `lead_posts`, `setter_claims`, `ig_guide_drafts` | nothing |
| 2 | Screen: drop clients (`loadActiveClientEmails`), `personal`, `guardian-off`, webinar-tagged contacts with no sales card (`screenConversation` reason `webinar`), lost and no-fit cards, cards in Call Booked or later, leads Ron holds in `ig_guide_drafts` younger than 12h | `screenConversation` in `lib/hotReplies.js` extended with `guardianScreen()` | same | `agent_activity` audit row with skip counts |
| 3 | Thread analysis: who spoke last, unanswered lead messages (anchor), time since our last message, Meta window state per channel | `analyzeThread` in `lib/hotReplies.js` plus new `windowState(convo, now)` | `ghlGetConversationMessages` | `_hotThreadCache` |
| 4 | Classify: intent and trajectory with Haiku, Kai's intent classes (`interested, wants_info, objection_timing, objection_budget, objection_relevance, objection_existing, referral, question, positive_not_ready, unsubscribe, angry, legal_risk, competitor_mention, sensitive, already_client, unclear`) plus the hot/normal/noise verdict already in use | `hotClassify` reused; prompt extended with the intent list from `ng-automation/lib/intent-classifier.js` (copied, not imported) | thread tail (6 messages) | `agent_knowledge` key `guardian:verdict:<newestId>` (verdict only, never the quote) |
| 5 | Decide: reply, strike, signal, or stop (table in 3.1) | `decideAction()` pure | steps 3 and 4 | one `guardian_actions` row (migration 022) status `drafted` or `signal` |
| 6 | Draft: the reply or strike in usted from the Persuasion Playbook and the Triage Call Script v1.0 (chat triage), with the bridge video or guide link when the step calls for it | `buildDraftPrompt()` pure; `MODEL_AGENT` with `THINKING_OFF`; same guard chain as `igGuideReplies` (no em dash, no English, no price outside the catalog, no promise) | step 5, contact facts, FAQ file `~/NG-content/guardian/faq.md` [confirm location] | `guardian_actions.draft` |
| 7 | Hand to Sebastián: one Slack DM per action with the card in 3.2; ✅ sends the draft, ❌ archives, a thread reply sends his text instead | reaction handler pattern from `runIgGuideCheck` (atomic claim on the row, double tap never sends twice) | `guardian_actions` | Slack DM ts on the row; `agent_activity` `guardian_card` |
| 8 | Send on approval through GHL `POST /conversations/messages` on the lead's channel, only if the Meta window is still open at send time; otherwise the card flips to "ventana cerrada" and the strike moves to email or waits | the direct send helper used by the open-deal ✅ (`index.js`, "Direct send helper used by the reaction handler") | row, window state | `guardian_actions.status = sent`, `sent_at`, `message_id` |
| 9 | Own the card: on the first sent message, assign contact and opportunity to GHL user "Max AI" (`IG_GUIDE_MAX_GHL_USER_ID`, `cfLku0XE7BznqtmBSDmX`) unless a human setter already owns it; on a signal handoff, assign to Sebastián (`Wdjte1temxfR0lpi5RGV`) | `igGuideAssignInGhl` generalised | GHL | GHL contact and opportunity `assignedTo`; `agent_activity` `guardian_assigned` |
| 10 | Stage: a sent strike counts as a touch for `runAutoStrikeMover` (today only human outbound touches count; the mover gains a rule: a message whose `userId` is Max AI's counts as a touch) | `runAutoStrikeMover` | `guardian_actions` | GHL stage, `strike_sweep` audit |
| 11 | 18:00 digest to Sebastián: cards still waiting for his ✅, grouped by expiry, and the day's counts | `formatSetterDigest` pattern | `guardian_actions` | Slack DM |

### 3.1 Decision table (step 5)

| Situation | Action | Who sees it |
|---|---|---|
| Lead wrote, unanswered, intent `interested`, `wants_info`, `question` inside the FAQ, `referral`, `positive_not_ready` | draft reply | Sebastián ✅ (phase A) |
| Lead asks for a call, proposes a time, says "llámeme" or shares a phone | **signal**, no draft: handoff card with the ficha fields and the booking link ready | Sebastián, within 15 min |
| Lead asks about price, process or fit outside the FAQ; `objection_*` not covered by the playbook; `competitor_mention` | **signal** with a suggested answer marked "sugerencia" | Sebastián |
| Verdict `hot` from the existing classifier | **signal** (keeps today's hot-reply poke) | Sebastián |
| `unsubscribe`, `angry`, `legal_risk`, `sensitive`, `already_client` | **stop**: no draft, card marked, tag `guardian-off`, DM to Ron for `legal_risk` and `sensitive` | Ron |
| Lead silent N days after our last message (N in 1, 2, 3, 4, 7) | draft strike N (nudge, qualifying question, proof, soft push, resource) on the channel the window allows | Sebastián ✅ |
| Silent after strike 5 | re-nurture at 60 and 75 days, at most 3 touches | Sebastián ✅ |
| `unclear` | draft one clarifying question | Sebastián ✅ |
| Instagram or Messenger lead inside the window with no phone and no email on the contact | the reply always ends by asking for WhatsApp or email (the move-off-channel rule) | Sebastián ✅ |

Channel per step: Instagram and Messenger only while `now < lastInbound + 24h` (an ad-started chat may carry a 7-day window; the guardian treats 24h as the limit until Meta's documentation is confirmed); WhatsApp inside 24h free text, outside 24h only an approved template [confirm what coexistence allows; until then WhatsApp strikes outside the window are not drafted]; email any time when the contact has one. A strike whose only open channel is Instagram and whose window has closed is skipped with reason `window_closed` and counted.

### 3.2 Card contract (step 7), Slack DM to Sebastián

```
🛡️ Guardián · <Nombre> · <WhatsApp|Instagram|Messenger|Email> · <intent in Spanish>
Lead (hace <N> min): "<quote, at most 220 chars>"
Etapa: <GHL stage> · Paso: <respuesta | strike N de 5 | re-nurture N>
Borrador:
<draft, usted, 1 to 3 short paragraphs>
Vence: <HH:MM CR> (ventana de Meta cierra <HH:MM> | sin ventana)
✅ enviar · ❌ descartar · responda en este hilo para enviar su texto
🔗 <GHL contact link>
```

Signal cards replace `Borrador:` with `Sugerencia:` and add `Ficha:` with the fields already known (empresa, a quién vende, dolor, urgencia) and `Agendar:` with the booking link for the setter calendar.

## 4. CRITERIA

### 4a. Structural contract (machine-checkable)

Every card MUST contain, each on its own line and non-empty after the label: `🛡️ Guardián ·`, `Lead (hace`, `Etapa:`, `Borrador:` or `Sugerencia:`, `Vence:`, `🔗 https://app.gohighlevel.com/`.
Every draft MUST: be in Spanish usted (no `tú`, `tienes`, `puedes`, `quieres` as whole words), be at most 3 paragraphs and 700 characters, carry no em dash (U+2014), no `[`, `{{`, `undefined`, `null`, `[object Object]`, no URL other than the approved set (booking link, guide, bridge videos), no digit followed by `$` or `USD` unless the catalog price string matches exactly.
A signal card MUST contain `Agendar:` with the setter calendar link.
Checked by: `lib/pipelineGuardian.js validateCard()` and `validateDraft()`, tested in `test/pipeline-guardian.test.js`. A failed draft is reprompted once; a second failure posts the card with `⚠️ BORRADOR INVÁLIDO` and no draft, so Sebastián still sees the lead.

### 4b. Numeric thresholds (machine-checkable), per day in CR time

| Metric | Green | Yellow | Red | Window | Warmup |
|---|---|---|---|---|---|
| Runs completed | 52 per day (every 15 min, 13 hours) | 48 to 51 | below 48 | 1 day | n ≥ 7 days |
| Drafts per run | ≤ `GUARDIAN_MAX_DRAFTS_PER_RUN` (8) | n/a | cap hit 3 runs in a row | 1 day | n ≥ 7 |
| Cards sent to Sebastián per day | 5 to 40 [confirm after shadow week] | 41 to 60 | 0 on a weekday with inbound messages, or above 60 | 1 day | n ≥ 7 |
| Approval rate (✅ ÷ (✅ + ❌)) | ≥ 70% | 50 to 69% | below 50% | 7 days | n ≥ 7 |
| Median time lead message to card | ≤ 15 min | 16 to 30 | above 30 | 1 day | n ≥ 7 |
| Cards expired unanswered (window closed before ✅) | ≤ 10% | 11 to 25% | above 25% | 7 days | n ≥ 7 |
| Messages sent to leads in `shadow` or `off` | 0 | n/a | any | always | none |
| Messages sent without a `guardian_actions` row status `approved` | 0 | n/a | any | always | none |
| Divergence: approved rows with no `sent_at` after 10 min | 0 | 1 | 2 or more | 1 day | none |

Red posts to #ng-pm-agent with `<@Ron>`; the two zero-tolerance rows post at once. Checked by `guardian_watchdog`, hourly, reading `guardian_actions` and `agent_activity` (divergence check shaped like `checkBookingAlertDivergence`).

### 4c. Rubric (LLM-judged, weekly sample)

Every Friday, 20 random sent drafts from the week are scored by Sonnet against `tasks/specs/guardian-rubric-v1.json` [to write with the first drafts]: voice (usted, warm, no agency jargon) 30, playbook fidelity (the step says what the playbook says for that intent) 30, correctness (no invented fact, no price outside the catalog) 25, brevity 15. Scale yes 1.0 / partial 0.5 / no 0.0, weights sum 100, emits `overall_score` 0 to 100, every judgment quotes the line. Pass: `overall_score ≥ 80` and no correctness item at 0. Three weeks under 80 freeze phase B.

### 4d. Human gate

Phase A: **every message to a lead needs Sebastián's ✅** (Ron's for the first week [confirm]). Default if nobody answers before the Meta window closes: **do nothing**, mark `expired`, move the strike to email if an email exists.
Never auto, in any phase: a price other than the catalog string, a discount, a promise of results, a reply to a contact tagged `personal` or `guardian-off`, a reply to a client email, a message outside Meta's window, a second message while ours is the newest in the thread (live thread check, as Kai), anything to a lead whose card is Call Booked or later.

## 5. On failure (fail closed)

- GHL read fails: the run ends, counts the failure, drafts nothing. 3 in a row: `⚠️ CHECK BROKEN` to #ng-pm-agent with `<@Ron>`, as the hot-reply check does.
- Classifier or drafter fails: reprompt once (backoff 15s); second failure posts the card without a draft (4a).
- Send fails (GHL 4xx or 5xx): retry once after 30s; then the row goes `send_failed`, the card gets a ❌ note, the lead stays assigned to whoever owned it, and the 18:00 digest lists it.
- Slack DM fails: the row stays `drafted`; the next run retries the DM; nothing is sent to the lead.
- Never: send partial output, send in `shadow`, post to #ng-sales-goats, retry a send that returned 2xx.

## 6. Proof it works

- [ ] `test/pipeline-guardian.test.js`: decision table (every row of 3.1), window rules per channel (23h59 kept, 24h01 skipped, ad window ignored), stop rules, cadence days from a real-shaped thread, `validateCard` and `validateDraft` red on a placeholder, a `tú`, an em dash, a foreign URL, a price.
- [ ] Golden draft test (skips cleanly without an API key): a fixture thread per intent, asserts the 4a contract on the model output. Fixtures use made-up names and numbers; the repo is public.
- [ ] 3 days in `shadow` to Ron's DM (target around 2026-11-03 [confirm]), then `draft` to Sebastián.
- [ ] The 4b check made to fail on purpose once (a planted approved row with no `sent_at`), and the failure path fired.
- [ ] One deliberately expired card: confirmed nothing was sent after the window.

## 7. Verdict line (exact strings, 18:00 digest and watchdog)

✅ ALL GREEN — guardián: <n> leads leídos, <n> borradores, <n> señales a Sebastián, <n> enviados, <n> vencidos, mediana <m> min, aprobación <p>%
⚠️ ISSUES — <qué falló · qué se corrigió solo · qué necesita a Ron>

## 8. Data

Migration `022_guardian_actions.sql` (apply before merge, as 016 and 017 were): `guardian_actions(id uuid, contact_id text, conversation_id text, opportunity_id text, channel text, step text, intent text, verdict text, draft text, status text check in (drafted, signal, approved, sent, rejected, expired, send_failed, skipped), window_closes_at timestamptz, slack_channel_id text, slack_message_ts text, approved_by text, sent_at timestamptz, message_id text, correlation_id text, created_at timestamptz default now(), updated_at timestamptz)`, unique on `(conversation_id, step, created_at::date)` so one strike per step per day, RLS on, insert and select for the service role, update only through an RPC `guardian_claim_send(id, actor)` that flips `approved → sent` first-writer-wins (the `ig_guide_drafts` pattern). Rows carry no message body other than the draft; the lead's quote stays out of the table (the Slack card shows it).

## 9. Cost (estimate, [confirm after the shadow week])

Population today is about 86 leads a week (`leads_total_7d` on 2026-10-23) plus the open backlog. Per lead and month: about 6 classifications on Haiku and 4 to 6 drafts on the agent model, roughly $0.10 to $0.20. At 350 to 400 leads a month: $40 to $80 a month in model cost, logged as `call_site = guardian_*` so the ICM cost report shows it on its own line.

## 10. Out of scope for phase A

Autonomous sends (phase B), slot proposals and booking (phase C), the Instagram guide flow (stays Ron-approved under `runIgGuideCheck`), VSL self-serve cards, Call Booked and later stages (open-deal follow-up owns them), any change to Kai in ng-automation.
