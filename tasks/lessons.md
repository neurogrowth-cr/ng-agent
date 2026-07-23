# Lessons

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
