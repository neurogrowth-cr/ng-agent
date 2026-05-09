# Lessons

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
