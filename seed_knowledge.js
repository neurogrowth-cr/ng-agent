require('dotenv').config();
const { createClient } = require('@supabase/supabase-js');

const supabase = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_ANON_KEY);

async function upsert(category, key, value) {
  const { error } = await supabase
    .from('agent_knowledge')
    .upsert(
      { category, key, value, source: 'seed', updated_at: new Date().toISOString() },
      { onConflict: 'category,key' }
    );
  if (error) console.error(`Error saving ${key}:`, error.message);
  else console.log(`Saved [${category}] ${key}`);
}

async function seed() {
  console.log('Seeding team knowledge base...\n');

  // ── RON ──
  await upsert('team', 'Ron Duarte', 'CEO and Founder of NeuroGrowth. All strategic decisions, client acquisitions, partnership closings, and major pivots run through Ron. Target: reduce operational time to <20% so he can focus on strategy and growth. Currently at ~$27K MRR (March 2026), target $35K+ Q2 2026. Do not escalate to Ron unless strategic input is genuinely required.');

  // ── TANIA ──
  await upsert('team', 'Tania', 'Client Success Operations Manager. 3 pillars: Executive Ops (30%) — contracts, invoices, OKR tracking, weekly ops summary for Ron. Client Success (50%) — primary contact for all non-strategic client comms, bi-weekly check-ins, health scores (target >80/100), 100% retention KPI, case studies, expansion opportunities. Project Coordination (20%) — cross-team comms, action item tracking. Responds to client inquiries within 2 hours. After a client closes and pays, Tania owns the relationship; Josue owns technical campaign performance. Decision authority: acts independently on routine comms, contracts, scheduling.');

  // ── JOSUE ──
  await upsert('team', 'Josue Duran', 'Technical Operations Manager. Moved to full-time fulfillment March 4, 2026. Single point of accountability for technical campaign excellence. 60% Build & Release: owns 14-day launch cycle — Phase 1 (Days 1-3) activation/onboarding, Phase 2 (Days 4-10) fulfillment coordination, Phase 3 (Days 11-13) technical QA, Phase 4 (Day 14) launch and handoff. 40% DFY: Monday 9AM 60-min campaign fix session, Friday portfolio deep dive (GREEN/YELLOW/RED). KPIs: 95%+ on-time launch rate, 90%+ SLA compliance, CEO ops time <5hrs/week. Escalate blocked clients or SLA risks to Ron immediately.');

  // ── DAVID ──
  await upsert('team', 'David McKinney', 'Lead Technology and Automation specialist. Builds and maintains Make.com scenarios, Supabase infrastructure, and the Neurogrowth Portal (neurogrowth.io/admin). On-demand resource — not daily ops. Owns Phase 3 Stabilization in client onboarding checklist. Coordinate with David for: Make.com scenario errors, portal bugs, Supabase schema changes, new automation builds. Active issue as of March 2026: Make.com scenario errors on Setter/Closer/Onboarding notifications.');

  // ── VALERIA ──
  await upsert('team', 'Valeria', 'Fulfillment Operations — creates client delivery documents using the LinkedIn Flywheel Delivery System. Runs Project 1 (Profile Optimization + Client Intelligence): takes onboarding form + activation call + LinkedIn PDF, runs quality gates, produces Doc 1 (client via WhatsApp), Doc 2 (fulfillment team), Doc 3 (intelligence bundle for Project 2). Also runs Project 2 (Campaign Factory): produces File 1 (internal campaign bible for Felipe) and File 2 (founder-facing overview). Owns: Voice Profile + Content Calendar, Video General Overview, Voice Profile Prompt in Phase 1 checklist. Manages outbound replies for legacy full-service clients.');

  // ── FELIPE ──
  await upsert('team', 'Felipe', 'Technical Campaign Specialist, part-time 4 hours/day. Executes what Valeria produces. 3 pillars: (1) LinkedIn Profile Optimization using Success GPT — target 48hrs per profile. (2) Campaign building in Prosp.ai using Campaign Factory GPT — sequences A/B/C (5 messages each), connection requests, CTAs, nurture. Target: <4 hrs per campaign. Benchmarks: 15%+ connection rate, 8%+ reply rate, 2%+ meeting rate. (3) Content pipeline: 8-12 posts/month + 2-4 long-form per client. Owns: LinkedIn Profile Optimization, Activation Post, Loom walkthroughs, Sales Navigator setup, Campaign Config in Prosp (Phase 1 and 2 checklist). Escalates blockers to Josue. Full client setup target: <12 hrs.');

  // ── JOSEPH ──
  await upsert('team', 'Joseph', 'Appointment Setter, commission-based. Works B2C LinkedIn outreach pipeline. Qualifies prospects (niche, service, price, ICP), runs setting script, handles objections, books discovery calls. Tags: "Net a Fit" for disqualified, "Send to the Ninjas" for Kevin warm transfers. Day-of-call: sends follow-up 9-10AM, confirms meeting, sends system overview doc before call. Files EOD report daily: calls booked, pipeline status, follow-up actions. Works in Spanish for LATAM market. Calendar link: https://calendly.com/ron-duarte/linkedin-flywheel');

  // ── JOSE ──
  await upsert('team', 'Jose', 'High-Ticket Closer (also known as Jonathan), commission-based. Takes booked calls from Joseph and closes them into paying clients. Builds and manages own sales pipeline. Runs discovery and closing calls, nurtures no-shows and maybes, re-engages cold leads. Collects payments on close. Enters new clients into Neurogrowth portal and GHL. Files EOD report daily: calls taken, deals closed, pipeline updates, follow-ups needed. Coordinates with Tania on client handoff once payment is confirmed.');

  // ── PROCESS KNOWLEDGE ──
  await upsert('process', 'client onboarding phases', 'Phase 1 (onboarding/setup): Slack Welcome Message (contact@), Activation Call (Josue), Voice Profile + Content Calendar (Valeria), LinkedIn Profile Optimization (Felipe), Activation Post Live (Felipe), Loom walkthrough (Felipe), Video General Overview (Valeria), Sales Navigator Coupon (Felipe), Voice Profile Prompt (Valeria), Fase 1 Completion (Josue). Phase 2 (setup/launch): Operating Guide SOP (Josue), Campaign Validation (Valeria), Campaign Config in Prosp AI (Felipe), Webhook + Tag Configuration (Josue), Loom walkthrough Phase 2 (Felipe), 15-day Reminder Config in Slack (Josue), 15-day campaign check-up (contact@), Fase 2 Completion (Josue). Phase 3 (stabilization): Stabilization (David McKinney).');

  await upsert('process', '14-day launch guarantee', 'Every Build & Release client must be fully launched within 14 days of signing. Josue owns this timeline. If any client is at risk of missing the 14-day window, escalate to Ron immediately. Phase 1 (Days 1-3): activation and onboarding. Phase 2 (Days 4-10): fulfillment coordination and campaign build. Phase 3 (Days 11-13): technical QA. Day 14: launch execution and handoff to Tania for relationship, Josue retains technical ownership.');

  await upsert('process', 'DFY campaign monitoring', 'Done-For-You clients: Josue monitors ongoing campaign performance. Monday 9AM = 60-min Campaign Fix session (flag RED and YELLOW campaigns). Friday = portfolio deep dive, update GREEN/YELLOW/RED status. Monthly audit every 30-45 days per client: health check, refresh opportunities, client communication. Benchmarks: 15%+ connection acceptance rate, 8%+ reply rate, 2%+ meeting booking rate. Below benchmark for 2+ weeks = flag and optimize.');

  await upsert('decision', 'MRR target Q2 2026', 'Current MRR as of March 2026: ~$27K (record). Q2 2026 target: $35K+. Core offer: Build & Release (LinkedIn Flywheel, 14-day delivery). Retention tiers: OMEGA (3mo community), ROLEX (6mo DWY), PATEK (6mo DFY).');

  console.log('\nSeed complete.');
}

seed().catch(console.error);
