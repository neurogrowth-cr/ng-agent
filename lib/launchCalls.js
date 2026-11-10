'use strict';
// Launch calls (Ron, 2026-11-09; strategy plan-of-record UPDATE 2026-11-09).
//
// The Fulfilment SOP books two calls on launch day: the official delivery call
// and the 1:1 review, which moved from day 20 to one month after launch. Max
// reads the client's GHL appointments and says in the fulfillment standup which
// of the two is not on the calendar, for every client in stabilization days 0
// to 30. Launch = client_dashboards.stabilization_started_at (the dash stamps
// it when every Phase 2 item is live). Pure: no GHL, no Slack.

const DAY = 86400000;
const REVIEW_DAY = 30;               // the 1:1 review, one month after launch
const REVIEW_DUE_SOON_DAY = 28;      // standup heads-up, two days before
const CHECK_UNTIL_DAY = REVIEW_DAY;  // after the review is due, the overdue flag speaks
// A booking counts when it falls in these windows, in days from launch. The
// delivery call is often held a few days before or after the switch-on.
const DELIVERY_WINDOW = [-14, 21];
const REVIEW_WINDOW = [25, 40];

const DELIVERY_CALENDAR_IDS = ['VTdxE8vEJ26U509wazNL'];   // LinkedIn Flywheel Official Delivery
// The 1:1 calendars live in GHL on 2026-11-09 (Josué's was deleted). Quick
// Sync does not count: it is the ad hoc call, not the review.
const REVIEW_CALENDAR_IDS = [
  'rRh3CZnC4DlF3Fo1HqXq', // LinkedIn Flywheel - Client 1:1 with Ron
  'WQPvlvNFh9Lp9Tb1OVez', // LinkedIn Flywheel - Session 1:1 Gerald
  'usjXU48iZdCDVL5gLR9D', // LinkedIn Flywheel - Session 1:1 Valeria
];
const DEAD_STATUSES = new Set(['cancelled', 'canceled', 'noshow', 'no-show', 'invalid']);

function stabilizationDay(startedAt, now = Date.now()) {
  const t = Date.parse(startedAt);
  return Number.isNaN(t) ? null : Math.floor((now - t) / DAY);
}

// appointments: [{ calendarId, startMs, status }]. Extra calendar ids (env)
// extend either list. Returns null outside days 0 to 30, else
// { day, delivery, review, missing: ['delivery'|'review'] }, where delivery and
// review are the start time (ms) of the booking found or null.
function launchCallsStatus({ startedAt, appointments = [], now = Date.now(), extraDelivery = [], extraReview = [] }) {
  const day = stabilizationDay(startedAt, now);
  if (day == null || day < 0 || day > CHECK_UNTIL_DAY) return null;
  const launch = Date.parse(startedAt);
  const live = (appointments || []).filter(a => a && a.startMs && !DEAD_STATUSES.has(String(a.status || '').toLowerCase()));
  const find = (ids, [from, to]) => {
    const set = new Set(ids);
    const hit = live.filter(a => set.has(a.calendarId) && a.startMs >= launch + from * DAY && a.startMs <= launch + to * DAY)
      .sort((a, b) => a.startMs - b.startMs)[0];
    return hit ? hit.startMs : null;
  };
  const delivery = find([...DELIVERY_CALENDAR_IDS, ...extraDelivery], DELIVERY_WINDOW);
  const review = find([...REVIEW_CALENDAR_IDS, ...extraReview], REVIEW_WINDOW);
  const missing = [];
  if (delivery == null) missing.push('delivery');
  if (review == null) missing.push('review');
  return { day, delivery, review, missing };
}

const MISSING_LABEL = { delivery: 'official delivery call', review: '1:1 review at one month' };

// The standup suffix for one client. Unread appointments say so, never "booked".
function launchCallsFlag(status, { readFailed = false, noContact = false } = {}) {
  if (readFailed) return ' ⚠️ launch calls: GHL read failed';
  if (noContact) return ' ⚠️ launch calls: no GHL contact for the client email';
  if (!status || !status.missing.length) return '';
  return ` 📞 not booked: ${status.missing.map(m => MISSING_LABEL[m]).join(', ')}`;
}

// The 1:1 reminder on the stabilization line, moved with the call to day 30.
function reviewFlag(day) {
  if (day == null) return '';
  if (day >= REVIEW_DAY) return ' 🔴 1:1 review overdue, schedule now';
  if (day >= REVIEW_DUE_SOON_DAY) return ' 📅 1:1 review due in ~2 days';
  return '';
}

module.exports = {
  REVIEW_DAY, REVIEW_DUE_SOON_DAY, CHECK_UNTIL_DAY, DELIVERY_WINDOW, REVIEW_WINDOW,
  DELIVERY_CALENDAR_IDS, REVIEW_CALENDAR_IDS,
  stabilizationDay, launchCallsStatus, launchCallsFlag, reviewFlag,
};
