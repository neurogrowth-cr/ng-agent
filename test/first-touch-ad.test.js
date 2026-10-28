// Rules test for the first-touch ad stamp (lib/firstTouchAd.js).
// Run:  node test/first-touch-ad.test.js
//
// Fixture shapes copy real GHL contacts of October 2026 with the ids and
// names replaced: an Instagram DM ad lead whose last attribution was already
// organic, a form lead whose last attribution became a calendar booking, and
// an organic Instagram DM with no ad.
const assert = require('assert');
const fs = require('fs');
const path = require('path');
const lib = require('../lib/firstTouchAd');

let failures = 0;
function check(label, fn) {
  try { fn(); console.log(`ok   ${label}`); }
  catch (e) { failures++; console.error(`FAIL ${label}: ${e.message}`); }
}

const igDmAd = {
  attributionSource: {
    sessionSource: 'Paid Social', medium: 'instagram', adId: '111',
    utmContent: 'Ugly ad house', campaign: 'Boss - IG DM', utmCampaign: 'Boss - IG DM', utmMedium: 'Publico abierto new',
  },
  lastAttributionSource: { sessionSource: 'Social media', medium: 'instagram', adId: null, utmContent: null },
};
const formAd = {
  attributionSource: {
    sessionSource: 'Paid Social', medium: 'facebook', adId: '222',
    utmContent: 'Canada Ad MX', utmCampaign: 'winner ads - form', utmMedium: 'Publico abierto - CDMX',
  },
  lastAttributionSource: { sessionSource: 'Direct traffic', medium: 'calendar', adId: null },
};
const organicIg = {
  attributionSource: { sessionSource: 'Social media', medium: 'instagram', adId: null, utmContent: null },
};
// The last touch has an ad but the first does not: first touch wins, no ad.
const lateAdOnly = {
  attributionSource: { sessionSource: 'Social media', medium: 'whatsapp_coex' },
  lastAttributionSource: { sessionSource: 'Paid Social', medium: 'whatsapp', adId: '333', adName: 'Ron Duarte' },
};

check('IG DM ad reads the first touch even when the last is organic', () => {
  assert.deepStrictEqual(lib.firstTouchAd(igDmAd), { adId: '111', adName: 'Ugly ad house', campaign: 'Boss - IG DM', adSet: 'Publico abierto new' });
});
check('form ad survives a calendar last touch', () => {
  assert.strictEqual(lib.firstTouchAd(formAd).adName, 'Canada Ad MX');
});
check('organic first touch has no ad', () => {
  assert.strictEqual(lib.firstTouchAd(organicIg), null);
});
check('never reads lastAttributionSource', () => {
  assert.strictEqual(lib.firstTouchAd(lateAdOnly), null);
});
check('null contact is safe', () => {
  assert.strictEqual(lib.firstTouchAd(null), null);
  assert.strictEqual(lib.adFieldValue(null), '');
});
check('field value is ad | campaign | ad set', () => {
  assert.strictEqual(lib.adFieldValue(lib.firstTouchAd(igDmAd)), 'Ugly ad house | Boss - IG DM | Publico abierto new');
});
check('falls back to the ad id when Meta sent no name', () => {
  const ad = lib.firstTouchAd({ attributionSource: { adId: '444' } });
  assert.strictEqual(lib.adFieldValue(ad), 'ad 444');
});
check('hasAdField only true for a filled field', () => {
  assert.strictEqual(lib.hasAdField({ customFields: [{ id: 'f1', value: 'x' }] }, 'f1'), true);
  assert.strictEqual(lib.hasAdField({ customFields: [{ id: 'f1', value: '  ' }] }, 'f1'), false);
  assert.strictEqual(lib.hasAdField({ customFields: [] }, 'f1'), false);
  assert.strictEqual(lib.hasAdField({ customFields: [{ id: 'f1', value: 'x' }] }, null), false);
});

// Wiring: the intake handler stamps the ad in GHL and keeps it off the Slack cards.
const SRC = fs.readFileSync(path.join(__dirname, '..', 'index.js'), 'utf8');
check('intake stamps the first-touch ad', () => {
  assert.ok(/firstTouchAdLib\.firstTouchAd\(ghlContact\)/.test(SRC));
  assert.ok(/stampFirstTouchAd\(contactId, leadAd\)/.test(SRC));
});
check('Slack lead cards do not carry the ad', () => {
  assert.ok(!/📣 Anuncio/.test(SRC));
});
check('index.js never reads the ad from lastAttributionSource', () => {
  assert.ok(!/lastAttributionSource\??\.(adId|utmContent)/.test(SRC));
});

if (failures) { console.error(`\n${failures} failing`); process.exit(1); }
console.log('\nall first-touch ad checks passed');
