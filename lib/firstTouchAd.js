// First-touch ad: which Meta ad a lead came from, read once at intake and
// stamped on the GHL contact so it can never be overwritten.
//
// Why: GHL keeps two attribution records per contact. `attributionSource` is
// the first touch and carries the ad (adId, utmContent = ad name, utmCampaign,
// utmMedium = ad set). `lastAttributionSource` is the latest session, and that
// is what the contact page shows. On an Instagram DM ad only the first message
// carries the ad referral; the next DM opens an organic "Social media" session
// and the ad vanishes from the UI within minutes (Henry Moya, 2026-10-12, ad
// "Neurogrowth ugly ad Ron's house"). Form leads lose it later, on the dash
// calendar booking. So this reads ONLY `attributionSource`, never the last one.
//
// Pure: no GHL, no Slack. index.js does the I/O.

const AD_FIELD_NAME = 'Anuncio de origen';

function clean(v) {
  return typeof v === 'string' ? v.trim() : '';
}

// null when the first touch was not an ad (organic IG/WhatsApp, direct, etc.).
function firstTouchAd(contact) {
  const a = (contact && contact.attributionSource) || {};
  const adId = clean(String(a.adId || ''));
  if (!adId) return null;
  return {
    adId,
    adName: clean(a.utmContent) || null,
    campaign: clean(a.utmCampaign) || clean(a.campaign) || null,
    adSet: clean(a.utmMedium) || null,
  };
}

// Value written to the GHL field: "ad | campaign | ad set", falling back to
// the ad id when Meta sent no name.
function adFieldValue(ad) {
  if (!ad) return '';
  return [ad.adName || `ad ${ad.adId}`, ad.campaign, ad.adSet].filter(Boolean).join(' | ');
}

// True when the contact already has the field filled, so a backfill never
// overwrites a value.
function hasAdField(contact, fieldId) {
  if (!fieldId) return false;
  const f = ((contact && contact.customFields) || []).find((x) => x.id === fieldId);
  return Boolean(f && clean(String(f.value == null ? '' : f.value)));
}

module.exports = { AD_FIELD_NAME, firstTouchAd, adFieldValue, hasAdField };
