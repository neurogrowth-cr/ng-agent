/**
 * NeuroGrowth Google OAuth Re-Auth Script
 * Run this locally to regenerate token.json with drive.readonly scope
 *
 * Usage:
 *   node reauth_google.js
 *
 * Then copy the new token.json content to Railway env var GOOGLE_TOKEN
 */

const { google } = require('googleapis');
const http = require('http');
const fs = require('fs');
const url = require('url');

// ── Load credentials ──────────────────────────────────────────────────────────
let credentials;
if (fs.existsSync('./credentials.json')) {
  credentials = JSON.parse(fs.readFileSync('./credentials.json'));
} else {
  console.error('ERROR: credentials.json not found in current directory.');
  console.error('Make sure you run this from your ng-agent folder.');
  process.exit(1);
}

const { client_id, client_secret } = credentials.installed;

// ── Scopes — must include all you need ───────────────────────────────────────
const SCOPES = [
  'https://www.googleapis.com/auth/gmail.readonly',
  'https://www.googleapis.com/auth/gmail.send',
  'https://www.googleapis.com/auth/calendar.events',
  'https://www.googleapis.com/auth/drive.readonly',          // ← NEW: read Drive files
  'https://www.googleapis.com/auth/drive.metadata.readonly', // ← NEW: search Drive
];

const REDIRECT_URI = 'http://localhost:3000/callback';
const oauth2Client = new google.auth.OAuth2(client_id, client_secret, REDIRECT_URI);

// ── Generate auth URL ─────────────────────────────────────────────────────────
const authUrl = oauth2Client.generateAuthUrl({
  access_type: 'offline',
  scope: SCOPES,
  prompt: 'consent', // force consent screen so we get a fresh refresh_token
});

console.log('\n=== NeuroGrowth Google OAuth Re-Auth ===\n');
console.log('Open this URL in your browser:\n');
console.log(authUrl);
console.log('\nWaiting for callback on http://localhost:3000/callback...\n');

// ── Temporary local server to catch the callback ──────────────────────────────
const server = http.createServer(async (req, res) => {
  const parsed = url.parse(req.url, true);
  if (parsed.pathname !== '/callback') return;

  const code = parsed.query.code;
  if (!code) {
    res.writeHead(400);
    res.end('No code found in callback.');
    return;
  }

  try {
    const { tokens } = await oauth2Client.getToken(code);
    oauth2Client.setCredentials(tokens);

    // Save locally
    fs.writeFileSync('./token.json', JSON.stringify(tokens, null, 2));
    console.log('✅ token.json saved successfully!\n');
    console.log('Scopes granted:', tokens.scope);
    console.log('\nNext steps:');
    console.log('1. Copy the content of token.json');
    console.log('2. Go to Railway → ng-agent → Variables → GOOGLE_TOKEN');
    console.log('3. Paste the full JSON as the value (it should be on one line)');
    console.log('\nOne-liner to get it formatted for Railway:');
    console.log('   node -e "console.log(JSON.stringify(require(\'./token.json\')))"\n');

    res.writeHead(200, { 'Content-Type': 'text/html' });
    res.end('<h2>✅ Auth successful!</h2><p>You can close this tab. Check your terminal for next steps.</p>');
  } catch (err) {
    console.error('❌ Error getting token:', err.message);
    res.writeHead(500);
    res.end('Error: ' + err.message);
  } finally {
    server.close();
    process.exit(0);
  }
});

server.listen(3000, () => {
  // Auto-open the URL in the default browser
  const { exec } = require('child_process');
  exec(`open "${authUrl}"`, (err) => {
    if (err) {
      // If auto-open fails, user can manually copy the URL
    }
  });
});
