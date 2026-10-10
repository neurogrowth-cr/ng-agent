'use strict';
// Aligned text tables for Slack posts (Ron, 2026-10-10: the reels reports must
// scan like a table). Slack mrkdwn has no tables, so a table is a code block
// with padded columns. Links do not render inside code blocks; callers put any
// links on a line after the table. No emoji inside cells: they are two columns
// wide on screen and break the alignment. Pure, tested in test/slack-table.test.js.

const width = (s) => [...String(s)].length;

/** Cuts `s` to `max` characters with an ellipsis; strips backticks so a cell can never close the code block. */
function cell(s, max) {
  const t = String(s == null ? '' : s).replace(/`/g, "'").replace(/\s+/g, ' ').trim();
  return max && width(t) > max ? `${[...t].slice(0, max - 1).join('').trimEnd()}…` : t;
}

/**
 * headers: ['Día', 'Reel', 'Alcance'] ; rows: [['Mar', 'Hook', '4,764'], ...]
 * align: per column 'l' or 'r' (default: first two columns left, the rest right).
 * headers may be null for a table without a header row.
 */
function table(headers, rows, align = []) {
  const all = headers ? [headers, ...rows] : rows;
  if (!all.length) return '';
  const cols = Math.max(...all.map((r) => r.length));
  const w = Array.from({ length: cols }, (_, c) => Math.max(...all.map((r) => width(r[c] == null ? '' : r[c]))));
  const side = (c) => align[c] || (c < 2 ? 'l' : 'r');
  const line = (r) => r.map((v, c) => {
    const s = String(v == null ? '' : v);
    const pad = ' '.repeat(w[c] - width(s));
    return side(c) === 'r' ? pad + s : s + pad;
  }).join('  ').trimEnd();
  return ['```', ...all.map(line), '```'].join('\n');
}

module.exports = { table, cell, width };
