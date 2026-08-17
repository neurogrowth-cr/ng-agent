# ng-agent — Session Rules

Multiple Claude Code sessions run against this repo in parallel. Three confirmed
incidents of one session clobbering another's work (see the 2026-08-12 entry in
[tasks/lessons.md](tasks/lessons.md)). These rules are hard requirements, not
suggestions.

## Git rules (non-negotiable)

1. **Never push to `main`.** Every change lands via PR. `main` auto-deploys to
   Railway (service ng-pm-MAX) — an unreviewed push is a production deploy.
2. **Work in an isolated worktree, never the shared checkout.** Before editing:
   `git fetch origin && git worktree add <scratchpad>/wt-<task> -b <branch> origin/main`.
   The shared checkout can change under you mid-session (another session's
   stash/checkout) — that's how a stale `index.js` reverted a merged PR on 2026-05-26.
3. **Rebase on `origin/main` immediately before opening a PR.** Stale branches
   merge stale code.
4. **Never hand-copy code from another session's branch.** Merge or cherry-pick
   the actual commit — hand-copying forked code and docs on 2026-08-11 (`f8931ac`).
5. **Delete your branch after merge. Never reuse branch names.**
6. **Check `gh pr list` before opening a PR** — another session may already have
   one open for the same work.

## Docs rules

- `tasks/project-state.md` and `tasks/lessons.md` are **append-at-bottom**
  (newest LAST). Never insert at the top, never rewrite an existing entry —
  they carry `merge=union`, so parallel appends merge cleanly but edits to
  existing lines can silently duplicate.
- A `docs/*` branch must not touch `index.js` (CI enforces this).
- Entries older than ~60 days move to `tasks/archive/`.

## Before marking done

- `node --check index.js` plus every test file in `test/` (plain `node`,
  no install needed).
- A PR that deletes >50 lines from `index.js` needs the `major-change` label —
  CI fails it otherwise (stale-copy revert detector).
