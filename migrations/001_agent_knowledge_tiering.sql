-- Adds per-user scoping to agent_knowledge.
-- Run once against the primary Supabase project (SUPABASE_URL).

ALTER TABLE agent_knowledge
  ADD COLUMN IF NOT EXISTS user_id text,
  ADD COLUMN IF NOT EXISTS visibility text NOT NULL DEFAULT 'shared'
    CHECK (visibility IN ('shared', 'private'));

UPDATE agent_knowledge
  SET user_id = 'U05HXGX18H3'
  WHERE user_id IS NULL;

CREATE INDEX IF NOT EXISTS idx_agent_knowledge_visibility_user
  ON agent_knowledge (visibility, user_id);
