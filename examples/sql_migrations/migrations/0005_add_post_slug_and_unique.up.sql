ALTER TABLE posts
  ADD COLUMN IF NOT EXISTS slug TEXT;

CREATE UNIQUE INDEX IF NOT EXISTS idx_posts_user_slug ON posts(user_id, slug);


