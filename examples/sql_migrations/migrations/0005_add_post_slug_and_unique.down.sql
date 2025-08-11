DROP INDEX IF EXISTS idx_posts_user_slug;
ALTER TABLE posts
  DROP COLUMN IF EXISTS slug;


