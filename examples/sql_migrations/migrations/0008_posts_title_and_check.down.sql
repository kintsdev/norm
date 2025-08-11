ALTER TABLE posts
  DROP CONSTRAINT IF EXISTS chk_posts_title_nonempty;

ALTER TABLE posts
  DROP COLUMN IF EXISTS title;


