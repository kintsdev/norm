ALTER TABLE posts
  ADD COLUMN IF NOT EXISTS title TEXT NOT NULL DEFAULT 'untitled';

-- add constraint to ensure non-empty title
ALTER TABLE posts ADD CONSTRAINT chk_posts_title_nonempty CHECK (char_length(title) > 0);


