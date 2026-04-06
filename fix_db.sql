-- ─────────────────────────────────────────────
-- إصلاحات قاعدة البيانات — شغّل في Supabase SQL Editor
-- ─────────────────────────────────────────────

-- 1. أضف عمود url_hash لمنع التكرار
ALTER TABLE articles 
ADD COLUMN IF NOT EXISTS url_hash text;

-- 2. أضف index للسرعة
CREATE INDEX IF NOT EXISTS idx_articles_url_hash ON articles(url_hash);
CREATE INDEX IF NOT EXISTS idx_articles_source_id ON articles(source_id);
CREATE INDEX IF NOT EXISTS idx_articles_published ON articles(published_at DESC);
CREATE INDEX IF NOT EXISTS idx_articles_importance ON articles(importance_score DESC);

-- 3. أضف url_hash للمقالات القديمة (بناءً على الرابط)
UPDATE articles 
SET url_hash = md5(original_url || ':' || LEFT(COALESCE(title_original, title_ar, ''), 60))
WHERE url_hash IS NULL AND original_url IS NOT NULL;

-- 4. تحقق من النتيجة
SELECT 
  COUNT(*) as total,
  COUNT(url_hash) as with_hash,
  COUNT(*) - COUNT(url_hash) as without_hash
FROM articles;
