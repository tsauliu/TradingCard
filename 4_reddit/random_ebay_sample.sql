-- Query to pull 100 random eBay mentions from Reddit (comments and submissions)
-- This combines both comments and submissions that mention eBay

WITH ebay_comments AS (
  SELECT 
    'comment' as content_type,
    id,
    author,
    created_utc,
    subreddit_name_prefixed,
    body as content,
    score,
    parent_id,
    link_id,
    RAND() as random_order
  FROM `handy-implement-454013-q2.reddit.comments`
  WHERE LOWER(body) LIKE '%ebay%'
    AND body != '[deleted]'
    AND author != '[deleted]'
),

ebay_submissions AS (
  SELECT 
    'submission' as content_type,
    id,
    author,
    created_utc,
    subreddit_name_prefixed,
    CONCAT(title, ' | ', COALESCE(selftext, '')) as content,
    score,
    CAST(NULL AS STRING) as parent_id,
    CAST(NULL AS STRING) as link_id,
    RAND() as random_order
  FROM `handy-implement-454013-q2.reddit.submissions`
  WHERE (LOWER(title) LIKE '%ebay%' OR LOWER(selftext) LIKE '%ebay%')
    AND author != '[deleted]'
    AND (selftext IS NULL OR selftext != '[deleted]')
),

combined_ebay_content AS (
  SELECT * FROM ebay_comments
  UNION ALL
  SELECT * FROM ebay_submissions
)

SELECT 
  content_type,
  id,
  author,
  DATETIME(created_utc) as created_datetime,
  subreddit_name_prefixed,
  content,
  score,
  parent_id,
  link_id
FROM combined_ebay_content
ORDER BY random_order
LIMIT 100;