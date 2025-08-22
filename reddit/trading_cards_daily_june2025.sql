-- Daily count of submissions and comments mentioning "trading card(s)" in June 2025

WITH trading_submissions AS (
  SELECT 
    DATE(created_utc) as date,
    COUNT(*) as submission_count
  FROM `handy-implement-454013-q2.reddit.submissions`
  WHERE DATE(created_utc) BETWEEN '2025-06-01' AND '2025-06-30'
    AND (
      REGEXP_CONTAINS(LOWER(title), r'trading card[s]?') OR
      REGEXP_CONTAINS(LOWER(selftext), r'trading card[s]?')
    )
  GROUP BY date
),
trading_comments AS (
  SELECT 
    DATE(created_utc) as date,
    COUNT(*) as comment_count
  FROM `handy-implement-454013-q2.reddit.comments`
  WHERE DATE(created_utc) BETWEEN '2025-06-01' AND '2025-06-30'
    AND REGEXP_CONTAINS(LOWER(body), r'trading card[s]?')
  GROUP BY date
)
SELECT 
  COALESCE(s.date, c.date) as date,
  COALESCE(s.submission_count, 0) as submission_count,
  COALESCE(c.comment_count, 0) as comment_count,
  COALESCE(s.submission_count, 0) + COALESCE(c.comment_count, 0) as total_count
FROM trading_submissions s
FULL OUTER JOIN trading_comments c ON s.date = c.date
ORDER BY date;