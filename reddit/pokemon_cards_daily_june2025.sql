-- Daily count of submissions and comments mentioning "Pokémon card(s)" or "Pokemon card(s)" in June 2025

WITH pokemon_submissions AS (
  SELECT 
    DATE(created_utc) as date,
    COUNT(*) as submission_count
  FROM `handy-implement-454013-q2.reddit.submissions`
  WHERE DATE(created_utc) BETWEEN '2025-06-01' AND '2025-06-30'
    AND (
      REGEXP_CONTAINS(LOWER(title), r'pok[eé]mon card[s]?') OR
      REGEXP_CONTAINS(LOWER(selftext), r'pok[eé]mon card[s]?')
    )
  GROUP BY date
),
pokemon_comments AS (
  SELECT 
    DATE(created_utc) as date,
    COUNT(*) as comment_count
  FROM `handy-implement-454013-q2.reddit.comments`
  WHERE DATE(created_utc) BETWEEN '2025-06-01' AND '2025-06-30'
    AND REGEXP_CONTAINS(LOWER(body), r'pok[eé]mon card[s]?')
  GROUP BY date
)
SELECT 
  COALESCE(s.date, c.date) as date,
  COALESCE(s.submission_count, 0) as submission_count,
  COALESCE(c.comment_count, 0) as comment_count,
  COALESCE(s.submission_count, 0) + COALESCE(c.comment_count, 0) as total_count
FROM pokemon_submissions s
FULL OUTER JOIN pokemon_comments c ON s.date = c.date
ORDER BY date;