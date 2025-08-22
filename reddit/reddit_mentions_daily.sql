-- Daily count of submissions and comments mentioning Pokémon cards, trading cards, and combined total (all available data)

WITH pokemon_card_submissions AS (
  SELECT 
    DATE(created_utc) as date,
    COUNT(*) as submission_count
  FROM `handy-implement-454013-q2.reddit.submissions`
  WHERE (
    REGEXP_CONTAINS(LOWER(title), r'pok[eé]mon card[s]?') OR
    REGEXP_CONTAINS(LOWER(selftext), r'pok[eé]mon card[s]?')
  )
  GROUP BY date
),
pokemon_card_comments AS (
  SELECT 
    DATE(created_utc) as date,
    COUNT(*) as comment_count
  FROM `handy-implement-454013-q2.reddit.comments`
  WHERE REGEXP_CONTAINS(LOWER(body), r'pok[eé]mon card[s]?')
  GROUP BY date
),
trading_card_submissions AS (
  SELECT 
    DATE(created_utc) as date,
    COUNT(*) as submission_count
  FROM `handy-implement-454013-q2.reddit.submissions`
  WHERE (
    REGEXP_CONTAINS(LOWER(title), r'trading card[s]?') OR
    REGEXP_CONTAINS(LOWER(selftext), r'trading card[s]?')
  )
  GROUP BY date
),
trading_card_comments AS (
  SELECT 
    DATE(created_utc) as date,
    COUNT(*) as comment_count
  FROM `handy-implement-454013-q2.reddit.comments`
  WHERE REGEXP_CONTAINS(LOWER(body), r'trading card[s]?')
  GROUP BY date
),
all_cards_submissions AS (
  SELECT 
    DATE(created_utc) as date,
    COUNT(*) as submission_count
  FROM `handy-implement-454013-q2.reddit.submissions`
  WHERE (
    REGEXP_CONTAINS(LOWER(title), r'pok[eé]mon card[s]?') OR
    REGEXP_CONTAINS(LOWER(selftext), r'pok[eé]mon card[s]?') OR
    REGEXP_CONTAINS(LOWER(title), r'trading card[s]?') OR
    REGEXP_CONTAINS(LOWER(selftext), r'trading card[s]?')
  )
  GROUP BY date
),
all_cards_comments AS (
  SELECT 
    DATE(created_utc) as date,
    COUNT(*) as comment_count
  FROM `handy-implement-454013-q2.reddit.comments`
  WHERE (
    REGEXP_CONTAINS(LOWER(body), r'pok[eé]mon card[s]?') OR
    REGEXP_CONTAINS(LOWER(body), r'trading card[s]?')
  )
  GROUP BY date
),
all_dates AS (
  SELECT DISTINCT date FROM pokemon_card_submissions
  UNION DISTINCT
  SELECT DISTINCT date FROM pokemon_card_comments
  UNION DISTINCT
  SELECT DISTINCT date FROM trading_card_submissions
  UNION DISTINCT
  SELECT DISTINCT date FROM trading_card_comments
  UNION DISTINCT
  SELECT DISTINCT date FROM all_cards_submissions
  UNION DISTINCT
  SELECT DISTINCT date FROM all_cards_comments
),
pokemon_card_totals AS (
  SELECT 
    d.date,
    COALESCE(ps.submission_count, 0) + COALESCE(pc.comment_count, 0) as pokemon_card_total_count
  FROM all_dates d
  LEFT JOIN pokemon_card_submissions ps ON d.date = ps.date
  LEFT JOIN pokemon_card_comments pc ON d.date = pc.date
),
trading_card_totals AS (
  SELECT 
    d.date,
    COALESCE(ts.submission_count, 0) + COALESCE(tc.comment_count, 0) as trading_card_total_count
  FROM all_dates d
  LEFT JOIN trading_card_submissions ts ON d.date = ts.date
  LEFT JOIN trading_card_comments tc ON d.date = tc.date
),
all_cards_totals AS (
  SELECT 
    d.date,
    COALESCE(acs.submission_count, 0) + COALESCE(acc.comment_count, 0) as all_cards_total_count
  FROM all_dates d
  LEFT JOIN all_cards_submissions acs ON d.date = acs.date
  LEFT JOIN all_cards_comments acc ON d.date = acc.date
)
SELECT 
  pct.date,
  pct.pokemon_card_total_count,
  tct.trading_card_total_count,
  act.all_cards_total_count
FROM pokemon_card_totals pct
JOIN trading_card_totals tct ON pct.date = tct.date
JOIN all_cards_totals act ON pct.date = act.date
ORDER BY pct.date;