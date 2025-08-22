-- Daily count of submissions and comments mentioning Pokémon cards, trading cards, eBay, and combinations (June 2025 only)

WITH pokemon_card_submissions AS (
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
pokemon_card_comments AS (
  SELECT 
    DATE(created_utc) as date,
    COUNT(*) as comment_count
  FROM `handy-implement-454013-q2.reddit.comments`
  WHERE DATE(created_utc) BETWEEN '2025-06-01' AND '2025-06-30'
    AND REGEXP_CONTAINS(LOWER(body), r'pok[eé]mon card[s]?')
  GROUP BY date
),
trading_card_submissions AS (
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
trading_card_comments AS (
  SELECT 
    DATE(created_utc) as date,
    COUNT(*) as comment_count
  FROM `handy-implement-454013-q2.reddit.comments`
  WHERE DATE(created_utc) BETWEEN '2025-06-01' AND '2025-06-30'
    AND REGEXP_CONTAINS(LOWER(body), r'trading card[s]?')
  GROUP BY date
),
all_cards_submissions AS (
  SELECT 
    DATE(created_utc) as date,
    COUNT(*) as submission_count
  FROM `handy-implement-454013-q2.reddit.submissions`
  WHERE DATE(created_utc) BETWEEN '2025-06-01' AND '2025-06-30'
    AND (
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
  WHERE DATE(created_utc) BETWEEN '2025-06-01' AND '2025-06-30'
    AND (
      REGEXP_CONTAINS(LOWER(body), r'pok[eé]mon card[s]?') OR
      REGEXP_CONTAINS(LOWER(body), r'trading card[s]?')
    )
  GROUP BY date
),
ebay_submissions AS (
  SELECT 
    DATE(created_utc) as date,
    COUNT(*) as submission_count
  FROM `handy-implement-454013-q2.reddit.submissions`
  WHERE DATE(created_utc) BETWEEN '2025-06-01' AND '2025-06-30'
    AND (
      REGEXP_CONTAINS(LOWER(title), r'ebay') OR
      REGEXP_CONTAINS(LOWER(selftext), r'ebay')
    )
  GROUP BY date
),
ebay_comments AS (
  SELECT 
    DATE(created_utc) as date,
    COUNT(*) as comment_count
  FROM `handy-implement-454013-q2.reddit.comments`
  WHERE DATE(created_utc) BETWEEN '2025-06-01' AND '2025-06-30'
    AND REGEXP_CONTAINS(LOWER(body), r'ebay')
  GROUP BY date
),
ebay_trading_submissions AS (
  SELECT 
    DATE(created_utc) as date,
    COUNT(*) as submission_count
  FROM `handy-implement-454013-q2.reddit.submissions`
  WHERE DATE(created_utc) BETWEEN '2025-06-01' AND '2025-06-30'
    AND (
      (REGEXP_CONTAINS(LOWER(title), r'ebay') AND REGEXP_CONTAINS(LOWER(title), r'trading card[s]?')) OR
      (REGEXP_CONTAINS(LOWER(selftext), r'ebay') AND REGEXP_CONTAINS(LOWER(selftext), r'trading card[s]?')) OR
      (REGEXP_CONTAINS(LOWER(title), r'ebay') AND REGEXP_CONTAINS(LOWER(selftext), r'trading card[s]?')) OR
      (REGEXP_CONTAINS(LOWER(selftext), r'ebay') AND REGEXP_CONTAINS(LOWER(title), r'trading card[s]?'))
    )
  GROUP BY date
),
ebay_trading_comments AS (
  SELECT 
    DATE(created_utc) as date,
    COUNT(*) as comment_count
  FROM `handy-implement-454013-q2.reddit.comments`
  WHERE DATE(created_utc) BETWEEN '2025-06-01' AND '2025-06-30'
    AND REGEXP_CONTAINS(LOWER(body), r'ebay') 
    AND REGEXP_CONTAINS(LOWER(body), r'trading card[s]?')
  GROUP BY date
),
ebay_pokemon_submissions AS (
  SELECT 
    DATE(created_utc) as date,
    COUNT(*) as submission_count
  FROM `handy-implement-454013-q2.reddit.submissions`
  WHERE DATE(created_utc) BETWEEN '2025-06-01' AND '2025-06-30'
    AND (
      (REGEXP_CONTAINS(LOWER(title), r'ebay') AND REGEXP_CONTAINS(LOWER(title), r'pok[eé]mon card[s]?')) OR
      (REGEXP_CONTAINS(LOWER(selftext), r'ebay') AND REGEXP_CONTAINS(LOWER(selftext), r'pok[eé]mon card[s]?')) OR
      (REGEXP_CONTAINS(LOWER(title), r'ebay') AND REGEXP_CONTAINS(LOWER(selftext), r'pok[eé]mon card[s]?')) OR
      (REGEXP_CONTAINS(LOWER(selftext), r'ebay') AND REGEXP_CONTAINS(LOWER(title), r'pok[eé]mon card[s]?'))
    )
  GROUP BY date
),
ebay_pokemon_comments AS (
  SELECT 
    DATE(created_utc) as date,
    COUNT(*) as comment_count
  FROM `handy-implement-454013-q2.reddit.comments`
  WHERE DATE(created_utc) BETWEEN '2025-06-01' AND '2025-06-30'
    AND REGEXP_CONTAINS(LOWER(body), r'ebay') 
    AND REGEXP_CONTAINS(LOWER(body), r'pok[eé]mon card[s]?')
  GROUP BY date
),
ebay_all_cards_submissions AS (
  SELECT 
    DATE(created_utc) as date,
    COUNT(*) as submission_count
  FROM `handy-implement-454013-q2.reddit.submissions`
  WHERE DATE(created_utc) BETWEEN '2025-06-01' AND '2025-06-30'
    AND (
      (REGEXP_CONTAINS(LOWER(title), r'ebay') AND (REGEXP_CONTAINS(LOWER(title), r'pok[eé]mon card[s]?') OR REGEXP_CONTAINS(LOWER(title), r'trading card[s]?'))) OR
      (REGEXP_CONTAINS(LOWER(selftext), r'ebay') AND (REGEXP_CONTAINS(LOWER(selftext), r'pok[eé]mon card[s]?') OR REGEXP_CONTAINS(LOWER(selftext), r'trading card[s]?'))) OR
      (REGEXP_CONTAINS(LOWER(title), r'ebay') AND (REGEXP_CONTAINS(LOWER(selftext), r'pok[eé]mon card[s]?') OR REGEXP_CONTAINS(LOWER(selftext), r'trading card[s]?'))) OR
      (REGEXP_CONTAINS(LOWER(selftext), r'ebay') AND (REGEXP_CONTAINS(LOWER(title), r'pok[eé]mon card[s]?') OR REGEXP_CONTAINS(LOWER(title), r'trading card[s]?')))
    )
  GROUP BY date
),
ebay_all_cards_comments AS (
  SELECT 
    DATE(created_utc) as date,
    COUNT(*) as comment_count
  FROM `handy-implement-454013-q2.reddit.comments`
  WHERE DATE(created_utc) BETWEEN '2025-06-01' AND '2025-06-30'
    AND REGEXP_CONTAINS(LOWER(body), r'ebay') 
    AND (REGEXP_CONTAINS(LOWER(body), r'pok[eé]mon card[s]?') OR REGEXP_CONTAINS(LOWER(body), r'trading card[s]?'))
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
  UNION DISTINCT
  SELECT DISTINCT date FROM ebay_submissions
  UNION DISTINCT
  SELECT DISTINCT date FROM ebay_comments
  UNION DISTINCT
  SELECT DISTINCT date FROM ebay_trading_submissions
  UNION DISTINCT
  SELECT DISTINCT date FROM ebay_trading_comments
  UNION DISTINCT
  SELECT DISTINCT date FROM ebay_pokemon_submissions
  UNION DISTINCT
  SELECT DISTINCT date FROM ebay_pokemon_comments
  UNION DISTINCT
  SELECT DISTINCT date FROM ebay_all_cards_submissions
  UNION DISTINCT
  SELECT DISTINCT date FROM ebay_all_cards_comments
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
),
ebay_totals AS (
  SELECT 
    d.date,
    COALESCE(es.submission_count, 0) + COALESCE(ec.comment_count, 0) as ebay_total_count
  FROM all_dates d
  LEFT JOIN ebay_submissions es ON d.date = es.date
  LEFT JOIN ebay_comments ec ON d.date = ec.date
),
ebay_trading_totals AS (
  SELECT 
    d.date,
    COALESCE(ets.submission_count, 0) + COALESCE(etc.comment_count, 0) as ebay_trading_total_count
  FROM all_dates d
  LEFT JOIN ebay_trading_submissions ets ON d.date = ets.date
  LEFT JOIN ebay_trading_comments etc ON d.date = etc.date
),
ebay_pokemon_totals AS (
  SELECT 
    d.date,
    COALESCE(eps.submission_count, 0) + COALESCE(epc.comment_count, 0) as ebay_pokemon_total_count
  FROM all_dates d
  LEFT JOIN ebay_pokemon_submissions eps ON d.date = eps.date
  LEFT JOIN ebay_pokemon_comments epc ON d.date = epc.date
),
ebay_all_cards_totals AS (
  SELECT 
    d.date,
    COALESCE(eacs.submission_count, 0) + COALESCE(eacc.comment_count, 0) as ebay_all_cards_total_count
  FROM all_dates d
  LEFT JOIN ebay_all_cards_submissions eacs ON d.date = eacs.date
  LEFT JOIN ebay_all_cards_comments eacc ON d.date = eacc.date
)
SELECT 
  pct.date,
  pct.pokemon_card_total_count,
  tct.trading_card_total_count,
  act.all_cards_total_count,
  et.ebay_total_count,
  ett.ebay_trading_total_count,
  ept.ebay_pokemon_total_count,
  eact.ebay_all_cards_total_count
FROM pokemon_card_totals pct
JOIN trading_card_totals tct ON pct.date = tct.date
JOIN all_cards_totals act ON pct.date = act.date
JOIN ebay_totals et ON pct.date = et.date
JOIN ebay_trading_totals ett ON pct.date = ett.date
JOIN ebay_pokemon_totals ept ON pct.date = ept.date
JOIN ebay_all_cards_totals eact ON pct.date = eact.date
ORDER BY pct.date;