-- Weekly count of submissions and comments mentioning Pokémon cards, trading cards, eBay, and combinations (from 2010-01-01)

WITH pokemon_card_submissions AS (
  SELECT 
    DATE_TRUNC(DATE(created_utc), WEEK(MONDAY)) as week_start,
    COUNT(*) as submission_count
  FROM `handy-implement-454013-q2.reddit.submissions`
  WHERE DATE(created_utc) >= '2010-01-01'
    AND (
      REGEXP_CONTAINS(LOWER(title), r'pok[eé]mon card[s]?') OR
      REGEXP_CONTAINS(LOWER(selftext), r'pok[eé]mon card[s]?')
    )
  GROUP BY week_start
),
pokemon_card_comments AS (
  SELECT 
    DATE_TRUNC(DATE(created_utc), WEEK(MONDAY)) as week_start,
    COUNT(*) as comment_count
  FROM `handy-implement-454013-q2.reddit.comments`
  WHERE DATE(created_utc) >= '2010-01-01'
    AND REGEXP_CONTAINS(LOWER(body), r'pok[eé]mon card[s]?')
  GROUP BY week_start
),
trading_card_submissions AS (
  SELECT 
    DATE_TRUNC(DATE(created_utc), WEEK(MONDAY)) as week_start,
    COUNT(*) as submission_count
  FROM `handy-implement-454013-q2.reddit.submissions`
  WHERE DATE(created_utc) >= '2010-01-01'
    AND (
      REGEXP_CONTAINS(LOWER(title), r'trading card[s]?') OR
      REGEXP_CONTAINS(LOWER(selftext), r'trading card[s]?')
    )
  GROUP BY week_start
),
trading_card_comments AS (
  SELECT 
    DATE_TRUNC(DATE(created_utc), WEEK(MONDAY)) as week_start,
    COUNT(*) as comment_count
  FROM `handy-implement-454013-q2.reddit.comments`
  WHERE DATE(created_utc) >= '2010-01-01'
    AND REGEXP_CONTAINS(LOWER(body), r'trading card[s]?')
  GROUP BY week_start
),
all_cards_submissions AS (
  SELECT 
    DATE_TRUNC(DATE(created_utc), WEEK(MONDAY)) as week_start,
    COUNT(*) as submission_count
  FROM `handy-implement-454013-q2.reddit.submissions`
  WHERE DATE(created_utc) >= '2010-01-01'
    AND (
      REGEXP_CONTAINS(LOWER(title), r'pok[eé]mon card[s]?') OR
      REGEXP_CONTAINS(LOWER(selftext), r'pok[eé]mon card[s]?') OR
      REGEXP_CONTAINS(LOWER(title), r'trading card[s]?') OR
      REGEXP_CONTAINS(LOWER(selftext), r'trading card[s]?')
    )
  GROUP BY week_start
),
all_cards_comments AS (
  SELECT 
    DATE_TRUNC(DATE(created_utc), WEEK(MONDAY)) as week_start,
    COUNT(*) as comment_count
  FROM `handy-implement-454013-q2.reddit.comments`
  WHERE DATE(created_utc) >= '2010-01-01'
    AND (
      REGEXP_CONTAINS(LOWER(body), r'pok[eé]mon card[s]?') OR
      REGEXP_CONTAINS(LOWER(body), r'trading card[s]?')
    )
  GROUP BY week_start
),
ebay_submissions AS (
  SELECT 
    DATE_TRUNC(DATE(created_utc), WEEK(MONDAY)) as week_start,
    COUNT(*) as submission_count
  FROM `handy-implement-454013-q2.reddit.submissions`
  WHERE DATE(created_utc) >= '2010-01-01'
    AND (
      REGEXP_CONTAINS(LOWER(title), r'ebay') OR
      REGEXP_CONTAINS(LOWER(selftext), r'ebay')
    )
  GROUP BY week_start
),
ebay_comments AS (
  SELECT 
    DATE_TRUNC(DATE(created_utc), WEEK(MONDAY)) as week_start,
    COUNT(*) as comment_count
  FROM `handy-implement-454013-q2.reddit.comments`
  WHERE DATE(created_utc) >= '2010-01-01'
    AND REGEXP_CONTAINS(LOWER(body), r'ebay')
  GROUP BY week_start
),
ebay_trading_submissions AS (
  SELECT 
    DATE_TRUNC(DATE(created_utc), WEEK(MONDAY)) as week_start,
    COUNT(*) as submission_count
  FROM `handy-implement-454013-q2.reddit.submissions`
  WHERE DATE(created_utc) >= '2010-01-01'
    AND (
      (REGEXP_CONTAINS(LOWER(title), r'ebay') AND REGEXP_CONTAINS(LOWER(title), r'trading card[s]?')) OR
      (REGEXP_CONTAINS(LOWER(selftext), r'ebay') AND REGEXP_CONTAINS(LOWER(selftext), r'trading card[s]?')) OR
      (REGEXP_CONTAINS(LOWER(title), r'ebay') AND REGEXP_CONTAINS(LOWER(selftext), r'trading card[s]?')) OR
      (REGEXP_CONTAINS(LOWER(selftext), r'ebay') AND REGEXP_CONTAINS(LOWER(title), r'trading card[s]?'))
    )
  GROUP BY week_start
),
ebay_trading_comments AS (
  SELECT 
    DATE_TRUNC(DATE(created_utc), WEEK(MONDAY)) as week_start,
    COUNT(*) as comment_count
  FROM `handy-implement-454013-q2.reddit.comments`
  WHERE DATE(created_utc) >= '2010-01-01'
    AND REGEXP_CONTAINS(LOWER(body), r'ebay') 
    AND REGEXP_CONTAINS(LOWER(body), r'trading card[s]?')
  GROUP BY week_start
),
ebay_pokemon_submissions AS (
  SELECT 
    DATE_TRUNC(DATE(created_utc), WEEK(MONDAY)) as week_start,
    COUNT(*) as submission_count
  FROM `handy-implement-454013-q2.reddit.submissions`
  WHERE DATE(created_utc) >= '2010-01-01'
    AND (
      (REGEXP_CONTAINS(LOWER(title), r'ebay') AND REGEXP_CONTAINS(LOWER(title), r'pok[eé]mon card[s]?')) OR
      (REGEXP_CONTAINS(LOWER(selftext), r'ebay') AND REGEXP_CONTAINS(LOWER(selftext), r'pok[eé]mon card[s]?')) OR
      (REGEXP_CONTAINS(LOWER(title), r'ebay') AND REGEXP_CONTAINS(LOWER(selftext), r'pok[eé]mon card[s]?')) OR
      (REGEXP_CONTAINS(LOWER(selftext), r'ebay') AND REGEXP_CONTAINS(LOWER(title), r'pok[eé]mon card[s]?'))
    )
  GROUP BY week_start
),
ebay_pokemon_comments AS (
  SELECT 
    DATE_TRUNC(DATE(created_utc), WEEK(MONDAY)) as week_start,
    COUNT(*) as comment_count
  FROM `handy-implement-454013-q2.reddit.comments`
  WHERE DATE(created_utc) >= '2010-01-01'
    AND REGEXP_CONTAINS(LOWER(body), r'ebay') 
    AND REGEXP_CONTAINS(LOWER(body), r'pok[eé]mon card[s]?')
  GROUP BY week_start
),
ebay_all_cards_submissions AS (
  SELECT 
    DATE_TRUNC(DATE(created_utc), WEEK(MONDAY)) as week_start,
    COUNT(*) as submission_count
  FROM `handy-implement-454013-q2.reddit.submissions`
  WHERE DATE(created_utc) >= '2010-01-01'
    AND (
      (REGEXP_CONTAINS(LOWER(title), r'ebay') AND (REGEXP_CONTAINS(LOWER(title), r'pok[eé]mon card[s]?') OR REGEXP_CONTAINS(LOWER(title), r'trading card[s]?'))) OR
      (REGEXP_CONTAINS(LOWER(selftext), r'ebay') AND (REGEXP_CONTAINS(LOWER(selftext), r'pok[eé]mon card[s]?') OR REGEXP_CONTAINS(LOWER(selftext), r'trading card[s]?'))) OR
      (REGEXP_CONTAINS(LOWER(title), r'ebay') AND (REGEXP_CONTAINS(LOWER(selftext), r'pok[eé]mon card[s]?') OR REGEXP_CONTAINS(LOWER(selftext), r'trading card[s]?'))) OR
      (REGEXP_CONTAINS(LOWER(selftext), r'ebay') AND (REGEXP_CONTAINS(LOWER(title), r'pok[eé]mon card[s]?') OR REGEXP_CONTAINS(LOWER(title), r'trading card[s]?')))
    )
  GROUP BY week_start
),
ebay_all_cards_comments AS (
  SELECT 
    DATE_TRUNC(DATE(created_utc), WEEK(MONDAY)) as week_start,
    COUNT(*) as comment_count
  FROM `handy-implement-454013-q2.reddit.comments`
  WHERE DATE(created_utc) >= '2010-01-01'
    AND REGEXP_CONTAINS(LOWER(body), r'ebay') 
    AND (REGEXP_CONTAINS(LOWER(body), r'pok[eé]mon card[s]?') OR REGEXP_CONTAINS(LOWER(body), r'trading card[s]?'))
  GROUP BY week_start
),
total_reddit_submissions AS (
  SELECT 
    DATE_TRUNC(DATE(created_utc), WEEK(MONDAY)) as week_start,
    COUNT(*) as total_submission_count
  FROM `handy-implement-454013-q2.reddit.submissions`
  WHERE DATE(created_utc) >= '2010-01-01'
  GROUP BY week_start
),
total_reddit_comments AS (
  SELECT 
    DATE_TRUNC(DATE(created_utc), WEEK(MONDAY)) as week_start,
    COUNT(*) as total_comment_count
  FROM `handy-implement-454013-q2.reddit.comments`
  WHERE DATE(created_utc) >= '2010-01-01'
  GROUP BY week_start
),
all_weeks AS (
  SELECT DISTINCT week_start FROM pokemon_card_submissions
  UNION DISTINCT
  SELECT DISTINCT week_start FROM pokemon_card_comments
  UNION DISTINCT
  SELECT DISTINCT week_start FROM trading_card_submissions
  UNION DISTINCT
  SELECT DISTINCT week_start FROM trading_card_comments
  UNION DISTINCT
  SELECT DISTINCT week_start FROM all_cards_submissions
  UNION DISTINCT
  SELECT DISTINCT week_start FROM all_cards_comments
  UNION DISTINCT
  SELECT DISTINCT week_start FROM ebay_submissions
  UNION DISTINCT
  SELECT DISTINCT week_start FROM ebay_comments
  UNION DISTINCT
  SELECT DISTINCT week_start FROM ebay_trading_submissions
  UNION DISTINCT
  SELECT DISTINCT week_start FROM ebay_trading_comments
  UNION DISTINCT
  SELECT DISTINCT week_start FROM ebay_pokemon_submissions
  UNION DISTINCT
  SELECT DISTINCT week_start FROM ebay_pokemon_comments
  UNION DISTINCT
  SELECT DISTINCT week_start FROM ebay_all_cards_submissions
  UNION DISTINCT
  SELECT DISTINCT week_start FROM ebay_all_cards_comments
  UNION DISTINCT
  SELECT DISTINCT week_start FROM total_reddit_submissions
  UNION DISTINCT
  SELECT DISTINCT week_start FROM total_reddit_comments
),
pokemon_card_totals AS (
  SELECT 
    w.week_start,
    COALESCE(ps.submission_count, 0) + COALESCE(pc.comment_count, 0) as pokemon_card_total_count
  FROM all_weeks w
  LEFT JOIN pokemon_card_submissions ps ON w.week_start = ps.week_start
  LEFT JOIN pokemon_card_comments pc ON w.week_start = pc.week_start
),
trading_card_totals AS (
  SELECT 
    w.week_start,
    COALESCE(ts.submission_count, 0) + COALESCE(tc.comment_count, 0) as trading_card_total_count
  FROM all_weeks w
  LEFT JOIN trading_card_submissions ts ON w.week_start = ts.week_start
  LEFT JOIN trading_card_comments tc ON w.week_start = tc.week_start
),
all_cards_totals AS (
  SELECT 
    w.week_start,
    COALESCE(acs.submission_count, 0) + COALESCE(acc.comment_count, 0) as all_cards_total_count
  FROM all_weeks w
  LEFT JOIN all_cards_submissions acs ON w.week_start = acs.week_start
  LEFT JOIN all_cards_comments acc ON w.week_start = acc.week_start
),
ebay_totals AS (
  SELECT 
    w.week_start,
    COALESCE(es.submission_count, 0) + COALESCE(ec.comment_count, 0) as ebay_total_count
  FROM all_weeks w
  LEFT JOIN ebay_submissions es ON w.week_start = es.week_start
  LEFT JOIN ebay_comments ec ON w.week_start = ec.week_start
),
ebay_trading_totals AS (
  SELECT 
    w.week_start,
    COALESCE(ets.submission_count, 0) + COALESCE(etc.comment_count, 0) as ebay_trading_total_count
  FROM all_weeks w
  LEFT JOIN ebay_trading_submissions ets ON w.week_start = ets.week_start
  LEFT JOIN ebay_trading_comments etc ON w.week_start = etc.week_start
),
ebay_pokemon_totals AS (
  SELECT 
    w.week_start,
    COALESCE(eps.submission_count, 0) + COALESCE(epc.comment_count, 0) as ebay_pokemon_total_count
  FROM all_weeks w
  LEFT JOIN ebay_pokemon_submissions eps ON w.week_start = eps.week_start
  LEFT JOIN ebay_pokemon_comments epc ON w.week_start = epc.week_start
),
ebay_all_cards_totals AS (
  SELECT 
    w.week_start,
    COALESCE(eacs.submission_count, 0) + COALESCE(eacc.comment_count, 0) as ebay_all_cards_total_count
  FROM all_weeks w
  LEFT JOIN ebay_all_cards_submissions eacs ON w.week_start = eacs.week_start
  LEFT JOIN ebay_all_cards_comments eacc ON w.week_start = eacc.week_start
),
total_reddit_totals AS (
  SELECT 
    w.week_start,
    COALESCE(trs.total_submission_count, 0) + COALESCE(trc.total_comment_count, 0) as total_reddit_count
  FROM all_weeks w
  LEFT JOIN total_reddit_submissions trs ON w.week_start = trs.week_start
  LEFT JOIN total_reddit_comments trc ON w.week_start = trc.week_start
)
SELECT 
  pct.week_start,
  pct.pokemon_card_total_count,
  tct.trading_card_total_count,
  act.all_cards_total_count,
  et.ebay_total_count,
  ett.ebay_trading_total_count,
  ept.ebay_pokemon_total_count,
  eact.ebay_all_cards_total_count,
  trt.total_reddit_count
FROM pokemon_card_totals pct
JOIN trading_card_totals tct ON pct.week_start = tct.week_start
JOIN all_cards_totals act ON pct.week_start = act.week_start
JOIN ebay_totals et ON pct.week_start = et.week_start
JOIN ebay_trading_totals ett ON pct.week_start = ett.week_start
JOIN ebay_pokemon_totals ept ON pct.week_start = ept.week_start
JOIN ebay_all_cards_totals eact ON pct.week_start = eact.week_start
JOIN total_reddit_totals trt ON pct.week_start = trt.week_start
ORDER BY pct.week_start;