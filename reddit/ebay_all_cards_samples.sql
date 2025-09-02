-- Query to extract 100 random samples of "eBay All Cards Total Count"
-- Pattern: eBay AND (Pokémon card(s) OR trading card(s))

WITH ebay_all_cards_comments AS (
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
  WHERE DATE(created_utc) >= '2010-01-01'
    AND REGEXP_CONTAINS(LOWER(body), r'ebay') 
    AND (REGEXP_CONTAINS(LOWER(body), r'pok[eé]mon card[s]?') OR REGEXP_CONTAINS(LOWER(body), r'trading card[s]?'))
    AND body != '[deleted]'
    AND author != '[deleted]'
),

ebay_all_cards_submissions AS (
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
  WHERE DATE(created_utc) >= '2010-01-01'
    AND (
      (REGEXP_CONTAINS(LOWER(title), r'ebay') AND (REGEXP_CONTAINS(LOWER(title), r'pok[eé]mon card[s]?') OR REGEXP_CONTAINS(LOWER(title), r'trading card[s]?'))) OR
      (REGEXP_CONTAINS(LOWER(selftext), r'ebay') AND (REGEXP_CONTAINS(LOWER(selftext), r'pok[eé]mon card[s]?') OR REGEXP_CONTAINS(LOWER(selftext), r'trading card[s]?'))) OR
      (REGEXP_CONTAINS(LOWER(title), r'ebay') AND (REGEXP_CONTAINS(LOWER(selftext), r'pok[eé]mon card[s]?') OR REGEXP_CONTAINS(LOWER(selftext), r'trading card[s]?'))) OR
      (REGEXP_CONTAINS(LOWER(selftext), r'ebay') AND (REGEXP_CONTAINS(LOWER(title), r'pok[eé]mon card[s]?') OR REGEXP_CONTAINS(LOWER(title), r'trading card[s]?')))
    )
    AND author != '[deleted]'
    AND (selftext IS NULL OR selftext != '[deleted]')
),

combined_ebay_all_cards AS (
  SELECT * FROM ebay_all_cards_comments
  UNION ALL
  SELECT * FROM ebay_all_cards_submissions
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
FROM combined_ebay_all_cards
ORDER BY random_order
LIMIT 100;