-- PSA BigQuery Deduplication and Cleanup Queries
-- Use these queries to clean up any duplicate data in the psa_auction_prices table

-- 1. Check for duplicate summary records (same item_id, grade, and day)
SELECT 
  item_id,
  grade,
  DATE(scraped_at) as scrape_date,
  COUNT(*) as duplicate_count,
  STRING_AGG(CAST(scraped_at AS STRING), ', ' ORDER BY scraped_at) as scrape_timestamps
FROM `rising-environs-456314-a3.tcg_data.psa_auction_prices`
WHERE record_type = 'summary'
GROUP BY item_id, grade, DATE(scraped_at)
HAVING COUNT(*) > 1
ORDER BY duplicate_count DESC, item_id, grade;

-- 2. Delete duplicate summary records (keep the latest timestamp for each combination)
CREATE OR REPLACE TABLE `rising-environs-456314-a3.tcg_data.psa_auction_prices_deduped` AS
SELECT * FROM (
  SELECT *,
    ROW_NUMBER() OVER (
      PARTITION BY item_id, grade, DATE(scraped_at), record_type 
      ORDER BY scraped_at DESC
    ) as rn
  FROM `rising-environs-456314-a3.tcg_data.psa_auction_prices`
) 
WHERE rn = 1;

-- 3. Replace original table with deduplicated version (CAREFUL - BACKUP FIRST!)
-- DROP TABLE `rising-environs-456314-a3.tcg_data.psa_auction_prices`;
-- ALTER TABLE `rising-environs-456314-a3.tcg_data.psa_auction_prices_deduped` 
-- RENAME TO psa_auction_prices;

-- 4. Count records by type and scrape date
SELECT 
  DATE(scraped_at) as scrape_date,
  record_type,
  COUNT(*) as record_count,
  COUNT(DISTINCT item_id) as unique_items,
  COUNT(DISTINCT CONCAT(item_id, '-', grade)) as unique_combinations
FROM `rising-environs-456314-a3.tcg_data.psa_auction_prices`
GROUP BY DATE(scraped_at), record_type
ORDER BY scrape_date DESC, record_type;

-- 5. Identify items with incomplete grade coverage
WITH grade_coverage AS (
  SELECT 
    item_id,
    COUNT(DISTINCT grade) as grades_scraped,
    STRING_AGG(DISTINCT grade ORDER BY grade) as grades_list
  FROM `rising-environs-456314-a3.tcg_data.psa_auction_prices`
  WHERE record_type = 'summary'
  GROUP BY item_id
)
SELECT 
  item_id,
  grades_scraped,
  19 - grades_scraped as missing_grades,
  grades_list
FROM grade_coverage
WHERE grades_scraped < 19
ORDER BY grades_scraped DESC, item_id;

-- 6. Summary statistics by item and grade
SELECT 
  item_id,
  grade,
  grade_label,
  total_sales_count,
  average_price,
  median_price,
  min_price,
  max_price,
  MAX(scraped_at) as latest_scrape
FROM `rising-environs-456314-a3.tcg_data.psa_auction_prices`
WHERE record_type = 'summary'
  AND total_sales_count > 0
GROUP BY item_id, grade, grade_label, total_sales_count, average_price, median_price, min_price, max_price
ORDER BY item_id, CAST(CASE WHEN grade = '0' THEN '0' ELSE grade END AS FLOAT64) DESC;

-- 7. Data quality checks
SELECT 
  'Total records' as metric,
  COUNT(*) as value,
  '' as details
FROM `rising-environs-456314-a3.tcg_data.psa_auction_prices`

UNION ALL

SELECT 
  'Summary records' as metric,
  COUNT(*) as value,
  '' as details
FROM `rising-environs-456314-a3.tcg_data.psa_auction_prices`
WHERE record_type = 'summary'

UNION ALL

SELECT 
  'Sale records' as metric,
  COUNT(*) as value,
  '' as details
FROM `rising-environs-456314-a3.tcg_data.psa_auction_prices`
WHERE record_type = 'sale'

UNION ALL

SELECT 
  'Unique items' as metric,
  COUNT(DISTINCT item_id) as value,
  '' as details
FROM `rising-environs-456314-a3.tcg_data.psa_auction_prices`

UNION ALL

SELECT 
  'Unique combinations' as metric,
  COUNT(DISTINCT CONCAT(item_id, '-', grade)) as value,
  '' as details
FROM `rising-environs-456314-a3.tcg_data.psa_auction_prices`
WHERE record_type = 'summary'

UNION ALL

SELECT 
  'Records with missing prices' as metric,
  COUNT(*) as value,
  CONCAT('(', ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM `rising-environs-456314-a3.tcg_data.psa_auction_prices` WHERE record_type = 'sale'), 2), '%)') as details
FROM `rising-environs-456314-a3.tcg_data.psa_auction_prices`
WHERE record_type = 'sale' AND (sale_price IS NULL OR sale_price <= 0)

ORDER BY metric;