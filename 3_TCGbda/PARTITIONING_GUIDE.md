# BigQuery Table Partitioning Guide

## Table Configuration

The `tcg_prices_bda` table is now optimized with:

### 1. **Daily Partitioning by `scrape_date`**
- Each day's data is stored in a separate partition
- Queries filtering by date only scan relevant partitions
- Significant cost reduction (pay only for data scanned)

### 2. **Clustering by `product_id`, `language`, `condition`**
- Data within each partition is organized by these fields
- Queries filtering by these fields run faster
- Most efficient when filters are applied in clustering order

## Optimized Query Examples

### ✅ BEST: Filter by partition and cluster fields
```sql
-- This query will be VERY fast and cheap
SELECT * FROM `tcg_data.tcg_prices_bda`
WHERE scrape_date = '2025-01-15'
  AND product_id = '12345'
  AND language = 'English'
```

### ✅ GOOD: Filter by partition field
```sql
-- Scans only one day's partition
SELECT COUNT(*) FROM `tcg_data.tcg_prices_bda`
WHERE scrape_date BETWEEN '2025-01-01' AND '2025-01-07'
```

### ✅ GOOD: Query specific partitions
```sql
-- Use partition decorator for specific date
SELECT * FROM `tcg_data.tcg_prices_bda$20250115`
WHERE product_id = '12345'
```

### ⚠️ EXPENSIVE: Full table scan
```sql
-- Avoid queries without date filters (scans all partitions)
SELECT * FROM `tcg_data.tcg_prices_bda`
WHERE product_id = '12345'  -- No scrape_date filter!
```

## Benefits

1. **Performance**: Queries run 10-100x faster when filtering by partition
2. **Cost**: Only pay for data in scanned partitions (not entire table)
3. **Management**: Easy to delete old data by dropping partitions
4. **Freshness**: Can query latest partition for most recent data

## Partition Management

### View partition information
```sql
SELECT 
  table_name,
  partition_id,
  total_rows,
  total_logical_bytes
FROM `tcg_data.INFORMATION_SCHEMA.PARTITIONS`
WHERE table_name = 'tcg_prices_bda'
ORDER BY partition_id DESC
```

### Delete old partitions (if needed)
```sql
-- Delete data older than 90 days
DELETE FROM `tcg_data.tcg_prices_bda`
WHERE scrape_date < DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
```

## Best Practices

1. **Always include `scrape_date` in WHERE clause** when possible
2. **Use date ranges** rather than individual dates for batch queries
3. **Order filters** by clustering fields: product_id → language → condition
4. **Monitor costs** in BigQuery console to verify partition pruning

## Cost Example

- **Without partitioning**: Query scans 10M rows = $$$
- **With partitioning**: Query scans 100K rows (1 day) = $ (100x cheaper!)

The partitioning will automatically work with the deduplication tracking system, ensuring efficient storage and retrieval of TCG price data.