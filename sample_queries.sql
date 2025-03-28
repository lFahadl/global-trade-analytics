-- Sample Queries for Global Trade Analytics
-- To run these queries in BigQuery:
-- 1. Open the BigQuery console: https://console.cloud.google.com/bigquery
-- 2. Select your project: global-trade-analytics
-- 3. Copy and paste the query you want to run
-- 4. Click "Run"

-- 1. Global Trade Volume with Year-over-Year Change (for a specific year range)
SELECT 
  year, 
  global_trade_volume, 
  trade_volume_pct_change
FROM `trade_analytics.v_global_trade_metrics`
WHERE year BETWEEN 2018 AND 2022
ORDER BY year;

-- 2. Average Economic Complexity with Year-over-Year Change (for a specific year range)
SELECT 
  year, 
  avg_economic_complexity, 
  eci_pct_change
FROM `trade_analytics.v_global_trade_metrics`
WHERE year BETWEEN 2018 AND 2022
ORDER BY year;

-- 3. Top 5 Traded Products for a specific year with Year-over-Year Change
SELECT 
  product_id, 
  global_trade_volume, 
  volume_pct_change
FROM `trade_analytics.v_top_traded_products`
WHERE year = 2022 AND rank <= 5
ORDER BY rank;

-- 4. Top 5 Trading Partners for a specific country and year
-- Example for USA (country_id = 840)
SELECT 
  partner_country_id, 
  exports_to_partner,  -- What USA sells to partner (green bars)
  imports_from_partner,  -- What USA buys from partner (blue bars)
  total_trade_with_partner
FROM `trade_analytics.v_top_trading_partners`
WHERE country_id = 840 AND year = 2022 AND rank <= 5
ORDER BY rank;

-- 5. Trade Balance Analysis for a specific country over time
-- Shows if a country has a trade surplus or deficit with its partners
SELECT
  year,
  SUM(exports_to_partner) AS total_exports,
  SUM(imports_from_partner) AS total_imports,
  SUM(exports_to_partner - imports_from_partner) AS trade_balance
FROM `trade_analytics.v_top_trading_partners`
WHERE country_id = 840  -- USA
GROUP BY year
ORDER BY year;

-- 6. Product Complexity Analysis - Find most complex products among top traded
SELECT
  product_id,
  global_trade_volume,
  product_complexity_index
FROM `trade_analytics.v_top_traded_products`
WHERE year = 2022 AND rank <= 20
ORDER BY product_complexity_index DESC
LIMIT 10;

-- 7. Country Comparison - Compare trade profiles of two countries (using ISO numeric country codes)
SELECT
  a.year,
  a.avg_economic_complexity AS global_avg_eci,
  b.total_exports AS country1_exports,
  b.total_imports AS country1_imports,
  c.total_exports AS country2_exports,
  c.total_imports AS country2_imports,
  b.economic_complexity_index AS country1_eci,
  c.economic_complexity_index AS country2_eci
FROM `trade_analytics.v_global_trade_metrics` a
JOIN `processed_trade_data.mv_country_annual_trade` b
  ON a.year = b.year
JOIN `processed_trade_data.mv_country_annual_trade` c
  ON a.year = c.year
WHERE b.country_id = 840  -- USA
  AND c.country_id = 156  -- China
  AND a.year BETWEEN 2018 AND 2022
ORDER BY a.year;

-- 8. NEW: ECI and Trade Balance Correlation Analysis
-- Shows countries with both improving ECI and trade balance
SELECT
  country_id,
  balance_trend,
  eci_trend,
  trend_category,
  complexity_concentration
FROM `trade_analytics.v_eci_trade_balance_correlation`
WHERE trend_category = 'Both improving'
ORDER BY eci_trend DESC
LIMIT 10;

-- 9. NEW: Countries with Improving ECI but Worsening Trade Balance
SELECT
  country_id,
  balance_trend,
  eci_trend,
  high_complexity_balance_trend,
  medium_complexity_balance_trend,
  low_complexity_balance_trend,
  complexity_concentration
FROM `trade_analytics.v_eci_trade_balance_correlation`
WHERE trend_category = 'ECI improving, balance worsening'
ORDER BY eci_trend DESC
LIMIT 10;

-- 10. NEW: Product Complexity Segment Analysis
-- Shows how trade balance trends vary across product complexity segments for a specific country
SELECT
  country_id,
  product_complexity_segment,
  segment_balance_trend,
  avg_segment_balance,
  trend_strength
FROM `trade_analytics.v_product_complexity_balance_trends`
WHERE country_id = '840'  -- USA
ORDER BY product_complexity_segment;

-- 11. NEW: Product Complexity Leaders - Top 10 countries with highest trade surplus in high-complexity products
SELECT
  country_id,
  year,
  high_complexity_exports,
  high_complexity_imports,
  high_complexity_trade_balance,
  high_complexity_export_pct,
  trade_balance_rank,
  top_product_category,
  specialization_pattern
FROM `trade_analytics.v_product_complexity_leaders`
WHERE year = 2022 AND trade_balance_rank <= 10
ORDER BY trade_balance_rank;

-- 12. NEW: Countries with Consistent Leadership in High-Complexity Products
SELECT
  country_id,
  year,
  high_complexity_exports,
  high_complexity_trade_balance,
  leadership_score,  -- Lower score means more consistent leadership
  specialization_pattern,
  top_product_category,
  second_product_category,
  third_product_category
FROM `trade_analytics.v_product_complexity_leaders`
WHERE year = 2022
ORDER BY leadership_score
LIMIT 15;

-- 13. NEW: High-Complexity Product Category Specialization by Country
-- Shows which countries specialize in which high-complexity product categories
SELECT
  country_id,
  year,
  top_product_category,
  specialization_pattern,
  high_complexity_export_pct,
  high_complexity_trade_balance
FROM `trade_analytics.v_product_complexity_leaders`
WHERE 
  year = 2022 AND 
  specialization_pattern IN ('High-complexity specialist', 'Balanced high-complexity exporter')
ORDER BY high_complexity_export_pct DESC
LIMIT 20;

-- 14. NEW: Trend Analysis - Countries Improving Their Position in High-Complexity Products
-- Shows countries that have improved their trade balance rank over time
WITH yearly_ranks AS (
  SELECT
    country_id,
    year,
    trade_balance_rank,
    high_complexity_trade_balance,
    LAG(trade_balance_rank) OVER(PARTITION BY country_id ORDER BY year) AS prev_rank
  FROM `trade_analytics.v_product_complexity_leaders`
)

SELECT
  country_id,
  year,
  trade_balance_rank AS current_rank,
  prev_rank,
  (prev_rank - trade_balance_rank) AS rank_improvement,
  high_complexity_trade_balance
FROM yearly_ranks
WHERE 
  year = 2022 AND 
  prev_rank IS NOT NULL AND
  (prev_rank - trade_balance_rank) > 0  -- Only countries that improved their ranking
ORDER BY rank_improvement DESC
LIMIT 15;
