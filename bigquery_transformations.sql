-- BigQuery Transformations for Global Trade Analytics

-- 1. Base Table: Annual Country Trade Summary
-- This materialized view aggregates trade data by country and year for faster querying
CREATE OR REPLACE MATERIALIZED VIEW `raw.mv_country_annual_trade` 
AS
SELECT
  country_id,
  year,
  SUM(export_value) AS total_exports,
  SUM(import_value) AS total_imports,
  SUM(export_value + import_value) AS total_trade_volume,
  AVG(eci) AS economic_complexity_index,
  AVG(coi) AS complexity_outlook_index
FROM `raw.hs12_country_country_product_year_4_2022`
GROUP BY country_id, year;

-- 2. Base Table: Country Pairs Annual Trade
-- This materialized view focuses on bilateral trade relationships
CREATE OR REPLACE MATERIALIZED VIEW `raw.mv_country_pairs_annual_trade` 
AS
SELECT
  country_id,
  partner_country_id,
  year,
  SUM(export_value) AS exports_to_partner,
  SUM(import_value) AS imports_from_partner,
  SUM(export_value + import_value) AS total_trade_with_partner
FROM `raw.hs12_country_country_product_year_4_2022`
GROUP BY country_id, partner_country_id, year;

-- 3. Base Table: Product Annual Trade
-- This materialized view aggregates trade by product and year
CREATE OR REPLACE MATERIALIZED VIEW `raw.mv_product_annual_trade` 
AS
SELECT
  product_id,
  year,
  SUM(export_value) AS global_export_value,
  SUM(import_value) AS global_import_value,
  SUM(export_value + import_value) AS global_trade_volume,
  AVG(pci) AS product_complexity_index
FROM `raw.hs12_country_country_product_year_4_2022`
GROUP BY product_id, year;

-- 4. Global Trade Metrics with Year-over-Year Change
-- This view calculates global metrics with YoY changes
CREATE OR REPLACE VIEW `raw.v_global_trade_metrics` 
AS
WITH annual_metrics AS (
  SELECT
    year,
    SUM(total_trade_volume) AS global_trade_volume,
    AVG(economic_complexity_index) AS avg_economic_complexity
  FROM `raw.mv_country_annual_trade`
  GROUP BY year
),
with_previous_year AS (
  SELECT
    current.year,
    current.global_trade_volume,
    current.avg_economic_complexity,
    prev.global_trade_volume AS prev_global_trade_volume,
    prev.avg_economic_complexity AS prev_avg_economic_complexity
  FROM annual_metrics current
  LEFT JOIN annual_metrics prev
    ON current.year = prev.year + 1
)
SELECT
  year,
  global_trade_volume,
  avg_economic_complexity,
  global_trade_volume - IFNULL(prev_global_trade_volume, 0) AS trade_volume_change,
  CASE 
    WHEN IFNULL(prev_global_trade_volume, 0) = 0 THEN NULL
    ELSE (global_trade_volume - prev_global_trade_volume) / prev_global_trade_volume * 100 
  END AS trade_volume_pct_change,
  avg_economic_complexity - IFNULL(prev_avg_economic_complexity, 0) AS eci_change,
  CASE 
    WHEN IFNULL(prev_avg_economic_complexity, 0) = 0 THEN NULL
    ELSE (avg_economic_complexity - prev_avg_economic_complexity) / prev_avg_economic_complexity * 100 
  END AS eci_pct_change
FROM with_previous_year
ORDER BY year;

-- 5. Top Traded Products with Year-over-Year Change
-- This view identifies the top traded products globally
CREATE OR REPLACE VIEW `raw.v_top_traded_products` 
AS
WITH ranked_products AS (
  SELECT
    year,
    product_id,
    global_trade_volume,
    product_complexity_index,
    ROW_NUMBER() OVER(PARTITION BY year ORDER BY global_trade_volume DESC) AS rank
  FROM `raw.mv_product_annual_trade`
),
top_products AS (
  SELECT * FROM ranked_products WHERE rank <= 20  -- Get top 20 for flexibility
),
with_previous_year AS (
  SELECT
    current.year,
    current.product_id,
    current.rank,
    current.global_trade_volume,
    current.product_complexity_index,
    prev.global_trade_volume AS prev_global_trade_volume
  FROM top_products current
  LEFT JOIN `raw.mv_product_annual_trade` prev
    ON current.product_id = prev.product_id AND current.year = prev.year + 1
)
SELECT
  year,
  product_id,
  rank,
  global_trade_volume,
  product_complexity_index,
  global_trade_volume - IFNULL(prev_global_trade_volume, 0) AS volume_change,
  CASE 
    WHEN IFNULL(prev_global_trade_volume, 0) = 0 THEN NULL
    ELSE (global_trade_volume - prev_global_trade_volume) / prev_global_trade_volume * 100 
  END AS volume_pct_change
FROM with_previous_year
ORDER BY year, rank;

-- 6. Top Trading Partners by Country
-- This view calculates top trading partners for each country
CREATE OR REPLACE VIEW `raw.v_top_trading_partners` 
AS
WITH ranked_partners AS (
  SELECT
    country_id,
    partner_country_id,
    year,
    exports_to_partner,
    imports_from_partner,
    total_trade_with_partner,
    ROW_NUMBER() OVER(
      PARTITION BY country_id, year 
      ORDER BY total_trade_with_partner DESC
    ) AS rank
  FROM `raw.mv_country_pairs_annual_trade`
  WHERE partner_country_id != country_id  -- Exclude self-trade if present
)
SELECT
  country_id,
  partner_country_id,
  year,
  exports_to_partner,
  imports_from_partner,
  total_trade_with_partner,
  rank
FROM ranked_partners
WHERE rank <= 10  -- Get top 10 partners for flexibility
ORDER BY country_id, year, rank;

-- Example Queries for the Required Metrics

-- 1. Global Trade Volume with YoY Change for a specific year range
-- SELECT year, global_trade_volume, trade_volume_pct_change
-- FROM `raw.v_global_trade_metrics`
-- WHERE year BETWEEN 2018 AND 2022
-- ORDER BY year;

-- 2. Average Economic Complexity with YoY Change for a specific year range
-- SELECT year, avg_economic_complexity, eci_pct_change
-- FROM `raw.v_global_trade_metrics`
-- WHERE year BETWEEN 2018 AND 2022
-- ORDER BY year;

-- 3. Top 5 Traded Products for a specific year
-- SELECT product_id, global_trade_volume, volume_pct_change
-- FROM `raw.v_top_traded_products`
-- WHERE year = 2022 AND rank <= 5
-- ORDER BY rank;

-- 4. Top 5 Trading Partners for a specific country and year
-- SELECT 
--   partner_country_id, 
--   exports_to_partner, 
--   imports_from_partner,
--   total_trade_with_partner
-- FROM `raw.v_top_trading_partners`
-- WHERE country_id = 840 AND year = 2022 AND rank <= 5  -- 840 is USA
-- ORDER BY rank;
