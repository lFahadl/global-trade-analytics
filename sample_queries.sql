-- Sample Queries for Global Trade Analytics

-- 1. Global Trade Volume with Year-over-Year Change (for a specific year range)
SELECT 
  year, 
  global_trade_volume, 
  trade_volume_pct_change
FROM `raw.v_global_trade_metrics`
WHERE year BETWEEN 2018 AND 2022
ORDER BY year;

-- 2. Average Economic Complexity with Year-over-Year Change (for a specific year range)
SELECT 
  year, 
  avg_economic_complexity, 
  eci_pct_change
FROM `raw.v_global_trade_metrics`
WHERE year BETWEEN 2018 AND 2022
ORDER BY year;

-- 3. Top 5 Traded Products for a specific year with Year-over-Year Change
SELECT 
  product_id, 
  global_trade_volume, 
  volume_pct_change
FROM `raw.v_top_traded_products`
WHERE year = 2022 AND rank <= 5
ORDER BY rank;

-- 4. Top 5 Trading Partners for a specific country and year
-- Example for USA (country_id = 840)
SELECT 
  partner_country_id, 
  exports_to_partner,  -- What USA sells to partner (green bars)
  imports_from_partner,  -- What USA buys from partner (blue bars)
  total_trade_with_partner
FROM `raw.v_top_trading_partners`
WHERE country_id = 840 AND year = 2022 AND rank <= 5
ORDER BY rank;

-- 5. Trade Balance Analysis for a specific country over time
-- Shows if a country has a trade surplus or deficit with its partners
SELECT
  year,
  SUM(exports_to_partner) AS total_exports,
  SUM(imports_from_partner) AS total_imports,
  SUM(exports_to_partner - imports_from_partner) AS trade_balance
FROM `raw.v_top_trading_partners`
WHERE country_id = 840  -- USA
GROUP BY year
ORDER BY year;

-- 6. Product Complexity Analysis - Find most complex products among top traded
SELECT
  product_id,
  global_trade_volume,
  product_complexity_index
FROM `raw.v_top_traded_products`
WHERE year = 2022 AND rank <= 20
ORDER BY product_complexity_index DESC
LIMIT 10;

-- 7. Country Comparison - Compare trade profiles of two countries
SELECT
  a.year,
  a.total_exports AS country1_exports,
  a.total_imports AS country1_imports,
  b.total_exports AS country2_exports,
  b.total_imports AS country2_imports,
  a.economic_complexity_index AS country1_eci,
  b.economic_complexity_index AS country2_eci
FROM `raw.mv_country_annual_trade` a
JOIN `raw.mv_country_annual_trade` b
  ON a.year = b.year
WHERE a.country_id = 840  -- USA
  AND b.country_id = 156  -- China
  AND a.year BETWEEN 2018 AND 2022
ORDER BY a.year;
