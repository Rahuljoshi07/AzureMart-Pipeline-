/*
 * analytical_queries.sql
 * A collection of useful reporting queries for the star schema.
 * Run these against Azure SQL after the Gold layer has been loaded.
 */

-- 1. Daily revenue breakdown
SELECT
    d.full_date,
    d.day_name,
    d.is_weekend,
    COUNT(DISTINCT f.transaction_id)    AS total_transactions,
    SUM(f.quantity)                     AS total_units_sold,
    SUM(f.total_amount)                AS gross_revenue,
    SUM(f.net_amount)                  AS net_revenue,
    AVG(f.discount_pct)                AS avg_discount,
    SUM(f.total_amount - f.net_amount) AS total_discounts
FROM dbo.fact_sales f
JOIN dbo.dim_date d ON f.date_key = d.date_key
WHERE f.order_status = 'Completed'
GROUP BY d.full_date, d.day_name, d.is_weekend
ORDER BY d.full_date;


-- 2. Top 10 products by revenue
SELECT TOP 10
    p.product_name,
    p.category,
    p.sub_category,
    p.brand,
    SUM(f.net_amount)               AS total_revenue,
    SUM(f.quantity)                  AS units_sold,
    AVG(f.unit_price)               AS avg_selling_price,
    COUNT(DISTINCT f.customer_sk)   AS unique_customers
FROM dbo.fact_sales f
JOIN dbo.dim_product p ON f.product_sk = p.product_sk
WHERE f.order_status = 'Completed'
  AND p.is_current = 1
GROUP BY p.product_name, p.category, p.sub_category, p.brand
ORDER BY total_revenue DESC;


-- 3. Customer segment analysis
SELECT
    c.customer_segment,
    COUNT(DISTINCT c.customer_id)        AS customer_count,
    COUNT(f.sales_sk)                    AS total_orders,
    SUM(f.net_amount)                    AS total_spend,
    AVG(f.net_amount)                    AS avg_order_value,
    SUM(f.net_amount) / COUNT(DISTINCT c.customer_id) AS revenue_per_customer,
    MIN(d.full_date)                     AS first_purchase,
    MAX(d.full_date)                     AS last_purchase
FROM dbo.fact_sales f
JOIN dbo.dim_customer c ON f.customer_sk = c.customer_sk
JOIN dbo.dim_date d     ON f.date_key = d.date_key
WHERE c.is_current = 1
  AND f.order_status = 'Completed'
GROUP BY c.customer_segment
ORDER BY total_spend DESC;


-- 4. Store performance comparison
SELECT
    s.store_name,
    s.store_city,
    s.store_state,
    COUNT(f.sales_sk)                   AS total_transactions,
    SUM(f.net_amount)                   AS total_revenue,
    AVG(f.net_amount)                   AS avg_transaction_value,
    SUM(f.quantity)                     AS total_units,
    SUM(CASE WHEN f.order_status = 'Returned' THEN 1 ELSE 0 END) AS return_count,
    CAST(SUM(CASE WHEN f.order_status = 'Returned' THEN 1.0 ELSE 0 END)
         / NULLIF(COUNT(*), 0) * 100 AS DECIMAL(5,2))            AS return_rate_pct
FROM dbo.fact_sales f
JOIN dbo.dim_store s ON f.store_sk = s.store_sk
WHERE s.is_current = 1
GROUP BY s.store_name, s.store_city, s.store_state
ORDER BY total_revenue DESC;


-- 5. Monthly revenue trend with month-over-month growth
WITH monthly AS (
    SELECT
        d.[year],
        d.[month],
        d.month_name,
        SUM(f.net_amount)           AS revenue,
        COUNT(DISTINCT f.transaction_id) AS transactions
    FROM dbo.fact_sales f
    JOIN dbo.dim_date d ON f.date_key = d.date_key
    WHERE f.order_status = 'Completed'
    GROUP BY d.[year], d.[month], d.month_name
)
SELECT
    [year],
    [month],
    month_name,
    revenue,
    transactions,
    LAG(revenue) OVER (ORDER BY [year], [month])        AS prev_month_revenue,
    CAST(
        (revenue - LAG(revenue) OVER (ORDER BY [year], [month]))
        / NULLIF(LAG(revenue) OVER (ORDER BY [year], [month]), 0) * 100
        AS DECIMAL(6,2)
    )                                                    AS mom_growth_pct
FROM monthly
ORDER BY [year], [month];


-- 6. Sub-category rankings within each category
SELECT
    p.category,
    p.sub_category,
    SUM(f.net_amount)       AS category_revenue,
    SUM(f.quantity)         AS units_sold,
    RANK() OVER (
        PARTITION BY p.category
        ORDER BY SUM(f.net_amount) DESC
    )                       AS sub_category_rank,
    CAST(
        SUM(f.net_amount) * 100.0 /
        SUM(SUM(f.net_amount)) OVER (PARTITION BY p.category)
        AS DECIMAL(5,2)
    )                       AS pct_of_category
FROM dbo.fact_sales f
JOIN dbo.dim_product p ON f.product_sk = p.product_sk
WHERE f.order_status = 'Completed' AND p.is_current = 1
GROUP BY p.category, p.sub_category
ORDER BY p.category, category_revenue DESC;


-- 7. Payment method breakdown
SELECT
    f.payment_method,
    COUNT(*) AS transaction_count,
    SUM(f.net_amount) AS total_revenue,
    AVG(f.net_amount) AS avg_transaction,
    CAST(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER () AS DECIMAL(5,2)) AS pct_of_total
FROM dbo.fact_sales f
WHERE f.order_status = 'Completed'
GROUP BY f.payment_method
ORDER BY total_revenue DESC;


-- 8. Running total revenue per store (window function)
SELECT
    s.store_name,
    d.full_date,
    f.net_amount,
    SUM(f.net_amount) OVER (
        PARTITION BY s.store_id
        ORDER BY d.full_date
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS running_total
FROM dbo.fact_sales f
JOIN dbo.dim_store s ON f.store_sk = s.store_sk
JOIN dbo.dim_date d  ON f.date_key = d.date_key
WHERE f.order_status = 'Completed'
ORDER BY s.store_name, d.full_date;


-- 9. Customer RFM analysis (Recency / Frequency / Monetary)
WITH rfm AS (
    SELECT
        c.customer_id,
        c.customer_name,
        c.customer_segment,
        DATEDIFF(DAY, MAX(d.full_date), GETUTCDATE()) AS recency_days,
        COUNT(DISTINCT f.transaction_id)               AS frequency,
        SUM(f.net_amount)                              AS monetary
    FROM dbo.fact_sales f
    JOIN dbo.dim_customer c ON f.customer_sk = c.customer_sk
    JOIN dbo.dim_date d     ON f.date_key = d.date_key
    WHERE c.is_current = 1 AND f.order_status = 'Completed'
    GROUP BY c.customer_id, c.customer_name, c.customer_segment
)
SELECT
    customer_id,
    customer_name,
    customer_segment,
    recency_days,
    frequency,
    monetary,
    NTILE(5) OVER (ORDER BY recency_days ASC)  AS recency_score,
    NTILE(5) OVER (ORDER BY frequency DESC)    AS frequency_score,
    NTILE(5) OVER (ORDER BY monetary DESC)     AS monetary_score
FROM rfm
ORDER BY monetary DESC;


-- ═══════════════════════════════════════════
-- 10. Weekend vs Weekday Performance
-- ═══════════════════════════════════════════
SELECT
    CASE WHEN d.is_weekend = 1 THEN 'Weekend' ELSE 'Weekday' END AS day_type,
    COUNT(DISTINCT d.full_date)       AS num_days,
    COUNT(f.sales_sk)                 AS total_transactions,
    SUM(f.net_amount)                 AS total_revenue,
    AVG(f.net_amount)                 AS avg_order_value,
    SUM(f.net_amount) / COUNT(DISTINCT d.full_date) AS avg_daily_revenue
FROM dbo.fact_sales f
JOIN dbo.dim_date d ON f.date_key = d.date_key
WHERE f.order_status = 'Completed'
GROUP BY d.is_weekend
ORDER BY day_type;
