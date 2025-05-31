
-- 1. Customer Lifetime Value (CLV) Analysis

-- Calculate CLV (total amount spent) per customer with running total
SELECT
    u.id AS customer_id,
    u.email,
    u.country,
    o.order_date,
    o.total_amount,
    SUM(o.total_amount) OVER (PARTITION BY u.id ORDER BY o.order_date) AS running_clv
FROM users u
         JOIN orders o ON u.id = o.user_id
WHERE o.status IN ('shipped', 'delivered')
ORDER BY u.id, o.order_date;

-- Rank customers by total CLV within their country
SELECT
    u.id AS customer_id,
    u.email,
    u.country,
    SUM(o.total_amount) AS clv,
    RANK() OVER (PARTITION BY u.country ORDER BY SUM(o.total_amount) DESC) AS clv_rank
FROM users u
         JOIN orders o ON u.id = o.user_id
WHERE o.status IN ('shipped', 'delivered')
GROUP BY u.id, u.email, u.country
ORDER BY u.country, clv_rank;

-- Month-over-Month CLV growth per customer
SELECT
    u.id AS customer_id,
    DATE_FORMAT(o.order_date, '%Y-%m') AS order_month,
    SUM(o.total_amount) AS monthly_clv,
    LAG(SUM(o.total_amount)) OVER (PARTITION BY u.id ORDER BY DATE_FORMAT(o.order_date, '%Y-%m')) AS prev_month_clv,
    ROUND(
            (SUM(o.total_amount) - LAG(SUM(o.total_amount)) OVER (PARTITION BY u.id ORDER BY DATE_FORMAT(o.order_date, '%Y-%m')))
                / NULLIF(LAG(SUM(o.total_amount)) OVER (PARTITION BY u.id ORDER BY DATE_FORMAT(o.order_date, '%Y-%m')), 0) * 100,
            2
    ) AS growth_percent
FROM users u
         JOIN orders o ON u.id = o.user_id
WHERE o.status IN ('shipped', 'delivered')
GROUP BY u.id, DATE_FORMAT(o.order_date, '%Y-%m')
ORDER BY u.id, order_month;

-- Identify top 20% of customers by CLV globally
WITH clv_data AS (
    SELECT
        u.id AS customer_id,
        u.email,
        SUM(o.total_amount) AS clv,
        PERCENT_RANK() OVER (ORDER BY SUM(o.total_amount)) AS clv_percentile
    FROM users u
             JOIN orders o ON u.id = o.user_id
    WHERE o.status IN ('shipped', 'delivered')
    GROUP BY u.id, u.email
)
SELECT *
FROM clv_data
WHERE clv_percentile >= 0.80
ORDER BY clv DESC;

-- 2. Advanced Sales Trend Analysis

-- Moving averages of daily sales over 7, 30, and 90 days
SELECT
    order_date,
    SUM(total_amount) AS daily_sales,
    ROUND(AVG(SUM(total_amount)) OVER (
    ORDER BY order_date
    ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
  ), 2) AS ma_7_day,
    ROUND(AVG(SUM(total_amount)) OVER (
    ORDER BY order_date
    ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
  ), 2) AS ma_30_day,
    ROUND(AVG(SUM(total_amount)) OVER (
    ORDER BY order_date
    ROWS BETWEEN 89 PRECEDING AND CURRENT ROW
  ), 2) AS ma_90_day
FROM orders
WHERE status IN ('shipped', 'delivered')
GROUP BY order_date
ORDER BY order_date;

-- Year-over-year growth for each month
SELECT
    YEAR(order_date) AS sales_year,
    MONTH(order_date) AS sales_month,
    SUM(total_amount) AS monthly_sales,
    LAG(SUM(total_amount)) OVER (
    PARTITION BY MONTH(order_date)
    ORDER BY YEAR(order_date)
    ) AS previous_year_sales,
    ROUND(
    (SUM(total_amount) - LAG(SUM(total_amount)) OVER (
    PARTITION BY MONTH(order_date)
    ORDER BY YEAR(order_date)
    )) / LAG(SUM(total_amount)) OVER (
    PARTITION BY MONTH(order_date)
    ORDER BY YEAR(order_date)
    ) * 100, 2
    ) AS yoy_growth_percent
FROM orders
WHERE status IN ('shipped', 'delivered')
GROUP BY YEAR(order_date), MONTH(order_date)
ORDER BY sales_month, sales_year;

-- Identify seasonal patterns by comparing sales to previous month
SELECT
    YEAR(order_date) AS sales_year,
    MONTH(order_date) AS sales_month,
    SUM(total_amount) AS monthly_sales,
    LAG(SUM(total_amount)) OVER (
    ORDER BY YEAR(order_date), MONTH(order_date)
    ) AS previous_month_sales,
    ROUND(
    (SUM(total_amount) - LAG(SUM(total_amount)) OVER (
    ORDER BY YEAR(order_date), MONTH(order_date)
    )) / LAG(SUM(total_amount)) OVER (
    ORDER BY YEAR(order_date), MONTH(order_date)
    ) * 100, 2
    ) AS month_over_month_change_percent
FROM orders
WHERE status IN ('shipped', 'delivered')
GROUP BY YEAR(order_date), MONTH(order_date)
ORDER BY sales_year, sales_month;

-- Calculate rolling retention rates for customer cohorts
WITH cohorts AS (
    SELECT
        id AS customer_id,
        DATE_FORMAT(registration_date, '%Y-%m') AS cohort_month
    FROM users
),
     orders_by_month AS (
         SELECT
             user_id,
             DATE_FORMAT(order_date, '%Y-%m') AS order_month
         FROM orders
         WHERE status IN ('shipped', 'delivered')
     ),
     retention AS (
         SELECT
             c.cohort_month,
             o.order_month,
             COUNT(DISTINCT o.user_id) AS active_customers
         FROM cohorts c
                  LEFT JOIN orders_by_month o ON c.customer_id = o.user_id
         WHERE o.order_month >= c.cohort_month
         GROUP BY c.cohort_month, o.order_month
     )
SELECT
    cohort_month,
    order_month,
    active_customers,
    ROUND(
            active_customers / NULLIF((SELECT COUNT(DISTINCT customer_id) FROM cohorts WHERE cohort_month = r.cohort_month), 0) * 100, 2
    ) AS retention_rate_percent
FROM retention r
ORDER BY cohort_month, order_month;

-- 3. Product Performance with Hierarchical Categories

--🔹 Show product sales with full category path (parent → child)

WITH RECURSIVE category_path AS (
    SELECT id, name, parent_id, CAST(name AS CHAR(255)) AS full_path
    FROM categories
    WHERE parent_id IS NULL
    UNION ALL
    SELECT c.id, c.name, c.parent_id, CONCAT(cp.full_path, ' → ', c.name)
    FROM categories c
             JOIN category_path cp ON c.parent_id = cp.id
)
SELECT
    p.id AS product_id,
    p.name AS product_name,
    cp.full_path AS category_path,
    SUM(oi.quantity * oi.unit_price) AS total_sales
FROM products p
         JOIN category_path cp ON p.category_id = cp.id
         LEFT JOIN order_items oi ON oi.product_id = p.id
GROUP BY p.id, p.name, cp.full_path
ORDER BY total_sales DESC;

-- Calculate market share percentage for each product within its category

WITH product_sales AS (
    SELECT
        p.id AS product_id,
        p.name AS product_name,
        p.category_id,
        SUM(oi.quantity * oi.unit_price) AS total_sales
    FROM products p
             LEFT JOIN order_items oi ON p.id = oi.product_id
    GROUP BY p.id, p.name, p.category_id
),
     category_sales AS (
         SELECT
             category_id,
             SUM(total_sales) AS category_total_sales
         FROM product_sales
         GROUP BY category_id
     )
SELECT
    ps.product_id,
    ps.product_name,
    ps.total_sales,
    cs.category_total_sales,
    ROUND((ps.total_sales / cs.category_total_sales) * 100, 2) AS market_share_percent
FROM product_sales ps
         JOIN category_sales cs ON ps.category_id = cs.category_id
ORDER BY cs.category_total_sales DESC, market_share_percent DESC;

-- Identify top-performing products with positive sales growth trends month-over-month
WITH monthly_sales AS (
    SELECT
        p.id AS product_id,
        p.name AS product_name,
        DATE_FORMAT(o.order_date, '%Y-%m') AS sales_month,
        SUM(oi.quantity * oi.unit_price) AS total_sales
    FROM products p
             JOIN order_items oi ON p.id = oi.product_id
             JOIN orders o ON oi.order_id = o.id
    WHERE o.status IN ('shipped', 'delivered')
    GROUP BY p.id, p.name, sales_month
),
     sales_with_lag AS (
         SELECT
             product_id,
             product_name,
             sales_month,
             total_sales,
             LAG(total_sales) OVER (PARTITION BY product_id ORDER BY sales_month) AS prev_month_sales
         FROM monthly_sales
     ),
     sales_growth AS (
         SELECT
             product_id,
             product_name,
             sales_month,
             total_sales,
             prev_month_sales,

             CASE
                 WHEN prev_month_sales IS NULL THEN NULL
                 ELSE ROUND(((total_sales - prev_month_sales) / prev_month_sales) * 100, 2)
                 END AS growth_percent
         FROM sales_with_lag
     )
SELECT
    product_id,
    product_name,
    sales_month,
    total_sales,
    growth_percent
FROM sales_growth
WHERE growth_percent > 0
ORDER BY product_id, sales_month;

-- Find products with negative sales growth month-over-month
WITH monthly_sales AS (
    SELECT
        p.id AS product_id,
        p.name AS product_name,
        DATE_FORMAT(o.order_date, '%Y-%m') AS sales_month,
        SUM(oi.quantity * oi.unit_price) AS total_sales
    FROM products p
             JOIN order_items oi ON p.id = oi.product_id
             JOIN orders o ON oi.order_id = o.id
    WHERE o.status IN ('shipped', 'delivered')
    GROUP BY p.id, p.name, sales_month
),
     sales_with_lag AS (
         SELECT
             product_id,
             product_name,
             sales_month,
             total_sales,
             LAG(total_sales) OVER (PARTITION BY product_id ORDER BY sales_month) AS prev_month_sales
         FROM monthly_sales
     ),
     sales_decline AS (
         SELECT
             product_id,
             product_name,
             sales_month,
             total_sales,
             prev_month_sales,

             CASE
                 WHEN prev_month_sales IS NULL THEN NULL
                 ELSE ROUND(((total_sales - prev_month_sales) / prev_month_sales) * 100, 2)
                 END AS growth_percent
         FROM sales_with_lag
     )
SELECT
    product_id,
    product_name,
    sales_month,
    total_sales,
    growth_percent
FROM sales_decline
WHERE growth_percent < 0
ORDER BY product_id, sales_month;

-- 4. Customer Segmentation Query

-- RFM Analysis with Percentile Ranks and Customer Segments
WITH rfm_base AS (
    SELECT
        u.id AS customer_id,
        MAX(o.order_date) AS last_order_date,
        DATEDIFF(CURDATE(), MAX(o.order_date)) AS recency,
        COUNT(o.id) AS frequency,
        SUM(o.total_amount) AS monetary
    FROM users u
             JOIN orders o ON u.id = o.user_id
    WHERE o.status IN ('shipped', 'delivered')
    GROUP BY u.id
),
     rfm_scores AS (
         SELECT
             customer_id,
             recency,
             frequency,
             monetary,
             PERCENT_RANK() OVER (ORDER BY recency ASC) AS recency_rank,
             PERCENT_RANK() OVER (ORDER BY frequency DESC) AS frequency_rank,
             PERCENT_RANK() OVER (ORDER BY monetary DESC) AS monetary_rank
         FROM rfm_base
     ),
     rfm_segments AS (
         SELECT *,
                CASE
                    WHEN recency_rank >= 0.8 AND frequency_rank >= 0.8 AND monetary_rank >= 0.8 THEN 'Champion'
                    WHEN recency_rank >= 0.6 AND frequency_rank >= 0.6 THEN 'Loyal'
                    WHEN recency_rank >= 0.4 AND monetary_rank >= 0.5 THEN 'Potential'
                    WHEN recency_rank <= 0.2 THEN 'At Risk'
                    ELSE 'Others'
                    END AS segment
         FROM rfm_scores
     )
SELECT * FROM rfm_segments;

-- Percentile Ranks for RFM dimensions
WITH rfm_base AS (
    SELECT
        u.id AS customer_id,
        MAX(o.order_date) AS last_order_date,
        DATEDIFF(CURDATE(), MAX(o.order_date)) AS recency,
        COUNT(o.id) AS frequency,
        SUM(o.total_amount) AS monetary
    FROM users u
             JOIN orders o ON u.id = o.user_id
    WHERE o.status IN ('shipped', 'delivered')
    GROUP BY u.id
),
     rfm_percentiles AS (
         SELECT
             customer_id,
             recency,
             frequency,
             monetary,
             PERCENT_RANK() OVER (ORDER BY recency ASC) AS recency_percentile,
             PERCENT_RANK() OVER (ORDER BY frequency DESC) AS frequency_percentile,
             PERCENT_RANK() OVER (ORDER BY monetary DESC) AS monetary_percentile
         FROM rfm_base
     )
SELECT * FROM rfm_percentiles;

-- RFM Segmentation
WITH rfm_base AS (
    SELECT
        u.id AS customer_id,
        MAX(o.order_date) AS last_order_date,
        DATEDIFF(CURDATE(), MAX(o.order_date)) AS recency,
        COUNT(o.id) AS frequency,
        SUM(o.total_amount) AS monetary
    FROM users u
             JOIN orders o ON u.id = o.user_id
    WHERE o.status IN ('shipped', 'delivered')
    GROUP BY u.id
),
     rfm_scores AS (
         SELECT
             customer_id,
             recency,
             frequency,
             monetary,
             PERCENT_RANK() OVER (ORDER BY recency ASC) AS recency_score,
             PERCENT_RANK() OVER (ORDER BY frequency DESC) AS frequency_score,
             PERCENT_RANK() OVER (ORDER BY monetary DESC) AS monetary_score
         FROM rfm_base
     ),
     combined_scores AS (
         SELECT *,
                ROUND((recency_score + frequency_score + monetary_score) / 3, 2) AS overall_rfm_score
         FROM rfm_scores
     )
SELECT *,
       CASE
           WHEN overall_rfm_score >= 0.8 THEN 'Champion'
           WHEN overall_rfm_score >= 0.6 THEN 'Loyal'
           WHEN overall_rfm_score >= 0.4 THEN 'Potential'
           WHEN overall_rfm_score >= 0.2 THEN 'At Risk'
           ELSE 'Hibernating'
           END AS segment
FROM combined_scores;

-- Segment migration over time (monthly)
WITH customer_orders AS (
    SELECT
        u.id AS customer_id,
        DATE_FORMAT(o.order_date, '%Y-%m') AS order_month,
        MAX(o.order_date) AS last_order_date,
        COUNT(o.id) AS frequency,
        SUM(o.total_amount) AS monetary
    FROM users u
             JOIN orders o ON u.id = o.user_id
    WHERE o.status IN ('shipped', 'delivered')
    GROUP BY u.id, order_month
),
     rfm_monthly AS (
         SELECT
             customer_id,
             order_month,
             DATEDIFF(LAST_DAY(STR_TO_DATE(order_month, '%Y-%m')), last_order_date) AS recency,
             frequency,
             monetary
         FROM customer_orders
     ),
     scored_monthly AS (
         SELECT *,
                PERCENT_RANK() OVER (PARTITION BY order_month ORDER BY recency ASC) AS recency_score,
             PERCENT_RANK() OVER (PARTITION BY order_month ORDER BY frequency DESC) AS frequency_score,
             PERCENT_RANK() OVER (PARTITION BY order_month ORDER BY monetary DESC) AS monetary_score
         FROM rfm_monthly
     ),
     final_segments AS (
         SELECT *,
                ROUND((recency_score + frequency_score + monetary_score)/3, 2) AS overall_rfm_score
         FROM scored_monthly
     )
SELECT
    customer_id,
    order_month,
    overall_rfm_score,
    CASE
        WHEN overall_rfm_score >= 0.8 THEN 'Champion'
        WHEN overall_rfm_score >= 0.6 THEN 'Loyal'
        WHEN overall_rfm_score >= 0.4 THEN 'Potential'
        WHEN overall_rfm_score >= 0.2 THEN 'At Risk'
        ELSE 'Hibernating'
        END AS segment
FROM final_segments
ORDER BY customer_id, order_month;
