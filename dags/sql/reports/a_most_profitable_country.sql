-- a. The most profitable country for Netflix.
-- The dataset has no cost data, so "profit" is measured by revenue:
--   * monthly_recurring_revenue : what the country brings in every month now
--   * estimated_lifetime_revenue: monthly fee x number of monthly bills from
--     join month to last-payment month (plan is billed monthly, 1 Month)
-- Both rankings are returned; rank 1 by MRR is the answer.
WITH subscription_revenue AS (
    SELECT
        c.country_name,
        s.monthly_revenue,
        (CAST(STRFTIME('%Y', s.last_payment_date) AS INTEGER) * 12 + CAST(STRFTIME('%m', s.last_payment_date) AS INTEGER))
      - (CAST(STRFTIME('%Y', s.join_date)         AS INTEGER) * 12 + CAST(STRFTIME('%m', s.join_date)         AS INTEGER))
      + 1 AS months_billed
    FROM subscription s
    JOIN customer    cu ON cu.customer_id = s.customer_id
    JOIN dim_country c  ON c.country_id   = cu.country_id
)
SELECT
    country_name,
    COUNT(*)                                          AS subscribers,
    SUM(monthly_revenue)                              AS monthly_recurring_revenue,
    ROUND(AVG(monthly_revenue), 2)                    AS avg_revenue_per_user,
    SUM(monthly_revenue * months_billed)              AS estimated_lifetime_revenue,
    RANK() OVER (ORDER BY SUM(monthly_revenue) DESC)                 AS rank_by_mrr,
    RANK() OVER (ORDER BY SUM(monthly_revenue * months_billed) DESC) AS rank_by_lifetime_revenue
FROM subscription_revenue
GROUP BY country_name
ORDER BY monthly_recurring_revenue DESC
