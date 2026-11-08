-- c. Which country could earn the most from an extra fee for sharing
--    Netflix outside the subscriber's own household?
-- Assumptions (documented in README):
--   * household_profile_ind = 2 means "profiles are NOT all from one household"
--   * an account can only share if it has more than 1 active profile
--     (single-profile accounts flagged 2 are a data error and are excluded)
--   * extra fee is a flat amount per sharing account per month (parameter below)
-- Ranking metric: additional monthly revenue. uplift_pct shows how big it
-- is relative to the country's current revenue.
WITH params AS (
    SELECT 2.99 AS extra_household_fee        -- change here to re-simulate
),
accounts AS (
    SELECT
        c.country_name,
        s.monthly_revenue,
        CASE WHEN s.household_profile_ind = 2 AND s.active_profiles > 1 THEN 1 ELSE 0 END AS is_sharing
    FROM subscription s
    JOIN customer    cu ON cu.customer_id = s.customer_id
    JOIN dim_country c  ON c.country_id   = cu.country_id
)
SELECT
    a.country_name,
    COUNT(*)                                              AS total_accounts,
    SUM(a.is_sharing)                                     AS sharing_accounts,
    ROUND(100.0 * SUM(a.is_sharing) / COUNT(*), 1)        AS sharing_accounts_pct,
    SUM(a.monthly_revenue)                                AS current_monthly_revenue,
    ROUND(SUM(a.is_sharing) * p.extra_household_fee, 2)   AS potential_extra_monthly_revenue,
    ROUND(100.0 * SUM(a.is_sharing) * p.extra_household_fee / SUM(a.monthly_revenue), 1) AS revenue_uplift_pct,
    RANK() OVER (ORDER BY SUM(a.is_sharing) * p.extra_household_fee DESC)               AS potential_rank
FROM accounts a
CROSS JOIN params p
GROUP BY a.country_name, p.extra_household_fee
ORDER BY potential_extra_monthly_revenue DESC
