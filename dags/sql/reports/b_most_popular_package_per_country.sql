-- b. The most popular package (subscription plan) per country.
-- Popularity = number of subscribers. RANK() keeps ties, so a country
-- with two equally popular plans returns both.
WITH plan_counts AS (
    SELECT
        c.country_name,
        p.plan_name,
        COUNT(*)             AS subscribers,
        SUM(s.monthly_revenue) AS monthly_revenue
    FROM subscription s
    JOIN customer              cu ON cu.customer_id = s.customer_id
    JOIN dim_country           c  ON c.country_id   = cu.country_id
    JOIN dim_subscription_plan p  ON p.plan_id      = s.plan_id
    GROUP BY c.country_name, p.plan_name
),
ranked AS (
    SELECT
        country_name,
        plan_name,
        subscribers,
        monthly_revenue,
        ROUND(100.0 * subscribers / SUM(subscribers) OVER (PARTITION BY country_name), 1) AS share_of_country_pct,
        RANK() OVER (PARTITION BY country_name ORDER BY subscribers DESC)               AS popularity_rank
    FROM plan_counts
)
SELECT country_name, plan_name AS most_popular_plan, subscribers, share_of_country_pct, monthly_revenue
FROM ranked
WHERE popularity_rank = 1
ORDER BY country_name
