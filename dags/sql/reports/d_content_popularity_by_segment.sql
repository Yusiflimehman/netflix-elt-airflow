-- d. Popularity of Movies vs Series by customer segment and device,
--    across all markets (countries).
-- Segment = age group x gender. Invalid ages are NULL in the model and are
-- shown as 'Unknown' instead of being dropped, so totals still reconcile.
WITH base AS (
    SELECT
        c.country_name,
        CASE
            WHEN cu.age IS NULL THEN 'Unknown'
            WHEN cu.age < 30    THEN '18-29'
            WHEN cu.age < 40    THEN '30-39'
            WHEN cu.age < 50    THEN '40-49'
            ELSE '50+'
        END                     AS age_group,
        COALESCE(cu.gender, 'Unknown') AS gender,
        d.device_name,
        v.movies_watched,
        v.series_watched
    FROM viewing_activity v
    JOIN subscription s  ON s.subscription_id = v.subscription_id
    JOIN customer     cu ON cu.customer_id    = s.customer_id
    JOIN dim_country  c  ON c.country_id      = cu.country_id
    JOIN dim_device   d  ON d.device_id       = v.device_id
)
SELECT
    country_name,
    age_group,
    gender,
    device_name,
    COUNT(*)                                   AS subscribers,
    SUM(movies_watched)                        AS total_movies_watched,
    SUM(series_watched)                        AS total_series_watched,
    ROUND(AVG(movies_watched), 2)              AS avg_movies_per_subscriber,
    ROUND(AVG(series_watched), 2)              AS avg_series_per_subscriber,
    ROUND(100.0 * SUM(movies_watched) / NULLIF(SUM(movies_watched) + SUM(series_watched), 0), 1) AS movies_share_pct,
    CASE
        WHEN SUM(movies_watched) > SUM(series_watched) THEN 'Movies'
        WHEN SUM(movies_watched) < SUM(series_watched) THEN 'Series'
        ELSE 'Equal'
    END                                        AS preferred_content
FROM base
GROUP BY country_name, age_group, gender, device_name
ORDER BY country_name, age_group, gender, device_name
