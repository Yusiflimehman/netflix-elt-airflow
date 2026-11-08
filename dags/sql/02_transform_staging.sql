-- =====================================================================
-- 02_transform_staging.sql
-- Rebuild staging from the CURRENT raw batch only.
--   * trim whitespace, standardize casing of coded values
--   * cast numbers ONLY when the text is really numeric (SQLite would
--     otherwise silently turn 'abc' into 0) -> bad values become NULL
--     and are caught by the quality checks
--   * convert dd.mm.yyyy to ISO yyyy-mm-dd and reject impossible dates
-- =====================================================================

DELETE FROM stg_netflix_subscriptions;

INSERT INTO stg_netflix_subscriptions (
    batch_id, user_id, subscription_type, monthly_revenue,
    join_date, last_payment_date, country, age, gender, device,
    plan_duration_months, active_profiles, household_profile_ind,
    movies_watched, series_watched
)
WITH cleaned AS (
    SELECT
        batch_id,
        TRIM(user_id)               AS user_id,
        TRIM(subscription_type)     AS subscription_type,
        TRIM(monthly_revenue)       AS monthly_revenue,
        TRIM(join_date)             AS join_date,
        TRIM(last_payment_date)     AS last_payment_date,
        TRIM(country)               AS country,
        TRIM(age)                   AS age,
        TRIM(gender)                AS gender,
        TRIM(device)                AS device,
        LOWER(TRIM(plan_duration))  AS plan_duration,
        TRIM(active_profiles)       AS active_profiles,
        TRIM(household_profile_ind) AS household_profile_ind,
        TRIM(movies_watched)        AS movies_watched,
        TRIM(series_watched)        AS series_watched
    FROM v_raw_current_batch
),
iso AS (
    SELECT
        c.*,
        SUBSTR(join_date, 7, 4) || '-' || SUBSTR(join_date, 4, 2) || '-' || SUBSTR(join_date, 1, 2)
            AS join_iso,
        SUBSTR(last_payment_date, 7, 4) || '-' || SUBSTR(last_payment_date, 4, 2) || '-' || SUBSTR(last_payment_date, 1, 2)
            AS last_payment_iso,
        TRIM(REPLACE(REPLACE(plan_duration, 'months', ''), 'month', '')) AS plan_duration_num
    FROM cleaned c
)
SELECT
    batch_id,
    CASE WHEN user_id <> '' AND user_id NOT GLOB '*[^0-9]*' THEN CAST(user_id AS INTEGER) END,
    CASE WHEN subscription_type <> ''
         THEN UPPER(SUBSTR(subscription_type, 1, 1)) || LOWER(SUBSTR(subscription_type, 2)) END,
    CASE WHEN monthly_revenue <> '' AND monthly_revenue NOT GLOB '*[^0-9.]*'
         THEN CAST(monthly_revenue AS NUMERIC) END,
    CASE WHEN join_date GLOB '[0-9][0-9].[0-9][0-9].[0-9][0-9][0-9][0-9]'
              AND DATE(join_iso) = join_iso THEN join_iso END,
    CASE WHEN last_payment_date GLOB '[0-9][0-9].[0-9][0-9].[0-9][0-9][0-9][0-9]'
              AND DATE(last_payment_iso) = last_payment_iso THEN last_payment_iso END,
    NULLIF(country, ''),
    CASE WHEN age <> '' AND age NOT GLOB '*[^0-9]*' THEN CAST(age AS INTEGER) END,
    CASE WHEN gender <> '' THEN UPPER(SUBSTR(gender, 1, 1)) || LOWER(SUBSTR(gender, 2)) END,
    NULLIF(device, ''),
    CASE WHEN plan_duration_num <> '' AND plan_duration_num NOT GLOB '*[^0-9]*'
         THEN CAST(plan_duration_num AS INTEGER) END,
    CASE WHEN active_profiles <> '' AND active_profiles NOT GLOB '*[^0-9]*'
         THEN CAST(active_profiles AS INTEGER) END,
    CASE WHEN household_profile_ind <> '' AND household_profile_ind NOT GLOB '*[^0-9]*'
         THEN CAST(household_profile_ind AS INTEGER) END,
    CASE WHEN movies_watched <> '' AND movies_watched NOT GLOB '*[^0-9]*'
         THEN CAST(movies_watched AS INTEGER) END,
    CASE WHEN series_watched <> '' AND series_watched NOT GLOB '*[^0-9]*'
         THEN CAST(series_watched AS INTEGER) END
FROM iso;
