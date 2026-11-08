-- =====================================================================
-- 04_load_model.sql
-- Upsert the validated staging batch into the normalized model.
-- INSERT ... ON CONFLICT makes the load idempotent: re-running the same
-- batch (or a later file with the same users) updates rows instead of
-- duplicating them.
-- ("WHERE true" is required by SQLite's parser for INSERT-SELECT upserts.)
-- =====================================================================

PRAGMA foreign_keys = ON;

-- Dimensions --------------------------------------------------------------
INSERT INTO dim_country (country_name)
SELECT DISTINCT country FROM stg_netflix_subscriptions WHERE true
ON CONFLICT (country_name) DO NOTHING;

INSERT INTO dim_device (device_name)
SELECT DISTINCT device FROM stg_netflix_subscriptions WHERE true
ON CONFLICT (device_name) DO NOTHING;

-- Customer (person) -------------------------------------------------------
INSERT INTO customer (customer_id, age, gender, country_id, last_batch_id)
SELECT s.user_id,
       CASE WHEN s.age BETWEEN 18 AND 100 THEN s.age END,   -- implausible ages -> NULL (see DQ)
       s.gender,
       c.country_id,
       s.batch_id
FROM stg_netflix_subscriptions s
JOIN dim_country c ON c.country_name = s.country
WHERE true
ON CONFLICT (customer_id) DO UPDATE SET
    age           = excluded.age,
    gender        = excluded.gender,
    country_id    = excluded.country_id,
    last_batch_id = excluded.last_batch_id;

-- Subscription (contract) -------------------------------------------------
INSERT INTO subscription (
    subscription_id, customer_id, plan_id, monthly_revenue, join_date,
    last_payment_date, plan_duration_months, active_profiles,
    household_profile_ind, last_batch_id
)
SELECT s.user_id,
       s.user_id,
       p.plan_id,
       s.monthly_revenue,
       s.join_date,
       s.last_payment_date,
       s.plan_duration_months,
       s.active_profiles,
       s.household_profile_ind,
       s.batch_id
FROM stg_netflix_subscriptions s
JOIN dim_subscription_plan p ON p.plan_name = s.subscription_type
WHERE true
ON CONFLICT (subscription_id) DO UPDATE SET
    plan_id               = excluded.plan_id,
    monthly_revenue       = excluded.monthly_revenue,
    join_date             = excluded.join_date,
    last_payment_date     = excluded.last_payment_date,
    plan_duration_months  = excluded.plan_duration_months,
    active_profiles       = excluded.active_profiles,
    household_profile_ind = excluded.household_profile_ind,
    last_batch_id         = excluded.last_batch_id;

-- Viewing activity (usage) ------------------------------------------------
INSERT INTO viewing_activity (subscription_id, device_id, movies_watched, series_watched, last_batch_id)
SELECT s.user_id, d.device_id, s.movies_watched, s.series_watched, s.batch_id
FROM stg_netflix_subscriptions s
JOIN dim_device d ON d.device_name = s.device
WHERE true
ON CONFLICT (subscription_id) DO UPDATE SET
    device_id      = excluded.device_id,
    movies_watched = excluded.movies_watched,
    series_watched = excluded.series_watched,
    last_batch_id  = excluded.last_batch_id;
