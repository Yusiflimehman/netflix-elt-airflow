-- =====================================================================
-- 03_quality_checks.sql
-- Every check returns the number of offending rows for the current batch.
--   CRITICAL -> the Airflow task fails, the model is NOT updated
--   WARNING  -> loaded, but flagged/handled (e.g. invalid age -> NULL)
--   INFO     -> observation about the data, no action
-- Results are stored in dq_check_results for auditing and trend monitoring.
-- =====================================================================

DELETE FROM dq_check_results
WHERE batch_id = (SELECT batch_id FROM etl_run_context);

INSERT INTO dq_check_results (batch_id, check_name, severity, failed_rows, description)
SELECT (SELECT batch_id FROM etl_run_context), check_name, severity, failed_rows, description
FROM (
    -- ------------------------------ CRITICAL ------------------------------
    SELECT 'batch_not_empty' AS check_name, 'CRITICAL' AS severity,
           CASE WHEN (SELECT COUNT(*) FROM v_raw_current_batch) = 0 THEN 1 ELSE 0 END AS failed_rows,
           'The source file produced at least one row' AS description

    UNION ALL
    SELECT 'row_count_raw_equals_staging', 'CRITICAL',
           ABS((SELECT COUNT(*) FROM v_raw_current_batch) - (SELECT COUNT(*) FROM stg_netflix_subscriptions)),
           'No rows lost between raw and staging'

    UNION ALL
    SELECT 'unique_user_id', 'CRITICAL',
           (SELECT COUNT(*) - COUNT(DISTINCT user_id) FROM stg_netflix_subscriptions),
           'User ID is the business key and must be unique'

    UNION ALL
    SELECT 'mandatory_fields_not_null_or_unparseable', 'CRITICAL',
           (SELECT COUNT(*) FROM stg_netflix_subscriptions
             WHERE user_id IS NULL OR subscription_type IS NULL OR monthly_revenue IS NULL
                OR join_date IS NULL OR last_payment_date IS NULL OR country IS NULL
                OR device IS NULL OR plan_duration_months IS NULL OR active_profiles IS NULL
                OR household_profile_ind IS NULL OR movies_watched IS NULL OR series_watched IS NULL),
           'Required values present, numeric fields numeric, dates valid dd.mm.yyyy'

    UNION ALL
    SELECT 'subscription_type_in_reference_list', 'CRITICAL',
           (SELECT COUNT(*) FROM stg_netflix_subscriptions s
             WHERE s.subscription_type NOT IN (SELECT plan_name FROM dim_subscription_plan)),
           'Plan must be Basic / Standard / Premium'

    UNION ALL
    SELECT 'household_profile_ind_domain', 'CRITICAL',
           (SELECT COUNT(*) FROM stg_netflix_subscriptions WHERE household_profile_ind NOT IN (1, 2)),
           'Household Profile Ind must be 1 or 2'

    UNION ALL
    SELECT 'revenue_positive', 'CRITICAL',
           (SELECT COUNT(*) FROM stg_netflix_subscriptions WHERE monthly_revenue <= 0),
           'Monthly revenue must be > 0'

    UNION ALL
    SELECT 'last_payment_not_before_join', 'CRITICAL',
           (SELECT COUNT(*) FROM stg_netflix_subscriptions WHERE last_payment_date < join_date),
           'A payment cannot happen before sign-up'

    UNION ALL
    SELECT 'dates_not_in_future', 'CRITICAL',
           (SELECT COUNT(*) FROM stg_netflix_subscriptions
             WHERE join_date > DATE('now') OR last_payment_date > DATE('now')),
           'Join / payment dates cannot be after the load date'

    -- ------------------------------ WARNING -------------------------------
    UNION ALL
    SELECT 'age_plausible_18_100', 'WARNING',
           (SELECT COUNT(*) FROM stg_netflix_subscriptions
             WHERE age IS NULL OR age NOT BETWEEN 18 AND 100),
           'Contract holder must be an adult with a realistic age; invalid ages are loaded as NULL'

    UNION ALL
    SELECT 'gender_in_expected_values', 'WARNING',
           (SELECT COUNT(*) FROM stg_netflix_subscriptions
             WHERE gender IS NULL OR gender NOT IN ('Male', 'Female')),
           'Gender outside the expected values'

    UNION ALL
    SELECT 'single_price_per_plan', 'WARNING',
           (SELECT COUNT(*) FROM stg_netflix_subscriptions
             WHERE subscription_type IN (SELECT subscription_type
                                           FROM stg_netflix_subscriptions
                                          GROUP BY subscription_type
                                         HAVING COUNT(DISTINCT monthly_revenue) > 1)),
           'Rows belonging to a plan that has more than one monthly price'

    UNION ALL
    SELECT 'premium_not_cheaper_than_basic', 'WARNING',
           (SELECT COUNT(*) FROM stg_netflix_subscriptions p
             WHERE p.subscription_type = 'Premium'
               AND p.monthly_revenue < (SELECT MAX(monthly_revenue)
                                          FROM stg_netflix_subscriptions
                                         WHERE subscription_type = 'Basic')),
           'Premium subscribers paying less than some Basic subscribers (price tier inversion)'

    UNION ALL
    SELECT 'active_profiles_within_limit_5', 'WARNING',
           (SELECT COUNT(*) FROM stg_netflix_subscriptions WHERE active_profiles > 5),
           'Netflix allows at most 5 profiles per account'

    UNION ALL
    SELECT 'single_profile_not_multi_household', 'WARNING',
           (SELECT COUNT(*) FROM stg_netflix_subscriptions
             WHERE active_profiles = 1 AND household_profile_ind = 2),
           'An account with one profile cannot have profiles in more than one household'

    -- ------------------------------ INFO ----------------------------------
    UNION ALL
    SELECT 'plan_duration_has_variation', 'INFO',
           CASE WHEN (SELECT COUNT(DISTINCT plan_duration_months) FROM stg_netflix_subscriptions) = 1
                THEN (SELECT COUNT(*) FROM stg_netflix_subscriptions) ELSE 0 END,
           'Plan Duration has a single value for every row (column carries no information)'

    UNION ALL
    SELECT 'customers_without_recent_payment', 'INFO',
           (SELECT COUNT(*) FROM stg_netflix_subscriptions
             WHERE last_payment_date < DATE((SELECT MAX(last_payment_date) FROM stg_netflix_subscriptions), '-1 month')),
           'Customers whose last payment is >1 month older than the newest payment (possible churn)'
);
