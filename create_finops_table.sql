--  USE DATABASE
-- USE SCHEMA
CREATE OR REPLACE TRANSIENT TABLE snowflake_finops AS 
    WITH warehouse_metering_history AS (
        SELECT 
            TO_CHAR(DATE_TRUNC('month', start_time), 'YYYYMM') AS maand,  
            ROUND(SUM(credits_used), 0) AS wmh_credits_used,
            ROUND(SUM(credits_used_compute), 0) AS wmh_credits_used_compute,
            ROUND(SUM(credits_used_cloud_services), 0) AS wmh_credits_used_cloud_services,
            ROUND(SUM(credits_attributed_compute_queries), 0) AS wmh_credits_attributed_compute_queries
        FROM snowflake.account_usage.warehouse_metering_history
        WHERE start_time >= '2025-01-01'
        GROUP BY ALL
    ),
    metering_daily_history AS (
        SELECT 
            TO_CHAR(DATE_TRUNC('month', usage_date), 'YYYYMM') AS mdh_maand, 
            ROUND(SUM(credits_billed), 0) AS mdh_credits_billed,
            ROUND(SUM(credits_used), 0) AS mdh_credits_used,
            ROUND(SUM(credits_used_compute), 0) AS mdh_credits_used_compute,
            ROUND(SUM(credits_used_cloud_services), 0) AS mdh_credits_used_cloud_services
        FROM snowflake.account_usage.metering_daily_history
        WHERE usage_date >= '2025-01-01'
        GROUP BY ALL
    )
    SELECT warehouse_metering_history.*, metering_daily_history.* EXCLUDE mdh_maand
    FROM warehouse_metering_history 
    JOIN metering_daily_history 
    ON maand = mdh_maand
    ORDER BY maand DESC
