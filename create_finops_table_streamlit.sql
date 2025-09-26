import streamlit as st
import snowflake.snowpark as snowpark

def main():
    st.title("Execute FinOps Table Creation")

    # Create Snowpark session
    session = snowpark.Session.builder.getOrCreate()

    # The query you provided
    query = """
    CREATE OR REPLACE TRANSIENT TABLE dev_front.finops_bronze.snowflake_finops AS 
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
    """

    if st.button("Run Table Creation Query"):
        try:
            session.sql(query).collect()
            st.success("✅ Table created or replaced successfully.")
        except Exception as e:
            st.error(f"❌ Failed to run query:\n{e}")

    st.caption("This app runs a CREATE OR REPLACE TABLE statement using Snowpark inside Snowflake Native App.")

if __name__ == "__main__":
    main()
