select 
to_char(date_trunc('month', usage_date), 'YYYY-MM') AS maand, 
round(sum(credits_billed),0) credits_billed,
round(sum(credits_used),0) credits_used,
round(sum(credits_used_compute),0) credits_used_compute,
round(sum(credits_used_cloud_services),0) credits_used_cloud_services,
from snowflake.account_usage.metering_daily_history
where usage_date > '2025-01-01'
group by all
;
