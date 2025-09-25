select 
to_char(date_trunc('month', start_time), 'YYYY-MM') AS maand,  
round(sum(credits_used)/400,0) credits_used,
round(sum(credits_used_compute)/400,0) credits_used_compute,
round(sum(credits_used_cloud_services)/400,0) credits_used_cloud_services,
round(sum(credits_attributed_compute_queries)/400,0) credits_attributed_compute_queries
from snowflake.account_usage.warehouse_metering_history
where start_time > '2025-01-01'
group by all;
