select count(*) from apache_log_parquet;

-- Find bounce rate as a percentage
SELECT (SUM(CASE WHEN count!=1 THEN 0 ELSE 1 END))*100/(SUM(count)) bounce_rate FROM (SELECT session_id, COUNT(*) count FROM apache_log_parquet GROUP BY session_id)t;
