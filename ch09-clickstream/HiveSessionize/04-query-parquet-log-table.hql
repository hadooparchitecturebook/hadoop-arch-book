-- Find total number of clicks in the data set
SELECT COUNT(*) FROM apache_log_parquet;

-- Find bounce rate as a percentage
SELECT
  (SUM(CASE WHEN count!=1 THEN 0 ELSE 1 END))*100/(COUNT(*)) bounce_rate
FROM (
  SELECT
    session_id,
    COUNT(*) count
  FROM
    apache_log_parquet
  GROUP BY
    session_id)t;

-- Distribution of session duration in seconds based on the hour of the day when the session was started
SELECT
  AVG(session_duration) as avg_duration,
  HOUR(FROM_UNIXTIME(session_start))
FROM (
  SELECT
    session_id,
    MAX(UNIX_TIMESTAMP(ts,'dd/MMM/yyyy:HH:mm:ss')) - MIN(UNIX_TIMESTAMP(ts,'dd/MMM/yyyy:HH:mm:ss')) as session_duration,
    MIN(UNIX_TIMESTAMP(ts,'dd/MMM/yyyy:HH:mm:ss')) as session_start 
  FROM
    apache_log_parquet
  GROUP BY
    session_id)t
GROUP BY
  HOUR(FROM_UNIXTIME(session_start));

-- Average session length
SELECT
  AVG(session_length)/60 avg_min
FROM (
  SELECT
    MAX(ts) - MIN(ts) session_length_in_sec
  FROM
    apache_log_parquet
  GROUP BY
    session_id
  ) t;
