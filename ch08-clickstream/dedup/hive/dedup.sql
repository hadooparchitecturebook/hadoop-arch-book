INSERT INTO table $deduped_log
SELECT
  ip,
  ts,
  url,
  referrer,
  user_agent,
  YEAR(FROM_UNIXTIME(unix_ts)) year,
  MONTH(FROM_UNIXTIME(unix_ts)) month
FROM (
  SELECT
    ip,
    ts,
    url,
    referrer,
    user_agent,
    UNIX_TIMESTAMP(ts,'dd/MMM/yyyy:HH:mm:ss') unix_ts
  FROM
    $raw_log
  WHERE
    year=${YEAR} AND
    month=${MONTH} AND
    day=${DAY}
  GROUP BY
    ip,
    ts,
    url,
    referrer,
    user_agent,
    UNIX_TIMESTAMP(ts,'dd/MMM/yyyy:HH:mm:ss')
  ) t;
