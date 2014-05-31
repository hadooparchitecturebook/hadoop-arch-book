ADD JAR /opt/cloudera/parcels/CDH/lib/hive/lib/hive-contrib.jar;

set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.dynamic.partition=true;
 
insert into table apache_log_parquet
partition(year,month)
select IP,timestamp,URL,referrer,user_agent,ts,
coalesce(sum(case when new='Y' then 1 end) over (PARTITION BY IP ORDER BY ts),0) session_id
, year(from_unixtime(ts)) year, month(from_unixtime(ts)) month, day(from_unixtime(ts)) day
from
(select IP,timestamp,URL,referrer,user_agent,ts,case when ts-lag(ts,1) OVER (PARTITION BY IP ORDER BY ts) > 30*60 then 'Y' else 'N' end new
from (
        select IP,timestamp,URL,referrer,user_agent,unix_timestamp(timestamp,'dd/MMM/yyyy:HH:mm:ss') as ts
        from raw_log
) t) s;
 
 
--select * from apache_log_parquet limit 5;
