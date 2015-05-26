DROP TABLE raw_log;
 
CREATE EXTERNAL TABLE raw_log(
        IP              STRING,
        ts              STRING,
        URL             STRING,
        referrer        STRING,
        user_agent      STRING)
PARTITIONED BY (
        year            INT,
        month           INT,
        day             INT)
ROW FORMAT SERDE 'org.apache.hadoop.hive.contrib.serde2.RegexSerDe'
WITH SERDEPROPERTIES (
        "input.regex" = "(\\d+.\\d+.\\d+.\\d+).*\\[(.*) \\].*GET (\\S*).*\\d+ \\d+ (\\S+) \"(.*)\""
)
LOCATION '/etl/BI/casualcyclist/clicks/rawlogs';

-- Need to be where generated access logs are
LOAD DATA LOCAL INPATH '*log*.log' OVERWRITE INTO TABLE raw_log PARTITION (year=2014, month=10, day=10);

SELECT * FROM raw_log LIMIT 5;
