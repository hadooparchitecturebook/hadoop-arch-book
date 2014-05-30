DROP TABLE raw_log;
 
-- I don't think timestamp is a good name for a column, changing it to ts since older versions of hive fail when creating a column with name ts.
CREATE EXTERNAL TABLE raw_log(
	IP		STRING,
	ts		STRING,
	URL		STRING,
	referrer	STRING,
	user_agent	STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.contrib.serde2.RegexSerDe'
WITH SERDEPROPERTIES (
	"input.regex" = "(\\d+.\\d+.\\d+.\\d+).*\\[(.*)\\].*GET (\\S*).*\\d+ \\d+ (\\S+) \"(.*)\""
)
location '/etl/bikeshop/clickstream/raw/year=2014/month=5/day=6'
;

--select * from raw_log limit 5
