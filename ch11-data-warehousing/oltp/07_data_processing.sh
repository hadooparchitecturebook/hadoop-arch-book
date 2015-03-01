#!/bin/bash
sudo -u hdfs hadoop fs -mkdir -p  /etl/movielens/user_rating_part
sudo -u hdfs hadoop fs -chown -R $USER: /etl/movielens/user_rating_part
hive -e "CREATE EXTERNAL TABLE IF NOT EXISTS
user_rating_part(id INT, timestamp BIGINT, user_id INT, movie_id INT, rating INT)
PARTITIONED BY (year INT, month INT, day INT)
ROW FORMAT SERDE 'parquet.hive.serde.ParquetHiveSerDe'
STORED AS
INPUTFORMAT 'parquet.hive.DeprecatedParquetInputFormat'
OUTPUTFORMAT 'parquet.hive.DeprecatedParquetOutputFormat'
LOCATION '/etl/movielens/user_rating_part'"

sudo -u hdfs hadoop fs -mkdir -p  /etl/movielens/user_rating
sudo -u hdfs hadoop fs -chown -R $USER: /etl/movielens/user_rating
# user_rating that compacts user_rating_fact to a single rating per user per movie
# No need for id's here, since multiple user_rating_fact id's would potentially get squashed into 1
# in this table, anyways.
hive -e "CREATE EXTERNAL TABLE IF NOT EXISTS
user_rating(user_id INT, movie_id INT, rating INT, last_modified BIGINT)
ROW FORMAT SERDE 'parquet.hive.serde.ParquetHiveSerDe'
STORED AS
INPUTFORMAT 'parquet.hive.DeprecatedParquetInputFormat'
OUTPUTFORMAT 'parquet.hive.DeprecatedParquetOutputFormat'
LOCATION '/etl/movielens/user_rating'"

hive -e "
INSERT INTO TABLE user_rating
SELECT
  user_id,
  movie_id,
  UNIX_TIMESTAMP(SPLIT(MAX(CONCAT(FROM_UNIXTIME(timestamp), ',', rating)), ',')[0]) AS last_modified,
  SPLIT(MAX(CONCAT(FROM_UNIXTIME(timestamp), ',', rating)), ',')[1] AS rating
FROM
  user_rating_fact
GROUP BY
  user_id,
  movie_id"

hive -e "
SET hive.exec.dynamic.partition.mode=nonstrict;
SET  hive.exec.dynamic.partition=true;
INSERT INTO TABLE user_rating_part PARTITION (year, month, day)
SELECT
  *,
  YEAR(last_modified),
  MONTH(last_modified),
  DAY(last_modified)
FROM
  user_rating"
