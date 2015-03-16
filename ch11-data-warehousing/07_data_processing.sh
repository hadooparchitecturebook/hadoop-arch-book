#!/bin/bash
sudo -u hdfs hadoop fs -mkdir -p  /data/movielens/user_rating
sudo -u hdfs hadoop fs -chown -R ${USER}: /data/movielens/user_rating
# Compact user_rating_fact to a single rating per user per movie
# No need for id's here, since multiple user_rating_fact id's would potentially
# get squashed into 1 in this table, anyways.
hive -e "CREATE EXTERNAL TABLE IF NOT EXISTS user_rating(
  user_id INT,
  movie_id INT,
  rating INT,
  last_modified TIMESTAMP)
ROW FORMAT SERDE 'parquet.hive.serde.ParquetHiveSerDe'
STORED AS
INPUTFORMAT 'parquet.hive.DeprecatedParquetInputFormat'
OUTPUTFORMAT 'parquet.hive.DeprecatedParquetOutputFormat'
LOCATION '/data/movielens/user_rating'"

#Pick the latest rating given by the user to each movie
hive -e "
INSERT INTO TABLE user_rating
SELECT
  user_id,
  movie_id,
  SPLIT(MAX(CONCAT(FROM_UNIXTIME(INT(timestamp/1000)), ',', rating)), ',')[1]
    AS rating,
  SPLIT(MAX(CONCAT(FROM_UNIXTIME(INT(timestamp/1000)), ',', rating)), ',')[0]
    AS last_modified
FROM
  user_rating_fact
GROUP BY
  user_id,
  movie_id"


sudo -u hdfs hadoop fs -mkdir -p  /data/movielens/user_rating_part
sudo -u hdfs hadoop fs -chown -R ${USER}: /data/movielens/user_rating_part
hive -e "CREATE EXTERNAL TABLE IF NOT EXISTS user_rating_part(
  user_id INT,
  movie_id INT,
  rating INT,
  last_modified TIMESTAMP)
PARTITIONED BY (year INT, month INT, day INT)
ROW FORMAT SERDE 'parquet.hive.serde.ParquetHiveSerDe'
STORED AS
INPUTFORMAT 'parquet.hive.DeprecatedParquetInputFormat'
OUTPUTFORMAT 'parquet.hive.DeprecatedParquetOutputFormat'
LOCATION '/data/movielens/user_rating_part'"

hive -e "
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.dynamic.partition=true;
SET hive.exec.max.dynamic.partitions.pernode=1000;
INSERT INTO TABLE user_rating_part PARTITION (year, month, day)
SELECT
  *,
  YEAR(last_modified),
  MONTH(last_modified),
  DAY(last_modified)
FROM
  user_rating"
