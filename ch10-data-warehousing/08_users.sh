#!/bin/bash
SQOOP_METASTORE_HOST=localhost
sudo -u hdfs hadoop fs -mkdir -p /data/movielens/user_history
sudo -u hdfs hadoop fs -chown -R ${USER}: /data/movielens/user_history

sudo -u hdfs hadoop fs -rm -r /etl/movielens/user_upserts || :
sudo -u hdfs hadoop fs -mkdir -p /etl/movielens/user_upserts
sudo -u hdfs hadoop fs -chown -R ${USER}: /etl/movielens/user_upserts

sudo -u hdfs hadoop fs -rm -r /user/hive/warehouse/user || :
sudo -u hdfs hadoop fs -mkdir -p /data/movielens/user
sudo -u hdfs hadoop fs -chown -R ${USER}: /data/movielens/user

DT=$(date "+%Y-%m-%d")

sudo -u hdfs hadoop fs -rm -r /data/movielens/user_${DT} || :
sudo -u hdfs hadoop fs -mkdir -p /data/movielens/user_${DT}
sudo -u hdfs hadoop fs -chown -R ${USER}: /data/movielens/user_${DT}

hive -e \
"CREATE EXTERNAL TABLE IF NOT EXISTS user_upserts(
  id INT,
  age INT,
  occupation STRING,
  zipcode STRING,
  last_modified BIGINT)
STORED AS AVRO
LOCATION '/etl/movielens/user_upserts'"

hive -e \
"CREATE EXTERNAL TABLE IF NOT EXISTS user_history(
  id INT,
  age INT,
  occupation STRING,
  zipcode STRING,
  last_modified BIGINT)
PARTITIONED BY (year INT, month INT, day INT)
STORED AS AVRO
LOCATION '/data/movielens/user_history'"

hive -e \
"CREATE EXTERNAL TABLE IF NOT EXISTS user(
  id INT,
  age INT,
  occupation STRING,
  zipcode STRING,
  last_modified TIMESTAMP)
ROW FORMAT SERDE 'parquet.hive.serde.ParquetHiveSerDe'
STORED AS
INPUTFORMAT 'parquet.hive.DeprecatedParquetInputFormat'
OUTPUTFORMAT 'parquet.hive.DeprecatedParquetOutputFormat'
LOCATION '/data/movielens/user'"

hive -e \
"CREATE EXTERNAL TABLE IF NOT EXISTS user_tmp(
  id INT,
  age INT,
  occupation STRING,
  zipcode STRING,
  last_modified TIMESTAMP)
ROW FORMAT SERDE 'parquet.hive.serde.ParquetHiveSerDe'
STORED AS
INPUTFORMAT 'parquet.hive.DeprecatedParquetInputFormat'
OUTPUTFORMAT 'parquet.hive.DeprecatedParquetOutputFormat'
LOCATION '/data/movielens/user_${DT}'"

sqoop job --delete user_upserts_import --meta-connect \
jdbc:hsqldb:hsql://${SQOOP_METASTORE_HOST}:16000/sqoop

# Couldn't do Parquet because there are some bugs related to incremental import
# and Parquet integration
sqoop job --create user_upserts_import --meta-connect \
jdbc:hsqldb:hsql://${SQOOP_METASTORE_HOST}:16000/sqoop \
-- import --connect jdbc:mysql://mgrover-haa-2.vpc.cloudera.com:3306/oltp --username root \
-m 8 --incremental append --check-column last_modified --split-by last_modified \
--as-avrodatafile --query \
'SELECT user.id, user.age, user.gender, 
occupation.occupation, zipcode, last_modified
FROM user
JOIN occupation ON (user.occupation_id = occupation.id)
WHERE ${CONDITIONS}' \
--target-dir /etl/movielens/user_upserts

sqoop job -exec user_upserts_import --meta-connect \
jdbc:hsqldb:hsql://${SQOOP_METASTORE_HOST}:16000/sqoop

hive -e "
INSERT OVERWRITE TABLE user_tmp
  SELECT
    user.*
  FROM
    user
    LEFT OUTER JOIN
    user_upserts
    ON (user.id = user_upserts.id)
  WHERE
    user_upserts.id IS NULL
  UNION ALL
  SELECT
    id, age, occupation, zipcode, TIMESTAMP(last_modified)
  FROM
    user_upserts"

YEAR=$(date "+%Y")
MONTH=$(date "+%m")
DAY=$(date "+%d")

sudo -u hdfs hadoop fs -mkdir -p \
/data/movielens/user_history/year=${YEAR}/month=${MONTH}/day=${DAY}
sudo -u hdfs hadoop fs -mv /etl/movielens/user_upserts/* \
/data/movielens/user_history/year=${YEAR}/month=${MONTH}/day=${DAY}

hive -e "ALTER TABLE user_history 
ADD PARTITION (year=${YEAR}, month=${MONTH}, day=${DAY})"
