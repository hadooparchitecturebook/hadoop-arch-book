#!/bin/bash
SQOOP_METASTORE_HOST=localhost
sudo -u hdfs hadoop fs -mkdir -p /etl/movielens/user_history
sudo -u hdfs hadoop fs -chown -R $USER: /etl/movielens/user_history

sudo -u hdfs hadoop fs -rm -r /etl/movielens/user_upserts || :
sudo -u hdfs hadoop fs -mkdir -p /etl/movielens/user_upserts
sudo -u hdfs hadoop fs -chown -R $USER: /etl/movielens/user_upserts

sudo -u hdfs hadoop fs -mkdir -p /data/movielens/user
sudo -u hdfs hadoop fs -chown -R $USER: /data/movielens/user

hive -e \
"CREATE EXTERNAL TABLE IF NOT EXISTS user_upserts(id INT, age INT, occupation STRING, zipcode STRING, last_modified TIMESTAMP)
PARTITIONED BY (year INT, month INT, day INT)
ROW FORMAT SERDE 'parquet.hive.serde.ParquetHiveSerDe'
STORED AS
INPUTFORMAT 'parquet.hive.DeprecatedParquetInputFormat'
OUTPUTFORMAT 'parquet.hive.DeprecatedParquetOutputFormat'
LOCATION '/etl/movielens/user_upserts'"

hive -e \
"CREATE EXTERNAL TABLE IF NOT EXISTS user_history(id INT, age INT, occupation STRING, zipcode STRING, last_modified TIMESTAMP)
PARTITIONED BY (year INT, month INT, day INT)
ROW FORMAT SERDE 'parquet.hive.serde.ParquetHiveSerDe'
STORED AS
INPUTFORMAT 'parquet.hive.DeprecatedParquetInputFormat'
OUTPUTFORMAT 'parquet.hive.DeprecatedParquetOutputFormat'
LOCATION '/etl/movielens/user_history'"

hive -e \
"CREATE EXTERNAL TABLE IF NOT EXISTS user(id INT, age INT, occupation STRING, zipcode STRING, last_modified TIMESTAMP)
ROW FORMAT SERDE 'parquet.hive.serde.ParquetHiveSerDe'
STORED AS
INPUTFORMAT 'parquet.hive.DeprecatedParquetInputFormat'
OUTPUTFORMAT 'parquet.hive.DeprecatedParquetOutputFormat'
LOCATION '/data/movielens/user'"

sqoop job --delete user_upserts_import --meta-connect jdbc:hsqldb:hsql://${SQOOP_METASTORE_HOST}:16000/sqoop

# Need to explictly export HIVE_HOME before this command if Hive is not present under /usr/lib/hive
# No need to do this if you are using Apache Sqoop 1.4.6 or later
sqoop job --create user_upserts_import --meta-connect jdbc:hsqldb:hsql://${SQOOP_METASTORE_HOST}:16000/sqoop \
-- import --connect jdbc:mysql://mgrover-haa-2.vpc.cloudera.com:3306/oltp --username root --append \
-m 8 --incremental lastmodified --check-column last_modified --split-by last_modified \
--query 'SELECT user.id, user.age, user.gender, 
occupation.occupation, zipcode, last_modified FROM user JOIN occupation 
ON (user.occupation_id = occupation.id) WHERE $CONDITIONS' --as-parquetfile \
--hive-import --hive-table user_upserts --target-dir /etl/movielens/user_upserts

sqoop job -exec user_upserts_import --meta-connect jdbc:hsqldb:hsql://${SQOOP_METASTORE_HOST}:16000/sqoop

hive -e "
INSERT OVERWRITE TABLE user
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
    *
  FROM
    user_upserts"

YEAR=$(date "+%y")
MONTH=$(date "+%M")
DAY=$(date "+%d")

sudo -u hdfs hadoop fs -mkdir -p /etl/movielens/user_history/year=${YEAR}/month=${MONTH}/day={DAY}
sudo -u hdfs hadoop fs -mv /etl/movielens/user_upserts/* /etl/movielens/user_history/year=${YEAR}/month=${MONTH}/day=${DAY}

hive -e "ALTER TABLE user_history ADD PARTITION (year={env:YEAR}, month={env:MONTH}, day={env:DAY})"
