#!/bin/bash
SQOOP_METASTORE_HOST=localhost
sudo -u hdfs hadoop fs -mkdir -p /etl/movielens/user_history
sudo -u hdfs hadoop fs -chown -R $USER: /etl/movielens/user_history

sudo -u hdfs hadoop fs -mkdir -p /data/movielens/user
sudo -u hdfs hadoop fs -chown -R $USER: /data/movielens/user

hive -e \
"CREATE EXTERNAL TABLE IF NOT EXISTS user_history(id INT, age INT, occupation STRING, zipcode STRING, last_modified TIMESTAMP)
PARTITIONED BY (dt STRING)
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

# Need to explictly export HIVE_HOME before this command if Hive is not present under /usr/lib/hive
# No need to do this if you are using Apache Sqoop 1.4.6 or later
sqoop job --create user_history_import --meta-connect jdbc:hsqldb:hsql://${SQOOP_METASTORE_HOST}:16000/sqoop \
-- import --connect jdbc:mysql://mgrover-haa-2.vpc.cloudera.com:3306/oltp --username root \
-m 8 --incremental append --check-column last_modified --split-by last_modified \
--as-parquetfile --target-dir /etl/movielens/user_history/dt=$(date "+%Y-%m-%d") --query 'SELECT user.id, user.age, user.gender, occupation.occupation, zipcode, last_modified FROM user JOIN occupation ON (user.occupation_id = occupation.id) WHERE $CONDITIONS'

sqoop job -exec user_history_import --meta-connect jdbc:hsqldb:hsql://${SQOOP_METASTORE_HOST}:16000/sqoop

hive -e "
INSERT OVERWRITE TABLE user
  SELECT
    user.*
  FROM
    user
    LEFT OUTER JOIN
    FROM (
      SELECT
        *
      FROM
        user_history
      WHERE
        dt=$(date "+%Y-%m-%d")
      )latest_updates
      ON (user.id = latest_updates.id)
    WHERE
      latest_updates.id IS NULL
  UNION ALL
  SELECT
    *
  FROM
    user_history
  WHERE
    dt=$(date "+%Y-%m-%d")"
