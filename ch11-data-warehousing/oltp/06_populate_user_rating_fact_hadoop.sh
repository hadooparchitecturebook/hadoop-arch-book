#!/bin/bash
SQOOP_METASTORE_HOST=localhost

sudo -u hdfs hadoop fs -mkdir -p  /etl/movielens/user_rating_fact
sudo -u hdfs hadoop fs -chown -R $USER: /etl/movielens/user_rating_fact

sqoop job --delete user_rating_import --meta-connect jdbc:hsqldb:hsql://${SQOOP_METASTORE_HOST}:16000/sqoop || :

sqoop job --create user_rating_import --meta-connect jdbc:hsqldb:hsql://${SQOOP_METASTORE_HOST}:16000/sqoop \
-- import --connect jdbc:mysql://mgrover-haa-2.vpc.cloudera.com:3306/oltp --username root \
--table user_rating -m 8 --incremental append --check-column timestamp \
--as-parquetfile --hive-import --warehouse-dir /etl/movielens/user_rating_fact --hive-table user_rating_fact

# The above command fails. We can either get rid of all hive things from the term or have to explicitly define
# HIVE_HOME and export it before we run this. Otherwise it fails with a misleading error message org.kitesdk.data.DatasetNotFoundException: Unknown dataset URI: hive?dataset=user_rating_fact

#Create user_rating_fact table in hive
hive -e "CREATE EXTERNAL TABLE IF NOT EXISTS user_rating_fact(id INT, timestamp TIMESTAMP,
 user_id INT, movie_id INT, rating INT)
ROW FORMAT SERDE 'parquet.hive.serde.ParquetHiveSerDe'
STORED AS
INPUTFORMAT 'parquet.hive.DeprecatedParquetInputFormat'
OUTPUTFORMAT 'parquet.hive.DeprecatedParquetOutputFormat'
LOCATION '/etl/movielens/user_rating_fact'"

sqoop job -exec user_rating_import --meta-connect jdbc:hsqldb:hsql://${SQOOP_METASTORE_HOST}:16000/sqoop

