#!/bin/bash
SQOOP_METASTORE_HOST=localhost

sudo -u hdfs hadoop fs -mkdir -p  /data/movielens/user_rating_fact
sudo -u hdfs hadoop fs -chown -R ${USER}: /data/movielens/user_rating_fact

# Delete the job if it already exists. If it doesn't exist, the last ||: 
# ensures, the exit code is still success and the script continues on.
sqoop job --delete user_rating_import --meta-connect \
jdbc:hsqldb:hsql://${SQOOP_METASTORE_HOST}:16000/sqoop || :

# TODO: Made minor changes. Test this
# Need to explictly export HIVE_HOME before this command if Hive
# is not present under /usr/lib/hive
# No need to do so if you are using Apache Sqoop 1.4.6 or later
sqoop job --create user_rating_import --meta-connect \
jdbc:hsqldb:hsql://${SQOOP_METASTORE_HOST}:16000/sqoop \
-- import --connect jdbc:mysql://mgrover-haa-2.vpc.cloudera.com:3306/oltp \
--username root --table user_rating -m 8 --incremental lastmodified \
--check-column timestamp --append --as-parquetfile --hive-import \
--warehouse-dir /data/movielens --hive-table user_rating_fact

# No need to create a table explictly.
# Sqoop does that for ya.

#Create user_rating_fact table in hive
#hive -e "CREATE EXTERNAL TABLE IF NOT EXISTS user_rating_fact(
#  id INT,
#  timestamp TIMESTAMP,
#  user_id INT,
#  movie_id INT,
#  rating INT)
#ROW FORMAT SERDE 'parquet.hive.serde.ParquetHiveSerDe'
#STORED AS
#INPUTFORMAT 'parquet.hive.DeprecatedParquetInputFormat'
#OUTPUTFORMAT 'parquet.hive.DeprecatedParquetOutputFormat'
#LOCATION '/data/movielens/user_rating_fact'"

sqoop job -exec user_rating_import --meta-connect \
jdbc:hsqldb:hsql://${SQOOP_METASTORE_HOST}:16000/sqoop

