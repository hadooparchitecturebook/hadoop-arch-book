#!/bin/bash
SQOOP_METASTORE_HOST=localhost

sudo -u hdfs hadoop fs -mkdir -p  /etl/movielens/user_rating_fact
sudo -u hdfs hadoop fs -chown -R $USER: /etl/movielens/user_rating_fact

sqoop job --delete user_rating_import --meta-connect jdbc:hsqldb:hsql://${SQOOP_METASTORE_HOST}:16000/sqoop || :

sqoop job --create user_rating_import --meta-connect jdbc:hsqldb:hsql://${SQOOP_METASTORE_HOST}:16000/sqoop \
-- import --connect jdbc:mysql://mgrover-haa-2.vpc.cloudera.com:3306/oltp --username root \
--table user_rating -m 8 --incremental append --check-column timestamp \
--as-parquetfile --hive-import --warehouse-dir /etl/movielens/user_rating_fact

# Need to create a hive table for user_rating_fact here!!

sqoop job -exec user_rating_import --meta-connect jdbc:hsqldb:hsql://${SQOOP_METASTORE_HOST}:16000/sqoop
