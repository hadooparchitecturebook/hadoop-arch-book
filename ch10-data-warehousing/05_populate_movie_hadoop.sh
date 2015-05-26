#!/bin/bash
AVRO_HOME=/opt/cloudera/parcels/CDH/lib/avro

sudo -u hdfs hadoop fs -mkdir -p /metadata/movielens/movie
sudo -u hdfs hadoop fs -chown -R ${USER}: /metadata/movielens/movie

# Clean up the destination schema file, if it exists. 
# The last ||: ensures that we carry on the script even 
# when the destination doesn't exist.
sudo -u hdfs hadoop fs -rm /metadata/movielens/movie/movie.avsc || :
hadoop jar ${AVRO_HOME}/avro-tools.jar getschema \
/data/movielens/movie/part-m-00000.avro | \
hadoop fs -put - /metadata/movielens/movie/movie.avsc

hive -e "CREATE EXTERNAL TABLE IF NOT EXISTS movie
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
STORED AS 
INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
LOCATION '/data/movielens/movie'
TBLPROPERTIES ('avro.schema.url'='/metadata/movielens/movie/movie.avsc')"
