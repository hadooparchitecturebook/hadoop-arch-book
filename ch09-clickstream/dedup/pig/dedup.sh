#!/bin/sh

sudo -u hdfs hadoop fs -rm -r /etl/BI/casualcyclist/clicks/deduplogs/year=2014/month=10/day=10 || :

sudo -u hdfs hadoop fs -mkdir -p /etl/BI/casualcyclist/clicks/deduplogs/year=2014/month=10
sudo -u hdfs hadoop fs -chmod -R 1777 /etl/BI/casualcyclist/clicks/deduplogs

pig -x mapreduce -param raw_log_dir='/etl/BI/casualcyclist/clicks/rawlogs/year=2014/month=10/day=10' -param deduped_log_dir='/etl/BI/casualcyclist/clicks/deduplogs/year=2014/month=10/day=10' dedup.pig
