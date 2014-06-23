package com.hadooparchitecturebook;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
* Created by mgrover on 6/22/14.
*/
class NaturalKeyPartitioner extends Partitioner<IpTimestampKey, Text> {

    @Override
    public int getPartition(IpTimestampKey key, Text value, int numPartitions) {
        return key.getIp().hashCode() % numPartitions;
    }
}
