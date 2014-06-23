package com.hadooparchitecturebook;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;
import com.hadooparchitecturebook.IpTimestampKey;

/**
 * Created by mgrover on 6/22/14.
 */
public class NaturalKeyPartitioner extends Partitioner<IpTimestampKey, Text> {

    @Override
    public int getPartition(IpTimestampKey key, Text value, int numPartitions) {
        return Math.abs(key.getIp().hashCode()) % numPartitions;
    }
}
