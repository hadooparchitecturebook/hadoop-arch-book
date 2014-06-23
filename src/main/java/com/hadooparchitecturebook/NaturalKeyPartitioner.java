package com.hadooparchitecturebook;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;
import com.hadooparchitecturebook.IpTimestampKey;

/**
 * We use this class as our custom partitioner in the MapReducer job. We need this because we want to use only
 * the natural key portion (i.e. the IP address) of the composite key when splitting the records into partitions
 * to be sent to various reducers. For more details, refer to the comment in main() of MRSessionize class.
 */
public class NaturalKeyPartitioner extends Partitioner<IpTimestampKey, Text> {

    @Override
    public int getPartition(IpTimestampKey key, Text value, int numPartitions) {
        return Math.abs(key.getIp().hashCode()) % numPartitions;
    }
}
