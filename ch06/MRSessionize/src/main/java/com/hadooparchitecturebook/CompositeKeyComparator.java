package com.hadooparchitecturebook;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import com.hadooparchitecturebook.IpTimestampKey;

/**
 * This comparator gets used as the Sort Comparator in the MR job. It takes both (ip address, timestamp) into account,
 * so the records when they go to the reducer are sorted in ascending order of timestamp for each ip address.
 */
public class CompositeKeyComparator extends WritableComparator {
    CompositeKeyComparator() {
        super(IpTimestampKey.class, true);
    }

    @Override
    public int compare(WritableComparable r1, WritableComparable r2) {
        IpTimestampKey key1 = (IpTimestampKey) r1;
        IpTimestampKey key2 = (IpTimestampKey) r2;

        int result = key1.getIp().compareTo(key2.getIp());
        if (result == 0) {
            result = key1.getUnixTimestamp().compareTo(key2.getUnixTimestamp());
        }
        return result;
    }
}
