package com.hadooparchitecturebook;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
* Created by mgrover on 6/22/14.
*/
public class NaturalKeyComparator extends WritableComparator {
    NaturalKeyComparator() {
        super(IpTimestampKey.class, true);
    }

    @Override
    public int compare(WritableComparable r1, WritableComparable r2) {
        IpTimestampKey key1 = (IpTimestampKey) r1;
        IpTimestampKey key2 = (IpTimestampKey) r2;

        return key1.getIp().compareTo(key2.getIp());
    }
}
