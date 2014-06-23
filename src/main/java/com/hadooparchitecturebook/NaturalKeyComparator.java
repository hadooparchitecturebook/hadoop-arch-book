package com.hadooparchitecturebook;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import com.hadooparchitecturebook.IpTimestampKey;

/**
* This comparator is used during the shuffle process. This only considers the natural key part (i.e. just the IP address)
 * of the composite key. We only want to shuffle by the ip address, hence the compare() only takes the IP address
 * into account.
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
