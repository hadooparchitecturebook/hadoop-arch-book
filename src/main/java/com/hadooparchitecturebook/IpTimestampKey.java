package com.hadooparchitecturebook;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * The composite key to be used for the map output key and the reduce input key.
 * We need this composite key because we want to do secondary sorting on the data entering the reducers
 * based on the timestamps. For more details, refer to the main() of the MRSessionize.java
 */
public class IpTimestampKey implements WritableComparable<IpTimestampKey> {
    private String ip;
    private Long unixTimestamp;

    IpTimestampKey() {
    }

    IpTimestampKey(String ip, Long unixTimestamp) {
        this.ip = ip;
        this.unixTimestamp = unixTimestamp;
    }

    public String getIp() {
        return ip;
    }

    public Long getUnixTimestamp() {
        return unixTimestamp;
    }

    @Override
    public int compareTo(IpTimestampKey ipTimestampKey) {
        int result = ip.compareTo(ipTimestampKey.ip);
        if (result == 0) {
            result = unixTimestamp.compareTo(ipTimestampKey.unixTimestamp);
        }
        return result;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        WritableUtils.writeString(dataOutput, ip);
        dataOutput.writeLong(unixTimestamp);

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        ip = WritableUtils.readString(dataInput);
        unixTimestamp = dataInput.readLong();

    }
}
