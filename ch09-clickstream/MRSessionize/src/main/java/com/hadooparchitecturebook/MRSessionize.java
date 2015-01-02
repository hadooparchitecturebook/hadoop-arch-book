package com.hadooparchitecturebook;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.ObjectUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

/**
 * This is the main class that is responsible for doing sessionization in MapReduce
 */
public class MRSessionize {

    private static final String LOG_RECORD_REGEX =
            "(\\d+.\\d+.\\d+.\\d+).*\\[(.*) \\].*GET (\\S*).*\\d+ \\d+ (\\S+) \\\"(.*)\\\"";
    private static final Pattern logRecordPattern = Pattern.compile(LOG_RECORD_REGEX);

    private static final String TIMESTAMP_PATTERN = "dd/MMM/yyyy:HH:mm:ss";
    private static final DateTimeFormatter TIMESTAMP_FORMATTER = DateTimeFormat.forPattern(TIMESTAMP_PATTERN);

    // In our sessionization algorithm, a session expires in 30 minutes. Another click from the same user after 30 or
    // more minutes is a part of a new session
    private static final int SESSION_TIMEOUT_IN_MINUTES = 30;
    private static final int SESSION_TIMEOUT_IN_MS = SESSION_TIMEOUT_IN_MINUTES * 60 * 1000;

    /**
     * Mapper class used in Sessionization
     */
    public static class SessionizeMapper
            extends Mapper<Object, Text, IpTimestampKey, Text> {

        private Matcher logRecordMatcher;

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            logRecordMatcher = logRecordPattern.matcher(value.toString());

            // We only emit something out if the record matches with our regex. Otherwise, we assume the record is
            // busted and simply ignore it
            if (logRecordMatcher.matches()) {
                String ip = logRecordMatcher.group(1);
                DateTime timestamp = DateTime.parse(logRecordMatcher.group(2), TIMESTAMP_FORMATTER);
                Long unixTimestamp = timestamp.getMillis();
                IpTimestampKey outputKey = new IpTimestampKey(ip, unixTimestamp);
                context.write(outputKey, value);
            }

        }
    }

    // TODO: NEED TO BUNDLE JODA TIME IN ASSEMBLY

    /**
     * Reducer class used in Sessionization
     */
    public static class SessionizeReducer
            extends Reducer<IpTimestampKey, Text, IpTimestampKey, Text> {
        private Text result = new Text();

        public void reduce(IpTimestampKey key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            // The sessionId generated here is per day, per IP. So, any queries
            // that will be done as if this session ID were global, would require
            // a combination of the day in question and IP as well.
            int sessionId = 0;
            Long lastTimeStamp = null;
            for (Text value : values) {
                String logRecord = value.toString();
                // If this is the first record for this user or it's been more than the timeout since
                // the last click from this user, let's increment the session ID.
                if (lastTimeStamp == null || (key.getUnixTimestamp() - lastTimeStamp > SESSION_TIMEOUT_IN_MS)) {
                    sessionId++;
                }
                lastTimeStamp = key.getUnixTimestamp();
                result.set(logRecord + " " + sessionId);
                // Since we only care about printing out the entire record in the result, with session ID appended
                // at the end, we just emit out "null" for the key
                context.write(null, result);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: MRSessionize <in> <out>");
            System.exit(2);
        }
        Job job = new Job(conf, "MapReduce Sessionization");
        job.setJarByClass(MRSessionize.class);
        job.setMapperClass(SessionizeMapper.class);
        job.setReducerClass(SessionizeReducer.class);

        // WARNING: do NOT set the Combiner class
        // from the same IP in one place before we can do sessionization
        // Also, our reducer doesn't return the same key,value types it takes
        // It can't be used on the result of a previous reducer

        job.setMapOutputKeyClass(IpTimestampKey.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        // We need these for secondary sorting.
        // We need to shuffle the records (between Map and Reduce phases) by using IP address as key, since that is
        // the field we are using for determining uniqueness of users. However, when the records arrive to the reducers,
        // we would like them to be sorted in ascending order of their timestamps. This concept is known as secondary
        // sorting since we are "secondarily" sorting the records by another key (timestamp, in our case) in addition
        // to the shuffle key (also called the "partition" key).

        // So, to get some terminology straight.
        // Natural key (aka Shuffle key or Partition key) is the key we use to shuffle. IP address in our case
        // Secondary Sorting Key is the key we use to sort within each partition that gets sent to the user. Timestamp
        // in our case.
        // Together, the natural key and secondary sorting key form what we call the composite key. This key is called
        // IpTimestampKey in our example.

        // For secondary sorting, even though we are partitioning and shuffling by only the natural key, the map output
        // key and the reduce input key is the composite key. We, however, use a custom partitioner and custom grouping
        // comparator that only uses the natural key part of the composite key to partition and group respectively (both
        // happen during the shuffle phase).

        // However, we have a different sort comparator which also gets used in the shuffle phase but determines how
        // the records are sorted when they enter the reduce phase. This custom sort comparator in our case will make use
        // of the entire composite key.

        // We found http://vangjee.wordpress.com/2012/03/20/secondary-sorting-aka-sorting-values-in-hadoops-mapreduce-programming-paradigm/
        // to be very helpful, if you'd like to read more on the subject.
        job.setPartitionerClass(NaturalKeyPartitioner.class);
        job.setGroupingComparatorClass(NaturalKeyComparator.class);
        job.setSortComparatorClass(CompositeKeyComparator.class);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
