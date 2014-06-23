/**
 * Created by mgrover on 6/22/14.
 */

package com.hadooparchitecturebook;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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

public class MRSessionize {
    private static final String LOG_RECORD_REGEX =
            "(\\\\d+.\\\\d+.\\\\d+.\\\\d+).*\\\\[(.*)\\\\].*GET (\\\\S*).*\\\\d+ \\\\d+ (\\\\S+) \\\"(.*)\\\"";
    private static final Pattern logRecordPattern = Pattern.compile(LOG_RECORD_REGEX);

    private static final String TIMESTAMP_PATTERN = "dd/MMM/yyyy:HH:mm:ss";
    private static final DateTimeFormatter TIMESTAMP_FORMATTER = DateTimeFormat.forPattern(TIMESTAMP_PATTERN);

    private static final int SESSION_TIMEOUT_IN_MINUTES = 30;
    private static final int SESSION_TIMEOUT_IN_MS = SESSION_TIMEOUT_IN_MINUTES * 60 * 1000;

    public static class SessionizeMapper
            extends Mapper<Object, Text, IpTimestampKey, Text> {

        private Matcher logRecordMatcher;

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            logRecordMatcher = logRecordPattern.matcher(value.toString());

            if (logRecordMatcher.matches()) {
                String ip = logRecordMatcher.group(1);
                DateTime timestamp = DateTime.parse(logRecordMatcher.group(2), TIMESTAMP_FORMATTER);
                Long unixTimestamp = timestamp.getMillis();
                IpTimestampKey outputKey = new IpTimestampKey(ip, unixTimestamp);
                context.write(outputKey, value);
            }

        }
    }

    public static class SessionizeReducer
            extends Reducer<IpTimestampKey, Text, Text, Text> {
        private Text result = new Text();
        private static int sessionId = 0;
        private Long lastTimeStamp = null;

        public void reduce(IpTimestampKey key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            for (Text value : values) {
                String logRecord = value.toString();
                if (lastTimeStamp == null || (key.getUnixTimestamp() - lastTimeStamp > SESSION_TIMEOUT_IN_MS)) {
                    sessionId++;
                }
                lastTimeStamp = key.getUnixTimestamp();
                result.set(logRecord + " " + sessionId);
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
        Job job = new Job(conf, "MRSessionize");
        job.setJarByClass(MRSessionize.class);
        job.setMapperClass(SessionizeMapper.class);
        // WARNING: do NOT set the Combiner class
        // from the same IP in one place before we can do sessionization
        // Also, our reducer doesn't return the same key,value types it takes
        // It can't be used on the result of a previous reducer
        job.setReducerClass(SessionizeReducer.class);
        //job.setOutputKeyClass(null);
        //job.setOutputValueClass(Text.class);

        job.setPartitionerClass(NaturalKeyPartitioner.class);
        job.setGroupingComparatorClass(NaturalKeyComparator.class);
        job.setSortComparatorClass(CompositeKeyComparator.class);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}