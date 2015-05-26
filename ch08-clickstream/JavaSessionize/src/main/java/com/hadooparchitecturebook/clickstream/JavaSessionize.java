package com.hadooparchitecturebook.clickstream;

import JavaSessionize.avro.LogLine;
import com.google.common.collect.Lists;
import org.apache.avro.io.*;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import parquet.avro.AvroParquetOutputFormat;
import parquet.avro.AvroWriteSupport;
import parquet.hadoop.ParquetOutputFormat;
import scala.Tuple2;

import java.io.IOException;
import java.io.Serializable;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.Date;

import com.google.common.io.Files;

import java.io.File;
import java.io.ObjectStreamException;

/**
 * Created by gshapira on 5/11/14.
 */
public final class JavaSessionize {

    public static final List<String> testLines = Lists.newArrayList(
            "233.19.62.103 - 16261 [15/Sep/2013:23:55:57] \"GET /code.js " +
                    "HTTP/1.0\" 200 3667 " +
                    "\"http://www.loudacre.com\"  \"Loudacre Mobile Browser " +
                    "Sorrento F10L\"",
            "16.180.70.237 - 128 [15/Sep/2013:23:59:53] \"GET /KBDOC-00031" +
                    ".html HTTP/1.0\" 200 1388 " +
                    "\"http://www.loudacre.com\"  \"Loudacre CSR Browser\"",
            "116.180.70.237 - 128 [15/Sep/2013:23:59:53] \"GET /theme.css " +
                    "HTTP/1.0\" 200 5531 " +
                    "\"http://www.loudacre.com\"  \"Loudacre CSR Browser\"",
            "116.180.70.237 - 128 [15/Sep/2013:23:59:53] \"GET /theme.css " +
                    "HTTP/1.0\" 200 5531 "
                    + "\"http://www.loudacre.com\"  \"Loudacre CSR Browser\""
    );


    public static final Pattern apacheLogRegex = Pattern.compile(
            "(\\d+.\\d+.\\d+.\\d+).*\\[(.*)\\].*GET (\\S+) \\S+ (\\d+) (\\d+)" +
                    " (\\S+) (.*)");

    public static File temp = Files.createTempDir();
    public static String outputPath = new File(temp,
            "output").getAbsolutePath();

    public static class SerializableLogLine extends LogLine implements
            Serializable {

        private void setValues(LogLine line) {
            setIp(line.getIp());
            setTimestamp(line.getTimestamp());
            setUrl(line.getUrl());
            setReferrer(line.getReferrer());
            setUseragent(line.getUseragent());
            setSessionid(line.getSessionid());
        }

        public SerializableLogLine(LogLine line) {
            setValues(line);
        }

        private void writeObject(java.io.ObjectOutputStream out)
                throws IOException {
            DatumWriter<LogLine> writer = new SpecificDatumWriter<LogLine>
                    (LogLine.class);
            Encoder encoder = EncoderFactory.get().binaryEncoder(out, null);
            writer.write(this, encoder);
            encoder.flush();
        }

        private void readObject(java.io.ObjectInputStream in)
                throws IOException, ClassNotFoundException {
            DatumReader<LogLine> reader =
                    new SpecificDatumReader<LogLine>(LogLine.class);
            Decoder decoder = DecoderFactory.get().binaryDecoder(in, null);
            setValues(reader.read(null, decoder));
        }

        private void readObjectNoData()
                throws ObjectStreamException {
        }

        @Override
        public int compareTo(SpecificRecord o) {
            if (this == o) return 0;
            if (o instanceof SerializableLogLine) {
                SerializableLogLine that = (SerializableLogLine) o;
                if (this.getTimestamp() < that.getTimestamp()) return -1;
                if (this.getTimestamp() > that.getTimestamp()) return 1;
                return 0;
            } else {
                throw new IllegalArgumentException("Can only compare two " +
                        "LogLines");
            }
        }

        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("{").
                    append("ip: ").append(getIp()).
                    append("timeStamp: ").append(new Date(getTimestamp())).
                    append("url: ").append(getUrl()).
                    append("referrer: ").append(getReferrer()).
                    append("userAgent: ").append(getUseragent()).
                    append("session id: ").append(getSessionid()).
                    append("}");

            return sb.toString();
        }

    }

    // get the IP of the click event, to use as a user identified
    public static String getIP(String line) {
        System.out.println("Line:" + line);
        System.out.println("regex:" + apacheLogRegex);
        Matcher m = apacheLogRegex.matcher(line);
        if (m.find())
            return m.group(1);
        else {
            System.out.println("no match");
            return "0";
        }
    }


    // get all the relevant fields of the event
    public static SerializableLogLine getFields(String line) throws
            ParseException {
        Matcher m = apacheLogRegex.matcher(line);
        if (m.find()) {
            String ip = m.group(1);
            Date timeStamp = new SimpleDateFormat("dd/MMM/yyyy:hh:mm:ss")
                    .parse(m.group(2));
            String url = m.group(3);
            String referrer = m.group(6);
            String userAgent = m.group(7);
            return new SerializableLogLine(new LogLine(ip,
                    timeStamp.getTime(), url, referrer, userAgent, 0));
        } else {
            System.out.println("no match");
            return new SerializableLogLine(new LogLine("0",
                    new Date().getTime(), "", "", "", 0));
        }
    }

    public static List<SerializableLogLine> sessionize
            (List<SerializableLogLine> lines) {
        List<SerializableLogLine> sessionizedLines = new
                ArrayList<SerializableLogLine>(lines);
        Collections.sort(sessionizedLines);
        int sessionId = 0;
        sessionizedLines.get(0).setSessionid(sessionId);
        for (int i = 1; i < sessionizedLines.size(); i++) {
            SerializableLogLine thisLine = sessionizedLines.get(i);
            SerializableLogLine prevLine = sessionizedLines.get(i - 1);

            if (thisLine.getTimestamp() - prevLine.getTimestamp() > 30 * 60 *
                    1000) {
                sessionId++;
            }
            thisLine.setSessionid(sessionId);
        }

        return sessionizedLines;
    }

    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            System.err.println("Usage: JavaSessionize <master> [input file]");
            System.exit(1);
        }

        System.out.println("Output:" + outputPath);

        JavaSparkContext jsc = new JavaSparkContext(args[0],
                "JavaSessionize",
                System.getenv("SPARK_HOME"),
                JavaSparkContext.jarOfClass(JavaSessionize.class));

        JavaRDD<String> dataSet = (args.length == 2) ? jsc.textFile(args[1])
                : jsc.parallelize(testLines);

        // @formatter:off
        JavaPairRDD<String, SerializableLogLine> parsed = dataSet.map(
                new PairFunction<String, String, SerializableLogLine>() {
                    // @formatter:on
                    @Override
                    public Tuple2<String, SerializableLogLine> call(String s)
                            throws
                            Exception {
                        return new Tuple2<String, SerializableLogLine>(getIP(s),
                                getFields(s));
                    }
                });

        // This groups clicks by IP address
        JavaPairRDD<String, List<SerializableLogLine>> grouped = parsed
                .groupByKey();

        JavaPairRDD<String, List<SerializableLogLine>> sessionized = grouped
                .mapValues(new Function<List<SerializableLogLine>,
                        List<SerializableLogLine>>() {
                    @Override
                    public List<SerializableLogLine> call
                            (List<SerializableLogLine> logLines)
                            throws
                            Exception {
                        return sessionize(logLines);
                    }
                });

        sessionized.foreach(new VoidFunction<Tuple2<String,
                List<SerializableLogLine>>>() {
            @Override
            public void call(Tuple2<String, List<SerializableLogLine>>
                                     stringListTuple2) throws Exception {
                System.out.println("IP: " + stringListTuple2._1());
                for (SerializableLogLine line : stringListTuple2._2()) {
                    System.out.println(line);
                }
            }
        });

        // right now sessionize is an RDD of pairs: <String,List<LogLine>>.
        // We want to output an RDD of <String,LogLine>
        // First, grab the Lists, then flatten them,
        // then pair them with something empty to make Hadoop happy

        // @formatter:off
        JavaRDD<List<SerializableLogLine>> nokeys = sessionized.map(
                new Function<Tuple2<String, List<SerializableLogLine>>,
                        List<SerializableLogLine>>() {
                    // @formatter:on
                    @Override
                    public List<SerializableLogLine> call(Tuple2<String,
                            List<SerializableLogLine>> stringListTuple2) throws
                            Exception {
                        return stringListTuple2._2();
                    }
                });

        // @formatter:off
        JavaRDD<SerializableLogLine> flatLines = nokeys.flatMap(
                new FlatMapFunction<List<SerializableLogLine>,
                        SerializableLogLine>() {
                    // @formatter:on
                    @Override
                    public Iterable<SerializableLogLine> call
                    (List<SerializableLogLine> serializableLogLines) throws
                            Exception {
                        return serializableLogLines;
                    }
                });

        JavaPairRDD<Void, SerializableLogLine> outputPairs = flatLines.map
                (new PairFunction<SerializableLogLine, Void,
                        SerializableLogLine>() {
                    @Override
                    public Tuple2<Void, SerializableLogLine> call
                            (SerializableLogLine
                                     serializableLogLine) throws Exception {
                        return new Tuple2<Void, SerializableLogLine>(null,
                                serializableLogLine);
                    }
                });

        Job job = new Job();

        ParquetOutputFormat.setWriteSupportClass(job, AvroWriteSupport.class);
        AvroParquetOutputFormat.setSchema(job, LogLine.SCHEMA$);

        //dummy instance, because that's the only way to get the class of a
        // parameterized type
        ParquetOutputFormat<LogLine> pOutput = new
                ParquetOutputFormat<LogLine>();

        //System.out.println("job write support - " +
        //        job.getConfiguration().get("parquet.write.support.class") +
        //        " job schema - " +  job.getConfiguration().get("parquet
        // .avro.schema"))  ;

        outputPairs.saveAsNewAPIHadoopFile(outputPath,    //path
                Void.class,               //key class
                LogLine.class,                    //value class
                pOutput.getClass(),               //output format class
                job.getConfiguration());          //configuration


    }
}
