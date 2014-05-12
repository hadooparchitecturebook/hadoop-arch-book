package com.apparchbook.clickstream;

import com.google.common.collect.Lists;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;
import java.io.Serializable;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.Date;

/**
 * Created by gshapira on 5/11/14.
 */
public final class JavaSessionize {

    public static final List<String> testLines = Lists.newArrayList(
            "233.19.62.103 - 16261 [15/Sep/2013:23:55:57] \"GET /code.js HTTP/1.0\" 200 3667 " +
                    "\"http://www.loudacre.com\"  \"Loudacre Mobile Browser Sorrento F10L\"",
            "16.180.70.237 - 128 [15/Sep/2013:23:59:53] \"GET /KBDOC-00031.html HTTP/1.0\" 200 1388 " +
                    "\"http://www.loudacre.com\"  \"Loudacre CSR Browser\"",
            "116.180.70.237 - 128 [15/Sep/2013:23:59:53] \"GET /theme.css HTTP/1.0\" 200 5531 " +
                    "\"http://www.loudacre.com\"  \"Loudacre CSR Browser\"",
            "116.180.70.237 - 128 [15/Sep/2013:23:59:53] \"GET /theme.css HTTP/1.0\" 200 5531 "
                    + "\"http://www.loudacre.com\"  \"Loudacre CSR Browser\""
    );


    public static final Pattern apacheLogRegex = Pattern.compile(
            "(\\d+.\\d+.\\d+.\\d+).*\\[(.*)\\].*GET (\\S+) \\S+ (\\d+) (\\d+) (\\S+) (.*)");

    public static class LogLine implements Serializable, Comparable {
        public final String ip;
        public final Date timeStamp;
        public final String url;
        public final String referrer;
        public final String userAgent;
        public int sessionId;

        public LogLine(String ip, Date timeStamp, String url, String referrer, String userAgent) {
            this.ip = ip;
            this.timeStamp = timeStamp;
            this.url = url;
            this.referrer = referrer;
            this.userAgent = userAgent;
        }

        @Override
        public int compareTo(Object o) {
            if (this == o) return 0;
            if (o instanceof LogLine) {
                LogLine that = (LogLine) o;
                if (this.timeStamp.before(that.timeStamp)) return -1;
                if (this.timeStamp.after(that.timeStamp)) return 1;
                return 0;
            }
            else {
                throw new IllegalArgumentException("Can only compare two LogLines");
            }
        }

        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("{").
                    append("ip: ").append(ip).
                    append("timeStamp: ").append(timeStamp).
                    append("url: ").append(url).
                    append("referrer: ").append(referrer).
                    append("userAgent: ").append(userAgent).
                    append("session id: ").append(sessionId).
                    append("}");

            return sb.toString();
        }

    }

    // get the IP of the click event, to use as a user identified
    public static String getIP(String line){
        System.out.println("Line:" + line)  ;
        System.out.println("regex:" + apacheLogRegex)  ;
        Matcher m = apacheLogRegex.matcher(line);
        if (m.find())
            return m.group(1);
        else {
            System.out.println("no match");
            return "0";
        }
    }


    // get all the relevant fields of the event
    public static LogLine getFields(String line) throws ParseException {
        Matcher m = apacheLogRegex.matcher(line);
        if (m.find()) {
            String ip = m.group(1);
            Date timeStamp = new SimpleDateFormat("dd/MMM/yyyy:hh:mm:ss").parse(m.group(2));
            String url = m.group(3);
            String referrer = m.group(6);
            String userAgent = m.group(7);
            return new LogLine(ip,timeStamp,url,referrer,userAgent);
        } else {
            System.out.println("no match");
            return new LogLine("0",new Date(),"","","");
        }
    }

    public static List<LogLine> sessionize(List<LogLine> lines) {
        List<LogLine> sessionizedLines = new ArrayList<LogLine>(lines);
        Collections.sort(sessionizedLines);
        int sessionId = 0;
        sessionizedLines.get(0).sessionId = sessionId;
        for (int i=1; i<sessionizedLines.size();i++) {
            LogLine thisLine = sessionizedLines.get(i);
            LogLine prevLine = sessionizedLines.get(i-1);

            if (thisLine.timeStamp.getTime() - prevLine.timeStamp.getTime() > 30*60*1000) {
                sessionId++;
            }
            thisLine.sessionId = sessionId;
        }

        return sessionizedLines;
    }

    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            System.err.println("Usage: JavaSessionize <master> [input file]");
            System.exit(1);
        }

        JavaSparkContext jsc = new JavaSparkContext(args[0],
                "JavaSessionize",
                System.getenv("SPARK_HOME"),
                JavaSparkContext.jarOfClass(JavaSessionize.class));

        JavaRDD<String> dataSet = (args.length == 2) ? jsc.textFile(args[1]) : jsc.parallelize(testLines);

        JavaPairRDD<String,LogLine>  parsed = dataSet.map(new PairFunction<String, String, LogLine>() {
            @Override
            public Tuple2<String, LogLine> call(String s) throws Exception {
                return new Tuple2<String,LogLine>(getIP(s),getFields(s));
            }
        });

        JavaPairRDD<String,List<LogLine>> grouped = parsed.groupByKey();

        JavaPairRDD<String,List<LogLine>> sessionized = grouped.mapValues(new Function<List<LogLine>, List<LogLine>>() {
            @Override
            public List<LogLine> call(List<LogLine> logLines) throws Exception {
                return sessionize(logLines);
            }
        });

        sessionized.foreach(new VoidFunction<Tuple2<String, List<LogLine>>>() {
            @Override
            public void call(Tuple2<String, List<LogLine>> stringListTuple2) throws Exception {
                System.out.println("IP: " + stringListTuple2._1());
                for(LogLine line: stringListTuple2._2()){
                    System.out.println(line);
                }
            }
        });

    }
}