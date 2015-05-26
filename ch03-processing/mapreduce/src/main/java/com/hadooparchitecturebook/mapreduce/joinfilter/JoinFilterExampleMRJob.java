package com.hadooparchitecturebook.mapreduce.joinfilter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.cloudera.sa.examples.crunch.JoinFilterExampleCrunch;

public class JoinFilterExampleMRJob implements Tool {
  public static final String FOO_TABLE_CONF = "custom.foo.table.file";
  public static final String BAR_TABLE_CONF = "custom.bar.table.file";
  public static final String FOO_VAL_MAX_CONF = "custom.foo.val.max";
  public static final String JOIN_VAL_MAX_CONF = "custom.join.val.max";

  public static final String FOO_SORT_FLAG = "B";
  public static final String BAR_SORT_FLAG = "A";

  public static void main(String[] args) throws Exception {
    ToolRunner.run(new Configuration(), new JoinFilterExampleMRJob(), args);
  }
  
  public int run(String[] args) throws Exception {
    String inputFoo = args[0];
    String inputBar = args[1];
    String output = args[2];
    String fooValueMaxFilter = args[3];
    String joinValueMaxFilter = args[4];
    int numberOfReducers = Integer.parseInt(args[5]);

    //A
    Job job = Job.getInstance();

    //B
    job.setJarByClass(JoinFilterExampleMRJob.class);
    job.setJobName("JoinFilterExampleMRJob");

    //C
    Configuration config = job.getConfiguration();
    config.set(FOO_TABLE_CONF, inputFoo);
    config.set(BAR_TABLE_CONF, inputBar);
    config.set(FOO_VAL_MAX_CONF, fooValueMaxFilter);
    config.set(JOIN_VAL_MAX_CONF, joinValueMaxFilter);

    // D
    job.setInputFormatClass(TextInputFormat.class);
    TextInputFormat.addInputPath(job, new Path(inputFoo));
    TextInputFormat.addInputPath(job, new Path(inputBar));

    // E
    job.setOutputFormatClass(TextOutputFormat.class);
    TextOutputFormat.setOutputPath(job, new Path(output));

    // F
    job.setMapperClass(JoinFilterMapper.class);
    job.setReducerClass(JoinFilterReducer.class);
    job.setPartitionerClass(JoinFilterPartitioner.class);

    // G
    job.setOutputKeyClass(NullWritable.class);
    job.setOutputValueClass(Text.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);

    //H
    job.setNumReduceTasks(numberOfReducers);

    // I
    job.waitForCompletion(true);
    return 0;
  }

  @Override
  public void setConf(Configuration conf) {
    // TODO Auto-generated method stub
    
  }

  @Override
  public Configuration getConf() {
    // TODO Auto-generated method stub
    return null;
  }
}
