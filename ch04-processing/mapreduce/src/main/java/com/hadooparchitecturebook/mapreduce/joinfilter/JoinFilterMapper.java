package com.hadooparchitecturebook.mapreduce.joinfilter;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class JoinFilterMapper extends 
  Mapper<LongWritable, Text, Text, Text> {

  boolean isFooBlock = false;
  int fooValFilter;

  public static final int FOO_ID_INX = 0;
  public static final int FOO_VALUE_INX = 1;
  public static final int FOO_BAR_ID_INX = 2;
  public static final int BAR_ID_INX = 0;
  public static final int BAR_VALUE_INX = 1;

  Text newKey = new Text();
  Text newValue = new Text();

  @Override
  public void setup(Context context) {

    //A
    Configuration config = context.getConfiguration();
    fooValFilter = config.getInt(JoinFilterExampleMRJob.FOO_VAL_MAX_CONF, -1);
    
    //B
    String fooRootPath = config.get(JoinFilterExampleMRJob.FOO_TABLE_CONF);
    FileSplit split = (FileSplit) context.getInputSplit();
    if (split.getPath().toString().contains(fooRootPath)) {
      isFooBlock = true;
    }
  }

  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {

    String[] cells = StringUtils.split(value.toString(), "|");

    //C
    if (isFooBlock) {
      int fooValue = Integer.parseInt(cells[FOO_VALUE_INX]);

      if (fooValue <= fooValFilter) {
        newKey.set(cells[FOO_BAR_ID_INX] + "|"
            + JoinFilterExampleMRJob.FOO_SORT_FLAG);
        newValue.set(cells[FOO_ID_INX] + "|" + cells[FOO_VALUE_INX]);
        //D
        context.write(newKey, newValue);
      } else {
        //E
        context.getCounter("Custom", "FooValueFiltered").increment(1);
      }
    } else {
      newKey
          .set(cells[BAR_ID_INX] + "|" + JoinFilterExampleMRJob.BAR_SORT_FLAG);
      newValue.set(cells[BAR_VALUE_INX]);
      context.write(newKey, newValue);
    }
  }
}
