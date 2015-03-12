package com.hadooparchitecturebook.mapreduce.joinfilter;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class JoinFilterReducer extends Reducer<Text, Text, NullWritable, Text> {

  int joinValFilter;
  String currentBarId = "";
  ArrayList<Integer> barBufferList = new ArrayList<Integer>();

  Text newValue = new Text();

  @Override
  public void setup(Context context) {
    Configuration config = context.getConfiguration();
    joinValFilter = config.getInt(JoinFilterExampleMRJob.JOIN_VAL_MAX_CONF, -1);
  }

  @Override
  public void reduce(Text key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {

    String keyString = key.toString();
    String barId = keyString.substring(0, keyString.length() - 2);
    String sortFlag = keyString.substring(keyString.length() - 1);

    System.out.println(keyString);

    if (!currentBarId.equals(barId)) {
      barBufferList.clear();
      currentBarId = barId;
    }

    if (sortFlag.equals(JoinFilterExampleMRJob.BAR_SORT_FLAG)) {
      for (Text value : values) {
        barBufferList.add(Integer.parseInt(value.toString()));
      }
    } else {
      if (barBufferList.size() > 0) {
        for (Text value : values) {
          for (Integer barValue : barBufferList) {

            String[] fooCells = StringUtils.split(value.toString(), "|");

            int fooValue = Integer.parseInt(fooCells[1]);
            int sumValue = barValue + fooValue;

            if (sumValue < joinValFilter) {

              newValue.set(fooCells[0] + "|" + barId + "|" + sumValue);
              context.write(NullWritable.get(), newValue);
            } else {
              context.getCounter("custom", "joinValueFiltered").increment(1);
            }
          }
        }
      } else {
        System.out.println("Matching with nothing");
      }
    }
  }
}
