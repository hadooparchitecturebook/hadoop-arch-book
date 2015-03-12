package com.hadooparchitecturebook.mapreduce.joinfilter;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class JoinFilterPartitioner extends Partitioner<Text, Text>{

  @Override
  public int getPartition(Text key, Text value, int numberOfReducers) {
    String keyStr = key.toString();
    
    return Math.abs(keyStr.substring(0, keyStr.length() - 2).hashCode()
        % numberOfReducers);
  }

}
