package com.hadooparchitecturebook.movingavg.trident;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import storm.trident.testing.FixedBatchSpout;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

public class FixedBatchSpoutBuilder {
  public static FixedBatchSpout buildSpout() {
    List<Values> ticks = new FixedBatchSpoutBuilder().readData();
    FixedBatchSpout spout = new FixedBatchSpout(new Fields("stock-ticks"), 5, ticks.toArray(new Values[ticks.size()]));
    spout.setCycle(false);
    return spout;
  }

  public List<Values> readData() {
    try {
      BufferedReader reader = new BufferedReader(new InputStreamReader(getClass().getResourceAsStream("/AAPL_daily_prices.csv")));

      List<Values> ticks = new ArrayList<Values>();
      String line = null;
      while ((line = reader.readLine()) != null) ticks.add(new Values(line));

      return ticks;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}