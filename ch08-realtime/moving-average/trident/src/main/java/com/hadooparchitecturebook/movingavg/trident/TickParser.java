package com.hadooparchitecturebook.movingavg.trident;

import backtype.storm.tuple.Values;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class TickParser extends BaseFunction {

  @Override
  public void execute(TridentTuple tuple,
		      TridentCollector collector) {

    String tick = tuple.getString(0);
    String[] parts = tick.split(",");
    System.out.println("TickParser: price=" + Double.valueOf(parts[4]));
    collector.emit(new Values(Double.valueOf(parts[4])));
  }

  @Override
  public void prepare(Map config,
                      TridentOperationContext context) {}

  @Override
  public void cleanup() {}
}
