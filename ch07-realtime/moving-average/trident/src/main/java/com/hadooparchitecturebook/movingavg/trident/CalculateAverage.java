package com.hadooparchitecturebook.movingavg.trident;

import backtype.storm.tuple.Values;
import storm.trident.operation.Aggregator;
import storm.trident.operation.BaseAggregator;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

public class CalculateAverage extends BaseAggregator <CalculateAverage.AverageState> {

  static class AverageState {
    double sum = 0;
  }

  @Override
  public AverageState init(Object batchId, TridentCollector collector) {
    return new AverageState();
  }

  @Override
  public void aggregate(AverageState state, TridentTuple tuple, TridentCollector collector) {
    state.sum = state.sum + Double.valueOf((Double)tuple.getValue(0));
  }
  
  @Override
  public void complete(AverageState state, TridentCollector collector) {
    collector.emit(new Values(state.sum/5));
  }
}