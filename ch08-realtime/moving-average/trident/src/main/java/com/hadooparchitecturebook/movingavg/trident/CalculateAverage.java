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
    System.out.println("CA.init() batchId=" + batchId);
    return new AverageState();
  }

  @Override
  public void aggregate(AverageState state, TridentTuple tuple, TridentCollector collector) {
    try {
      System.out.println("CA.aggregate() value=" + tuple.getValue(0));
      state.sum = state.sum + Double.valueOf((Double)tuple.getValue(0));
    } catch(Exception e) {
      System.out.println(e.getMessage());
    }
    //state.sum = state.sum + tuple.getValue(0).doubleValue();
  }
  
  @Override
  public void complete(AverageState state, TridentCollector collector) {
    System.out.println("CA.complete() sum=" + state.sum + " avg=" + state.sum/5);
    collector.emit(new Values(state.sum/5));
  }
}