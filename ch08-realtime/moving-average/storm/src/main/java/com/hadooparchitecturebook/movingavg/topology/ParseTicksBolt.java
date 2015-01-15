package com.hadooparchitecturebook.movingavg.topology;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.Map;

/**
 * For each tick tuple in input stream, parse the ticker and closing price, 
 * and emit into topology.
 */
public class ParseTicksBolt extends BaseRichBolt {

  private OutputCollector outputCollector;

  @Override
  public void prepare(Map config,
		      TopologyContext topologyContext,			
		      OutputCollector collector) {
    outputCollector = collector;
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("ticker", "price"));
  }

  @Override
  public void execute(Tuple tuple) {
    String tick = tuple.getStringByField("tick");
    String[] parts = tick.split(",");
    outputCollector.emit(new Values(parts[0], parts[4]));
    outputCollector.ack(tuple); 
  }
}