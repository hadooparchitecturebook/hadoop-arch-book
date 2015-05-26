package com.hadooparchitecturebook.movingavg.topology;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import org.apache.commons.io.IOUtils;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;

/**
 * Ingest tick data and emit into our topology. This spout just reads tick data
 * from a file.
 */
public class StockTicksSpout extends BaseRichSpout {
  private SpoutOutputCollector outputCollector;
  private List<String> ticks;

  @Override
  public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    outputFieldsDeclarer.declare(new Fields("tick"));
  }

  /**
   * Open file with stock tick data and read into List object.
   */
  @Override
  public void open(Map map,
                   TopologyContext context,
                   SpoutOutputCollector outputCollector) {
    this.outputCollector = outputCollector;

    try {
      ticks = 
        IOUtils.readLines(ClassLoader.getSystemResourceAsStream(
	  "NASDAQ_daily_prices_A.csv"),
          Charset.defaultCharset().name());
    } catch (IOException e) {
        throw new RuntimeException(e);
    }
  }

  /**
   * Emit each tick record in List into topology.
   */
  @Override
  public void nextTuple() {
    for (String tick : ticks) {
      // In order to ensure reliability, tuples need to be "anchored" with
      // a tuple ID. In this case we're just using the tuple itself as an ID. 
      // In a real topology this would probably be something like a message ID.
      outputCollector.emit(new Values(tick), tick);
    }
  }
}