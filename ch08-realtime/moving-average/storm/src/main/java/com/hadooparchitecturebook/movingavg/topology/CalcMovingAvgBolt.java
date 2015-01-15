package com.hadooparchitecturebook.movingavg.topology;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

/**
 * Calculate a moving average over stock tick data.
 * 
 * SMA algo adapted from
 *   http://rosettacode.org/wiki/Averages/Simple_moving_average#Java.
 * See http://www.gnu.org/licenses/fdl-1.2.html
 */
public class CalcMovingAvgBolt extends BaseRichBolt {

  private OutputCollector outputCollector;
  // Create map to hold tuples for unique tickers:
  private Map<String, LinkedList<Double>> windowMap;
  private final int period = 5;

  @Override
  public void prepare(Map config,
		      TopologyContext topologyContext,			
		      OutputCollector collector) {
    outputCollector = collector;
    windowMap = new HashMap<String, LinkedList<Double>>();
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    // Since we're not actually emitting any values from this bolt we don't declare any output fields.
  }

  /**
   * For each ticker in input stream, calculate the moving average.
   */
  @Override
  public void execute(Tuple tuple) {
    String ticker = tuple.getStringByField("ticker");
    String quote = tuple.getStringByField("price");
      
    Double num = Double.parseDouble(quote);
    LinkedList<Double> window = (LinkedList)getQuotesForTicker(ticker);
    window.add(num);
      
    // Print to System.out for test purposes. In a real implementation this
    // would go to a downstream bolt for further processing, or persisted, etc.
    System.out.println("----------------------------------------");
    System.out.println("moving average for ticker " + ticker + "=" + getAvg(window)); 
    System.out.println("----------------------------------------");
  }

  /**
   * Return the current window of prices for a ticker.
   */
  private LinkedList<Double> getQuotesForTicker(String ticker) {
    LinkedList<Double> window = windowMap.get(ticker);
    if (window == null) {
      window = new LinkedList<Double>();
      windowMap.put(ticker, window);
    }
    return window;
  }

  /**
   * Return the average for the current window.
   */ 
  public double getAvg(LinkedList<Double> window) {
    if (window.isEmpty()) {
      return 0;
    }
    
    // Drop the oldest price from the window:
    if (window.size() > period) {
      window.remove();
    }

    double sum = 0;

    for (Double price : window) {
      sum += price.doubleValue();
    }

    return sum / window.size();
  }
}