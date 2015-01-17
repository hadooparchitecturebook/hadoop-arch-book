package com.hadooparchitecturebook.movingavg.trident;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;
import static com.hadooparchitecturebook.movingavg.trident.FixedBatchSpoutBuilder.buildSpout;
import storm.trident.Stream;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.testing.MemoryMapState;
import storm.trident.operation.builtin.Sum;
import storm.trident.operation.builtin.Count;

public class MovingAvgLocalTopologyRunner {

  public static void main(String[] args) 
      throws Exception {
    
    Config conf = new Config();
    LocalCluster cluster = new LocalCluster();
    
    TridentTopology topology = new TridentTopology();

    Stream movingAvgStream =
      topology.newStream("ticks-spout", buildSpout())
      .each(new Fields("stock-ticks"), new TickParser(), new Fields("price"))
      .aggregate(new Fields("price"), new CalculateAverage(), new Fields("count"));

    cluster.submitTopology("moving-avg", conf, topology.build());
  }
}