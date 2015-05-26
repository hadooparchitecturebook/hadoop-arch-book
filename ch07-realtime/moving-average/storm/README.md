Example Storm topology to calculate moving averages over stock tick data.

To build and run the example in local mode:

Local Mode:

* Make sure the following lines are un-commented in MovingAvgLocalTopologyRunner.java:
  LocalCluster localCluster = new LocalCluster();
  localCluster.submitTopology("local-moving-avg", config, topology);
* In the project home directory, build the code: 
    $ mvn clean package
* Execute the example:
    $ java -jar target/moving-average-0.1.jar

