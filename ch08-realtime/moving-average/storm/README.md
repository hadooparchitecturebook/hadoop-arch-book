Create a local repo for storm-hdfs:

* Checkout and build storm-hdfs
* cd storm-moving-avg
* mkdir lib
* cp <storm-hdfs-home>/target/storm-hdfs-<version>.jar lib/
* mkdir repo
* mvn org.apache.maven.plugins:maven-install-plugin:2.3.1:install-file \
  -Dfile=lib/storm-hdfs-<version>.jar \
  -DgroupId=storm-hdfs -DartifactId=storm-hdfs \
  -Dversion=<storm-hdfs-version> -Dpackaging=jar \
  -DlocalRepositoryPath=`pwd`/repo/

*Running*

Local Mode:

* Un-comment the following lines in MovingAvgLocalTopologyRunner.java:
  LocalCluster localCluster = new LocalCluster();
  localCluster.submitTopology("local-moving-avg", config, topology);
* mvn clean package
* java -jar target/moving-average-0.1.jar

Cluster Mode:

* The following are instructions for running in cluster mode on a single node. Refer to the Storm documentation for instructions for running on distributed nodes.
* Comment out the lines above for running in local mode, and Un-comment the following lines in MovingAvgLocalTopologyRunner.java:
  StormSubmitter.submitTopology(“local-moving-avg”,
                                config,
				topology);
* mvn clean package
* Download and unzip the Storm distribution. This code was developed using the 0.9.2-incubating version of storm.
* The following instructions assume the Storm distribution is in /opt/storm.
