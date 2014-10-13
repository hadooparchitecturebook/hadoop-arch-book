MRSessionize
============

Sessionization code written in MapReduce

1. You may have to copy joda-time jar to HADOOP_HOME/lib until we start building an uber jar:-(.

2. Run the code with something a command like this:
<pre>
<code>
hadoop jar ./target/MRSessionize-1.0-SNAPSHOT.jar com.hadooparchitecturebook.MRSessionize /etl/BI/casualcyclist/clicks/deduplogs/year=2014/month=10/day=10/* /data/bikeshop/clickstream/year=2014/month=10/day=10
</code>
</pre>
