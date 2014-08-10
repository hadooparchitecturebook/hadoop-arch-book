MRSessionize
============

Sessionization code written in MapReduce

1. You may have to copy joda-time jar to HADOOP_HOME/lib until we start building an uber jar:-(.

2. Run the code with something a command like this:
<pre>
<code>
hadoop jar ~/MRSessionize/target/MRSessionize-1.0-SNAPSHOT.jar com.hadooparchitecturebook.MRSessionize -libjars ~/joda-time-2.3.jar /etl/bikeshop/clickstream/raw/year=2013/month=10/day=10/access_log_20141010-013027.log /tmp/sessionize
</code>
</pre>
