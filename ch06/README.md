Usage
=====

*On local cluster:*

<pre>
<src>
java -cp /Users/gshapira/workspaces/JavaSessionize/target/JavaSessionize-1.0-SNAPSHOT.jar:/Users/gshapira/spark-0.9.1/conf:\
/Users/gshapira/spark-0.9.1/assembly/target/scala-2.10/spark-assembly-0.9.1-hadoop1.0.4.jar \
com.hadooparchitecturebook.clickstream.JavaSessionize local
</src>
</pre>

or

<pre>
<src>
java -cp /Users/gshapira/workspaces/JavaSessionize/target/JavaSessionize-1.0-SNAPSHOT.jar:/Users/gshapira/spark-0.9.1/conf:\
/Users/gshapira/spark-0.9.1/assembly/target/scala-2.10/spark-assembly-0.9.1-hadoop1.0.4.jar \
com.hadooparchitecturebook.clickstream.JavaSessionize local /Users/gshapira/workspaces/access_log_20140512-143638.log
</pre>
</src>
