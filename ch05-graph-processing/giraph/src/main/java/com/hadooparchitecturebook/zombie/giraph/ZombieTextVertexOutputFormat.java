package com.hadooparchitecturebook.zombie.giraph;

import java.io.IOException;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.formats.TextVertexOutputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.log4j.Logger;

public class ZombieTextVertexOutputFormat extends
        TextVertexOutputFormat<LongWritable, Text, LongWritable> {
    private static final Logger LOG = Logger.getLogger(ZombieTextVertexOutputFormat.class);

    @Override
    public TextVertexWriter createVertexWriter(
            TaskAttemptContext context) throws IOException, InterruptedException {
        return new ZombieRecordTextWriter();
    }

    public class ZombieRecordTextWriter extends TextVertexWriter {
        Text newKey = new Text();
        Text newValue = new Text();

        public void writeVertex(Vertex<LongWritable, Text, LongWritable> vertex)
                throws IOException, InterruptedException {
            Iterable<Edge<LongWritable, LongWritable>> edges = vertex.getEdges();

            StringBuilder strBuilder = new StringBuilder();

            boolean isFirst = true;
            for (Edge<LongWritable, LongWritable> edge : edges) {
                if (isFirst) {
                    isFirst = false;
                } else {
                    strBuilder.append(",");
                }
                strBuilder.append(edge.getValue());
            }

            newKey.set(vertex.getId().get() + "|" + vertex.getValue() + "|"
                + strBuilder.toString());

            getRecordWriter().write(newKey, newValue);
        }
    }
}
