package com.hadooparchitecturebook.zombie.giraph;

import java.io.IOException;
import java.util.ArrayList;
import org.apache.giraph.edge.DefaultEdge;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.formats.TextVertexInputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.log4j.Logger;

public class ZombieTextVertexInputFormat extends TextVertexInputFormat<LongWritable, Text, LongWritable> {
    private static final Logger LOG = Logger.getLogger(ZombieTextVertexInputFormat.class);

    @Override
    public TextVertexReader createVertexReader(
            InputSplit split, TaskAttemptContext context) throws IOException {
        return new ZombieTextReader();
    }

    //InputFormat
    //{vertexId}|{Type}|{common seperated vertexId of bitable people}
    public class ZombieTextReader extends TextVertexReader {

        @Override
        public boolean nextVertex() throws IOException, InterruptedException {
            return getRecordReader().nextKeyValue();
        }

        @Override
        public Vertex<LongWritable, Text, LongWritable> getCurrentVertex()
                throws IOException, InterruptedException {
            Text line = getRecordReader().getCurrentValue();
            String[] majorParts = line.toString().split("\\|");
            LongWritable id = new LongWritable(Long.parseLong(majorParts[0]));
            Text value = new Text(majorParts[1]);

            ArrayList<Edge<LongWritable, LongWritable>> edgeIdList = new ArrayList<Edge<LongWritable, LongWritable>>();

            if (majorParts.length > 2) {
                String[] edgeIds = majorParts[2].split(",");
                for (String edgeId:  edgeIds) {
                    DefaultEdge<LongWritable, LongWritable> edge = new DefaultEdge<LongWritable, LongWritable>();
                    LongWritable longEdgeId = new LongWritable(Long.parseLong(edgeId));
                    edge.setTargetVertexId(longEdgeId);
                    edge.setValue(longEdgeId); // dummy value
                    edgeIdList.add(edge);
                }
            }

            Vertex<LongWritable, Text, LongWritable> vertex = getConf().createVertex();

            vertex.initialize(id, value, edgeIdList);
            return vertex;
        }
    }
}
