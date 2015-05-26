package com.hadooparchitecturebook.zombie.giraph;

import java.io.IOException;
import java.util.Iterator;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.log4j.Logger;

public class ZombieComputation extends BasicComputation<LongWritable,
        Text, LongWritable, LongWritable> {
    private static final Logger LOG = Logger.getLogger(ZombieComputation.class);

    Text zombeText = new Text("Zombie");
    LongWritable longIncrement = new LongWritable(1);

    @Override
    public void compute(
            Vertex<LongWritable, Text, LongWritable> vertex,
            Iterable<LongWritable> messages) throws IOException {

        Context context = getContext();
        long superstep = getSuperstep();

        if (superstep == 0) {
            if (vertex.getValue().toString().equals("Zombie")) {

                zombeText.set("Zombie." + superstep);
                vertex.setValue(zombeText);

                LongWritable newMessage = new LongWritable();
                newMessage.set(superstep+1);

                aggregate("zombe.count",longIncrement );

                for (Edge<LongWritable, LongWritable> edge : vertex.getEdges()) {
                    this.sendMessage(edge.getTargetVertexId(), newMessage);
                }
            }
        } else {
            if (vertex.getValue().toString().equals("Human")) {

                Iterator<LongWritable> it = messages.iterator();
                if (it.hasNext()) {
                    zombeText.set("Zombie." + superstep);
                    vertex.setValue(zombeText);
                    aggregate("zombe.count",longIncrement );

                    LongWritable newMessage = new LongWritable();
                    newMessage.set(superstep+1);

                    for (Edge<LongWritable, LongWritable> edge : vertex.getEdges()) {
                        this.sendMessage(edge.getTargetVertexId(), newMessage);
                    }
                } else {
                    vertex.voteToHalt();
                }

            } else {
                vertex.voteToHalt();
            }
        }
    }
}
