package com.hadooparchitecturebook.zombie.giraph;

import org.apache.giraph.aggregators.LongSumAggregator;
import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.hadoop.io.LongWritable;
import org.apache.log4j.Logger;

public class ZombieMasterCompute
        extends DefaultMasterCompute {
    private static final Logger LOG = Logger.getLogger(ZombieMasterCompute.class);

    @Override
    public void compute() {
        LongWritable zombes = getAggregatedValue("zombe.count");

        System.out.println("Superstep "+String.valueOf(getSuperstep())+" - zombes:" + zombes);
        System.out.println("Superstep "+String.valueOf(getSuperstep())+" - getTotalNumEdges():" + getTotalNumEdges());
        System.out.println("Superstep "+String.valueOf(getSuperstep())+" - getTotalNumVertices():" + getTotalNumVertices());
    }

    @Override
    public void initialize() throws InstantiationException,
            IllegalAccessException {
        LOG.info("Registering aggregator: zombe.count");
        registerAggregator("zombe.count", LongSumAggregator.class);
    }
}
