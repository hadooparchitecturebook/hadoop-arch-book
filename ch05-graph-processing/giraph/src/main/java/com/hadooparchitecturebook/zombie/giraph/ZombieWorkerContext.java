package com.hadooparchitecturebook.zombie.giraph;

import org.apache.giraph.worker.WorkerContext;
import org.apache.log4j.Logger;

@SuppressWarnings("rawtypes")
public class ZombieWorkerContext extends WorkerContext {
    private static final Logger LOG = Logger.getLogger(ZombieWorkerContext.class);

    @Override
    public void preApplication() {
        System.out.println("PreApplication # of Zombies: " + getAggregatedValue("zombe.count"));
    }

    @Override
    public void postApplication() {
        System.out.println("PostApplication # of Zombies: " + getAggregatedValue("zombe.count"));
    }

    @Override
    public void preSuperstep() {
        System.out.println("PreSuperstep # of Zombies: " + getAggregatedValue("zombe.count"));
    }

    @Override
    public void postSuperstep() {
        System.out.println("PostSuperstep # of Zombies: " + getAggregatedValue("zombe.count"));
    }
}
