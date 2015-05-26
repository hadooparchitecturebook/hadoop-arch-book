package com.hadooparchitecturebook.zombie.giraph;

import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.io.formats.GiraphFileInputFormat;
import org.apache.giraph.job.GiraphJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

public class ZombieBiteJob implements Tool {
    private static final Logger LOG = Logger.getLogger(ZombieBiteJob.class);
    private Configuration conf;

    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
    }

    @Override
    public Configuration getConf() {
        return conf;
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 3) {
            throw new IllegalArgumentException(
                "Syntax error: Must have 3 arguments <numbersOfWorkers> <inputLocaiton> <outputLocation>");
        }

        int numberOfWorkers = Integer.parseInt(args[0]);
        String inputLocation = args[1];
        String outputLocation = args[2];

        GiraphJob job = new GiraphJob(getConf(), getClass().getName());
        GiraphConfiguration gconf = job.getConfiguration();
        gconf.setWorkerConfiguration(numberOfWorkers, numberOfWorkers, 100.0f);

        GiraphFileInputFormat.addVertexInputPath(gconf, new Path(inputLocation));
        FileOutputFormat.setOutputPath(job.getInternalJob(), new Path(outputLocation));

        gconf.setComputationClass(ZombieComputation.class);
        gconf.setMasterComputeClass(ZombieMasterCompute.class);
        gconf.setVertexInputFormatClass(ZombieTextVertexInputFormat.class);
        gconf.setVertexOutputFormatClass(ZombieTextVertexOutputFormat.class);
        gconf.setWorkerContextClass(ZombieWorkerContext.class);

        boolean verbose = true;
        if (job.run(verbose)) {
            return 0;
        } else {
            return -1;
        }
    }

    public static void main(String[] args) throws Exception {
        int ret = ToolRunner.run(new ZombieBiteJob(), args);
        if (ret == 0) {
            System.out.println("Ended Good");
        } else {
            System.out.println("Ended with Failure");
        }
        System.exit(ret);
    }
}
