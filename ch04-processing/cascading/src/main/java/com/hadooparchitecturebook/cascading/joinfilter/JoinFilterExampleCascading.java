package com.hadooparchitecturebook.cascading.joinfilter;

import java.util.Properties;

import cascading.flow.Flow;
import cascading.flow.FlowDef;
import cascading.flow.FlowProcess;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.operation.BaseOperation;
import cascading.operation.Filter;
import cascading.operation.FilterCall;
import cascading.pipe.Each;
import cascading.pipe.HashJoin;
import cascading.pipe.Pipe;
import cascading.property.AppProps;
import cascading.property.ConfigDef.Mode;
import cascading.scheme.hadoop.TextDelimited;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;
import cascading.tuple.collect.SpillableProps;


public class JoinFilterExampleCascading {
  public static void main(String[] args) {
    String fooInputPath = args[0];
    String barInputPath = args[1];
    String outputPath = args[2];
    int fooValMax = Integer.parseInt(args[3]);
    int joinValMax = Integer.parseInt(args[4]);
    int numberOfReducers = Integer.parseInt(args[5]);

    Properties properties = new Properties();
    AppProps.setApplicationJarClass(properties,
        JoinFilterExampleCascading.class);
    properties.setProperty("mapred.reduce.tasks", Integer.toString(numberOfReducers));
    properties.setProperty("mapreduce.job.reduces", Integer.toString(numberOfReducers));
    
    SpillableProps props = SpillableProps.spillableProps()
        .setCompressSpill( true )
        .setMapSpillThreshold( 50 * 1000 );
        

    
    HadoopFlowConnector flowConnector = new HadoopFlowConnector(properties);

    // create source and sink taps
    Fields fooFields = new Fields("fooId", "fooVal", "foobarId");
    Tap fooTap = new Hfs(new TextDelimited(fooFields, "|"), fooInputPath);
    Fields barFields = new Fields("barId", "barVal");
    Tap barTap = new Hfs(new TextDelimited(barFields, "|"), barInputPath);

    Tap outputTap = new Hfs(new TextDelimited(false, "|"), outputPath);

    Fields joinFooFields = new Fields("foobarId");
    Fields joinBarFields = new Fields("barId");

    Pipe fooPipe = new Pipe("fooPipe");
    Pipe barPipe = new Pipe("barPipe");

    Pipe fooFiltered = new Each(fooPipe, fooFields, new FooFilter(fooValMax));

    Pipe joinedPipe = new HashJoin(fooFiltered, joinFooFields, barPipe,
        joinBarFields);
    props.setProperties( joinedPipe.getConfigDef(), Mode.REPLACE );
    
    
    Fields joinFields = new Fields("fooId", "fooVal", "foobarId", "barVal");
    Pipe joinedFilteredPipe = new Each(joinedPipe, joinFields,
        new JoinedFilter(joinValMax));

    FlowDef flowDef = FlowDef.flowDef().setName("wc")
        .addSource(fooPipe, fooTap).addSource(barPipe, barTap)
        .addTailSink(joinedFilteredPipe, outputTap);

    Flow wcFlow = flowConnector.connect(flowDef);
    wcFlow.writeDOT("dot/wc.dot");
    wcFlow.complete();
  }

  public static class FooFilter extends BaseOperation implements Filter {

    int fooValMax;

    FooFilter(int fooValMax) {
      this.fooValMax = fooValMax;
    }

    @Override
    public boolean isRemove(FlowProcess flowProcess, FilterCall filterCall) {

      int fooValue = filterCall.getArguments().getTuple().getInteger(1);

      return fooValue <= fooValMax;
    }
  }

  public static class JoinedFilter extends BaseOperation implements Filter {

    int joinValMax;

    JoinedFilter(int joinValMax) {
      this.joinValMax = joinValMax;
    }

    @Override
    public boolean isRemove(FlowProcess flowProcess, FilterCall filterCall) {

      int fooValue = filterCall.getArguments().getTuple().getInteger(1);
      int barValue = filterCall.getArguments().getTuple().getInteger(3);

      return fooValue + barValue <= joinValMax;
    }
  }
}
