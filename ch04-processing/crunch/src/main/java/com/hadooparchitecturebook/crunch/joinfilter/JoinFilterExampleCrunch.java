package com.hadooparchitecturebook.crunch;

import org.apache.hadoop.conf.Configuration;
import org.apache.crunch.lib.join.DefaultJoinStrategy;
import org.apache.crunch.FilterFn;
import org.apache.crunch.MapFn;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.Pair;
import org.apache.crunch.PCollection;
import org.apache.crunch.Pipeline;
import org.apache.crunch.PipelineResult;
import org.apache.crunch.PTable;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.crunch.types.writable.Writables;
import org.apache.crunch.types.avro.Avros;
import org.apache.crunch.lib.join.JoinType;
import org.apache.commons.lang.StringUtils;
import org.apache.crunch.Target.WriteMode;
import org.apache.crunch.io.At;

public class JoinFilterExampleCrunch implements Tool {

  public static final int FOO_ID_INX = 0;
  public static final int FOO_VALUE_INX = 1;
  public static final int FOO_BAR_ID_INX = 2;

  public static final int BAR_ID_INX = 0;
  public static final int BAR_VALUE_INX = 1;

  public static void main(String[] args) throws Exception {
    ToolRunner.run(new Configuration(), new JoinFilterExampleCrunch(), args);
  }

  Configuration config;

  public Configuration getConf() {
    return config;
  }

  public void setConf(Configuration config) {
    this.config = config;
  }

  public int run(String[] args) throws Exception {

    String fooInputPath = args[0];
    String barInputPath = args[1];
    String outputPath = args[2];
    int fooValMax = Integer.parseInt(args[3]);
    int joinValMax = Integer.parseInt(args[4]);
    int numberOfReducers = Integer.parseInt(args[5]);

    Pipeline pipeline = new MRPipeline(JoinFilterExampleCrunch.class, getConf()); //<1>
    
    PCollection<String> fooLines = pipeline.readTextFile(fooInputPath);  //<2>
    PCollection<String> barLines = pipeline.readTextFile(barInputPath);

    PTable<Long, Pair<Long, Integer>> fooTable = fooLines.parallelDo(  //<3>
        new FooIndicatorFn(),
        Avros.tableOf(Avros.longs(),
        Avros.pairs(Avros.longs(), Avros.ints())));

    fooTable = fooTable.filter(new FooFilter(fooValMax));  //<4>

    PTable<Long, Integer> barTable = barLines.parallelDo(new BarIndicatorFn(),
        Avros.tableOf(Avros.longs(), Avros.ints()));

    DefaultJoinStrategy<Long, Pair<Long, Integer>, Integer> joinStrategy =   //<5>
        new DefaultJoinStrategy
          <Long, Pair<Long, Integer>, Integer>
          (numberOfReducers);

    PTable<Long, Pair<Pair<Long, Integer>, Integer>> joinedTable = joinStrategy //<6>
        .join(fooTable, barTable, JoinType.INNER_JOIN);

    PTable<Long, Pair<Pair<Long, Integer>, Integer>> filteredTable = joinedTable.filter(new JoinFilter(joinValMax));

    filteredTable.write(At.textFile(outputPath), WriteMode.OVERWRITE); //<7>

    PipelineResult result = pipeline.done();

    return result.succeeded() ? 0 : 1;
  }

  public static class FooIndicatorFn extends
      MapFn<String, Pair<Long, Pair<Long, Integer>>> {

    private static final long serialVersionUID = 1L;

    @Override
    public Pair<Long, Pair<Long, Integer>> map(String input) {
      String[] cells = StringUtils.split(input.toString(), "|");

      Pair<Long, Integer> valuePair = new Pair<Long, Integer>(
          Long.parseLong(cells[FOO_ID_INX]),
          Integer.parseInt(cells[FOO_VALUE_INX]));

      return new Pair<Long, Pair<Long, Integer>>(
          Long.parseLong(cells[FOO_BAR_ID_INX]), valuePair);
    }
  }

  public static class FooFilter extends 
      FilterFn<Pair<Long, Pair<Long, Integer>>> {

    private static final long serialVersionUID = 1L;

    int fooValMax;

    FooFilter(int fooValMax) {
      this.fooValMax = fooValMax;
    }

    @Override
    public boolean accept(Pair<Long, Pair<Long, Integer>> input) {
      return input.second().second() <= fooValMax;
    }
  }

  public static class BarIndicatorFn extends MapFn<String, Pair<Long, Integer>> {

    private static final long serialVersionUID = 1L;

    @Override
    public Pair<Long, Integer> map(String input) {
      String[] cells = StringUtils.split(input.toString(), "|");

      return new Pair<Long, Integer>(Long.parseLong(cells[BAR_ID_INX]),
          Integer.parseInt(cells[BAR_VALUE_INX]));
    }
  }

  public static class JoinFilter extends
      FilterFn<Pair<Long, Pair<Pair<Long, Integer>, Integer>>> {

    private static final long serialVersionUID = 1L;

    int joinValMax;

    JoinFilter(int joinValMax) {
      this.joinValMax = joinValMax;
    }

    @Override
    public boolean accept(Pair<Long, Pair<Pair<Long, Integer>, Integer>> input) {
      return input.second().first().second() + input.second().second() <= joinValMax;
    }

  }
}
