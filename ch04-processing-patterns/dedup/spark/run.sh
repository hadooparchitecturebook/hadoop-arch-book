#!/bin/bash

readonly BASEDIR=$( dirname $0 )
readonly OUTPUTDIR=$BASEDIR/output
readonly TARGETDIR=$BASEDIR/target
readonly JARFILE=$TARGETDIR/BellCode.jar

export JAVA_HOME=${JAVAHOME:-$( ls -d1 /usr/java/jdk* | tail -1 )}
export PATH=$JAVA_HOME/bin:$PATH

# Local setup
rm -rf $OUTPUTDIR
mkdir -p $OUTPUTDIR
hdfs dfs -rm -skipTrash -R pnv_input pnv_output ded_input ded_output tim_input1 tim_output1 tim_input2 tim_output2
hdfs dfs -mkdir -p pnv_input ded_input tim_input1 tim_input2
if [ ! -f $JARFILE ]; then
  mvn clean package
fi

# Execute
echo "Peaks and Valleys - Generating data"
spark-submit --class com.cloudera.sa.book.commonprocessing.peaksandvalleys.GenPeaksAndValleys $JARFILE \
  pnv_input/pnv.txt 1000000 100 2>&1 | tee $OUTPUTDIR/GenPeaksAndValleys.out
echo "Peaks and Valleys - Executing example"
spark-submit --class com.cloudera.sa.book.commonprocessing.peaksandvalleys.SparkPeaksAndValleysExecution $JARFILE \
  pnv_input pnv_output 10 2>&1 | tee $OUTPUTDIR/SparkPeaksAndValleysExecution.out

echo "Deduping - Generating data"
spark-submit --class com.cloudera.sa.book.commonprocessing.deduping.GenDedupInput $JARFILE \
  ded_input/ded.txt 1000000 100 2>&1 | tee $OUTPUTDIR/GenDedupInput.out
echo "Deduping - Executing example"
spark-submit --class com.cloudera.sa.book.commonprocessing.deduping.SparkDedupExecution $JARFILE \
  ded_input ded_output 2>&1 | tee $OUTPUTDIR/SparkDedupExecution.out

echo "Time Series - Generating data - Initial data"
spark-submit --class com.cloudera.sa.book.commonprocessing.timeseries.GenTimeSeriesInput $JARFILE \
  tim_input1/tim.txt 1000000 100 30000000 2>&1 | tee $OUTPUTDIR/GenTimeSeriesInput1.out
echo "Time Series - Executing example - Initial data"
spark-submit --class com.cloudera.sa.book.commonprocessing.timeseries.SparkTimeSeriesExecution $JARFILE \
  tim_input1 tim_output1 10 2>&1 | tee $OUTPUTDIR/SparkTimeSeriesExecution1.out

echo "Time Series - Generating data - Appending"
spark-submit --class com.cloudera.sa.book.commonprocessing.timeseries.GenTimeSeriesInput $JARFILE \
  tim_input2/tim.txt 1000000 100 40000000 2>&1 | tee $OUTPUTDIR/GenTimeSeriesInput2.out
echo "Time Series - Executing example - Appending"
spark-submit --class com.cloudera.sa.book.commonprocessing.timeseries.SparkTimeSeriesExecution $JARFILE \
  tim_input2 tim_output1 tim_output2 10 2>&1 | tee $OUTPUTDIR/SparkTimeSeriesExecution2.out
