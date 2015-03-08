#!/bin/bash

readonly BASEDIR=$( dirname $0 )
readonly OUTPUTDIR=$BASEDIR/output
readonly JARFILE=$BASEDIR/target/CrunchCode.jar

export JAVA_HOME=${JAVAHOME:-$( ls -d1 /usr/java/jdk* | tail -1 )}
export PATH=$JAVA_HOME/bin:$PATH

# Setup
$BASEDIR/../setup.sh

# Local setup
hdfs dfs -rm -skipTrash -R cru_output
rm -rf $OUTPUTDIR
mkdir -p $OUTPUTDIR
if [ ! -f $JARFILE ]; then
  mvn clean package
fi

# Execute
hadoop jar $JARFILE com.hadooparchitecturebook.crunch.joinfilter.JoinFilterExampleCrunch foo bar cru_output 500 500 10 2>&1 | tee $OUTPUTDIR/script.out
hdfs dfs -cat cru_output/part* > $OUTPUTDIR/data.txt

