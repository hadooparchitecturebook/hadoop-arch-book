#!/bin/bash
set -e

readonly BASEDIR=$( dirname $0 )
readonly OUTPUTDIR=$BASEDIR/output
readonly TARGETDIR=$BASEDIR/target
readonly JARFILE=$TARGETDIR/GiraphCode.jar

export JAVA_HOME=${JAVAHOME:-$( ls -d1 /usr/java/jdk* | tail -1 )}
export PATH=$JAVA_HOME/bin:$PATH

# Local setup
old_md5=/tmp/old_md5.digests
new_md5=/tmp/new_md5.digests
find . -type f ! -path "./target/*" \( -name "*java" -o -name "*xml" \) | sort | xargs md5sum > $new_md5
if [ ! -f $old_md5 ]; then
  changed=1
else
  set +e
  diff $old_md5 $new_md5 > /dev/null 2>&1
  ret=$?
  set -e
  if [ $ret == 0 ]; then
    changed=0
  else
    changed=1
  fi
fi
if [ ! -f $JARFILE -o $changed == 1 ]; then
  mvn clean package -DskipTests
  mv $new_md5 $old_md5
fi
set +e
hdfs dfs -rm -R -skipTrash gir_input gir_output
hdfs dfs -mkdir -p gir_input
hdfs dfs -put input.txt gir_input
set -e
mkdir -p $OUTPUTDIR

# Execute
hadoop jar target/GiraphCode.jar com.cloudera.sa.giraph.zombe.bite.ZombieBiteJob -Dmapred.job.tracker=yarn 3 gir_input gir_output 2>&1 | tee $OUTPUTDIR/scripts.out
hdfs dfs -cat gir_output/part* | sort -t\| -k1 -n | tee $OUTPUTDIR/data.txt

