#!/bin/bash

readonly BASEDIR=$( dirname $0 )
readonly OUTPUTDIR=$BASEDIR/output

# Setup
$BASEDIR/../setup.sh

# Local setup
hdfs dfs -rm -skipTrash -R pig
rm -rf $OUTPUTDIR
mkdir -p $OUTPUTDIR

# Execute
pig JoinFilterExample.pig 2>&1 | tee $OUTPUTDIR/script.out
hdfs dfs -cat pig/out/* | tee $OUTPUTDIR/data.txt
