#!/bin/bash
# Raw log directory is /etl/BI/casualcyclist/clicks/rawlogs/year=2014/month=10/day=10
# Deduped log directory is /etl/BI/casualcyclist/clicks/deduplogs/year=2014/month=10/day=10
hive -f dedup.sql
