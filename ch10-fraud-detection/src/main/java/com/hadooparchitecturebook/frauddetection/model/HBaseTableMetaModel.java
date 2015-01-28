package com.hadooparchitecturebook.frauddetection.model;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.HashMap;

/**
 * Created by ted.malaska on 1/18/15.
 */
public class HBaseTableMetaModel {
  public static TableName profileCacheTableName = TableName.valueOf("profileCacheTableName");
  public static byte[] profileCacheColumnFamily = Bytes.toBytes("c");
  public static int profileCacheNumberOfProfileCacheSalts = 3;
  public static byte[] profileCacheTsColumn = Bytes.toBytes("t");
  public static byte[] profileCacheJsonColumn = Bytes.toBytes("x");

  public static TableName validationRulesTableName = TableName.valueOf("validationRules");
  public static byte[] validationRulesColumnFamily = Bytes.toBytes("c");
  public static int validationRulesNumberOfProfileCacheSalts = 1;
  public static byte[] validationRulesRowKey = Bytes.toBytes("r");

  public static HashMap<byte[], Integer> splitMap = new HashMap<byte[], Integer>();

  static {
    splitMap.put(profileCacheTableName.getName(), profileCacheNumberOfProfileCacheSalts);
    splitMap.put(validationRulesTableName.getName(), validationRulesNumberOfProfileCacheSalts);
  }


}
