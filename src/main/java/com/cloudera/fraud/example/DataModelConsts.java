package com.cloudera.fraud.example;

import org.apache.hadoop.hbase.util.Bytes;

public class DataModelConsts {
  public static final byte[] PROFILE_TABLE = Bytes.toBytes("profile");
  public static final byte[] PROFILE_COLUMN_FAMILY = Bytes.toBytes("p");
  
  public static final byte[] FIXED_INFO_COL = Bytes.toBytes("fl");
  public static final byte[] LAST_LOG_IN_COL = Bytes.toBytes("ll");
  public static final byte[] LOG_IN_COUNT_COL = Bytes.toBytes("lc");
  public static final byte[] TOTAL_SELLS_COL = Bytes.toBytes("ts");
  public static final byte[] TOTAL_VALUE_OF_PAST_SELLS_COL = Bytes.toBytes("vs");
  public static final byte[] CURRENT_LOG_IN_SELLS_VALUE_COL = Bytes.toBytes("Vs");
  public static final byte[] TOTAL_PURCHASES_COL = Bytes.toBytes("tp");
  public static final byte[] TOTAL_VALUE_OF_PAST_PURCHASES_COL = Bytes.toBytes("vp");
  public static final byte[] CURRENT_LOG_IN_PURCHASES_VALUE_COL = Bytes.toBytes("Vp");
  public static final byte[] LOG_IN_IP_ADDERSSES = Bytes.toBytes("IP");
  
  
}
