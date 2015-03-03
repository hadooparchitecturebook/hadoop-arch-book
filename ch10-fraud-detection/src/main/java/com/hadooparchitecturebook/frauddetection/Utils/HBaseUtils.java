package com.hadooparchitecturebook.frauddetection.Utils;

import com.hadooparchitecturebook.frauddetection.model.HBaseTableMetaModel;
import com.hadooparchitecturebook.frauddetection.model.UserProfile;
import com.hadooparchitecturebook.frauddetection.model.ValidationRules;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

/**
 * Created by ted.malaska on 1/16/15.
 */
public class HBaseUtils {

  static Logger log = Logger.getLogger(HBaseUtils.class);

  public static byte[] convertKeyToRowKey(TableName tableName, String key) {

    Integer splits = HBaseTableMetaModel.splitMap.get(tableName.getName()).intValue();

    if (splits == 1) {
      return Bytes.toBytes(key);
    } else if (splits < 26) {

      char salt = (char)('A' + splits);

      return Bytes.toBytes(salt + key);
    } else {
      log.error("Splits too high for this simple example");
      throw new RuntimeException("Splits too high for this simple example");
    }
  }

  public static void populateUserProfile(HConnection connection, UserProfile userProfile) throws Exception {
    HTableInterface table = connection.getTable(HBaseTableMetaModel.profileCacheTableName);

    try {
      Put put = new Put(convertKeyToRowKey(HBaseTableMetaModel.profileCacheTableName, userProfile.userId));
      put.add(HBaseTableMetaModel.profileCacheColumnFamily, HBaseTableMetaModel.profileCacheJsonColumn, Bytes.toBytes(userProfile.getJSONObject().toString()));
      put.add(HBaseTableMetaModel.profileCacheColumnFamily, HBaseTableMetaModel.profileCacheTsColumn, Bytes.toBytes(System.currentTimeMillis()));
      table.put(put);
    } finally {
      table.close();
    }
  }

  public static void populateValidationRules(HConnection connection, ValidationRules rules) throws Exception {
    HTableInterface table = connection.getTable(HBaseTableMetaModel.profileCacheTableName);

    try {
      Put put = new Put(HBaseTableMetaModel.validationRulesRowKey);
      put.add(HBaseTableMetaModel.profileCacheColumnFamily, HBaseTableMetaModel.validationRulesRowKey, Bytes.toBytes(rules.getJSONObject().toString()));
      table.put(put);
    } finally {
      table.close();
    }
  }
}
