package com.hadooparchitecturebook.frauddetection;

import com.hadooparchitecturebook.frauddetection.model.HBaseTableMetaModel;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HBaseAdmin;

import java.io.IOException;

/**
 * Created by ted.malaska on 1/18/15.
 */
public class RemoveTables {
  public static void main(String[] args) throws IOException {

    Configuration config = HBaseConfiguration.create();

    executeDeleteTables(config);
  }

  public static void executeDeleteTables(Configuration config) throws IOException {
    HBaseAdmin admin = new HBaseAdmin(config);

    if (admin.tableExists(HBaseTableMetaModel.profileCacheTableName)) {
      admin.disableTable(HBaseTableMetaModel.profileCacheTableName);
      admin.deleteTable(HBaseTableMetaModel.profileCacheTableName);
    }

    if (admin.tableExists(HBaseTableMetaModel.validationRulesTableName)) {
      admin.disableTable(HBaseTableMetaModel.validationRulesTableName);
      admin.deleteTable(HBaseTableMetaModel.validationRulesTableName);
    }

    admin.close();
  }
}
