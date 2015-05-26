package com.hadooparchitecturebook.frauddetection;

import com.hadooparchitecturebook.frauddetection.model.HBaseTableMetaModel;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.regionserver.DisabledRegionSplitPolicy;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * Created by ted.malaska on 1/18/15.
 */
public class CreateTables {
  public static void main(String[] args) throws IOException {

    Configuration config = HBaseConfiguration.create();

    executeCreateTables(config);
  }

  public static void executeCreateTables(Configuration config) throws IOException {
    HBaseAdmin admin = new HBaseAdmin(config);

    createProfileCacheTable(admin);

    createValidationRuleTable(admin);

    admin.close();
  }

  private static void createValidationRuleTable(HBaseAdmin admin) throws IOException {
    HTableDescriptor tableDescriptor = new HTableDescriptor(HBaseTableMetaModel.validationRulesTableName);

    HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(HBaseTableMetaModel.validationRulesColumnFamily);
    hColumnDescriptor.setMaxVersions(1);

    tableDescriptor.addFamily(hColumnDescriptor);
    tableDescriptor.setValue(tableDescriptor.SPLIT_POLICY, DisabledRegionSplitPolicy.class.getName());

    admin.createTable(tableDescriptor);
  }

  private static void createProfileCacheTable(HBaseAdmin admin) throws IOException {
    HTableDescriptor tableDescriptor = new HTableDescriptor(HBaseTableMetaModel.profileCacheTableName);

    HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(HBaseTableMetaModel.profileCacheColumnFamily);
    hColumnDescriptor.setMaxVersions(1);


    tableDescriptor.addFamily(hColumnDescriptor);
    tableDescriptor.setValue(tableDescriptor.SPLIT_POLICY, DisabledRegionSplitPolicy.class.getName());

    byte[][] splitKeys = new byte[HBaseTableMetaModel.profileCacheNumberOfProfileCacheSalts][];

    for (int i = 0; i < HBaseTableMetaModel.profileCacheNumberOfProfileCacheSalts; i++) {
      char salt = (char)('A' + i);
      splitKeys[i] = Bytes.toBytes(salt);
    }

    admin.createTable(tableDescriptor, splitKeys);
  }
}
