package com.cloudera.fraud.example.service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.util.Bytes;

import com.cloudera.fraud.example.DataModelConsts;
import com.cloudera.fraud.example.model.ItemSaleEvent;
import com.cloudera.fraud.example.model.ProfilePojo;

public class BasicFraudHBaseService extends AbstractFraudHBaseService{
  
  static HTablePool hTablePool;
  
  static {
    Configuration hConfig = HBaseConfiguration.create();
    hTablePool = new HTablePool(hConfig, 10);
  }
  
  public ProfilePojo[] getProfilesFromHBase(List<Long> userIds) throws IOException {
    
    ArrayList<Get> getList = new ArrayList<Get>();
    for (Long userId: userIds) {
      getList.add(new Get(generateProfileRowKey(userId)));
    }
    
    HTableInterface profileTable = hTablePool.getTable(DataModelConsts.PROFILE_TABLE);
    Result[] results = profileTable.get(getList);
    
    ProfilePojo[] profiles = new ProfilePojo[getList.size()];
    
    int counter = 0;
    for (Result result: results) {
      
      String[] fixedInfoValues = Bytes.toString(result.getColumnLatest(DataModelConsts.PROFILE_COLUMN_FAMILY, DataModelConsts.FIXED_INFO_COL).getValue()).split("|");
      String username = fixedInfoValues[0];
      int age = Integer.parseInt(fixedInfoValues[1]);
      long firstLogIn = Long.parseLong(fixedInfoValues[2]);
      
      long lastLogIn = Bytes.toLong(result.getColumnLatest(DataModelConsts.PROFILE_COLUMN_FAMILY, DataModelConsts.LAST_LOG_IN_COL).getValue());
      long logInCount = Bytes.toLong(result.getColumnLatest(DataModelConsts.PROFILE_COLUMN_FAMILY, DataModelConsts.LOG_IN_COUNT_COL).getValue());
      
      long logInCountHavingASell = Bytes.toLong(result.getColumnLatest(DataModelConsts.PROFILE_COLUMN_FAMILY, DataModelConsts.TOTAL_SELLS_COL).getValue());
      long logInCountHavingAPurchase = Bytes.toLong(result.getColumnLatest(DataModelConsts.PROFILE_COLUMN_FAMILY, DataModelConsts.TOTAL_PURCHASES_COL).getValue());
      
      long totalValueOfPastPurchases = Bytes.toLong(result.getColumnLatest(DataModelConsts.PROFILE_COLUMN_FAMILY, DataModelConsts.TOTAL_VALUE_OF_PAST_PURCHASES_COL).getValue());
      long totalValueOfPastSells = Bytes.toLong(result.getColumnLatest(DataModelConsts.PROFILE_COLUMN_FAMILY, DataModelConsts.TOTAL_VALUE_OF_PAST_SELLS_COL).getValue());
      
      long currentLogInSellsValue = Bytes.toLong(result.getColumnLatest(DataModelConsts.PROFILE_COLUMN_FAMILY, DataModelConsts.CURRENT_LOG_IN_SELLS_VALUE_COL).getValue());
      long currentLogInPurchasesValue = Bytes.toLong(result.getColumnLatest(DataModelConsts.PROFILE_COLUMN_FAMILY, DataModelConsts.CURRENT_LOG_IN_PURCHASES_VALUE_COL).getValue());
      
      HashSet<String> last20LogOnIpAddresses = new HashSet<String>();
      
      List<KeyValue> ipAddresses = result.getColumn(DataModelConsts.PROFILE_COLUMN_FAMILY, DataModelConsts.LOG_IN_IP_ADDERSSES);
      boolean isFirst = true;
      String lastLogInIpAddress = null;
      
      for (KeyValue kv: ipAddresses) {
        if (isFirst) {
          isFirst = false;
          lastLogInIpAddress = Bytes.toString(kv.getValue());
        } else {
          last20LogOnIpAddresses.add(Bytes.toString(kv.getValue()));
        }
      }
      
      ProfilePojo pojo = new ProfilePojo(username, age, firstLogIn, lastLogIn, lastLogInIpAddress,
      logInCount, logInCountHavingASell, totalValueOfPastSells,
      currentLogInSellsValue, logInCountHavingAPurchase,
      totalValueOfPastPurchases, currentLogInPurchasesValue,
      last20LogOnIpAddresses);
      
      profiles[counter++] = pojo;
    }
    
    return profiles;
  }
  
  public void updateProfileCountsForSaleInHBase(Long buyerId, Long sellerId, ItemSaleEvent event) throws IOException, InterruptedException {
    HTableInterface profileTable = hTablePool.getTable(DataModelConsts.PROFILE_TABLE);
    ArrayList<Row> actions = new ArrayList<Row>();
    
    Increment buyerValueIncrement = new Increment(generateProfileRowKey(buyerId));
    buyerValueIncrement.addColumn(DataModelConsts.PROFILE_COLUMN_FAMILY, DataModelConsts.CURRENT_LOG_IN_PURCHASES_VALUE_COL, event.getItemValue());
    buyerValueIncrement.addColumn(DataModelConsts.PROFILE_COLUMN_FAMILY, DataModelConsts.TOTAL_VALUE_OF_PAST_SELLS_COL, event.getItemValue());
    actions.add(buyerValueIncrement);
    
    Increment sellerValueIncrement = new Increment(generateProfileRowKey(sellerId));
    sellerValueIncrement.addColumn(DataModelConsts.PROFILE_COLUMN_FAMILY, DataModelConsts.CURRENT_LOG_IN_SELLS_VALUE_COL, event.getItemValue());
    sellerValueIncrement.addColumn(DataModelConsts.PROFILE_COLUMN_FAMILY, DataModelConsts.TOTAL_VALUE_OF_PAST_SELLS_COL, event.getItemValue());
    actions.add(sellerValueIncrement);
    
    profileTable.batch(actions);
    
  }
  
  public void logInProfileInHBase(long userId, String ipAddress) throws IOException, Exception {
    HTableInterface profileTable = hTablePool.getTable(DataModelConsts.PROFILE_TABLE);
    
    ArrayList<Row> actions = new ArrayList<Row>();
    
    byte[] profileRowKey = generateProfileRowKey(userId);

    Delete delete = new Delete(profileRowKey);
    delete.deleteColumn(DataModelConsts.PROFILE_COLUMN_FAMILY, DataModelConsts.CURRENT_LOG_IN_PURCHASES_VALUE_COL);
    delete.deleteColumn(DataModelConsts.PROFILE_COLUMN_FAMILY, DataModelConsts.CURRENT_LOG_IN_SELLS_VALUE_COL);
    actions.add(delete);
    
    Increment increment = new Increment(profileRowKey);
    increment.addColumn(DataModelConsts.PROFILE_COLUMN_FAMILY, DataModelConsts.LOG_IN_COUNT_COL, 1);
    actions.add(increment);
    
    Put put = new Put(profileRowKey);
    put.add(DataModelConsts.PROFILE_COLUMN_FAMILY, DataModelConsts.LAST_LOG_IN_COL, Bytes.toBytes(System.currentTimeMillis()));
    put.add(DataModelConsts.PROFILE_COLUMN_FAMILY, DataModelConsts.LOG_IN_IP_ADDERSSES, Bytes.toBytes(ipAddress));
    actions.add(put);
    
    profileTable.batch(actions);
  }

  @Override
  public void createProfile(long userId, ProfilePojo pojo, String ipAddress) throws Exception {
    HTableInterface profileTable = hTablePool.getTable(DataModelConsts.PROFILE_TABLE);
    
    ArrayList<Row> actions = new ArrayList<Row>();
    
    byte[] rowKey = generateProfileRowKey(userId);
    Put put = new Put(rowKey);
    put.add(DataModelConsts.PROFILE_COLUMN_FAMILY, DataModelConsts.FIXED_INFO_COL, Bytes.toBytes(pojo.getUsername() + "|" + pojo.getAge() + "|" + System.currentTimeMillis()));
    put.add(DataModelConsts.PROFILE_COLUMN_FAMILY, DataModelConsts.LOG_IN_IP_ADDERSSES, Bytes.toBytes(ipAddress));
    put.add(DataModelConsts.PROFILE_COLUMN_FAMILY, DataModelConsts.LAST_LOG_IN_COL, Bytes.toBytes(System.currentTimeMillis()));
    actions.add(put);
    
    Increment increment = new Increment(rowKey);
    
    increment.addColumn(DataModelConsts.PROFILE_COLUMN_FAMILY, DataModelConsts.LOG_IN_COUNT_COL, 1);
    increment.addColumn(DataModelConsts.PROFILE_COLUMN_FAMILY, DataModelConsts.TOTAL_SELLS_COL, 0);
    increment.addColumn(DataModelConsts.PROFILE_COLUMN_FAMILY, DataModelConsts.TOTAL_PURCHASES_COL, 0);
    increment.addColumn(DataModelConsts.PROFILE_COLUMN_FAMILY, DataModelConsts.TOTAL_VALUE_OF_PAST_PURCHASES_COL, 0);
    increment.addColumn(DataModelConsts.PROFILE_COLUMN_FAMILY, DataModelConsts.TOTAL_VALUE_OF_PAST_SELLS_COL, 0);
    increment.addColumn(DataModelConsts.PROFILE_COLUMN_FAMILY, DataModelConsts.CURRENT_LOG_IN_SELLS_VALUE_COL, 0);
    increment.addColumn(DataModelConsts.PROFILE_COLUMN_FAMILY, DataModelConsts.CURRENT_LOG_IN_PURCHASES_VALUE_COL, 0);
    actions.add(increment);
    
    profileTable.batch(actions);
  }

  @Override
  public void createBulkProfile(ArrayList<ProfileCreatePojo> pojoList)
      throws Exception {
    
    HTableInterface profileTable = hTablePool.getTable(DataModelConsts.PROFILE_TABLE);
    ArrayList<Row> actions = new ArrayList<Row>();
    
    for (ProfileCreatePojo pojo: pojoList) {
      
      
      byte[] rowKey = generateProfileRowKey(pojo.getUserId());
      Put put = new Put(rowKey);
      put.add(DataModelConsts.PROFILE_COLUMN_FAMILY, DataModelConsts.FIXED_INFO_COL, Bytes.toBytes(pojo.getPojo().getUsername() + "|" + pojo.getPojo().getAge() + "|" + System.currentTimeMillis()));
      put.add(DataModelConsts.PROFILE_COLUMN_FAMILY, DataModelConsts.LOG_IN_IP_ADDERSSES, Bytes.toBytes(pojo.getIpAddress()));
      put.add(DataModelConsts.PROFILE_COLUMN_FAMILY, DataModelConsts.LAST_LOG_IN_COL, Bytes.toBytes(System.currentTimeMillis()));
      actions.add(put);
      
      Increment increment = new Increment(rowKey);
      
      increment.addColumn(DataModelConsts.PROFILE_COLUMN_FAMILY, DataModelConsts.LOG_IN_COUNT_COL, 1);
      increment.addColumn(DataModelConsts.PROFILE_COLUMN_FAMILY, DataModelConsts.TOTAL_SELLS_COL, 0);
      increment.addColumn(DataModelConsts.PROFILE_COLUMN_FAMILY, DataModelConsts.TOTAL_PURCHASES_COL, 0);
      increment.addColumn(DataModelConsts.PROFILE_COLUMN_FAMILY, DataModelConsts.TOTAL_VALUE_OF_PAST_PURCHASES_COL, 0);
      increment.addColumn(DataModelConsts.PROFILE_COLUMN_FAMILY, DataModelConsts.TOTAL_VALUE_OF_PAST_SELLS_COL, 0);
      increment.addColumn(DataModelConsts.PROFILE_COLUMN_FAMILY, DataModelConsts.CURRENT_LOG_IN_SELLS_VALUE_COL, 0);
      increment.addColumn(DataModelConsts.PROFILE_COLUMN_FAMILY, DataModelConsts.CURRENT_LOG_IN_PURCHASES_VALUE_COL, 0);
      actions.add(increment);
    }

    profileTable.batch(actions);
  }
  
  
}
