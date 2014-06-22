package com.cloudera.fraud.example;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.regionserver.ConstantSizeRegionSplitPolicy;
import org.apache.hadoop.hbase.regionserver.StoreFile.BloomType;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.io.hfile.Compression;

import com.cloudera.fraud.example.model.Alert;
import com.cloudera.fraud.example.model.ItemSaleEvent;
import com.cloudera.fraud.example.model.ProfilePojo;
import com.cloudera.fraud.example.service.AbstractCacheFraudService;
import com.cloudera.fraud.example.service.AbstractFraudHBaseService;
import com.cloudera.fraud.example.service.EventualPutFraudHBaseService;
import com.cloudera.fraud.example.service.ServiceFactory;
import com.cloudera.fraud.example.service.AbstractFraudHBaseService.ProfileCreatePojo;

public class App {
  
  public static void main(String[] args) throws Exception {
    
    if (args.length == 0) {
      System.out.println("FraudAlerterMain {numberOfUsers} {numberOfEvents} {numberOfThreads} {cacheLayer} {hbaseServiceLayer}");
      return;
    }
    
    int numberOfUsers = Integer.parseInt(args[0]);
    int numberOfEvents = Integer.parseInt(args[1]);
    int numberOfThreads = Integer.parseInt(args[2]);
    String cacheLayer = args[3];
    String hbaseServiceLayer = args[4];
    
    if (createProfileTable()) {
      populateUsers(numberOfUsers);
    }
    
    runEventTest(numberOfUsers, numberOfEvents, numberOfThreads, cacheLayer, hbaseServiceLayer);
  }

  private static void runEventTest(final int numberOfUsers, int numberOfEvents,
      int numberOfThreads, String cacheServiceType, String hbaseServiceType) {
    
    final AbstractFraudHBaseService hbaseService = ServiceFactory.initHBaseService(hbaseServiceType);
    final AbstractCacheFraudService cacheService = ServiceFactory.initCacheService(cacheServiceType, hbaseService);
    
    ExecutorService executorService = Executors.newFixedThreadPool(numberOfThreads);
    
    final AtomicLong alertCount = new AtomicLong();
    final AtomicLong eventCount = new AtomicLong();
    
    for (int i = 0; i < numberOfEvents/5; i++) {
      executorService.execute(new Runnable() {

        public void run() {
          Random r = new Random();
          
          String ipAddress;
          
          if (r.nextInt(100) == 50) {
            ipAddress = TestConsts.UNSAFE_UP_ADDRESS_LIST.get(r.nextInt(TestConsts.UNSAFE_UP_ADDRESS_LIST.size()));
          } else {
            ipAddress = TestConsts.SAFE_UP_ADDRESS_LIST.get(r.nextInt(TestConsts.SAFE_UP_ADDRESS_LIST.size()));
          }
          
          try {
            int buyerId = r.nextInt(numberOfUsers);
            int sellerId = r.nextInt(numberOfUsers);
            
            cacheService.logInProfileInHBase(r.nextInt(numberOfUsers),ipAddress);
            int eventsToDo = r.nextInt(10);
            for (int i = 0; i < eventsToDo; i++) {
              eventCount.addAndGet(1);
              long itemValue;
              if (r.nextInt(50) == 25) {
                itemValue = r.nextInt(100000);
              } else {
                itemValue = r.nextInt(10000);
              }
              
              ItemSaleEvent event = new ItemSaleEvent(buyerId, sellerId, r.nextLong(),
                  itemValue);
              
              List<Alert> alerts = cacheService.processEventForAlerts(event);
              
              alertCount.addAndGet(alerts.size());
              
            }
            
          } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
          }
          System.out.print(".");
        }
      });
    }
    
    System.out.println("Total Events:" + eventCount.get());
    System.out.println("Total Alerts:" + alertCount.get());
    System.out.println("Total LogIns:" + numberOfEvents/5);
  }

  public static boolean createProfileTable() throws IOException {

    long regionMaxSize = 107374182400l;

    HBaseAdmin admin = new HBaseAdmin(new Configuration());

    boolean results = createTable(DataModelConsts.PROFILE_TABLE,
        DataModelConsts.PROFILE_COLUMN_FAMILY, (short) 10, regionMaxSize, admin);

    admin.close();
    return results;
  }

  private static boolean createTable(byte[] tableName, byte[] columnFamilyName,
      short regionCount, long regionMaxSize, HBaseAdmin admin)
      throws IOException {

    if (admin.tableExists(tableName)) {
      return false;
    }

    HTableDescriptor tableDescriptor = new HTableDescriptor();
    tableDescriptor.setName(tableName);

    HColumnDescriptor columnDescriptor = new HColumnDescriptor(columnFamilyName);

    columnDescriptor.setCompressionType(Compression.Algorithm.SNAPPY);
    columnDescriptor.setBlocksize(64 * 1024);
    columnDescriptor.setBloomFilterType(BloomType.ROW);
    columnDescriptor.setMaxVersions(10);
    tableDescriptor.addFamily(columnDescriptor);

    tableDescriptor.setMaxFileSize(regionMaxSize);
    tableDescriptor.setValue(tableDescriptor.SPLIT_POLICY,
        ConstantSizeRegionSplitPolicy.class.getName());

    tableDescriptor.setDeferredLogFlush(true);

    regionCount = (short) Math.abs(regionCount);

    int regionRange = Short.MAX_VALUE / regionCount;
    int counter = 0;

    byte[][] splitKeys = new byte[regionCount][];
    for (byte[] splitKey : splitKeys) {
      counter = counter + regionRange;
      String key = StringUtils.leftPad(Integer.toString(counter), 5, '0');
      splitKey = Bytes.toBytes(key);
      System.out.println(" - Split: " + splitKey);
    }
    return true;
  }

  private static void populateUsers(int numberOfUsers) throws Exception {
    EventualPutFraudHBaseService service = new EventualPutFraudHBaseService();

    ArrayList<ProfileCreatePojo> pojoList = new ArrayList<ProfileCreatePojo>();
    
    Random r = new Random();
    
    for (long i = 0; i < numberOfUsers; i++) {
      
      String ipAddress = TestConsts.SAFE_UP_ADDRESS_LIST.get(r.nextInt(TestConsts.SAFE_UP_ADDRESS_LIST.size()));
      
      pojoList.add(new ProfileCreatePojo(i, new ProfilePojo("user-" + i, (int)(i%30 + 10), ipAddress), ipAddress));
      
      if (i%1000 == 0) {
        service.createBulkProfile(pojoList);
        pojoList.clear();
      }
    }
    if (pojoList.size() > 0) {
      service.createBulkProfile(pojoList);
    }
  }
}
