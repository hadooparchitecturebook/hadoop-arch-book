package com.hadooparchitecturebook.frauddetection;

import com.hadooparchitecturebook.frauddetection.Utils.HBaseUtils;
import com.hadooparchitecturebook.frauddetection.Utils.UserProfileUtils;
import com.hadooparchitecturebook.frauddetection.model.*;
import com.hadooparchitecturebook.frauddetection.model.Action;
import com.google.common.cache.*;
import org.apache.flume.Event;
import org.apache.flume.api.NettyAvroRpcClient;
import org.apache.flume.api.RpcClientConfigurationConstants;
import org.apache.flume.api.RpcClientFactory;
import org.apache.flume.event.SimpleEvent;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;

public class EventProcessor {

  public static final int MAX_BATCH_PUT_SIZE = 1000;
  public static final int HBASE_PULL_FLUSH_WAIT_TIME = 5;


  static Logger LOG = Logger.getLogger(EventProcessor.class);

  //Models
  static LoadingCache<String, UserProfile> profileLocalCache;
  static ValidationRules validationRules;

  static HConnection hConnection;

  static EventProcessor eventProcessor;

  static LinkedBlockingQueue<Map.Entry<UserProfile, UserEvent>> pendingUserProfileUpdates = new LinkedBlockingQueue<Map.Entry<UserProfile, UserEvent>>();
  static LinkedBlockingQueue<Action> pendingFlumeSubmits = new LinkedBlockingQueue<Action>();

  static ScheduledThreadPoolExecutor scheduledThreadPoolExecutor;
  static ThreadPoolExecutor executorService;
  static ThreadPoolExecutor checkPutThreadPoolExecutor;

  static boolean isRunning;

  static List<String> flumeHostPortList = new ArrayList<String>();
  static boolean doCheckPutOnUserProfiles;

  private EventProcessor() {

  }

  public static synchronized EventProcessor initAndStartEventProcess(Configuration hbaseConfig, List<String> flumeList, boolean doCheckPutOnUserProfiles) throws IOException {
    if (eventProcessor == null) {
      eventProcessor = new EventProcessor();

      LOG.info("Init caching object");
      eventProcessor.profileLocalCache =
              CacheBuilder.newBuilder().maximumSize(10000).initialCapacity(1000).removalListener(new RemovalListener<String, UserProfile>() {
                @Override
                public void onRemoval(RemovalNotification<String, UserProfile> notification) {
                  LOG.info("LoadingCache Removing: key " + notification.getKey());
                }
              }).build(new CacheLoader<String, UserProfile>() {
                public UserProfile load(String key) { // no checked exception
                  LOG.info("LoadingCache load: key " + key);
                  return eventProcessor.loadProfileFromHBase(key);
                }
              });

      LOG.info("Init HBase Connection");
      //Configuration config = HBaseConfiguration.create();
      hConnection = HConnectionManager.createConnection(hbaseConfig);

      scheduledThreadPoolExecutor = new ScheduledThreadPoolExecutor(5);

      ValidationRuleFetcher.updateValidationRules();

      scheduledThreadPoolExecutor.scheduleAtFixedRate(new ValidationRuleFetcher(), 5l, 5l, TimeUnit.MINUTES);

      executorService = new ThreadPoolExecutor(20, 20, 10, TimeUnit.MINUTES, new LinkedBlockingQueue<Runnable>());

      EventProcessor.doCheckPutOnUserProfiles = doCheckPutOnUserProfiles;

      if (doCheckPutOnUserProfiles) {
        checkPutThreadPoolExecutor = new ThreadPoolExecutor(20, 20, 10, TimeUnit.MINUTES, new LinkedBlockingQueue<Runnable>());
      }


      flumeHostPortList.addAll(flumeList);

      isRunning = true;



      Thread hbaseFlusher = new Thread(new HBaseFlusher());
      hbaseFlusher.start();

      Thread flumeFluster = new Thread(new FlumeFlusher());
      flumeFluster.start();



    }
    return eventProcessor;
  }

  public static void stopAllEventProcessing() {
    isRunning = false;
    scheduledThreadPoolExecutor.shutdown();
  }

  public Action reviewUserEvent(final UserEvent userEvent) throws ExecutionException, InterruptedException {

    final UserProfile userProfile = profileLocalCache.get(userEvent.userId);

    final Action action = UserProfileUtils.reviewUserEvent(userEvent, userProfile, validationRules);

    if (action.accept) {
      userProfile.updateWithUserEvent(userEvent);
    }

    Future<Boolean> futureHBase = executorService.submit(new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        pendingUserProfileUpdates.put(new AbstractMap.SimpleEntry<UserProfile, UserEvent>(userProfile, userEvent));

        LOG.info("EventProcessor: pushed to HBase buffer:" + action.alert);
        synchronized (userProfile) {
          userProfile.wait();
        }
        LOG.info("EventProcessor: pushed to HBase:" + action.alert);
        return true;
      }
    });

    Future<Boolean> futureFlume = executorService.submit(new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        LOG.info("EventProcessor: push to flume:" + action.alert);
        pendingFlumeSubmits.put(action);
        LOG.info("EventProcessor: pushed to flume buffer:" + action.alert);
        synchronized (action) {
          action.wait();
        }
        LOG.info("EventProcessor: pushed to flume:" + action.alert);
        return true;
      }
    });

    futureFlume.get();
    futureHBase.get();

    return action;
  }

  public UserProfile loadProfileFromHBase(String key) {
    LOG.info("Getting " + key + " from HBase with rowkey{" + key + "}");

    try {
      byte[] rowKey = HBaseUtils.convertKeyToRowKey(HBaseTableMetaModel.profileCacheTableName, key);

      LOG.info("Getting " + key + " from HBase with rowkey{" + Bytes.toString(rowKey) + "}");

      Get get = new Get(rowKey);

      HTableInterface table = hConnection.getTable(HBaseTableMetaModel.profileCacheTableName);

      try {
        Result result = table.get(get);

        if (result != null) {
          NavigableMap<byte[], byte[]> familyMap = result
                  .getFamilyMap(HBaseTableMetaModel.profileCacheColumnFamily);



          return UserProfileUtils.createUserProfile(familyMap);
        } else {

            // If the UserProfile didn't exist, we create one and write it to HBase immediately
            // Since it never existed, no need to checkAndPut
          UserProfile userProfile = new UserProfile();
          userProfile.userId = key;

          Put put = new Put(rowKey);
          put.add(HBaseTableMetaModel.profileCacheColumnFamily, HBaseTableMetaModel.profileCacheJsonColumn, Bytes.toBytes(userProfile.getJSONObject().toString()));
          put.add(HBaseTableMetaModel.profileCacheColumnFamily, HBaseTableMetaModel.profileCacheTsColumn, Bytes.toBytes(System.currentTimeMillis()));
          table.put(put);

          return userProfile;
        }

      } finally {
        table.close();
      }
    } catch (Exception e) {
      throw new RuntimeException("Unable to get record from HBase:" + key, e);
    }
  }

  public static class ValidationRuleFetcher implements Runnable {
    @Override
    public void run() {
      updateValidationRules();
    }

    public static void updateValidationRules() {
      LOG.info("Updating validationRules");
      try {
        Get get = new Get(HBaseTableMetaModel.validationRulesRowKey);

        HTableInterface table = hConnection.getTable(HBaseTableMetaModel.validationRulesTableName);

        NavigableMap<byte[], byte[]> familyMap = table.get(get)
                .getFamilyMap(HBaseTableMetaModel.validationRulesColumnFamily);

        table.close();

        LOG.info("loading validations");
        validationRules = ValidationRules.Builder.buildValidationRules(familyMap);
      } catch (Exception e) {
        LOG.error(e);
        throw new RuntimeException("Unable to create validation rules: ", e);
      }
    }
  }

  public static class HBaseFlusher implements Runnable {
    @Override
    public void run() {
      while (isRunning) {
        List<Map.Entry<UserProfile, UserEvent>> userProfileList = new ArrayList<Map.Entry<UserProfile, UserEvent>>();

          // We read a batch of profiles from an event queue
        try {
          for (int i = 0; i < MAX_BATCH_PUT_SIZE; i++) {
            Map.Entry<UserProfile, UserEvent> entry = pendingUserProfileUpdates.poll();
            if (entry == null) {
              break;
            }
            userProfileList.add(entry);
          }
          if (userProfileList.size() > 0) {
            final HTableInterface table = hConnection.getTable(HBaseTableMetaModel.profileCacheTableName);

            if (doCheckPutOnUserProfiles) {

              List< Future<Map.Entry<UserProfile, UserEvent>>> futureList = new ArrayList< Future<Map.Entry<UserProfile, UserEvent>>>();

              while(!userProfileList.isEmpty()) {
                for (final Map.Entry<UserProfile, UserEvent> entry: userProfileList) {
                  futureList.add(checkPutThreadPoolExecutor.submit(new Callable<Map.Entry<UserProfile, UserEvent>>() {
                    @Override
                    public Map.Entry<UserProfile, UserEvent> call() throws Exception {
                      try {
                        byte[] rowKey = HBaseUtils.convertKeyToRowKey(HBaseTableMetaModel.profileCacheTableName, entry.getKey().userId);
                        Put put = new Put(rowKey);
                        put.add(HBaseTableMetaModel.profileCacheColumnFamily,
                                HBaseTableMetaModel.profileCacheJsonColumn,
                                Bytes.toBytes(entry.getKey().getJSONObject().toString()));

                        put.add(HBaseTableMetaModel.profileCacheColumnFamily,
                                HBaseTableMetaModel.profileCacheTsColumn,
                                Bytes.toBytes(Long.toString(System.currentTimeMillis())));

                        long timeStamp = entry.getKey().lastUpdatedTimeStamp;

                        while (!table.checkAndPut(rowKey,
                                HBaseTableMetaModel.profileCacheColumnFamily,
                                HBaseTableMetaModel.profileCacheTsColumn,
                                Bytes.toBytes(Long.toString(timeStamp)),
                                put)) {
                          //We reached here because someone else modified out userProfile
                          Get get = new Get(rowKey);

                          Result result = table.get(get);

                          NavigableMap<byte[], byte[]> familyMap = result.getFamilyMap(HBaseTableMetaModel.profileCacheColumnFamily);

                          timeStamp = Bytes.toLong(familyMap.get(HBaseTableMetaModel.profileCacheTsColumn));

                          UserProfile userProfile = new UserProfile(
                                  Bytes.toString(familyMap.get(HBaseTableMetaModel.profileCacheJsonColumn)), timeStamp);

                          userProfile.updateWithUserEvent(entry.getValue());


                          put = new Put(rowKey);
                          put.add(HBaseTableMetaModel.profileCacheColumnFamily,
                                  HBaseTableMetaModel.profileCacheJsonColumn,
                                  Bytes.toBytes(userProfile.getJSONObject().toString()));

                          put.add(HBaseTableMetaModel.profileCacheColumnFamily,
                                  HBaseTableMetaModel.profileCacheTsColumn,
                                  Bytes.toBytes(Long.toString(System.currentTimeMillis())));

                        }

                      } catch (IOException e) {
                        return entry;
                      }
                      return null;
                    }
                  }));
                }
                userProfileList.clear();
                for ( Future<Map.Entry<UserProfile, UserEvent>> future : futureList) {
                  Map.Entry<UserProfile, UserEvent> entry = future.get();
                  if (entry != null) {
                    userProfileList.add(entry);
                  }
                }
              }

            } else {

              List<Put> putList = new ArrayList<Put>();

              for (Map.Entry<UserProfile, UserEvent> entry: userProfileList) {
                Put put = new Put(HBaseUtils.convertKeyToRowKey(HBaseTableMetaModel.profileCacheTableName, entry.getKey().userId));
                put.add(HBaseTableMetaModel.profileCacheColumnFamily,
                        HBaseTableMetaModel.profileCacheJsonColumn,
                        Bytes.toBytes(entry.getKey().getJSONObject().toString()));

                put.add(HBaseTableMetaModel.profileCacheColumnFamily,
                        HBaseTableMetaModel.profileCacheTsColumn,
                        Bytes.toBytes(Long.toString(System.currentTimeMillis())));

                putList.add(put);
              }

              table.put(putList);
            }


            table.close();
          }
        } catch (Throwable t) {
          try {
            LOG.error("Problem in HBaseFlusher", t);
            pendingUserProfileUpdates.addAll(userProfileList);
          } catch (Throwable t2) {
            LOG.error("Problem in HBaseFlusher when trying to return puts to queue", t2);
          }
        } finally {
          for (Map.Entry<UserProfile, UserEvent> entry: userProfileList) {
            UserProfile userProfile = entry.getKey();
            synchronized (userProfile) {
              userProfile.notify();
            }
          }
        }
      }
      try {
        Thread.sleep(HBASE_PULL_FLUSH_WAIT_TIME);
      } catch (InterruptedException e) {
        LOG.error("Problem in HBaseFlusher", e);
      }
    }
  }

  public static class FlumeFlusher implements Runnable {

    int flumeHost = 0;

    @Override
    public void run() {

      NettyAvroRpcClient client = null;
      while (isRunning) {
        if (client == null) {
          client = getClient();
        }
        List<Event> eventActionList = new ArrayList<Event>();
        List<Action> actionList = new ArrayList<Action>();
        try {
          for (int i = 0; i < MAX_BATCH_PUT_SIZE; i++) {
            Action action = pendingFlumeSubmits.poll();
            if (action == null) {
              break;
            }
            Event event = new SimpleEvent();
            event.setBody(Bytes.toBytes(action.getJSONObject().toString()));
            eventActionList.add(event);
            actionList.add(action);
          }
          if (eventActionList.size() > 0) {
            client.appendBatch(eventActionList);
          }
        } catch (Throwable t) {
          try {
            LOG.error("Problem in HBaseFlusher", t);
            pendingFlumeSubmits.addAll(actionList);
            actionList.clear();
            client = null;
          } catch (Throwable t2) {
            LOG.error("Problem in HBaseFlusher when trying to return puts to queue", t2);
          }
        } finally {
          for (Action action: actionList) {
            synchronized (action) {
              action.notify();
            }
          }
        }
      }
      try {
        Thread.sleep(HBASE_PULL_FLUSH_WAIT_TIME);
      } catch (InterruptedException e) {
        LOG.error("Problem in HBaseFlusher", e);
      }
    }

    private NettyAvroRpcClient getClient() {
      Properties starterProp = new Properties();
      starterProp.setProperty(RpcClientConfigurationConstants.CONFIG_HOSTS, "h1");

      String hostPort = flumeHostPortList.get(flumeHost);

      starterProp.setProperty(RpcClientConfigurationConstants.CONFIG_HOSTS_PREFIX + "h1",  hostPort);

      flumeHost++;
      if (flumeHost == flumeHostPortList.size()) { flumeHost = 0; }

      LOG.info("EventProcessor: Trying to connect to " + hostPort);

      NettyAvroRpcClient client = (NettyAvroRpcClient) RpcClientFactory.getInstance(starterProp);

      LOG.info("EventProcessor: Connected to " + hostPort);

      return client;
    }
  }
}
