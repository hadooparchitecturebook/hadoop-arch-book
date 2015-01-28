package com.hadooparchitecturebook.frauddetection;

import com.hadooparchitecturebook.frauddetection.Utils.HBaseUtils;
import com.hadooparchitecturebook.frauddetection.model.UserProfile;
import com.hadooparchitecturebook.frauddetection.model.ValidationRules;
import org.apache.avro.AvroRemoteException;
import org.apache.avro.ipc.NettyServer;
import org.apache.avro.ipc.Responder;
import org.apache.avro.ipc.specific.SpecificResponder;
import org.apache.flume.source.avro.AvroFlumeEvent;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.avro.ipc.Server;
import org.apache.flume.source.avro.AvroSourceProtocol;
import org.apache.log4j.Logger;

import java.net.InetSocketAddress;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import org.apache.flume.source.avro.Status;

/**
 * Created by ted.malaska on 1/19/15.
 */
public class RunLocalTest {

  public static Logger log = Logger.getLogger(RunLocalTest.class);

  public static void main(String[] args) throws Exception{

    HBaseTestingUtility htu = HBaseTestingUtility.createLocalHTU();
    Configuration config = htu.getConfiguration();

    htu.cleanupTestDir();
    htu.startMiniZKCluster();
    htu.startMiniHBaseCluster(1, 1);

    RemoveTables.executeDeleteTables(config);

    CreateTables.executeCreateTables(config);

    List<String> flumePorts = new ArrayList<String>();
    flumePorts.add("localhost:4243");

    EventReviewServer server = new EventReviewServer(4242, config, flumePorts, false);

    EventClient client = new EventClient("localhost", 4242);

    //Start up servers
    server.startServer();
    Server flumeTestServer = startTestFlumeServer(4243);
    client.startClient();

    HConnection connection = HConnectionManager.createConnection(config);

    //popoulate initial data
    populateUserProfileData(connection);
    populateValidationRules(connection);

    //populate user events
    //TODO send test userEvents

    //shut down servers
    client.closeClient();
    server.closeServer();
    stopTestFlumeServer(flumeTestServer);
    htu.shutdownMiniCluster();

  }

  private static void populateValidationRules(HConnection connection) throws Exception {
    HashSet<String> banndedVandors = new HashSet<String>();
    banndedVandors.add("badVendor");

    ValidationRules rules = new ValidationRules(banndedVandors, 2.0);

    HBaseUtils.populateValidationRules(connection, rules);

  }

  private static void populateUserProfileData(HConnection connection) throws Exception {
    UserProfile up1 = new UserProfile();
    up1.userId = "101";
    up1.lastUpdatedTimeStamp = System.currentTimeMillis() - 1000;
    up1.historicAvg90PercentSingleDaySpend = 90.0;
    up1.historicAvgSingleDaySpend = 50.0;
    up1.todayMaxSpend = 0.0;
    up1.todayNumOfPurchases = 0l;
    HBaseUtils.populateUserProfile(connection, up1);

    up1.userId = "102";
    up1.lastUpdatedTimeStamp = System.currentTimeMillis() - 1000;
    up1.historicAvg90PercentSingleDaySpend = 90.0;
    up1.historicAvgSingleDaySpend = 50.0;
    up1.todayMaxSpend = 0.0;
    up1.todayNumOfPurchases = 0l;
    HBaseUtils.populateUserProfile(connection, up1);

    up1.userId = "103";
    up1.lastUpdatedTimeStamp = System.currentTimeMillis() - 1000;
    up1.historicAvg90PercentSingleDaySpend = 90.0;
    up1.historicAvgSingleDaySpend = 50.0;
    up1.todayMaxSpend = 0.0;
    up1.todayNumOfPurchases = 0l;
    HBaseUtils.populateUserProfile(connection, up1);
  }



  public static Server startTestFlumeServer(int port) {
    Responder responder = new SpecificResponder(AvroSourceProtocol.class,
            new OKAvroHandler());
    Server server = new NettyServer(responder,
              new InetSocketAddress("localhost", port));

    server.start();
    log.info("Server started on test flume server hostname: localhost, port: " + port);

    try {

      Thread.sleep(300L);

    } catch (InterruptedException ex) {
      log.error("Thread interrupted. Exception follows.", ex);
      Thread.currentThread().interrupt();
    }

    return server;
  }

  public static void stopTestFlumeServer(Server server) {
    try {
      server.close();
      server.join();
    } catch (InterruptedException ex) {
      log.error("Thread interrupted. Exception follows.", ex);
      Thread.currentThread().interrupt();
    }
  }

  public static class OKAvroHandler implements AvroSourceProtocol {

    @Override
    public Status append(AvroFlumeEvent event) throws AvroRemoteException {
      log.info("OK: Received event from append(): " + new String(event.getBody().array(), Charset.forName("UTF8")));
      return Status.OK;
    }

    @Override
    public Status appendBatch(List<AvroFlumeEvent> events) throws
            AvroRemoteException {
      log.info("OK: Received " + events.size() + " events from appendBatch()");
      return Status.OK;
    }

  }
}
