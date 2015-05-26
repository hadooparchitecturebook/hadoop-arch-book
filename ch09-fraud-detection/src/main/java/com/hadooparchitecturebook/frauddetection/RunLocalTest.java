package com.hadooparchitecturebook.frauddetection;

import com.hadooparchitecturebook.frauddetection.Utils.HBaseUtils;
import com.hadooparchitecturebook.frauddetection.model.UserEvent;
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

  public static Logger LOG = Logger.getLogger(RunLocalTest.class);

  public static void main(String[] args) throws Exception{

    HBaseTestingUtility htu = HBaseTestingUtility.createLocalHTU();
    Configuration config = htu.getConfiguration();

    htu.cleanupTestDir();
    htu.startMiniZKCluster();
    htu.startMiniHBaseCluster(1, 1);

    RemoveTables.executeDeleteTables(config);

    CreateTables.executeCreateTables(config);



    //Start up servers
    Server flumeTestServer = startTestFlumeServer(4243);

    List<String> flumePorts = new ArrayList<String>();
    flumePorts.add("127.0.0.1:4243");
    EventReviewServer server = new EventReviewServer(4242, config, flumePorts, false);
    server.startServer();

    EventClient client = new EventClient("127.0.0.1", 4242);
    client.startClient();

    HConnection connection = HConnectionManager.createConnection(config);

    //popoulate initial data
    populateUserProfileData(connection);
    populateValidationRules(connection);

    //populate user events
    UserEvent userEvent = new UserEvent("101", System.currentTimeMillis(),
            "127.0.0.1", "1", "55555",
            "42", 100.0, "101", true);

    client.submitUserEvent(userEvent);

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
              new InetSocketAddress("127.0.0.1", port));

    server.start();
    LOG.info("Server started on test flume server hostname: localhost, port: " + port);

    try {

      Thread.sleep(1000L);

    } catch (InterruptedException ex) {
      LOG.error("Thread interrupted. Exception follows.", ex);
      Thread.currentThread().interrupt();
    }

    return server;
  }

  public static void stopTestFlumeServer(Server server) {
    try {
      server.close();
      server.join();
    } catch (InterruptedException ex) {
      LOG.error("Thread interrupted. Exception follows.", ex);
      Thread.currentThread().interrupt();
    }
  }

  public static class OKAvroHandler implements AvroSourceProtocol {

    @Override
    public Status append(AvroFlumeEvent event) throws AvroRemoteException {
      LOG.info("OK: Received event from append(): " + new String(event.getBody().array(), Charset.forName("UTF8")));
      return Status.OK;
    }

    @Override
    public Status appendBatch(List<AvroFlumeEvent> events) throws
            AvroRemoteException {
      LOG.info("OK: Received " + events.size() + " events from appendBatch()");
      return Status.OK;
    }

  }
}
