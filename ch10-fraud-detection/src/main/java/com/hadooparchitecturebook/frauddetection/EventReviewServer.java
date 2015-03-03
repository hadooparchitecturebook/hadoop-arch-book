package com.hadooparchitecturebook.frauddetection;

import com.hadooparchitecturebook.frauddetection.model.Action;
import com.hadooparchitecturebook.frauddetection.model.UserEvent;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import org.codehaus.jettison.json.JSONException;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.*;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.Executors;



/**
 * Created by ted.malaska on 1/18/15.
 */
public class EventReviewServer {

  static Logger LOG = Logger.getLogger(EventReviewServer.class);

  static EventServerHandler handler;
  static Channel channel;
  public static EventProcessor eventProcessor;

  int portNumber;
  Configuration config;
  List<String> flumePorts;
  boolean useCheckPut;

  public EventReviewServer(int portNumber, Configuration config, List<String> flumePorts, boolean useCheckPut) {
    this.portNumber = portNumber;
    this.config = config;
    this.flumePorts = flumePorts;
    this.useCheckPut = useCheckPut;
  }

  public void startServer() throws Exception {

    eventProcessor = EventProcessor.initAndStartEventProcess(config, flumePorts, useCheckPut);

    handler = new EventServerHandler();

    ServerBootstrap bootstrap = new ServerBootstrap(
            new NioServerSocketChannelFactory(
                    Executors.newCachedThreadPool(),
                    Executors.newCachedThreadPool()));

    bootstrap.setOption("child.tcpNoDelay", true);
    bootstrap.setOption("child.receiveBufferSize", 1048576);
    bootstrap.setOption("child.sendBufferSize", 1048576);

    bootstrap.setPipelineFactory(new ChannelPipelineFactory() {
      public ChannelPipeline getPipeline() {
        ChannelPipeline p = Channels.pipeline();
        p.addLast("batch", handler);
        return p;
      }
    });

    LOG.info("EventReviewServer: binding to :" + portNumber);
    channel = bootstrap.bind(new InetSocketAddress(portNumber));

    LOG.info("EventReviewServer: bound to :" + portNumber + " " + channel.isBound() + " " + channel.getLocalAddress());


  }

  public void closeServer() {
    channel.close();
  }

  public static class EventServerHandler extends SimpleChannelUpstreamHandler {

    private long transferredBytes;

    public long getTransferredBytes() {
      return transferredBytes;
    }

    public EventServerHandler() {
    }

    @Override
    public void handleUpstream(ChannelHandlerContext ctx, ChannelEvent e) throws Exception {
      if (e instanceof ChannelStateEvent) {
        System.err.println(e);
      }

      // Let SimpleChannelHandler call actual event handler methods below.
      super.handleUpstream(ctx, e);
    }

    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) {
      // Discard received data silently by doing nothing.
      //String[] events = ((ChannelBuffer) e.getMessage()).toString().split("\n");

      String jsonString = Bytes.toString(((ChannelBuffer) e.getMessage()).array());

      try {
        LOG.info("EventReviewServer: Get Message: " + jsonString);
        UserEvent userEvent = new UserEvent(jsonString);
        Action action = eventProcessor.reviewUserEvent(userEvent);


        String json = action.getJSONObject().toString();
        LOG.info("EventReviewServer: Got Action: " + json);
        e.getChannel().write(ChannelBuffers.wrappedBuffer(Bytes.toBytes(json)));

      } catch (JSONException e1) {
        throw new RuntimeException("Unable to parse JSON:" + jsonString, e1);
      } catch (Exception e1) {
        throw new RuntimeException("Unable to process JSON:" + jsonString, e1);
      }


    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) {
      // Close the connection when an exception is raised.
      e.getCause().printStackTrace();
      e.getChannel().close();
    }
  }

}

