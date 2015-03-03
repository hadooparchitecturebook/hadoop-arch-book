package com.hadooparchitecturebook.frauddetection;

import com.hadooparchitecturebook.frauddetection.model.Action;
import com.hadooparchitecturebook.frauddetection.model.UserEvent;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import org.codehaus.jettison.json.JSONException;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.*;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.group.DefaultChannelGroup;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;

import java.net.InetSocketAddress;
import java.util.concurrent.Executors;

/**
 * This class emulates the end-point of our Fraud Detection system.
 * This is where events come from. For example, this can represent the machine where users swipe credit-cards
 * Or a mobile app for payment processing
 */
public class EventClient {

  Logger LOG = Logger.getLogger(EventClient.class);

  String host;
  int port;
  NettyClientHandler handler;
  static ChannelGroup allChannels;


  public EventClient(String host, int port) {
    this.host = host;
    this.port = port;
  }

  public void startClient() {
    ClientBootstrap bootstrap = new ClientBootstrap(
            new NioClientSocketChannelFactory(
                    Executors.newCachedThreadPool(),
                    Executors.newCachedThreadPool()));

    try {
      bootstrap.setPipelineFactory(new ChannelPipelineFactory() {
        public ChannelPipeline getPipeline() {
          ChannelPipeline p = Channels.pipeline();

          handler = new NettyClientHandler();

          p.addLast("handler", handler);
          return p;
        }
      });

      bootstrap.setOption("tcpNoDelay", true);
      bootstrap.setOption("receiveBufferSize", 1048576);
      bootstrap.setOption("sendBufferSize", 1048576);

      // Start the connection attempt.

      LOG.info("EventClient: Connecting " + host + "," + port);
      ChannelFuture future = bootstrap.connect(new InetSocketAddress(host, port));
      LOG.info("EventClient: Connected " + host + "," + port);

      allChannels = new DefaultChannelGroup();

      // Wait until the connection is closed or the connection attempt fails.
      allChannels.add(future.getChannel());
      LOG.info("EventClient: Added to Channels ");

    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public void closeClient() {
    allChannels.close();
  }

  public void submitUserEvent(UserEvent userEvent) throws Exception{
    handler.submitUserEvent(userEvent);
  }

  public class NettyClientHandler extends SimpleChannelUpstreamHandler {

    private final byte[] content;
    ChannelStateEvent channelStateEvent;

    Action action;

    public NettyClientHandler() {
      content = new byte[10000];
    }


    @Override
    public void handleUpstream(ChannelHandlerContext ctx, ChannelEvent e) throws Exception {
      if (e instanceof ChannelStateEvent) {
        if (((ChannelStateEvent) e).getState() != ChannelState.INTEREST_OPS) {
          System.err.println(e);
        }
        this.channelStateEvent = (ChannelStateEvent)e;
      }

      // Let SimpleChannelHandler call actual event handler methods below.
      super.handleUpstream(ctx, e);
    }

    @Override
    public void channelConnected(ChannelHandlerContext ctx, ChannelStateEvent e) {

      this.channelStateEvent = e;
    }

    @Override
    public void channelInterestChanged(ChannelHandlerContext ctx, ChannelStateEvent e) {

      this.channelStateEvent = e;
    }

    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) {
      // Server is supposed to send nothing.  Therefore, do nothing.

      try {
        String actionStr = Bytes.toString(((ChannelBuffer) e.getMessage()).array());
        LOG.info("EventClient:ActionReplied:" + actionStr);
        action = new Action(actionStr);

        /* This is where a real end-point system would show the user whether the transaction was authorized or denied */
        LOG.info("Was transaction accepted? " + action.accept);
      } catch (JSONException e1) {
        LOG.error(e1);
        throw new RuntimeException("Unable to parse Action JSON", e1);
      }
      synchronized (this) {
        this.notify();
      }
    }

    @Override
    public void writeComplete(ChannelHandlerContext ctx, WriteCompletionEvent e) {
      //transferredBytes += e.getWrittenAmount();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent ex) {
      // Close the connection when an exception is raised.
      ex.getCause().printStackTrace();
      ex.getChannel().close();
    }

    public Action submitUserEvent(UserEvent userEvent) throws Exception {
      Channel channel = channelStateEvent.getChannel();

      if (channel.isWritable()) {
        String json = userEvent.getJSONObject().toString();
        ChannelBuffer m = ChannelBuffers.wrappedBuffer(Bytes.toBytes(json));

        LOG.info("EventClient:sending " + json);

        channel.write(m);
      } else {
        throw new RuntimeException("Channel is not writable");
      }

      synchronized (this) {
        long startTime = System.currentTimeMillis();
        LOG.info("-- EventClient:Waiting for response " + startTime);
        this.wait();
        LOG.info("-- EventClient:Got response " + (System.currentTimeMillis() - startTime));
      }

      LOG.info("EventClient:action " + action.alert);
      return action;
    }


  }
}
