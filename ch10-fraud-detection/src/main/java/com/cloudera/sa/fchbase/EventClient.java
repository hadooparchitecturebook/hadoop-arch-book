package com.cloudera.sa.fchbase;

import com.cloudera.sa.fchbase.model.Action;
import com.cloudera.sa.fchbase.model.UserEvent;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import org.codehaus.jettison.json.JSONException;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.*;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;

import java.net.InetSocketAddress;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by ted.malaska on 1/18/15.
 */
public class EventClient {

  Logger log = Logger.getLogger(EventClient.class);

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

          p.addLast("handler", handler);
          return p;
        }
      });

      bootstrap.setOption("tcpNoDelay", true);
      bootstrap.setOption("receiveBufferSize", 1048576);
      bootstrap.setOption("sendBufferSize", 1048576);

      // Start the connection attempt.
      ChannelFuture future = bootstrap.connect(new InetSocketAddress(host, port));

      // Wait until the connection is closed or the connection attempt fails.
      allChannels.add(future.getChannel());

    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      // Shut down thread pools to exit.
      bootstrap.releaseExternalResources();
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
    ChannelStateEvent e;

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
      }
      // Let SimpleChannelHandler call actual event handler methods below.
      super.handleUpstream(ctx, e);
    }

    @Override
    public void channelConnected(ChannelHandlerContext ctx, ChannelStateEvent e) {
      this.e = e;
    }

    @Override
    public void channelInterestChanged(ChannelHandlerContext ctx, ChannelStateEvent e) {
      this.e = e;
    }

    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) {
      // Server is supposed to send nothing.  Therefore, do nothing.

      try {
        String actionStr = e.getMessage().toString();
        action = new Action(actionStr);
        log.info("ActionReplied:" + actionStr);
        action.notify();
      } catch (JSONException e1) {
        log.error(e1);
      }
      action = null;
      action.notify();

    }

    @Override
    public void writeComplete(ChannelHandlerContext ctx, WriteCompletionEvent e) {
      //transferredBytes += e.getWrittenAmount();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) {
      // Close the connection when an exception is raised.
      e.getCause().printStackTrace();
      e.getChannel().close();
    }

    public synchronized Action submitUserEvent(UserEvent userEvent) throws Exception {
      Channel channel = e.getChannel();

      if (channel.isWritable()) {
        ChannelBuffer m = ChannelBuffers.wrappedBuffer(Bytes.toBytes(userEvent.getJSONObject().toString()));
        channel.write(m);
      } else {
        throw new RuntimeException("WTF");
      }

      synchronized (action) {
        action.wait();
      }
      return action;
    }


  }
}
