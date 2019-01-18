/*
 * Copyright (C) 2009 Swedish Institute of Computer Science (SICS) Copyright (C)
 * 2009 Royal Institute of Technology (KTH)
 *
 * GVoD is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
 */
package se.sics.dela.network.ledbat2;

import com.google.common.base.Optional;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.AdaptiveRecvByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.DatagramChannel;
import io.netty.channel.socket.DatagramPacket;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.util.concurrent.Future;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang.NotImplementedException;
import org.slf4j.Logger;
import org.slf4j.MDC;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Handler;
import se.sics.kompics.KompicsEvent;
import se.sics.kompics.Negative;
import se.sics.kompics.Start;
import se.sics.kompics.Stop;
import se.sics.kompics.network.Address;
import se.sics.kompics.network.ConnectionStatus;
import se.sics.kompics.network.MessageNotify;
import se.sics.kompics.network.Msg;
import se.sics.kompics.network.Network;
import se.sics.kompics.network.NetworkException;
import se.sics.kompics.network.Transport;
import se.sics.kompics.network.netty.AckRequestMsg;
import se.sics.kompics.network.netty.CheckChannelActive;
import se.sics.kompics.network.netty.CloseChannel;
import se.sics.kompics.network.netty.DisambiguateConnection;
import se.sics.kompics.network.netty.NettyAddress;
import se.sics.kompics.network.netty.NotifyAck;
import se.sics.kompics.network.netty.serialization.Serializers;

/**
 *
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class LedbatNetwork extends ComponentDefinition {

  static final int RECV_BUFFER_SIZE = 65536;
  static final int SEND_BUFFER_SIZE = 65536;
  static final int INITIAL_BUFFER_SIZE = 512;
  private static final int CONNECT_TIMEOUT_MS = 5000;

  Negative<Network> net = provides(Network.class);

  private Bootstrap bootstrapUDP;
  private DatagramChannel udpChannel;
  final NettyAddress self;
  private final int boundPort;
  private InetAddress alternativeBindIf = null;

  final MessageQueueManager messages = new MessageQueueManager(this);

  public static final String MDC_KEY_PORT = "knet-port";
  public static final String MDC_KEY_IF = "knet-if";
  public static final String MDC_KEY_ALT_IF = "knet-alt-if";
  final Logger extLog = this.logger;

  private final Map<String, String> customLogCtx = new HashMap<>();

  void setCustomMDC() {
    MDC.setContextMap(customLogCtx);
  }

  private void initLoggingCtx() {
    for (Map.Entry<String, String> e : customLogCtx.entrySet()) {
      loggingCtxPutAlways(e.getKey(), e.getValue());
    }
  }

  public LedbatNetwork(Init init) {
    // probably useless to set here as it won't be re-read in most JVMs after start
    System.setProperty("java.net.preferIPv4Stack", "true");

    self = new NettyAddress(init.self);
    customLogCtx.put(MDC_KEY_PORT, Integer.toString(self.getPort()));
    customLogCtx.put(MDC_KEY_IF, self.getIp().getHostAddress());

    boundPort = self.getPort();

    // CONFIG
    Optional<InetAddress> abiO = config().readValue("netty.bindInterface", InetAddress.class);
    if (abiO.isPresent()) {
      alternativeBindIf = abiO.get();
      customLogCtx.put(MDC_KEY_ALT_IF, self.getIp().getHostAddress());
    }

    initLoggingCtx();

    logger.info("Alternative Bind Interface set to {}", alternativeBindIf);

    subscribe(startHandler, control);
    subscribe(stopHandler, control);
    subscribe(msgHandler, net);
  }

  Handler<Start> startHandler = new Handler<Start>() {

    @Override
    public void handle(Start event) {
      // Prepare listening sockets
      InetAddress bindIp = self.getIp();
      if (alternativeBindIf != null) {
        bindIp = alternativeBindIf;
      }
      bindUdpPort(bindIp, self.getPort());
    }
  };

  Handler<Stop> stopHandler = new Handler<Stop>() {

    @Override
    public void handle(Stop event) {
      clearConnections();
    }
  };

  Handler<Msg> msgHandler = new Handler<Msg>() {

    @Override
    public void handle(Msg event) {
      if (event.getDestination().sameHostAs(self)) {
        logger.trace("Delivering message {} locally.", event);
        trigger(event, net);
        return;
      }

      messages.send(event);
    }
  };

  private boolean bindUdpPort(final InetAddress addr, final int port) {

    EventLoopGroup group = new NioEventLoopGroup();
    bootstrapUDP = new Bootstrap();
    bootstrapUDP.group(group).channel(NioDatagramChannel.class)
      .handler(new DatagramHandler(this, Transport.UDP));

    bootstrapUDP.option(ChannelOption.RCVBUF_ALLOCATOR, new AdaptiveRecvByteBufAllocator(1500, 1500, RECV_BUFFER_SIZE));
    bootstrapUDP.option(ChannelOption.SO_RCVBUF, RECV_BUFFER_SIZE);
    bootstrapUDP.option(ChannelOption.SO_SNDBUF, SEND_BUFFER_SIZE);
    // bootstrap.setOption("trafficClass", trafficClass);
    // bootstrap.setOption("soTimeout", soTimeout);
    // bootstrap.setOption("broadcast", broadcast);
    bootstrapUDP.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, CONNECT_TIMEOUT_MS);
    bootstrapUDP.option(ChannelOption.SO_REUSEADDR, true);

    try {
      InetSocketAddress iAddr = new InetSocketAddress(addr, port);
      udpChannel = (DatagramChannel) bootstrapUDP.bind(iAddr).sync().channel();

      //addLocalSocket(iAddr, c);
      logger.info("Successfully bound to ip:port {}:{}", addr, port);
    } catch (InterruptedException e) {
      logger.error("Problem when trying to bind to {}:{}", addr.getHostAddress(), port);
      return false;
    }

    return true;
  }

  protected void networkException(NetworkException networkException) {
    throw new NotImplementedException("ups");
  }

  protected void networkStatus(ConnectionStatus status) {
    throw new NotImplementedException("ups");
  }

  protected void deliverMessage(Msg message, Channel c) {
    if (message instanceof NotifyAck) {
      NotifyAck ack = (NotifyAck) message;
      logger.trace("Got NotifyAck for {}", ack.id);
      messages.ack(ack);
      return;
    }
    logger.debug("Delivering message {} from {} to {} protocol {}",
      new Object[]{message.toString(), message.getSource(), message.getDestination(), message.getProtocol()});
    trigger(message, net);
  }

  ChannelFuture sendUdpMessage(MessageWrapper msgw) {
    ByteBuf buf = udpChannel.alloc().ioBuffer(INITIAL_BUFFER_SIZE, SEND_BUFFER_SIZE);
    try {
      if (msgw.notify.isPresent() && msgw.notify.get().notifyOfDelivery) {
        MessageNotify.Req msgr = msgw.notify.get();
        AckRequestMsg arm = new AckRequestMsg(msgw.msg, msgr.getMsgId());
        Serializers.toBinary(arm, buf);
      } else {
        Serializers.toBinary(msgw.msg, buf);
      }
      msgw.injectSize(buf.readableBytes(), System.nanoTime());
      DatagramPacket pack = new DatagramPacket(buf, msgw.msg.getDestination().asSocket());
      logger.debug("Sending Datagram message {} ({}bytes)", msgw.msg, buf.readableBytes());
      return udpChannel.writeAndFlush(pack);
    } catch (Exception e) { // serialization might fail horribly with size bounded buff
      logger.warn("Could not send Datagram message {}, error was: {}", msgw, e);
      return null;
    }
  }

  private void clearConnections() {

    long tstart = System.currentTimeMillis();

    try {
      udpChannel.close().syncUninterruptibly();
    } catch (Exception ex) {
      logger.warn("Error during Ledbat shutdown. Messages might have been lost! \n {}", ex);
    }

    long tend = System.currentTimeMillis();

    logger.info("Closed all connections in {}ms", tend - tstart);
  }

  @Override
  public void tearDown() {
    long tstart = System.currentTimeMillis();

    clearConnections();

    logger.info("Shutting down handler groups...");
    Future endUdp = bootstrapUDP.group().shutdownGracefully(1, 5, TimeUnit.MILLISECONDS);
    endUdp.syncUninterruptibly();
    bootstrapUDP = null;

    long tend = System.currentTimeMillis();

    logger.info("Ledbat shutdown complete. It took {}ms", tend - tstart);
  }

  void trigger(KompicsEvent event) {
    if (event instanceof Msg) {
      throw new RuntimeException("Not support anymore!");
      //trigger(event, net.getPair());
    } else {
      trigger(event, onSelf);
    }
  }

  void notify(MessageNotify.Req notify) {
    answer(notify);
  }

  void notify(MessageNotify.Req notify, MessageNotify.Resp response) {
    answer(notify, response);
  }

  public static class Init extends se.sics.kompics.Init<LedbatNetwork> {

    public final Address self;

    public Init(Address self) {
      this.self = self;
    }
  }
}
