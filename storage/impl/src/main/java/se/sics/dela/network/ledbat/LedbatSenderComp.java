/*
 * Copyright (C) 2009 Swedish Institute of Computer Science (SICS) Copyright (C)
 * 2009 Royal Institute of Technology (KTH)
 *
 * KompicsToolbox is free software; you can redistribute it and/or
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
 * along with this program; if not, loss to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
 */
package se.sics.dela.network.ledbat;

import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import org.apache.commons.math3.stat.descriptive.SummaryStatistics;
import se.sics.kompics.ClassMatchedHandler;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Handler;
import se.sics.kompics.Negative;
import se.sics.kompics.Positive;
import se.sics.kompics.Start;
import se.sics.kompics.network.Network;
import se.sics.kompics.network.Transport;
import se.sics.kompics.timer.Timer;
import se.sics.kompics.util.Identifiable;
import se.sics.kompics.util.Identifier;
import se.sics.ktoolbox.nutil.timer.TimerProxy;
import se.sics.ktoolbox.nutil.timer.TimerProxyImpl;
import se.sics.ktoolbox.util.config.impl.SystemKCWrapper;
import se.sics.ktoolbox.util.identifiable.BasicIdentifiers;
import se.sics.ktoolbox.util.identifiable.IdentifierFactory;
import se.sics.ktoolbox.util.identifiable.IdentifierRegistryV2;
import se.sics.ktoolbox.util.network.KAddress;
import se.sics.ktoolbox.util.network.KContentMsg;
import se.sics.ktoolbox.util.network.KHeader;
import se.sics.ktoolbox.util.network.basic.BasicContentMsg;
import se.sics.ktoolbox.util.network.basic.BasicHeader;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class LedbatSenderComp extends ComponentDefinition {

  Negative<LedbatSenderPort> appPort = provides(LedbatSenderPort.class);
  Positive<Network> networkPort = requires(Network.class);
  Positive<Timer> timerPort = requires(Timer.class);
  TimerProxy timer;
  private final KAddress selfAdr;
  private final KAddress dstAdr;
  private final Identifier rivuletId;
  private final LedbatSender sender;
  private final IdentifierFactory msgIds;

//  private SummaryStatistics s1 = new SummaryStatistics();
//  private SummaryStatistics s2 = new SummaryStatistics();
//  private SummaryStatistics s3 = new SummaryStatistics();
//  private SummaryStatistics s4 = new SummaryStatistics();

  public LedbatSenderComp(Init init) {
    this.selfAdr = init.selfAdr;
    this.dstAdr = init.dstAdr;
    this.rivuletId = init.rivuletId;
    timer = new TimerProxyImpl(dstAdr.getId()).setup(proxy, logger);
    sender = new LedbatSender().setup(timer, config(), logger, networkSend(), appSend());

    loggingCtxPutAlways("srcId", init.selfAdr.getId().toString());
    loggingCtxPutAlways("dstId", init.dstAdr.getId().toString());

    SystemKCWrapper systemConfig = new SystemKCWrapper(config());
    long ledbatSeed = systemConfig.seed
      + init.selfAdr.getId().partition(Integer.MAX_VALUE)
      + init.dstAdr.getId().partition(Integer.MAX_VALUE);
    this.msgIds = IdentifierRegistryV2.instance(BasicIdentifiers.Values.MSG, Optional.of(ledbatSeed));

    subscribe(handleStart, control);
    subscribe(handleReq, appPort);
    subscribe(handleAck, networkPort);
  }

  Handler handleStart = new Handler<Start>() {
    @Override
    public void handle(Start event) {
      sender.start();
      timer.setup(proxy, logger);
    }
  };

  @Override
  public void tearDown() {
    sender.stop();
    timer.cancel();
  }

  Handler handleReq = new Handler<LedbatSenderEvent.Request>() {
    @Override
    public void handle(LedbatSenderEvent.Request req) {
      logger.trace("buffer:{}", req);
      sender.bufferData(req);
    }
  };

  ClassMatchedHandler handleAck
    = new ClassMatchedHandler<LedbatMsg.Ack, KContentMsg<?, ?, LedbatMsg.Ack>>() {
    @Override
    public void handle(LedbatMsg.Ack content, KContentMsg<?, ?, LedbatMsg.Ack> msg) {
      logger.trace("ack:{}", msg);
      sender.ackData(content);
    }
  };

  //mean 5-6 micros
  private BiConsumer<LedbatSenderEvent.Request, LedbatSenderEvent.Indication> appSend() {
    return (LedbatSenderEvent.Request req, LedbatSenderEvent.Indication ind) -> answer(req, ind);
  }

  //mean 11-12 micros
  private Consumer<Identifiable> networkSend() {
    return (Identifiable data) -> {
      LedbatMsg.Datum content = new LedbatMsg.Datum(msgIds.randomId(), data);
      KHeader header = new BasicHeader(selfAdr, dstAdr, Transport.UDP);
      KContentMsg msg = new BasicContentMsg(header, content);
      logger.trace("sending:{}", msg);
      //mean 6-8 micros
      trigger(msg, networkPort);
    };
  }

  public static class Init extends se.sics.kompics.Init<LedbatSenderComp> {

    public final KAddress selfAdr;
    public final KAddress dstAdr;
    public final Identifier rivuletId;

    public Init(KAddress selfAdr, KAddress dstAdr, Identifier rivuletId) {
      this.selfAdr = selfAdr;
      this.dstAdr = dstAdr;
      this.rivuletId = rivuletId;
    }
  }
}
