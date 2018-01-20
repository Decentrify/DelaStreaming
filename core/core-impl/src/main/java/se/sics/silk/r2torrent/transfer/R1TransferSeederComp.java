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
package se.sics.silk.r2torrent.transfer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.kompics.Channel;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.ComponentProxy;
import se.sics.kompics.Handler;
import se.sics.kompics.KompicsEvent;
import se.sics.kompics.Negative;
import se.sics.kompics.Positive;
import se.sics.kompics.Start;
import se.sics.kompics.network.Network;
import se.sics.kompics.network.Transport;
import se.sics.kompics.timer.Timer;
import se.sics.kompics.util.Identifiable;
import se.sics.kompics.util.Identifier;
import se.sics.ktoolbox.util.identifiable.basic.PairIdentifier;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayId;
import se.sics.ktoolbox.util.network.KAddress;
import se.sics.ktoolbox.util.network.KContentMsg;
import se.sics.ktoolbox.util.network.KHeader;
import se.sics.ktoolbox.util.network.basic.BasicContentMsg;
import se.sics.ktoolbox.util.network.basic.BasicHeader;
import se.sics.nutil.network.bestEffort.event.BestEffortMsg;
import se.sics.silk.SelfPort;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class R1TransferSeederComp extends ComponentDefinition {

  private static final Logger LOG = LoggerFactory.getLogger(R1TransferSeederComp.class);
  private String logPrefix;

  private final Ports ports;
  public final KAddress selfAdr;
  public final KAddress seederAdr;

  public R1TransferSeederComp(Init init) {
    ports = new Ports(proxy);
    selfAdr = init.selfAdr;
    seederAdr = init.seederAdr;
    subscribe(handleStart, control);
  }
  
  public static Identifier baseId(OverlayId torrentId, Identifier fileId, Identifier seederId) {
    return new PairIdentifier(new PairIdentifier(torrentId, fileId), seederId);
  }

  Handler handleStart = new Handler<Start>() {

    @Override
    public void handle(Start event) {
      
    }
  };

  private <C extends KompicsEvent & Identifiable> void bestEffortMsg(C content) {
    KHeader header = new BasicHeader(selfAdr, seederAdr, Transport.UDP);
    BestEffortMsg.Request wrap
      = new BestEffortMsg.Request(content, HardCodedConfig.beRetries, HardCodedConfig.beRetryInterval);
    KContentMsg msg = new BasicContentMsg(header, wrap);
    trigger(msg, ports.networkP);
  }

  private <C extends KompicsEvent & Identifiable> void msg(C content) {
    KHeader header = new BasicHeader(selfAdr, seederAdr, Transport.UDP);
    KContentMsg msg = new BasicContentMsg(header, content);
    trigger(msg, ports.networkP);
  }

  public static class Init extends se.sics.kompics.Init<R1TransferSeederComp> {

    public final KAddress selfAdr;
    public final OverlayId torrentId;
    public final Identifier fileId;
    public final KAddress seederAdr;

    public Init(KAddress selfAdr, OverlayId torrentId, Identifier fileId, KAddress seederAdr) {
      this.selfAdr = selfAdr;
      this.torrentId = torrentId;
      this.fileId = fileId;
      this.seederAdr = seederAdr;
    }
  }

  public static class Ports {

    public final Negative ctrlP;
    public final Positive networkP;
    public final Positive timerP;
    public final Negative<SelfPort> loopbackSend;
    public final Positive<SelfPort> loopbackSubscribe;
    
    public Ports(ComponentProxy proxy) {
      ctrlP = proxy.provides(DownloadPort.class);
      networkP = proxy.requires(Network.class);
      timerP = proxy.requires(Timer.class);
      loopbackSend = proxy.provides(SelfPort.class);
      loopbackSubscribe = proxy.requires(SelfPort.class);
      proxy.connect(loopbackSend.getPair(), loopbackSubscribe.getPair(), Channel.TWO_WAY);
    }
  }

  public static class HardCodedConfig {

    public static int beRetries = 1;
    public static int beRetryInterval = 2000;
    public static final long pingTimerPeriod = 1000;
    public static final int deadPings = 5;
  }
}
