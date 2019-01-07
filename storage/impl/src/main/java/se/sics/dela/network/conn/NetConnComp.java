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
package se.sics.dela.network.conn;

import java.util.HashSet;
import java.util.Set;
import se.sics.dela.network.ledbat.LedbatReceiverComp;
import se.sics.dela.network.ledbat.LedbatReceiverPort;
import se.sics.dela.network.ledbat.LedbatSenderComp;
import se.sics.dela.network.ledbat.LedbatSenderPort;
import se.sics.kompics.Channel;
import se.sics.kompics.Component;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Handler;
import se.sics.kompics.Kill;
import se.sics.kompics.Negative;
import se.sics.kompics.Positive;
import se.sics.kompics.Start;
import se.sics.kompics.network.Network;
import se.sics.kompics.timer.Timer;
import se.sics.kompics.util.Identifier;
import se.sics.ktoolbox.util.identifiable.basic.PairIdentifier;
import se.sics.ktoolbox.util.network.KAddress;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class NetConnComp extends ComponentDefinition {

  private final Negative<NetConnPort> connPort = provides(NetConnPort.class);
  private final Negative<LedbatSenderPort> ledbatSenderPort = provides(LedbatSenderPort.class);
  private final Negative<LedbatReceiverPort> ledbatReceiverPort = provides(LedbatReceiverPort.class);
  private final Positive<Network> networkPort = requires(Network.class);
  private final Positive<Timer> timerPort = requires(Timer.class);
  private final KAddress self;
  private final KAddress partner;

  private Component ledbatSender = null;
  private Component ledbatReceiver = null;

  private final Set<Identifier> ledbatSenderChannels = new HashSet<>();
  private final Set<Identifier> ledbatReceiverChannels = new HashSet<>();

  public NetConnComp(Init init) {
    this.self = init.self;
    this.partner = init.partner;
    subscribe(handleStart, control);
    subscribe(handleLedbatSenderCreate, connPort);
    subscribe(handleLedbatReceiverCreate, connPort);
    subscribe(handleLedbatSenderKill, connPort);
    subscribe(handleLedbatReceiverKill, connPort);
  }

  Handler handleStart = new Handler<Start>() {
    @Override
    public void handle(Start event) {
    }
  };

  Handler handleLedbatSenderCreate = new Handler<NetConnEvents.LedbatSenderCreate>() {
    @Override
    public void handle(NetConnEvents.LedbatSenderCreate req) {
      if (ledbatSenderChannels.isEmpty()) {
        createLedbatSender();
      }
      ledbatSenderChannels.add(req.channelId);
      answer(req, req.ack());
    }
  };

  Handler handleLedbatReceiverCreate = new Handler<NetConnEvents.LedbatReceiverCreate>() {
    @Override
    public void handle(NetConnEvents.LedbatReceiverCreate req) {
      if (ledbatReceiverChannels.isEmpty()) {
        createLedbatReceiver();
      }
      ledbatReceiverChannels.add(req.channelId);
      answer(req, req.ack());
    }
  };

  Handler handleLedbatSenderKill = new Handler<NetConnEvents.LedbatSenderKill>() {
    @Override
    public void handle(NetConnEvents.LedbatSenderKill req) {
      ledbatSenderChannels.remove(req.channelId);
      if (ledbatSenderChannels.isEmpty()) {
        killLedbatSender();
      }
    }
  };

  Handler handleLedbatReceiverKill = new Handler<NetConnEvents.LedbatReceiverKill>() {
    @Override
    public void handle(NetConnEvents.LedbatReceiverKill req) {
      ledbatReceiverChannels.remove(req.channelId);
      if (ledbatReceiverChannels.isEmpty()) {
        killLedbatSender();
      }
    }
  };

  private void createLedbatSender() {
    Identifier sender = self.getId();
    Identifier receiver = partner.getId();
    Identifier connId = new PairIdentifier(sender, receiver);
    LedbatSenderComp.Init init = new LedbatSenderComp.Init(self, partner, connId);
    ledbatSender = create(LedbatSenderComp.class, init);
    connect(ledbatSenderPort, ledbatSender.getPositive(LedbatSenderPort.class), Channel.TWO_WAY);
    connect(ledbatSender.getNegative(Network.class), networkPort, Channel.TWO_WAY);
    connect(ledbatSender.getNegative(Timer.class), timerPort, Channel.TWO_WAY);
    trigger(Start.event, ledbatSender.control());
  }

  private void createLedbatReceiver() {
    Identifier sender = partner.getId();
    Identifier receiver = self.getId();
    Identifier connId = new PairIdentifier(sender, receiver);
    LedbatReceiverComp.Init init = new LedbatReceiverComp.Init(self, partner, connId);
    ledbatReceiver = create(LedbatReceiverComp.class, init);
    connect(ledbatReceiverPort, ledbatReceiver.getPositive(LedbatReceiverPort.class), Channel.TWO_WAY);
    connect(ledbatReceiver.getNegative(Network.class), networkPort, Channel.TWO_WAY);
    connect(ledbatReceiver.getNegative(Timer.class), timerPort, Channel.TWO_WAY);
    trigger(Start.event, ledbatSender.control());
  }

  private void killLedbatSender() {
    trigger(Kill.event, ledbatSender.control());
    disconnect(ledbatSenderPort, ledbatSender.getPositive(LedbatSenderPort.class));
    disconnect(ledbatSender.getNegative(Network.class), networkPort);
    disconnect(ledbatSender.getNegative(Timer.class), timerPort);
    ledbatSender = null;
  }

  private void killLedbatReceiver() {
    trigger(Kill.event, ledbatReceiver.control());
    disconnect(ledbatReceiverPort, ledbatReceiver.getPositive(LedbatReceiverPort.class));
    disconnect(ledbatReceiver.getNegative(Network.class), networkPort);
    disconnect(ledbatReceiver.getNegative(Timer.class), timerPort);
    ledbatReceiver = null;
  }

  public static class Init extends se.sics.kompics.Init<NetConnComp> {

    public final KAddress self;
    public final KAddress partner;

    public Init(KAddress self, KAddress dst) {
      this.self = self;
      this.partner = dst;
    }
  }
}
