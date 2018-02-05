///*
// * Copyright (C) 2009 Swedish Institute of Computer Science (SICS) Copyright (C)
// * 2009 Royal Institute of Technology (KTH)
// *
// * GVoD is free software; you can redistribute it and/or
// * modify it under the terms of the GNU General Public License
// * as published by the Free Software Foundation; either version 2
// * of the License, or (at your option) any later version.
// *
// * This program is distributed in the hope that it will be useful,
// * but WITHOUT ANY WARRANTY; without even the implied warranty of
// * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// * GNU General Public License for more details.
// *
// * You should have received a copy of the GNU General Public License
// * along with this program; if not, write to the Free Software
// * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
// */
//package se.sics.silk.r2torrent.helper;
//
//import se.sics.kompics.Channel;
//import se.sics.kompics.Component;
//import se.sics.kompics.ComponentDefinition;
//import se.sics.kompics.Handler;
//import se.sics.kompics.KompicsEvent;
//import se.sics.kompics.Negative;
//import se.sics.kompics.Port;
//import se.sics.kompics.fsm.event.FSMWrongState;
//import se.sics.kompics.network.Network;
//import se.sics.kompics.timer.Timer;
//import se.sics.kompics.util.Identifier;
//import se.sics.ktoolbox.util.network.KAddress;
//import se.sics.ktoolbox.util.network.basic.BasicContentMsg;
//import se.sics.ktoolbox.util.network.ports.ChannelIdExtractor;
//import se.sics.ktoolbox.util.network.ports.One2NChannel;
//import se.sics.silk.mocktimer.MockNetworkComp;
//import se.sics.silk.mocktimer.MockTimerComp;
//import se.sics.silk.mocktimer.MockTimerComp.TriggerTimeout;
//import se.sics.silk.r2torrent.R2TorrentComp;
//import se.sics.silk.r2torrent.torrent.R1TorrentCtrlPort;
//import se.sics.silk.r2torrent.torrent.R1Torrent;
//import se.sics.silk.r2torrent.torrent.event.R1TorrentCtrlEvents;
//
///**
// *
// * @author Alex Ormenisan <aaor@kth.se>
// */
//public class LeecherSeederComp extends ComponentDefinition {
//
//  Negative<MockTorrentCtrlPort> ctrlPort = provides(MockTorrentCtrlPort.class);
//  Negative<MockTimerComp.Port> mockTimer = provides(MockTimerComp.Port.class);
//  public Component n1;
//  public Component n2;
//  Component mockTimer1;
//  Component mockTimer2;
//  Component mockNetwork;
//  One2NChannel<Network> networkChannel;
//  One2NChannel<MockTimerComp.Port> mockTimerChannel;
//  R2TorrentComp mngrComp1;
//  R2TorrentComp mngrComp2;
//  KAddress adr1;
//  KAddress adr2;
//
//  public LeecherSeederComp(Init init) {
//    adr1 = init.adr1;
//    adr2 = init.adr2;
//    mockTimer1 = create(MockTimerComp.class, Init.NONE);
//    mockTimer2 = create(MockTimerComp.class, Init.NONE);
//    mockNetwork = create(MockNetworkComp.class, new MockNetworkComp.Init(init.adr1, init.adr2));
//    n1 = create(R2TorrentComp.class, new R2TorrentComp.Init(init.adr1));
//    n2 = create(R2TorrentComp.class, new R2TorrentComp.Init(init.adr2));
//    mngrComp1 = (R2TorrentComp) n1.getComponent();
//    mngrComp2 = (R2TorrentComp) n2.getComponent();
//
//    mockTimerChannel = One2NChannel.getChannel("mock-timer", mockTimer, new TimerCtrlIdExtractor());
//    networkChannel = One2NChannel.getChannel("mock-network", mockNetwork.getPositive(Network.class),
//      new NetworkIdExtractor());
//
//    connect(n1.getNegative(Timer.class), mockTimer1.getPositive(Timer.class), Channel.TWO_WAY);
//    connect(n2.getNegative(Timer.class), mockTimer2.getPositive(Timer.class), Channel.TWO_WAY);
//
//    networkChannel.addChannel(init.adr1.getId(), n1.getNegative(Network.class));
//    networkChannel.addChannel(init.adr2.getId(), n2.getNegative(Network.class));
//    mockTimerChannel.addChannel(init.adr1.getId(), mockTimer1.getPositive(MockTimerComp.Port.class));
//    mockTimerChannel.addChannel(init.adr2.getId(), mockTimer2.getPositive(MockTimerComp.Port.class));
//  
//    subscribe(handleTorrentCtrlReq, ctrlPort);
//    subscribeTorrentCtrl(adr1, n1.getPositive(R1TorrentCtrlPort.class));
//    subscribeTorrentCtrl(adr2, n2.getPositive(R1TorrentCtrlPort.class));
//  }
//  
//  private void subscribeTorrentCtrl(KAddress adr, Port port) {
//    subscribe(ctrlHandler(R1TorrentCtrlEvents.MetaGetSucc.class, adr), port);
//    subscribe(ctrlHandler(R1TorrentCtrlEvents.MetaGetFail.class, adr), port);
//    subscribe(ctrlHandler(R1TorrentCtrlEvents.TorrentBaseInfo.class, adr), port);
//    subscribe(ctrlHandler(R1TorrentCtrlEvents.StopAck.class, adr), port);
//    subscribe(ctrlHandler(FSMWrongState.class, adr), port);
//  }
//  
//  public <E extends KompicsEvent> Handler<E> ctrlHandler(Class<E> eventType, KAddress target) {
//    return new Handler<E>(eventType) {
//
//      @Override
//      public void handle(E event) {
//        trigger(new MockTorrentCtrlEvent(target, event), ctrlPort);
//      }
//    };
//  }
//
//  Handler handleTorrentCtrlReq = new Handler<MockTorrentCtrlEvent>() {
//    @Override
//    public void handle(MockTorrentCtrlEvent wrapper) {
//      if(wrapper.adr.equals(adr1)) {
//        trigger(wrapper.event, n1.getPositive(R1TorrentCtrlPort.class));
//      } else if(wrapper.adr.equals(adr2)) {
//        trigger(wrapper.event, n2.getPositive(R1TorrentCtrlPort.class));
//      } else {
//        throw new RuntimeException("logic");
//      }
//    }
//  };
//  
//  public static class Init extends se.sics.kompics.Init<LeecherSeederComp> {
//
//    KAddress adr1;
//    KAddress adr2;
//
//    public Init(KAddress adr1, KAddress adr2) {
//      this.adr1 = adr1;
//      this.adr2 = adr2;
//    }
//  }
//
//  public static class NetworkIdExtractor extends ChannelIdExtractor<BasicContentMsg, Identifier> {
//
//    NetworkIdExtractor() {
//      super(BasicContentMsg.class);
//    }
//
//    @Override
//    public Identifier getValue(BasicContentMsg msg) {
//      return msg.getDestination().getId();
//    }
//  }
//
//  public static class TimerCtrlIdExtractor extends ChannelIdExtractor<TriggerTimeout, Identifier> {
//
//    TimerCtrlIdExtractor() {
//      super(TriggerTimeout.class);
//    }
//
//    @Override
//    public Identifier getValue(TriggerTimeout event) {
//      return event.adr.getId();
//    }
//  }
//
//  public static class TorrentCtrlIdExtractor extends ChannelIdExtractor<R1Torrent.CtrlEvent, Identifier> {
//
//    TorrentCtrlIdExtractor() {
//      super(R1Torrent.CtrlEvent.class);
//    }
//
//    @Override
//    public Identifier getValue(R1Torrent.CtrlEvent event) {
//      return R1Torrent.fsmBaseId(event.torrentId());
//    }
//  }
//}
