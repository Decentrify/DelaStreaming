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
//package se.sics.nstream.torrent.channel;
//
//import java.util.LinkedList;
//import java.util.Random;
//import static org.junit.Assert.assertEquals;
//import org.junit.Before;
//import org.junit.Test;
//import se.sics.fsm.helper.FSMHelper;
//import se.sics.kompics.Component;
//import se.sics.kompics.Port;
//import se.sics.kompics.network.Network;
//import se.sics.kompics.network.Transport;
//import se.sics.kompics.testkit.Direction;
//import se.sics.kompics.testkit.TestContext;
//import se.sics.ktoolbox.util.identifiable.overlay.OverlayId;
//import se.sics.ktoolbox.util.identifiable.overlay.OverlayIdFactory;
//import se.sics.ktoolbox.util.network.KAddress;
//import se.sics.ktoolbox.util.network.KContentMsg;
//import se.sics.ktoolbox.util.network.basic.BasicContentMsg;
//import se.sics.ktoolbox.util.network.basic.BasicHeader;
//import se.sics.nstream.torrent.channel.msg.TorrentMsg;
//
///**
// * @author Alex Ormenisan <aaor@kth.se>
// */
//public class TorrentConnectionSeederSideTest {
//
//  private static final Direction incoming = Direction.INCOMING;
//  private static final Direction outgoing = Direction.OUTGOING;
//
//  private static Random rand = new Random(1234);
//  private KAddress seederAdr;
//  private KAddress leecherAdr;
//  
//  private static OverlayIdFactory torrentIdFactory;
//  private TestContext<ChannelComp> tc;
//  private Component seeder;
//  
//  private LinkedList<OverlayId> torrents = new LinkedList<>();
//
//  @Before
//  public void setup() {
//    String[] fsmNames = new String[]{SeederTorrentFSM.NAME, LeecherTorrentFSM.NAME};
//    torrentIdFactory = FSMHelper.systemSetup("src/test/resources/channel/seeder1/application.conf", fsmNames);
//    seederAdr = FSMHelper.getAddress(0);
//    leecherAdr = FSMHelper.getAddress(1);
//    tc = TorrentFSMHelper.getContext(seederAdr);
//    seeder = tc.getComponentUnderTest();
//    torrents.add(torrentIdFactory.randomId());
//  }
//
//  @Test
//  public void connectTest() {
//    TestContext<ChannelComp> atc = connect(tc.body().repeat(1));
//    atc.repeat(1).body().end();
//    assertEquals(tc.check(), tc.getFinalState());
//  }
//
//  public TestContext<ChannelComp> connect(TestContext<ChannelComp> tc) {
//    Port<Network> network = seeder.getNegative(Network.class);
//    OverlayId torrentId = torrents.removeFirst();
//    TestContext<ChannelComp> atc = tc.body()
//      .trigger(getConnect(torrentId), network)
//      .expect(BasicContentMsg.class, FSMHelper.anyEvent(BasicContentMsg.class), network, outgoing)
//      .inspect(TorrentFSMHelper.inspectSeederState(torrentId, SeederTorrentStates.CONNECTED))
//      .end();
//    return atc;
//  }
//
//  private KContentMsg getConnect(OverlayId torrentId) {
//    TorrentMsg.Connect payload = new TorrentMsg.Connect(torrentId);
//    BasicHeader msgHeader = new BasicHeader(leecherAdr, seederAdr, Transport.UDP);
//    KContentMsg msg = new BasicContentMsg(msgHeader, payload);
//    return msg;
//  }
//  
//}
