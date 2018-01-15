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
package se.sics.silk.r2torrent;

import java.util.LinkedList;
import java.util.List;
import org.junit.After;
import static org.junit.Assert.assertTrue;
import org.junit.Before;
import org.junit.BeforeClass;
import se.sics.kompics.Component;
import se.sics.kompics.Port;
import se.sics.kompics.fsm.FSMException;
import se.sics.kompics.network.Msg;
import se.sics.kompics.network.Network;
import se.sics.kompics.testing.Direction;
import se.sics.kompics.testing.Future;
import se.sics.kompics.testing.TestContext;
import se.sics.ktoolbox.util.identifiable.BasicBuilders;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayId;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayIdFactory;
import se.sics.ktoolbox.util.network.KAddress;
import se.sics.silk.SystemHelper;
import se.sics.silk.SystemSetup;
import se.sics.silk.r2torrent.conn.event.R2NodeSeederEvents;
import se.sics.silk.r2torrent.event.R1HashEvents;
import se.sics.silk.r2torrent.event.R1MetadataEvents;
import se.sics.silk.r2torrent.event.R2TorrentCtrlEvents;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class R2TorrentTest {

  private TestContext<R2TorrentComp> tc;
  private Component comp;
  private Port<R2TorrentCtrlPort> ctrlP;
  private Port<Network> networkP;
  private Port<R2TorrentPort> loopbackPP;
  private Port<R2TorrentPort> loopbackNP;
  private static OverlayIdFactory torrentIdFactory;
  private KAddress selfAdr;

  @BeforeClass
  public static void setup() throws FSMException {
    torrentIdFactory = SystemSetup.systemSetup("src/test/resources/application.conf");
  }

  @Before
  public void testSetup() {
    tc = getContext();
    comp = tc.getComponentUnderTest();
    ctrlP = comp.getPositive(R2TorrentCtrlPort.class);
    networkP = comp.getNegative(Network.class);
    loopbackPP = comp.getPositive(R2TorrentPort.class);
    loopbackNP = comp.getNegative(R2TorrentPort.class);
  }

  private TestContext<R2TorrentComp> getContext() {
    selfAdr = SystemHelper.getAddress(0);
    R2TorrentComp.Init init = new R2TorrentComp.Init(selfAdr);
    TestContext<R2TorrentComp> context = TestContext.newInstance(R2TorrentComp.class, init);
    return context;
  }

  @After
  public void clean() {
  }

//  @Test
  public void testSimple() {
    OverlayId torrent1 = torrentIdFactory.id(new BasicBuilders.IntBuilder(1));
    KAddress seeder = SystemHelper.getAddress(1);

    Future<Msg, Msg> connAcc = R2NodeConnHelper.connAcc();
    tc = tc.body();
    tc = tc
      .trigger(ctrlMetaGetReq(torrent1, seeder), ctrlP)
      .expect(R1MetadataEvents.MetaGetReq.class, loopbackPP, Direction.OUT)
      .expect(R2NodeSeederEvents.ConnectReq.class, loopbackPP, Direction.OUT)
      .answerRequest(Msg.class, networkP, connAcc)
      .trigger(connAcc, networkP)
      .expect(R2NodeSeederEvents.ConnectSucc.class, loopbackPP, Direction.OUT)
      .expect(R1MetadataEvents.MetaGetSucc.class, loopbackPP, Direction.OUT)
      .expect(R2TorrentCtrlEvents.MetaGetSucc.class, ctrlP, Direction.OUT)
      .trigger(ctrlDownload(torrent1), ctrlP)
      .expect(R1HashEvents.HashReq.class, loopbackPP, Direction.OUT)
      .expect(R1HashEvents.HashSucc.class, loopbackPP, Direction.OUT)
      .expect(R2TorrentCtrlEvents.TorrentBaseInfo.class, ctrlP, Direction.OUT);
    tc.repeat(1).body().end();
    assertTrue(tc.check());
  }

  public R2TorrentCtrlEvents.MetaGetReq ctrlMetaGetReq(OverlayId torrentId, KAddress partner) {
    List<KAddress> partners = new LinkedList<>();
    partners.add(partner);
    return new R2TorrentCtrlEvents.MetaGetReq(torrentId, partners);
  }

  public R2TorrentCtrlEvents.Download ctrlDownload(OverlayId torrentId) {
    return new R2TorrentCtrlEvents.Download(torrentId);
  }
}
