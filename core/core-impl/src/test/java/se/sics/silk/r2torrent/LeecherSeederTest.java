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

import se.sics.silk.SelfPort;
import com.google.common.base.Predicate;
import java.util.LinkedList;
import java.util.List;
import org.junit.After;
import static org.junit.Assert.assertTrue;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import se.sics.kompics.Component;
import se.sics.kompics.KompicsEvent;
import se.sics.kompics.Port;
import se.sics.kompics.fsm.FSMException;
import se.sics.kompics.testing.Direction;
import se.sics.kompics.testing.TestContext;
import se.sics.ktoolbox.util.identifiable.BasicBuilders;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayId;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayIdFactory;
import se.sics.ktoolbox.util.network.KAddress;
import se.sics.silk.PredicateHelper;
import se.sics.silk.SystemHelper;
import se.sics.silk.SystemSetup;
import static se.sics.silk.TorrentTestHelper.torrentBaseInfo;
import se.sics.silk.r2torrent.helper.LeecherSeederComp;
import se.sics.silk.r2torrent.helper.MockTorrentCtrlEvent;
import se.sics.silk.r2torrent.helper.MockTorrentCtrlPort;
import se.sics.silk.r2torrent.torrent.event.R2TorrentCtrlEvents;
import se.sics.silk.r2torrent.util.R2TorrentStatus;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class LeecherSeederTest {

  private TestContext<LeecherSeederComp> tc;
  private Component comp;
  private Port<MockTorrentCtrlPort> ctrlP;
  private Port<SelfPort> leecherP;
  private static OverlayIdFactory torrentIdFactory;
  private KAddress seeder;
  private KAddress leecher;

  @BeforeClass
  public static void setup() throws FSMException {
    torrentIdFactory = SystemSetup.systemSetup("src/test/resources/application.conf");
  }

  @Before
  public void testSetup() {
    tc = getContext();
    comp = tc.getComponentUnderTest();
    ctrlP = comp.getPositive(MockTorrentCtrlPort.class);
  }

  private TestContext<LeecherSeederComp> getContext() {
    seeder = SystemHelper.getAddress(0);
    leecher = SystemHelper.getAddress(1);
    LeecherSeederComp.Init init = new LeecherSeederComp.Init(seeder, leecher);
    TestContext<LeecherSeederComp> context = TestContext.newInstance(LeecherSeederComp.class, init);
    return context;
  }

  @After
  public void clean() {
  }

  @Test
  public void testEmpty() {
    tc = tc.body();
    tc.repeat(1).body().end();
    assertTrue(tc.check());
  }

  @Test
  public void testSimple() {
    OverlayId torrent = torrentIdFactory.id(new BasicBuilders.IntBuilder(1));

//    tc = tc.setTimeout(10*60*1000);
    tc = tc.body();
    tc = upload(tc, seeder, torrent); //1-3
    tc = download(tc, leecher, torrent, seeder); //4-7
    tc.repeat(1).body().end();
    assertTrue(tc.check());
  }

  private TestContext upload(TestContext tc, KAddress target, OverlayId torrentId) {
    tc = mockIn(tc, ctrlP, target, new R2TorrentCtrlEvents.Upload(torrentId));
    tc = mockOut(tc, ctrlP, target, torrentBaseInfo(R2TorrentStatus.META_SERVE));
    tc = mockOut(tc, ctrlP, target, torrentBaseInfo(R2TorrentStatus.HASH));
    return tc;
  }
  
  private TestContext download(TestContext tc, KAddress target, OverlayId torrentId, KAddress seeder) {
    List<KAddress> seeders = new LinkedList<>();
    seeders.add(seeder);
    tc = mockIn(tc, ctrlP, target, new R2TorrentCtrlEvents.MetaGetReq(torrentId, seeders));
    tc = mockOut(tc, ctrlP, target, new PredicateHelper.ContentPredicate(R2TorrentCtrlEvents.MetaGetSucc.class));
    tc = mockOut(tc, ctrlP, target, torrentBaseInfo(R2TorrentStatus.META_SERVE));
    tc = mockIn(tc, ctrlP, target, new R2TorrentCtrlEvents.Download(torrentId));
    tc = mockOut(tc, ctrlP, target, torrentBaseInfo(R2TorrentStatus.HASH));
    return tc;
  }

  private TestContext mockIn(TestContext tc, Port triggerP, KAddress target, KompicsEvent content) {
    return tc.trigger(new MockTorrentCtrlEvent(target, content), triggerP);
  }
  
  private TestContext mockOut(TestContext tc, Port expectP, KAddress target, Predicate p) {
    return tc.expect(MockTorrentCtrlEvent.class, new MockPredicate(target, p), expectP, Direction.OUT);
  }

  public static class MockPredicate implements Predicate<MockTorrentCtrlEvent> {
    private final KAddress target;
    private final Predicate contentP;
    
    public MockPredicate(KAddress target, Predicate contentP) {
      this.target = target;
      this.contentP = contentP;
    }
    @Override
    public boolean apply(MockTorrentCtrlEvent t) {
      if(!target.equals(t.adr)) {
        return false;
      }
      return contentP.apply(t.event);
    }
  }
}
