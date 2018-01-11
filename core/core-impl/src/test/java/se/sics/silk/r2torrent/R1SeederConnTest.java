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

import org.junit.After;
import static org.junit.Assert.assertTrue;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import se.sics.kompics.Component;
import se.sics.kompics.LoopbackPort;
import se.sics.kompics.Port;
import se.sics.kompics.fsm.FSMException;
import se.sics.kompics.testing.TestContext;
import se.sics.ktoolbox.util.identifiable.BasicBuilders;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayId;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayIdFactory;
import se.sics.ktoolbox.util.network.KAddress;
import se.sics.silk.SystemHelper;
import se.sics.silk.SystemSetup;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class R1SeederConnTest {
  private TestContext<R2TorrentComp> tc;
  private Component r2MngrComp;
  private Port<R2TorrentCtrlPort> ctrlP;
  private Port<LoopbackPort> transferP;
  private static OverlayIdFactory torrentIdFactory;
  private KAddress selfAdr;

  @BeforeClass
  public static void setup() throws FSMException {
    torrentIdFactory = SystemSetup.systemSetup("src/test/resources/application.conf");
  }

  @Before
  public void testSetup() {
    tc = getContext();
    r2MngrComp = tc.getComponentUnderTest();
    ctrlP = r2MngrComp.getPositive(R2TorrentCtrlPort.class);
    transferP = r2MngrComp.getPositive(LoopbackPort.class);
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
  
  @Test
  public void testEmpty() {
    OverlayId torrent1 = torrentIdFactory.id(new BasicBuilders.IntBuilder(1));
    KAddress seeder = SystemHelper.getAddress(1);
    
    tc = tc.body();
    tc.repeat(1).body().end();
    assertTrue(tc.check());
  }
}
