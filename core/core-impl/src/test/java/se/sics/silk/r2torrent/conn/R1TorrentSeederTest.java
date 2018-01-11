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
package se.sics.silk.r2torrent.conn;

import java.util.Random;
import org.junit.After;
import static org.junit.Assert.assertTrue;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import se.sics.kompics.Component;
import se.sics.kompics.Port;
import se.sics.kompics.fsm.FSMException;
import se.sics.kompics.testing.TestContext;
import se.sics.kompics.util.Identifier;
import se.sics.ktoolbox.util.identifiable.BasicBuilders;
import se.sics.ktoolbox.util.identifiable.basic.IntIdFactory;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayId;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayIdFactory;
import se.sics.ktoolbox.util.network.KAddress;
import se.sics.silk.SystemHelper;
import se.sics.silk.SystemSetup;
import se.sics.silk.r2torrent.R2TorrentPort;
import static se.sics.silk.r2torrent.conn.R1TorrentSeederHelper.torrentSeederConnFail;
import static se.sics.silk.r2torrent.conn.R1TorrentSeederHelper.torrentSeederConnReq;
import static se.sics.silk.r2torrent.conn.R1TorrentSeederHelper.torrentSeederConnSucc;
import static se.sics.silk.r2torrent.conn.R1TorrentSeederHelper.torrentSeederDisconnect;
import static se.sics.silk.r2torrent.conn.R2NodeSeederHelper.nodeSeederConnFailLoc;
import static se.sics.silk.r2torrent.conn.R2NodeSeederHelper.nodeSeederConnReq;
import static se.sics.silk.r2torrent.conn.R2NodeSeederHelper.nodeSeederConnSuccLoc;
import static se.sics.silk.r2torrent.conn.R2NodeSeederHelper.nodeSeederDisconnectLoc;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class R1TorrentSeederTest {
  private TestContext<R1TorrentSeederAuxComp> tc;
  private Component comp;
  private Port<R2TorrentPort> triggerP;
  private Port<R2TorrentPort> expectP;
  private static OverlayIdFactory torrentIdFactory;
  private IntIdFactory intIdFactory;
  private KAddress selfAdr;

  @BeforeClass
  public static void setup() throws FSMException {
    torrentIdFactory = SystemSetup.systemSetup("src/test/resources/application.conf");
  }

  @Before
  public void testSetup() {
    tc = getContext();
    comp = tc.getComponentUnderTest();
    triggerP = comp.getNegative(R2TorrentPort.class);
    expectP = comp.getPositive(R2TorrentPort.class);
    intIdFactory = new IntIdFactory(new Random());
  }

  private TestContext<R1TorrentSeederAuxComp> getContext() {
    selfAdr = SystemHelper.getAddress(0);
    R1TorrentSeederAuxComp.Init init = new R1TorrentSeederAuxComp.Init(selfAdr);
    TestContext<R1TorrentSeederAuxComp> context = TestContext.newInstance(R1TorrentSeederAuxComp.class, init);
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
  public void testConnSucc1() {
    OverlayId torrent1 = torrentIdFactory.id(new BasicBuilders.IntBuilder(1));
    KAddress seeder = SystemHelper.getAddress(1);
    Identifier file1 = intIdFactory.id(new BasicBuilders.IntBuilder(1));
    
    tc = tc.body();
    tc = torrentSeederConnReq(tc, triggerP, torrentSeederConnReq(torrent1, file1, seeder));
    tc = nodeSeederConnSuccLoc(tc, expectP, triggerP);
    tc = torrentSeederConnSucc(tc, expectP);
    tc.repeat(1).body().end();
    assertTrue(tc.check());
  }
  
  @Test
  public void testConnSucc2() {
    OverlayId torrent1 = torrentIdFactory.id(new BasicBuilders.IntBuilder(1));
    KAddress seeder = SystemHelper.getAddress(1);
    Identifier file1 = intIdFactory.id(new BasicBuilders.IntBuilder(1));
    Identifier file2 = intIdFactory.id(new BasicBuilders.IntBuilder(2));
    Identifier file3 = intIdFactory.id(new BasicBuilders.IntBuilder(3));
    
    tc = tc.body();
    tc = torrentSeederConnReq(tc, triggerP, torrentSeederConnReq(torrent1, file1, seeder)); //1
    tc = torrentSeederConnReq(tc, triggerP, torrentSeederConnReq(torrent1, file2, seeder)); //2
    tc = torrentSeederDisconnect(tc, triggerP, torrentSeederDisconnect(torrent1, file2, seeder.getId())); //3
    tc = torrentSeederConnReq(tc, triggerP, torrentSeederConnReq(torrent1, file3, seeder));//4
    tc = nodeSeederConnSuccLoc(tc, expectP, triggerP); //5-6
    tc = tc.repeat(2).body();
    tc = torrentSeederConnSucc(tc, expectP); //7-8
    tc = tc.end();
    tc = torrentSeederConnReq(tc, triggerP, torrentSeederConnReq(torrent1, file2, seeder));//9
    tc = torrentSeederConnSucc(tc, expectP); //10
    tc.repeat(1).body().end();
    assertTrue(tc.check());
  }
  
  @Test
  public void testConnFail() {
    OverlayId torrent1 = torrentIdFactory.id(new BasicBuilders.IntBuilder(1));
    KAddress seeder = SystemHelper.getAddress(1);
    Identifier file1 = intIdFactory.id(new BasicBuilders.IntBuilder(1));
    
    tc = tc.body();
    tc = torrentSeederConnReq(tc, triggerP, torrentSeederConnReq(torrent1, file1, seeder));
    tc = nodeSeederConnFailLoc(tc, expectP, triggerP);
    tc = torrentSeederConnFail(tc, expectP);
    tc.repeat(1).body().end();
    assertTrue(tc.check());
  }
  
  @Test
  public void testDisconnect1() {
    OverlayId torrent1 = torrentIdFactory.id(new BasicBuilders.IntBuilder(1));
    KAddress seeder = SystemHelper.getAddress(1);
    Identifier file1 = intIdFactory.id(new BasicBuilders.IntBuilder(1));
    
    tc = tc.body();
    tc = torrentSeederConnReq(tc, triggerP, torrentSeederConnReq(torrent1, file1, seeder));
    tc = nodeSeederConnReq(tc, expectP);
    tc = torrentSeederDisconnect(tc, triggerP, torrentSeederDisconnect(torrent1, file1, seeder.getId()));
    tc = nodeSeederDisconnectLoc(tc, expectP);
    tc.repeat(1).body().end();
    assertTrue(tc.check());
  }
  
  @Test
  public void testDisconnect2() {
    OverlayId torrent1 = torrentIdFactory.id(new BasicBuilders.IntBuilder(1));
    KAddress seeder = SystemHelper.getAddress(1);
    Identifier file1 = intIdFactory.id(new BasicBuilders.IntBuilder(1));
    Identifier file2 = intIdFactory.id(new BasicBuilders.IntBuilder(2));
    
    tc = tc.body();
    tc = torrentSeederConnReq(tc, triggerP, torrentSeederConnReq(torrent1, file1, seeder));
    tc = nodeSeederConnReq(tc, expectP);
    tc = torrentSeederConnReq(tc, triggerP, torrentSeederConnReq(torrent1, file2, seeder));
    tc = torrentSeederDisconnect(tc, triggerP, torrentSeederDisconnect(torrent1, file1, seeder.getId()));
    tc = torrentSeederDisconnect(tc, triggerP, torrentSeederDisconnect(torrent1, file2, seeder.getId()));
    tc = nodeSeederDisconnectLoc(tc, expectP);
    tc.repeat(1).body().end();

    assertTrue(tc.check());
  }
  
  @Test
  public void testDisconnect3() {
    OverlayId torrent1 = torrentIdFactory.id(new BasicBuilders.IntBuilder(1));
    KAddress seeder = SystemHelper.getAddress(1);
    Identifier file1 = intIdFactory.id(new BasicBuilders.IntBuilder(1));
    Identifier file2 = intIdFactory.id(new BasicBuilders.IntBuilder(2));
    
    tc = tc.body();
    tc = torrentSeederConnReq(tc, triggerP, torrentSeederConnReq(torrent1, file1, seeder)); //1
    tc = nodeSeederConnSuccLoc(tc, expectP, triggerP); //2-3
    tc = torrentSeederConnSucc(tc, expectP);//4
    tc = torrentSeederConnReq(tc, triggerP, torrentSeederConnReq(torrent1, file2, seeder));//5
    tc = torrentSeederConnSucc(tc, expectP);//6
    tc = nodeSeederConnFailLoc(tc, triggerP, nodeSeederConnFailLoc(torrent1, seeder.getId()));//7
    tc = tc.repeat(2).body();
    tc = torrentSeederConnFail(tc, expectP);//8-9
    tc = tc.end();
    tc.repeat(1).body().end();

    assertTrue(tc.check());
  }
}
