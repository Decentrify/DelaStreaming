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

import com.google.common.base.Predicate;
import java.util.Random;
import org.junit.After;
import static org.junit.Assert.assertTrue;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import se.sics.kompics.Component;
import se.sics.kompics.Port;
import se.sics.kompics.fsm.FSMException;
import se.sics.kompics.fsm.FSMStateName;
import se.sics.kompics.testing.Future;
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
import se.sics.silk.r2torrent.conn.R1TorrentSeeder.States;
import static se.sics.silk.r2torrent.conn.R1TorrentSeederHelper.torrentSeederConnFail;
import static se.sics.silk.r2torrent.conn.R1TorrentSeederHelper.torrentSeederConnReq;
import static se.sics.silk.r2torrent.conn.R1TorrentSeederHelper.torrentSeederConnSucc;
import static se.sics.silk.r2torrent.conn.R1TorrentSeederHelper.torrentSeederDisconnect;
import static se.sics.silk.r2torrent.conn.R2NodeSeederHelper.nodeConnFailLoc;
import static se.sics.silk.r2torrent.conn.R2NodeSeederHelper.nodeConnSuccLoc;
import static se.sics.silk.r2torrent.conn.R2NodeSeederHelper.nodeSeederConnFailLoc;
import static se.sics.silk.r2torrent.conn.R2NodeSeederHelper.nodeSeederConnReqLoc;
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
  
  //*********************************************CONNECT TO CONNECT*****************************************************
  @Test
  public void  testNewConnect1() {
    OverlayId torrent1 = torrentIdFactory.id(new BasicBuilders.IntBuilder(1));
    KAddress seeder = SystemHelper.getAddress(1);
    Identifier file1 = intIdFactory.id(new BasicBuilders.IntBuilder(1));
    Identifier file2 = intIdFactory.id(new BasicBuilders.IntBuilder(2));
    
    tc = tc.body();
    tc = inactiveFSM(tc, torrent1, seeder); //1
    tc = torrentSeederConnReq(tc, triggerP, torrentSeederConnReq(torrent1, file1, seeder));//2
    tc = nodeSeederConnReqLoc(tc, expectP);//3
    tc = torrentSeederConnReq(tc, triggerP, torrentSeederConnReq(torrent1, file2, seeder));//4
    tc = state(tc, torrent1, seeder, States.CONNECT);//5
    tc.repeat(1).body().end();
    assertTrue(tc.check());
  }
  
  @Test
  public void  testDisconnect1() {
    OverlayId torrent1 = torrentIdFactory.id(new BasicBuilders.IntBuilder(1));
    KAddress seeder = SystemHelper.getAddress(1);
    Identifier file1 = intIdFactory.id(new BasicBuilders.IntBuilder(1));
    Identifier file2 = intIdFactory.id(new BasicBuilders.IntBuilder(2));
    
    tc = tc.body();
    tc = inactiveFSM(tc, torrent1, seeder); //1
    tc = torrentSeederConnReq(tc, triggerP, torrentSeederConnReq(torrent1, file1, seeder));//2
    tc = nodeSeederConnReqLoc(tc, expectP);//3
    tc = torrentSeederConnReq(tc, triggerP, torrentSeederConnReq(torrent1, file2, seeder)); //4
    tc = torrentSeederDisconnect(tc, triggerP, torrentSeederDisconnect(torrent1, file2, seeder.getId()));//5
    tc = state(tc, torrent1, seeder, States.CONNECT);//6
    tc.repeat(1).body().end();
    assertTrue(tc.check());
  }
  //********************************************CONNECT TO CONNECTED****************************************************
  @Test
  public void  testConnected() {
    OverlayId torrent1 = torrentIdFactory.id(new BasicBuilders.IntBuilder(1));
    KAddress seeder = SystemHelper.getAddress(1);
    Identifier file1 = intIdFactory.id(new BasicBuilders.IntBuilder(1));
    
    tc = tc.body();
    tc = inactiveFSM(tc, torrent1, seeder); //1
    tc = connected(tc, torrent1, file1, seeder); //2-5
    tc = state(tc, torrent1, seeder, States.CONNECTED);//6
    tc.repeat(1).body().end();
    assertTrue(tc.check());
  }
  private TestContext connected(TestContext tc, OverlayId torrentId, Identifier fileId, KAddress seeder) {
    tc = torrentSeederConnReq(tc, triggerP, torrentSeederConnReq(torrentId, fileId, seeder));//1 
   tc = nodeSeederConnSuccLoc(tc, expectP, triggerP);//2-3
    tc = torrentSeederConnSucc(tc, expectP);//4
    return tc;
  }
  //*******************************************CONNECTED TO CONNECTED***************************************************
  @Test
  public void  testNewConnect2() {
    OverlayId torrent1 = torrentIdFactory.id(new BasicBuilders.IntBuilder(1));
    KAddress seeder = SystemHelper.getAddress(1);
    Identifier file1 = intIdFactory.id(new BasicBuilders.IntBuilder(1));
    Identifier file2 = intIdFactory.id(new BasicBuilders.IntBuilder(2));
    
    tc = tc.body();
    tc = inactiveFSM(tc, torrent1, seeder); //1
    tc = connected(tc, torrent1, file1, seeder); //2-5
    tc = torrentSeederConnReq(tc, triggerP, torrentSeederConnReq(torrent1, file2, seeder));//6
    tc = torrentSeederConnSucc(tc, expectP);//7
    tc = state(tc, torrent1, seeder, States.CONNECTED);//8
    tc.repeat(1).body().end();
    assertTrue(tc.check());
  }
  
  
  @Test
  public void  testDisconnect2() {
    OverlayId torrent1 = torrentIdFactory.id(new BasicBuilders.IntBuilder(1));
    KAddress seeder = SystemHelper.getAddress(1);
    Identifier file1 = intIdFactory.id(new BasicBuilders.IntBuilder(1));
    Identifier file2 = intIdFactory.id(new BasicBuilders.IntBuilder(2));
    
    tc = tc.body();
    tc = inactiveFSM(tc, torrent1, seeder); //1
    tc = connected(tc, torrent1, file1, seeder); //2-5
    tc = torrentSeederConnReq(tc, triggerP, torrentSeederConnReq(torrent1, file2, seeder)); //6
    tc = torrentSeederConnSucc(tc, expectP);//7
    tc = torrentSeederDisconnect(tc, triggerP, torrentSeederDisconnect(torrent1, file2, seeder.getId()));//8
    tc = state(tc, torrent1, seeder, States.CONNECTED);//6
    tc.repeat(1).body().end();
    assertTrue(tc.check());
  }
  //*********************************************START TO CONNECTED*****************************************************
  @Test
  public void  testNewConnect3() {
    OverlayId torrent1 = torrentIdFactory.id(new BasicBuilders.IntBuilder(1));
    KAddress seeder = SystemHelper.getAddress(1);
    Identifier file1 = intIdFactory.id(new BasicBuilders.IntBuilder(1));
    Identifier file2 = intIdFactory.id(new BasicBuilders.IntBuilder(2));
    
    Future nodeConnSucc = nodeConnSuccLoc();
    tc = tc.body();
    tc = inactiveFSM(tc, torrent1, seeder); //1
    tc = torrentSeederConnReq(tc, triggerP, torrentSeederConnReq(torrent1, file1, seeder));//2
    tc = nodeSeederConnReqLoc(tc, expectP, nodeConnSucc);//3
    tc = torrentSeederConnReq(tc, triggerP, torrentSeederConnReq(torrent1, file2, seeder));//4
    tc = nodeSeederConnSuccLoc(tc, triggerP, nodeConnSucc); //5
    tc = tc.unordered();
    tc = torrentSeederConnSucc(tc, expectP);//6
    tc = torrentSeederConnSucc(tc, expectP);//6
    tc = tc.end();
    tc = state(tc, torrent1, seeder, States.CONNECTED);//7
    tc.repeat(1).body().end();
    assertTrue(tc.check());
  }
  //********************************************CONNECT TO DISCONNECT***************************************************
  @Test
  public void  testConnFail1() {
    OverlayId torrent1 = torrentIdFactory.id(new BasicBuilders.IntBuilder(1));
    KAddress seeder = SystemHelper.getAddress(1);
    Identifier file1 = intIdFactory.id(new BasicBuilders.IntBuilder(1));
    Identifier file2 = intIdFactory.id(new BasicBuilders.IntBuilder(2));
    
    Future disc = nodeConnFailLoc();
    tc = tc.body();
    tc = inactiveFSM(tc, torrent1, seeder); //1
    tc = torrentSeederConnReq(tc, triggerP, torrentSeederConnReq(torrent1, file1, seeder));//2
    tc = nodeSeederConnReqLoc(tc, expectP, disc);//3
    tc = torrentSeederConnReq(tc, triggerP, torrentSeederConnReq(torrent1, file2, seeder));//4
    tc = nodeSeederConnFailLoc(tc, triggerP, disc); //5
    tc = tc.unordered();
    tc = torrentSeederConnFail(tc, expectP); //6
    tc = torrentSeederConnFail(tc, expectP); //6
    tc = tc.end();
    tc = inactiveFSM(tc, torrent1, seeder); //7
    tc.repeat(1).body().end();
    assertTrue(tc.check());
  }
  
  @Test
  public void  testLocDisconnect1() {
    OverlayId torrent1 = torrentIdFactory.id(new BasicBuilders.IntBuilder(1));
    KAddress seeder = SystemHelper.getAddress(1);
    Identifier file1 = intIdFactory.id(new BasicBuilders.IntBuilder(1));
    
    tc = tc.body();
    tc = inactiveFSM(tc, torrent1, seeder); //1
    tc = torrentSeederConnReq(tc, triggerP, torrentSeederConnReq(torrent1, file1, seeder));//2
    tc = nodeSeederConnReqLoc(tc, expectP);//3
    tc = torrentSeederDisconnect(tc, triggerP, torrentSeederDisconnect(torrent1, file1, seeder.getId())); //4
    tc = nodeSeederDisconnectLoc(tc, expectP); //5
    tc = inactiveFSM(tc, torrent1, seeder); //6
    tc.repeat(1).body().end();
    assertTrue(tc.check());
  }
  //********************************************CONNECTED TO DISCONNECT*************************************************
  @Test
  public void  testConnFail2() {
    OverlayId torrent1 = torrentIdFactory.id(new BasicBuilders.IntBuilder(1));
    KAddress seeder = SystemHelper.getAddress(1);
    Identifier file1 = intIdFactory.id(new BasicBuilders.IntBuilder(1));
    Identifier file2 = intIdFactory.id(new BasicBuilders.IntBuilder(2));
    
    tc = tc.body();
    tc = inactiveFSM(tc, torrent1, seeder); //1
    tc = connected(tc, torrent1, file1, seeder); //2-5
    tc = torrentSeederConnReq(tc, triggerP, torrentSeederConnReq(torrent1, file2, seeder));//6
    tc = torrentSeederConnSucc(tc, expectP); //7
    tc = nodeSeederConnFailLoc(tc, triggerP, nodeSeederConnFailLoc(torrent1, seeder.getId()));//8
    tc = tc.unordered();
    tc = torrentSeederConnFail(tc, expectP);//9
    tc = torrentSeederConnFail(tc, expectP);//9
    tc = tc.end();
    tc = inactiveFSM(tc, torrent1, seeder); //10
    tc.repeat(1).body().end();
    assertTrue(tc.check());
  }
  
  @Test
  public void  testLocDisconnect2() {
    OverlayId torrent1 = torrentIdFactory.id(new BasicBuilders.IntBuilder(1));
    KAddress seeder = SystemHelper.getAddress(1);
    Identifier file1 = intIdFactory.id(new BasicBuilders.IntBuilder(1));
    
    tc = tc.body();
    tc = inactiveFSM(tc, torrent1, seeder); //1
    tc = connected(tc, torrent1, file1, seeder);//2-5
    tc = torrentSeederDisconnect(tc, triggerP, torrentSeederDisconnect(torrent1, file1, seeder.getId())); //6
    tc = nodeSeederDisconnectLoc(tc, expectP); //7
    tc = inactiveFSM(tc, torrent1, seeder); //8
    tc.repeat(1).body().end();
    assertTrue(tc.check());
  }
  //********************************************************************************************************************
  
  TestContext inactiveFSM(TestContext tc, OverlayId torrentId, KAddress seeder) {
    return tc.inspect(inactiveFSM(torrentId, seeder));
  }
  
  TestContext activeFSM(TestContext tc, OverlayId torrentId, KAddress seeder) {
    return tc.inspect(activeFSM(torrentId, seeder));
  }
  
  TestContext state(TestContext tc, OverlayId torrentId, KAddress seeder, FSMStateName expectedState) {
    return tc.inspect(state(torrentId, seeder, expectedState));
  }
  
  Predicate<R1TorrentSeederAuxComp> inactiveFSM(OverlayId torrentId, KAddress seeder) {
    return (R1TorrentSeederAuxComp t) -> !t.activeSeederFSM(torrentId, seeder);
  }
  
  Predicate<R1TorrentSeederAuxComp> activeFSM(OverlayId torrentId, KAddress seeder) {
    return (R1TorrentSeederAuxComp t) -> t.activeSeederFSM(torrentId, seeder);
  }
  
  Predicate<R1TorrentSeederAuxComp> state(OverlayId torrentId, KAddress seeder, FSMStateName expectedState) {
    return (R1TorrentSeederAuxComp t) -> {
      FSMStateName currentState = t.seederState(torrentId, seeder);
      return currentState.equals(expectedState);
    };
  }
}
