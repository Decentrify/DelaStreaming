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

import java.util.Random;
import org.junit.After;
import static org.junit.Assert.assertTrue;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import se.sics.kompics.Component;
import se.sics.kompics.Port;
import se.sics.kompics.fsm.FSMException;
import se.sics.kompics.network.Network;
import se.sics.kompics.testing.TestContext;
import se.sics.kompics.timer.Timer;
import se.sics.kompics.util.Identifier;
import se.sics.ktoolbox.util.identifiable.BasicBuilders;
import se.sics.ktoolbox.util.identifiable.basic.IntIdFactory;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayId;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayIdFactory;
import se.sics.ktoolbox.util.network.KAddress;
import se.sics.silk.SystemHelper;
import se.sics.silk.SystemSetup;
import static se.sics.silk.TorrentTestHelper.eCtrlBaseInfoInd;
import static se.sics.silk.TorrentTestHelper.eCtrlMetaGetSucc;
import static se.sics.silk.TorrentTestHelper.eCtrlStopAck;
import static se.sics.silk.TorrentTestHelper.eHashReq;
import static se.sics.silk.TorrentTestHelper.eHashSucc;
import static se.sics.silk.TorrentTestHelper.eMetadataGetReq;
import static se.sics.silk.TorrentTestHelper.eMetadataGetSucc;
import static se.sics.silk.TorrentTestHelper.eMetadataServeReq;
import static se.sics.silk.TorrentTestHelper.eMetadataServeStop;
import static se.sics.silk.TorrentTestHelper.eMetadataServeStopAck;
import static se.sics.silk.TorrentTestHelper.eMetadataServeSucc;
import static se.sics.silk.TorrentTestHelper.eNetMetadataServe;
import static se.sics.silk.TorrentTestHelper.eNetNodeConnAcc;
import static se.sics.silk.TorrentTestHelper.eNodeSeederConnSucc;
import static se.sics.silk.TorrentTestHelper.eTimerSchedulePeriodicTimeout;
import static se.sics.silk.TorrentTestHelper.eTorrentSeederConnReq;
import static se.sics.silk.TorrentTestHelper.eTorrentSeederConnSucc;
import static se.sics.silk.TorrentTestHelper.netMetadata;
import static se.sics.silk.TorrentTestHelper.netNodeConnAcc;
import static se.sics.silk.TorrentTestHelper.tCtrlDownloadReq;
import static se.sics.silk.TorrentTestHelper.tCtrlMetaGetReq;
import static se.sics.silk.TorrentTestHelper.tCtrlStopReq;
import static se.sics.silk.TorrentTestHelper.tCtrlUploadReq;
import static se.sics.silk.TorrentTestHelper.tNetMetadataGet;
import static se.sics.silk.TorrentTestHelper.tNetNodeConnReq;
import static se.sics.silk.r2torrent.conn.helper.R2NodeSeederHelper.eNodeSeederConnReq;
import se.sics.silk.r2torrent.util.R2TorrentStatus;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class R2TorrentCompTest {

  private TestContext<R2TorrentComp> tc;
  private Component comp;
  private Port<R2TorrentCtrlPort> ctrlP;
  private Port<Network> networkP;
  private Port<Timer> timerP;
  private Port<R2TorrentPort> expectP;
  private Port<R2TorrentPort> triggerP;
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
    ctrlP = comp.getPositive(R2TorrentCtrlPort.class);
    networkP = comp.getNegative(Network.class);
    timerP = comp.getNegative(Timer.class);
    expectP = comp.getPositive(R2TorrentPort.class);
    triggerP = comp.getNegative(R2TorrentPort.class);
    intIdFactory = new IntIdFactory(new Random());
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
    tc = tc.body();
    tc.repeat(1).body().end();
    assertTrue(tc.check());
  }

  @Test
  public void testLeecher() {
    OverlayId torrent = torrentIdFactory.id(new BasicBuilders.IntBuilder(1));
    KAddress seeder = SystemHelper.getAddress(1);

//    tc = tc.setTimeout(10*60*1000);
    tc = tc.body();
    tc = tCtrlMetaGetReq(tc, ctrlP, torrent, seeder); //1
    tc = eMetadataGetReq(tc, expectP); //2
    tc = eTorrentSeederConnReq(tc, expectP); //3
    tc = eNodeSeederConnReq(tc, expectP); //4
    tc = netNodeConnAcc(tc, networkP); //5-6
    tc = eTimerSchedulePeriodicTimeout(tc, timerP);//7
    tc = eNodeSeederConnSucc(tc, expectP); //8
    tc = eTorrentSeederConnSucc(tc, expectP); //9
    tc = netMetadata(tc, networkP); //10-11
    tc = eMetadataGetSucc(tc, expectP); //12
    tc = eCtrlMetaGetSucc(tc, ctrlP); //13
    tc = eMetadataServeReq(tc, expectP); //14
    tc = eMetadataServeSucc(tc, expectP); //15
    tc = eCtrlBaseInfoInd(tc, ctrlP, R2TorrentStatus.META_SERVE); //16
    tc = tCtrlDownloadReq(tc, ctrlP, torrent);//17
    tc = eHashReq(tc, expectP);//18
    tc = eHashSucc(tc, expectP);//19
    tc = eCtrlBaseInfoInd(tc, ctrlP, R2TorrentStatus.HASH); //20
    tc = stop(tc, torrent); //21-24
    tc.repeat(1).body().end();
    assertTrue(tc.check());
  }

  private TestContext stop(TestContext tc, OverlayId torrent) {
    tc = tCtrlStopReq(tc, ctrlP, torrent); //1
    tc = eMetadataServeStop(tc, expectP); //2
    tc = eMetadataServeStopAck(tc, expectP); //3
    tc = eCtrlStopAck(tc, ctrlP); //4
    return tc;
  }

  @Test
  public void testSeeder() {
    OverlayId torrent = torrentIdFactory.id(new BasicBuilders.IntBuilder(1));
    KAddress seeder = SystemHelper.getAddress(1);
    KAddress leecher = SystemHelper.getAddress(2);
    Identifier file0 = intIdFactory.id(new BasicBuilders.IntBuilder(0));

    tc = tc.body();
    tc = tCtrlUploadReq(tc, ctrlP, torrent); //1
    tc = eMetadataServeReq(tc, expectP); //2
    tc = eMetadataServeSucc(tc, expectP); //3
    tc = eCtrlBaseInfoInd(tc, ctrlP, R2TorrentStatus.META_SERVE); //4
    tc = eHashReq(tc, expectP); //5
    tc = eHashSucc(tc, expectP); //6
    tc = eCtrlBaseInfoInd(tc, ctrlP, R2TorrentStatus.HASH); //7
    tc = tNetNodeConnReq(tc, networkP, leecher, seeder); //8
    tc = tc.unordered();
    tc = eTimerSchedulePeriodicTimeout(tc, timerP);//9
    tc = eNetNodeConnAcc(tc, networkP);//10
    tc = tc.end();
    tc = tNetMetadataGet(tc, networkP, leecher, seeder, torrent, file0); //11
    tc = eNetMetadataServe(tc, networkP, seeder, leecher); //12
    tc = stop(tc, torrent); //13-16
    tc.repeat(1).body().end();
    assertTrue(tc.check());
  }
}
