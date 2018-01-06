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
package se.sics.silk.r2mngr;

import com.google.common.base.Predicate;
import org.javatuples.Pair;
import org.junit.After;
import static org.junit.Assert.assertTrue;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import se.sics.kompics.Component;
import se.sics.kompics.Port;
import se.sics.kompics.fsm.FSMException;
import se.sics.kompics.fsm.FSMStateName;
import se.sics.kompics.fsm.event.FSMWrongState;
import se.sics.kompics.network.Network;
import se.sics.kompics.testing.Direction;
import se.sics.kompics.testing.TestContext;
import se.sics.kompics.util.Identifier;
import se.sics.ktoolbox.util.identifiable.BasicBuilders;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayId;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayIdFactory;
import se.sics.ktoolbox.util.network.KAddress;
import se.sics.silk.SystemHelper;
import se.sics.silk.SystemSetup;
import se.sics.silk.r2mngr.R2Torrent.Event;
import se.sics.silk.r2mngr.R2Torrent.States;
import se.sics.silk.r2mngr.event.R2TorrentEvents;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class R2TorrentTest {

  private TestContext<R2MngrWrapperComp> tc;
  private Component r2MngrComp;
  private Port<R2TorrentPort> torrentP;
  private Port<Network> networkP;
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
    torrentP = r2MngrComp.getPositive(R2TorrentPort.class);
    networkP = r2MngrComp.getNegative(Network.class);
  }

  private TestContext<R2MngrWrapperComp> getContext() {
    selfAdr = SystemHelper.getAddress(0);
    R2MngrWrapperComp.Init init = new R2MngrWrapperComp.Init(selfAdr);
    TestContext<R2MngrWrapperComp> context = TestContext.newInstance(R2MngrWrapperComp.class, init);
    return context;
  }

  @After
  public void clean() {
  }

  @Test
  public void testBadStatesAtStart() {
    OverlayId torrent1 = torrentIdFactory.id(new BasicBuilders.IntBuilder(1));

    tc = tc.body();
    tc = torWrongEventAtStart(tc, torHash(torrent1)); //1-4
    tc = torWrongEventAtStart(tc, torDwnl(torrent1)); //5-8
    tc = torWrongEventAtStart(tc, torUpld(torrent1)); //9-12
//    tc = torrentWrongEventAtStart(tc, torDwnlSlot(torrent1, 1, 1)); //13-16
//    tc = torrentWrongEventAtStart(tc, torUpldSlot(torrent1, 1, 1)); //17-20
    tc.repeat(1).body().end();
    assertTrue(tc.check());
  }
  
  @Test
  public void testDownload1() {
    OverlayId torrent1 = torrentIdFactory.id(new BasicBuilders.IntBuilder(1));
    Pair<Event, States> start = Pair.with(torGetMeta(torrent1), States.GET_META);
    Pair[] timeline = new Pair[]{
      Pair.with(torServeMeta(torrent1), States.SERVE_META),
      Pair.with(torHash(torrent1), States.HASHING),
      Pair.with(torDwnl(torrent1), States.DOWNLOAD),
      Pair.with(torUpld(torrent1), States.UPLOAD)
    };
    Event end = torStop(torrent1);
    runTimeline(tc, start, timeline, end);
  }
  
  @Test
  public void testDownload2() {
    OverlayId torrent1 = torrentIdFactory.id(new BasicBuilders.IntBuilder(1));
    Pair<Event, States> start = Pair.with(torGetMeta(torrent1), States.GET_META);
    Pair[] timeline = new Pair[]{
      Pair.with(torServeMeta(torrent1), States.SERVE_META),
      Pair.with(torDwnl(torrent1), States.DOWNLOAD),
      Pair.with(torUpld(torrent1), States.UPLOAD)
    };
    Event end = torStop(torrent1);
    runTimeline(tc, start, timeline, end);
  }
  
  @Test
  public void testUpload1() {
    OverlayId torrent1 = torrentIdFactory.id(new BasicBuilders.IntBuilder(1));
    Pair<Event, States> start = Pair.with(torGetMeta(torrent1), States.GET_META);
    Pair[] timeline = new Pair[]{
      Pair.with(torServeMeta(torrent1), States.SERVE_META),
      Pair.with(torUpld(torrent1), States.UPLOAD)
    };
    Event end = torStop(torrent1);
    runTimeline(tc, start, timeline, end);
  }
  
  @Test
  public void testUpload2() {
    OverlayId torrent1 = torrentIdFactory.id(new BasicBuilders.IntBuilder(1));
    Pair<Event, States> start = Pair.with(torGetMeta(torrent1), States.GET_META);
    Pair[] timeline = new Pair[]{
      Pair.with(torServeMeta(torrent1), States.SERVE_META),
      Pair.with(torHash(torrent1), States.HASHING),
      Pair.with(torUpld(torrent1), States.UPLOAD)
    };
    Event end = torStop(torrent1);
    runTimeline(tc, start, timeline, end);
  }
  
  @Test
  public void testUpload3() {
    OverlayId torrent1 = torrentIdFactory.id(new BasicBuilders.IntBuilder(1));
    Pair<Event, States> start = Pair.with(torServeMeta(torrent1), States.SERVE_META);
    Pair[] timeline = new Pair[]{
      Pair.with(torHash(torrent1), States.HASHING),
      Pair.with(torUpld(torrent1), States.UPLOAD)
    };
    Event end = torStop(torrent1);
    runTimeline(tc, start, timeline, end);
  }
  
  @Test
  public void testUpload4() {
    OverlayId torrent1 = torrentIdFactory.id(new BasicBuilders.IntBuilder(1));
    Pair<Event, States> start = Pair.with(torServeMeta(torrent1), States.SERVE_META);
    Pair[] timeline = new Pair[]{
      Pair.with(torUpld(torrent1), States.UPLOAD)
    };
    Event end = torStop(torrent1);
    runTimeline(tc, start, timeline, end);
  }
  
  @Test
  public void testStop1() {
    OverlayId torrent1 = torrentIdFactory.id(new BasicBuilders.IntBuilder(1));
    Pair<Event, States> start = Pair.with(torGetMeta(torrent1), States.GET_META);
    Pair[] timeline = new Pair[]{
    };
    Event end = torStop(torrent1);
    runTimeline(tc, start, timeline, end);
  }
  
  @Test
  public void testStop2() {
    OverlayId torrent1 = torrentIdFactory.id(new BasicBuilders.IntBuilder(1));
    Pair<Event, States> start = Pair.with(torServeMeta(torrent1), States.SERVE_META);
    Pair[] timeline = new Pair[]{
    };
    Event end = torStop(torrent1);
    runTimeline(tc, start, timeline, end);
  }
  
  @Test
  public void testStop3() {
    OverlayId torrent1 = torrentIdFactory.id(new BasicBuilders.IntBuilder(1));
    Pair<Event, States> start = Pair.with(torServeMeta(torrent1), States.SERVE_META);
    Pair[] timeline = new Pair[]{
      Pair.with(torHash(torrent1), States.HASHING)
    };
    Event end = torStop(torrent1);
    runTimeline(tc, start, timeline, end);
  }
  
  @Test
  public void testStop4() {
    OverlayId torrent1 = torrentIdFactory.id(new BasicBuilders.IntBuilder(1));
    Pair<Event, States> start = Pair.with(torGetMeta(torrent1), States.GET_META);
    Pair[] timeline = new Pair[]{
      Pair.with(torServeMeta(torrent1), States.SERVE_META),
      Pair.with(torDwnl(torrent1), States.DOWNLOAD)
    };
    Event end = torStop(torrent1);
    runTimeline(tc, start, timeline, end);
  }
  
  @Test
  public void testStop5() {
    OverlayId torrent1 = torrentIdFactory.id(new BasicBuilders.IntBuilder(1));
    Pair<Event, States> start = Pair.with(torServeMeta(torrent1), States.SERVE_META);
    Pair[] timeline = new Pair[]{
      Pair.with(torUpld(torrent1), States.UPLOAD)
    };
    Event end = torStop(torrent1);
    runTimeline(tc, start, timeline, end);
  }
  
  
  private void runTimeline(TestContext<R2MngrWrapperComp> tc, Pair<Event, States> start, Pair[] timeline, Event end) {
    tc = tc.body();
    tc = torInit(tc, start.getValue0(), start.getValue1());
    for(Pair<Event, States> step : timeline) {
      tc = torTo(tc, step.getValue0(), step.getValue1());
    }
    tc = torFinal(tc, end);
    tc.repeat(1).body().end();
    assertTrue(tc.check());
  }
//  @Test
//  public void testDownload1() {
//    OverlayId torrent1 = torrentIdFactory.id(new BasicBuilders.IntBuilder(1));
//    tc = tc.body();
//    tc = torTo(tc, torGetMeta(torrent1), States.GET_META);
//    tc = torFromTo(tc, torServeMeta(torrent1), States.GET_META, States.SERVE_META);
//    tc = torFromTo(tc, torHash(torrent1), States.SERVE_META, States.HASHING);
//    tc = torFromTo(tc, torHash(torrent1), States.HASHING, States.DOWNLOAD);
//    tc = torStop(tc, torStop(torrent1), States.UPLOAD);
//    tc.repeat(1).body().end();
//    assertTrue(tc.check());
//  }
  
//  @Test
//  public void testDownload3() {
//    OverlayId torrent1 = torrentIdFactory.id(new BasicBuilders.IntBuilder(1));
//    tc = tc.body();
//    tc = torGetMeta(tc, torGetMeta(torrent1));
//    tc = torServeMeta2(tc, torServeMeta(torrent1));
//    tc = torStop(tc, torStop(torrent1), States.UPLOAD);
//    tc.repeat(1).body().end();
//    assertTrue(tc.check());
//  }
//
//  @Test
//  public void testUpload1() {
//    OverlayId torrent1 = torrentIdFactory.id(new BasicBuilders.IntBuilder(1));
//    tc = tc.body();
//    tc = torServeMeta1(tc, torServeMeta(torrent1));
//    tc = torUpld(tc, torUpld(torrent1), States.SERVE_META);
//    tc = torStop(tc, torStop(torrent1), States.UPLOAD);
//    tc.repeat(1).body().end();
//    assertTrue(tc.check());
//  }
//
//  @Test
//  public void testUpload2() {
//    OverlayId torrent1 = torrentIdFactory.id(new BasicBuilders.IntBuilder(1));
//    tc = tc.body();
//    tc = torServeMeta1(tc, torServeMeta(torrent1));
//    tc = torHash(tc, torHash(torrent1));
//    tc = torUpld(tc, torUpld(torrent1), States.HASHING);
//    tc = torStop(tc, torStop(torrent1), States.UPLOAD);
//    tc.repeat(1).body().end();
//    assertTrue(tc.check());
//  }
//
//  @Test
//  public void testUpload3() {
//    OverlayId torrent1 = torrentIdFactory.id(new BasicBuilders.IntBuilder(1));
//    tc = tc.body();
//    tc = torGetMeta(tc, torGetMeta(torrent1));
//    tc = torServeMeta2(tc, torServeMeta(torrent1));
//    tc = torUpld(tc, torUpld(torrent1), States.SERVE_META);
//    tc = torStop(tc, torStop(torrent1), States.UPLOAD);
//    tc.repeat(1).body().end();
//    assertTrue(tc.check());
//  }
//
//  @Test
//  public void testUpload4() {
//    OverlayId torrent1 = torrentIdFactory.id(new BasicBuilders.IntBuilder(1));
//    tc = tc.body();
//    tc = torGetMeta(tc, torGetMeta(torrent1));
//    tc = torServeMeta2(tc, torServeMeta(torrent1));
//    tc = torHash(tc, torHash(torrent1));
//    tc = torUpld(tc, torUpld(torrent1), States.HASHING);
//    tc = torStop(tc, torStop(torrent1), States.UPLOAD);
//    tc.repeat(1).body().end();
//    assertTrue(tc.check());
//  }

  private TestContext<R2MngrWrapperComp> torInit(TestContext<R2MngrWrapperComp> tc,
    R2Torrent.Event event, FSMStateName to) {
    return tc
      .inspect(inactiveFSM(event.getR2TorrentFSMId()))
      .trigger(event, torrentP)
      .inspect(state(event.getR2TorrentFSMId(), to));
  }
  
  private TestContext<R2MngrWrapperComp> torTo(TestContext<R2MngrWrapperComp> tc,
    R2Torrent.Event event, FSMStateName to) {
    return tc
      .trigger(event, torrentP)
      .inspect(state(event.getR2TorrentFSMId(), to));
  }
  
  private TestContext<R2MngrWrapperComp> torFinal(TestContext<R2MngrWrapperComp> tc,
    R2Torrent.Event event) {
    return tc
      .trigger(event, torrentP)
      .inspect(inactiveFSM(event.getR2TorrentFSMId()));
  }
  
  private TestContext<R2MngrWrapperComp> torWrongEventAtStart(TestContext<R2MngrWrapperComp> tc,
    R2Torrent.Event event) {
    return tc
      .inspect(inactiveFSM(event.getR2TorrentFSMId()))
      .trigger(event, torrentP)
      .expect(FSMWrongState.class, torrentP, Direction.OUT)
      .inspect(inactiveFSM(event.getR2TorrentFSMId()));
  }

  Predicate<R2MngrWrapperComp> inactiveFSM(Identifier seederId) {
    return (R2MngrWrapperComp t) -> !t.activeTorrentFSM(seederId);
  }

  Predicate<R2MngrWrapperComp> state(Identifier baseTorrentId, FSMStateName expectedState) {
    return (R2MngrWrapperComp t) -> {
      FSMStateName currentState = t.getTorrentState(baseTorrentId);
      return currentState.equals(expectedState);
    };
  }

  private R2TorrentEvents.GetMeta torGetMeta(OverlayId torrentId) {
    return new R2TorrentEvents.GetMeta(torrentId);
  }

  private R2TorrentEvents.ServeMeta torServeMeta(OverlayId torrentId) {
    return new R2TorrentEvents.ServeMeta(torrentId);
  }

  private R2TorrentEvents.Hashing torHash(OverlayId torrentId) {
    return new R2TorrentEvents.Hashing(torrentId);
  }

  private R2TorrentEvents.Download torDwnl(OverlayId torrentId) {
    return new R2TorrentEvents.Download(torrentId);
  }

  private R2TorrentEvents.Upload torUpld(OverlayId torrentId) {
    return new R2TorrentEvents.Upload(torrentId);
  }
  
  private R2TorrentEvents.Stop torStop(OverlayId torrentId) {
    return new R2TorrentEvents.Stop(torrentId);
  }

  private R2TorrentEvents.DownloadSlotReq torDwnlSlot(OverlayId torrentId, int fileNr, int reqBlockSlots) {
    return new R2TorrentEvents.DownloadSlotReq(torrentId, fileNr, reqBlockSlots);
  }

  private R2TorrentEvents.UploadSlotReq torUpldSlot(OverlayId torrentId, int fileNr, int reqBlockSlots) {
    return new R2TorrentEvents.UploadSlotReq(torrentId, fileNr, reqBlockSlots);
  }
}
