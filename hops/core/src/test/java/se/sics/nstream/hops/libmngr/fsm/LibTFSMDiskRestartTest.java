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
//package se.sics.nstream.hops.libmngr.fsm;
//
//import com.google.common.base.Function;
//import com.google.common.base.Predicate;
//import static org.junit.Assert.assertEquals;
//import org.junit.Before;
//import org.junit.Test;
//import se.sics.kompics.Component;
//import se.sics.kompics.Direct;
//import se.sics.kompics.KompicsEvent;
//import se.sics.kompics.Port;
//import se.sics.kompics.Promise;
//import se.sics.kompics.fsm.FSMException;
//import se.sics.ktoolbox.util.identifiable.BasicBuilders;
//import se.sics.ktoolbox.util.identifiable.overlay.OverlayId;
//import se.sics.ktoolbox.util.identifiable.overlay.OverlayIdFactory;
//import se.sics.nstream.library.LibraryMngrComp;
//import se.sics.nstream.library.restart.TorrentRestart;
//import se.sics.nstream.library.restart.TorrentRestartPort;
//import se.sics.nstream.storage.durable.DEndpointCtrlPort;
//import se.sics.nstream.storage.durable.events.DEndpoint;
//import se.sics.nstream.torrent.transfer.TransferCtrlPort;
//import se.sics.nstream.torrent.transfer.event.ctrl.SetupTransfer;
//import se.sics.silk.torrent.TorrentMngrPort;
//import se.sics.silk.torrentmngr.event.StartTorrent;
//
///**
// * @author Alex Ormenisan <aaor@kth.se>
// */
//public class LibTFSMDiskRestartTest {
//
//  private static Direction incoming = Direction.INCOMING;
//  private static Direction outgoing = Direction.OUTGOING;
//
//  private static OverlayIdFactory torrentIdFactory;
//  private TestContext<LibraryMngrComp> tc;
//  private Component libMngr;
//  private OverlayId torrentId;
//
//  @Before
//  public void setup() throws FSMException {
//    torrentIdFactory = LibTFSMHelper.systemSetup("src/test/resources/libtfsm/restartdisk1/application.conf");
//    tc = LibTFSMHelper.getContext();
//    libMngr = tc.getComponentUnderTest();
//    String testTorrentId = "1e1d968c-2458-49e4-b32c-78af06754dca_demo_testavro1491398341";
//    torrentId = torrentIdFactory.id(new BasicBuilders.StringBuilder(testTorrentId));
//  }
//  //********************************************************************************************************************
//  @Test
//  public void testKafka() {
//    Port<TorrentRestartPort> port1 = libMngr.getNegative(TorrentRestartPort.class);
//    Port<DEndpointCtrlPort> port2 = libMngr.getNegative(DEndpointCtrlPort.class);
//    Port<TorrentMngrPort> port3 = libMngr.getNegative(TorrentMngrPort.class);
//    Port<TransferCtrlPort> port4 = libMngr.getNegative(TransferCtrlPort.class);
//    tc.body()
//      .expect(TorrentRestart.UpldReq.class, anyPredicate(TorrentRestart.UpldReq.class), port1, outgoing)
//      .inspect(fsmState(LibTStates.PREPARE_MANIFEST_STORAGE))
//      .expectWithMapper()
//        .expect(DEndpoint.Connect.class, port2, port2, successMapper(DEndpoint.Connect.class))
//      .end()
//      .inspect(fsmState(LibTStates.PREPARE_TRANSFER))
//      .expectWithMapper()
//        .expect(StartTorrent.Request.class, port3, port3, successMapper(StartTorrent.Request.class))
//      .end()
//      .inspect(fsmState(LibTStates.ADVANCE_TRANSFER))
//      .expectWithMapper()
//        .expect(SetupTransfer.Request.class, port4, port4, successMapper(SetupTransfer.Request.class))
//      .end()
//      .inspect(fsmState(LibTStates.UPLOADING))
//      .repeat(1).body().end()
//      ; 
//    
//    assertEquals(tc.check(), tc.getFinalState());
//  }
//  //********************************************************************************************************************
//  public <DR extends Direct.Response, P extends Promise<DR>> Function<P, DR> successMapper(Class<P> promiseType) {
//    return LibTFSMHelper.promiseSuccessMapper(promiseType);
//  }
//  
//  public <K extends KompicsEvent> Predicate<K> anyPredicate(Class<K> msgType) {
//    return LibTFSMHelper.anyPredicate(msgType);
//  }
//  
//  public Predicate<LibraryMngrComp> fsmState(final LibTStates expectedState) {
//    return LibTFSMHelper.inspectState(torrentId, expectedState);
//  }
//}
