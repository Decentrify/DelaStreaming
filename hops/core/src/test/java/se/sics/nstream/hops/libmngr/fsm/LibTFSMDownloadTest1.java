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
//import com.google.common.base.Optional;
//import com.google.common.base.Predicate;
//import java.util.HashMap;
//import java.util.LinkedList;
//import java.util.List;
//import java.util.Map;
//import static org.junit.Assert.assertEquals;
//import org.junit.Before;
//import org.junit.Test;
//import se.sics.kompics.Component;
//import se.sics.kompics.ComponentDefinition;
//import se.sics.kompics.Direct;
//import se.sics.kompics.Handler;
//import se.sics.kompics.KompicsEvent;
//import se.sics.kompics.Port;
//import se.sics.kompics.Positive;
//import se.sics.kompics.Promise;
//import se.sics.kompics.Start;
//import se.sics.kompics.fsm.FSMException;
//import se.sics.kompics.testkit.Direction;
//import se.sics.kompics.testkit.TestContext;
//import se.sics.ktoolbox.util.identifiable.overlay.OverlayId;
//import se.sics.ktoolbox.util.identifiable.overlay.OverlayIdFactory;
//import se.sics.ktoolbox.util.network.KAddress;
//import se.sics.ktoolbox.util.result.Result;
//import se.sics.nstream.hops.hdfs.HDFSHelper;
//import se.sics.nstream.hops.hdfs.HDFSHelperMock;
//import se.sics.nstream.hops.kafka.KafkaEndpoint;
//import se.sics.nstream.hops.kafka.KafkaResource;
//import se.sics.nstream.hops.libmngr.fsm.hopsrestart1.HDFSHelperMockImpl;
//import se.sics.nstream.hops.library.HopsTorrentPort;
//import se.sics.nstream.hops.library.event.core.HopsTorrentDownloadEvent;
//import se.sics.nstream.hops.manifest.ManifestHelper;
//import se.sics.nstream.hops.manifest.ManifestJSON;
//import se.sics.nstream.hops.storage.hdfs.HDFSEndpoint;
//import se.sics.nstream.hops.storage.hdfs.HDFSResource;
//import se.sics.nstream.library.LibraryMngrComp;
//import se.sics.nstream.storage.durable.DEndpointCtrlPort;
//import se.sics.nstream.storage.durable.events.DEndpoint;
//import se.sics.silk.torrent.TorrentMngrPort;
//import se.sics.silk.torrentmngr.event.StartTorrent;
//import se.sics.nstream.torrent.transfer.TransferCtrlPort;
//import se.sics.nstream.torrent.transfer.event.ctrl.GetRawTorrent;
//import se.sics.nstream.torrent.transfer.event.ctrl.SetupTransfer;
//import se.sics.nstream.transfer.MyTorrent;
//
///**
// * Download with Kafka as secondary sink
// * <p>
// * @author Alex Ormenisan <aaor@kth.se>
// */
//public class LibTFSMDownloadTest1 {
//
//  private final static Direction incoming = Direction.INCOMING;
//  private final static Direction outgoing = Direction.OUTGOING;
//  private final static String torrentPath = "src/test/resources/libtfsm/download1/t1";
//
//  private static OverlayIdFactory torrentIdFactory;
//  private TestContext<LibraryMngrComp> tc;
//  private Component libMngr;
//  private Component mock;
//
//  private OverlayId torrentId;
//
//  @Before
//  public void setup() throws FSMException {
//    HDFSHelper.mock = new HDFSHelperMockImpl("");
//    torrentIdFactory = LibTFSMHelper.systemSetup("src/test/resources/libtfsm/download1/application.conf");
//    tc = LibTFSMHelper.getContext();
//    libMngr = tc.getComponentUnderTest();
//
//    torrentId = torrentIdFactory.randomId();
//    mock = tc.create(MockComponent.class, new Init(startDownload(), advanceDownload()));
//  }
//
//  //********************************************************************************************************************
//  @Test
//  public void test() {
//    tc.connect(libMngr.getPositive(HopsTorrentPort.class), mock.getNegative(HopsTorrentPort.class));
//    Port<HopsTorrentPort> port1 = libMngr.getPositive(HopsTorrentPort.class);
//    Port<DEndpointCtrlPort> port2 = libMngr.getNegative(DEndpointCtrlPort.class);
//    Port<TorrentMngrPort> port3 = libMngr.getNegative(TorrentMngrPort.class);
//    Port<TransferCtrlPort> port4 = libMngr.getNegative(TransferCtrlPort.class);
//    tc.body()
//        .expect(HopsTorrentDownloadEvent.StartRequest.class, anyPredicate(
//            HopsTorrentDownloadEvent.StartRequest.class), port1, incoming)
//        .inspect(fsmState(LibTStates.PREPARE_MANIFEST_STORAGE))
//        .expectWithMapper()
//          .expect(DEndpoint.Connect.class, port2, port2, successMapper(DEndpoint.Connect.class))
//        .end()
//        .inspect(fsmState(LibTStates.PREPARE_TRANSFER))
//        .expectWithMapper()
//          .expect(StartTorrent.Request.class, port3, port3, successMapper(StartTorrent.Request.class))
//        .end()
//        .inspect(fsmState(LibTStates.DOWNLOAD_MANIFEST))
//        .expectWithMapper()
//          .expect(GetRawTorrent.Request.class, port4, port4, successMapper(GetRawTorrent.Request.class, getManifest()))
//        .end()
//        .inspect(fsmState(LibTStates.EXTENDED_DETAILS))
////      TODO Alex - fix Direct.Request/Response - once fixed, reenable
////        .expect(HopsTorrentDownloadEvent.StartSuccess.class, LibTFSMHelper.anyPredicate(
////            HopsTorrentDownloadEvent.StartSuccess.class), port1, outgoing)
//        .expect(HopsTorrentDownloadEvent.AdvanceRequest.class, anyPredicate(
//            HopsTorrentDownloadEvent.AdvanceRequest.class), port1, incoming)
//        .inspect(fsmState(LibTStates.PREPARE_FILES_STORAGE))
//        .expectWithMapper()
//          .expect(DEndpoint.Connect.class, port2, port2, successMapper(DEndpoint.Connect.class))
//        .end()
//        .inspect(fsmState(LibTStates.ADVANCE_TRANSFER))
//        .expectWithMapper()
//          .expect(SetupTransfer.Request.class, port4, port4, successMapper(SetupTransfer.Request.class))
//        .end()
//        .inspect(fsmState(LibTStates.DOWNLOADING))
//        .repeat(1).body().end()
//      ;
//
//    assertEquals(tc.check(), tc.getFinalState());
//  }
//
//  //********************************************************************************************************************
//  public HopsTorrentDownloadEvent.StartRequest startDownload() {
//    HDFSEndpoint endpoint = HDFSEndpoint.getBasic("mock_url", "mock_user");
//    HDFSResource manifest = new HDFSResource("mock_path", "mock_filename");
//    List<KAddress> partners = new LinkedList<>();
//    partners.add(LibTFSMHelper.getAddress(1));
//    HopsTorrentDownloadEvent.StartRequest msg = new HopsTorrentDownloadEvent.StartRequest(torrentId, "t1", 0, 0,
//      endpoint, manifest, partners);
//    return msg;
//  }
//
//  public Result<MyTorrent.Manifest> getManifest() {
//    String manifestPath = torrentPath + "/manifest.json";
//    HDFSHelperMock mockHelper = new HDFSHelperMockImpl(manifestPath);
//    Result<ManifestJSON> manifestJSON = mockHelper.readManifest(null, null, null);
//    return Result.success(ManifestHelper.getManifest(manifestJSON.getValue()));
//  }
//
//  public HopsTorrentDownloadEvent.AdvanceRequest advanceDownload() {
//    HDFSEndpoint hdfsEndpoint = HDFSEndpoint.getBasic("mock_url", "mock_user");
//    KafkaEndpoint kafkaEndpoint = new KafkaEndpoint("mock_broker", "mock_restendpoint", "mock_domain", "mock_projectid",
//      "mock_keystore", "mock_truststore");
//    Map<String, HDFSResource> hdfsDetails = new HashMap<>();
//    HDFSResource hdfsFile1 = new HDFSResource("mock_path", "file1");
//    hdfsDetails.put("file1", hdfsFile1);
//    Map<String, KafkaResource> kafkaDetails = new HashMap<>();
//    KafkaResource kafkaFile1 = new KafkaResource("mock_session", "mock_topic");
//    kafkaDetails.put("file1", kafkaFile1);
//    HopsTorrentDownloadEvent.AdvanceRequest msg = new HopsTorrentDownloadEvent.AdvanceRequest(torrentId, hdfsEndpoint,
//      Optional.of(kafkaEndpoint), hdfsDetails, kafkaDetails);
//    return msg;
//  }
//
//  public <DR extends Direct.Response, P extends Promise<DR>> Function<P, DR> successMapper(Class<P> promiseType) {
//    return LibTFSMHelper.promiseSuccessMapper(promiseType);
//  }
//  
//  public <DR extends Direct.Response, P extends Promise<DR>> Function<P, DR> successMapper(Class<P> promiseType, final Result r) {
//    return LibTFSMHelper.promiseSuccessMapper(promiseType, r);
//  }
//  
//  public <K extends KompicsEvent> Predicate<K> anyPredicate(Class<K> msgType) {
//    return LibTFSMHelper.anyPredicate(msgType);
//  }
//  
//  public Predicate<LibraryMngrComp> fsmState(final LibTStates expectedState) {
//    return LibTFSMHelper.inspectState(torrentId, expectedState);
//  }
//  //*************************************FIX FOR DIRECT.REQUEST/RESPONSE NOT WORING*************************************
//
//  public static class MockComponent extends ComponentDefinition {
//
//    private final Positive<HopsTorrentPort> hopsPort = requires(HopsTorrentPort.class);
//    private final Init init;
//
//    public MockComponent(Init init) {
//      this.init = init;
//      subscribe(handleStart, control);
//      subscribe(handleStartDownloadSuccess, hopsPort);
//    }
//
//    Handler handleStart = new Handler<Start>() {
//      @Override
//      public void handle(Start event) {
//        trigger(init.startReq, hopsPort);
//      }
//    };
//
//    Handler handleStartDownloadSuccess = new Handler<HopsTorrentDownloadEvent.StartSuccess>() {
//      @Override
//      public void handle(HopsTorrentDownloadEvent.StartSuccess event) {
//        trigger(init.advanceReq, hopsPort);
//      }
//    };
//  }
//
//  public static class Init extends se.sics.kompics.Init<MockComponent> {
//
//    public final HopsTorrentDownloadEvent.StartRequest startReq;
//    public final HopsTorrentDownloadEvent.AdvanceRequest advanceReq;
//
//    public Init(HopsTorrentDownloadEvent.StartRequest startReq, HopsTorrentDownloadEvent.AdvanceRequest advanceReq) {
//      this.startReq = startReq;
//      this.advanceReq = advanceReq;
//    }
//  }
//}
