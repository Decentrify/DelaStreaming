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
package se.sics.nstream.hops.libmngr.fsm;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import static org.junit.Assert.assertEquals;
import org.junit.Before;
import org.junit.Test;
import se.sics.kompics.Component;
import se.sics.kompics.Direct;
import se.sics.kompics.Port;
import se.sics.kompics.Promise;
import se.sics.kompics.testkit.Direction;
import se.sics.kompics.testkit.TestContext;
import se.sics.kompics.testkit.Testkit;
import se.sics.ktoolbox.nutil.fsm.ids.FSMIdRegistry;
import se.sics.ktoolbox.util.identifiable.BasicBuilders;
import se.sics.ktoolbox.util.identifiable.BasicIdentifiers;
import se.sics.ktoolbox.util.identifiable.Identifier;
import se.sics.ktoolbox.util.identifiable.IdentifierFactory;
import se.sics.ktoolbox.util.identifiable.IdentifierRegistry;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayId;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayIdFactory;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayRegistry;
import se.sics.ktoolbox.util.network.KAddress;
import se.sics.ktoolbox.util.network.basic.BasicAddress;
import se.sics.ktoolbox.util.network.nat.NatAwareAddressImpl;
import se.sics.ktoolbox.util.result.Result;
import se.sics.nstream.TorrentIds;
import se.sics.nstream.hops.SystemOverlays;
import se.sics.nstream.hops.hdfs.HDFSHelper;
import se.sics.nstream.hops.libmngr.fsm.hopsrestart1.HDFSHelperMockImpl;
import se.sics.nstream.hops.library.HopsLibraryProvider;
import se.sics.nstream.library.LibraryMngrComp;
import se.sics.nstream.library.LibraryProvider;
import se.sics.nstream.library.restart.TorrentRestart;
import se.sics.nstream.library.restart.TorrentRestartPort;
import se.sics.nstream.storage.durable.DEndpointCtrlPort;
import se.sics.nstream.storage.durable.events.DEndpoint;
import se.sics.nstream.torrent.TorrentMngrPort;
import se.sics.nstream.torrent.event.StartTorrent;
import se.sics.nstream.torrent.transfer.TransferCtrlPort;
import se.sics.nstream.torrent.transfer.event.ctrl.SetupTransfer;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class LibTFSMHopsRestartTest1 {

  private static Direction incoming = Direction.INCOMING;
  private static Direction outgoing = Direction.OUTGOING;

  private static OverlayIdFactory torrentIdFactory;
  private TestContext<LibraryMngrComp> tc;
  private Component libMngr;

  @Before
  public void setup() {
    String manifestPath = "src/test/resources/libtfsm/restarthdfs1/t1/manifest.json"; 
    HDFSHelper.mock = new HDFSHelperMockImpl(manifestPath);
    Properties props = System.getProperties();
    props.setProperty("config.file", "src/test/resources/libtfsm/restarthdfs1/application.conf");
    systemSetup();
    tc = getContext();
    libMngr = tc.getComponentUnderTest();
  }

  private void systemSetup() {
    TorrentIds.registerDefaults(1234);
    overlaysSetup();
    fsmSetup();
  }

  private void overlaysSetup() {
    OverlayRegistry.initiate(new SystemOverlays.TypeFactory(), new SystemOverlays.Comparator());

    byte torrentOwnerId = 1;
    OverlayRegistry.registerPrefix(TorrentIds.TORRENT_OVERLAYS, torrentOwnerId);

    IdentifierFactory torrentBaseIdFactory = IdentifierRegistry.lookup(BasicIdentifiers.Values.OVERLAY.toString());
    torrentIdFactory = new OverlayIdFactory(torrentBaseIdFactory, TorrentIds.Types.TORRENT, torrentOwnerId);
  }

  private void fsmSetup() {
    FSMIdRegistry.registerPrefix(LibTFSM.NAME, (byte) 1);
  }

  private TestContext<LibraryMngrComp> getContext() {
    Identifier nodeId = BasicIdentifiers.nodeId(new BasicBuilders.IntBuilder(1));
    KAddress selfAdr;
    try {
      selfAdr = NatAwareAddressImpl.open(new BasicAddress(InetAddress.getLocalHost(), 10000, nodeId));
    } catch (UnknownHostException ex) {
      throw new RuntimeException(ex);
    }
    LibraryProvider libProvider = new HopsLibraryProvider();
    LibraryMngrComp.Init init = new LibraryMngrComp.Init(selfAdr, libProvider);
    TestContext<LibraryMngrComp> context = Testkit.newTestContext(LibraryMngrComp.class, init);
    return context;
  }

  //********************************************************************************************************************
  @Test
  public void testKafka() {

    OverlayId torrentId = torrentIdFactory.randomId();
    Port<TorrentRestartPort> port1 = libMngr.getNegative(TorrentRestartPort.class);
    Port<DEndpointCtrlPort> port2 = libMngr.getNegative(DEndpointCtrlPort.class);
    Port<TorrentMngrPort> port3 = libMngr.getNegative(TorrentMngrPort.class);
    Port<TransferCtrlPort> port4 = libMngr.getNegative(TransferCtrlPort.class);
    tc.body()
      .expect(TorrentRestart.UpldReq.class, upldRestartMsg(), port1, outgoing)
      .expectWithMapper()
        .expect(DEndpoint.Connect.class, port2, port2, promiseSuccessMapper(DEndpoint.Connect.class))
      .end()
      .expectWithMapper()
        .expect(StartTorrent.Request.class, port3, port3, promiseSuccessMapper(StartTorrent.Request.class))
      .end()
      .expectWithMapper()
        .expect(SetupTransfer.Request.class, port4, port4, promiseSuccessMapper(SetupTransfer.Request.class))
      .end()
      ; 
    
    //.trigger(msg1, port1);
    assertEquals(tc.check(), tc.getFinalState());
  }
  
  private List<KAddress> getPartners() {
    Identifier nodeId = BasicIdentifiers.nodeId(new BasicBuilders.IntBuilder(1));
    KAddress p1;
    try {
      p1 = NatAwareAddressImpl.open(new BasicAddress(InetAddress.getLocalHost(), 10000, nodeId));
    } catch (UnknownHostException ex) {
      throw new RuntimeException(ex);
    }
    List<KAddress> partners = new LinkedList<>();
    partners.add(p1);
    return partners;
  }

  private Predicate<DEndpoint.Connect> connectMsg() {
    return new Predicate<DEndpoint.Connect>() {
      @Override
      public boolean apply(DEndpoint.Connect t) {
        String expectedBase = "1e1d968c-2458-49e4-b32c-78af06754dca_demo_testavro1491398341";
        OverlayId expected = torrentIdFactory.id(new BasicBuilders.StringBuilder(expectedBase));
        return expected.equals(t.torrentId);
      }
    };
  }
  
  private Predicate<TorrentRestart.UpldReq> upldRestartMsg() {
    return new Predicate<TorrentRestart.UpldReq>() {
      @Override
      public boolean apply(TorrentRestart.UpldReq t) {
        return true;
      }
    };
  }
  
  private <DR extends Direct.Response, P extends Promise<DR>> Function<P, DR> promiseSuccessMapper(Class<P> promiseType) {
    return new Function<P, DR>() {
    @Override
    public DR apply(P promise) {
      return promise.success(Result.success(true));
    }
  };
  }
}
