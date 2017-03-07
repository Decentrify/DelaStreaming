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
import java.util.Properties;
import se.sics.kompics.Direct;
import se.sics.kompics.KompicsEvent;
import se.sics.kompics.Promise;
import se.sics.kompics.testkit.TestContext;
import se.sics.kompics.testkit.Testkit;
import se.sics.ktoolbox.nutil.fsm.MultiFSM;
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
import se.sics.nstream.SystemOverlays;
import se.sics.nstream.hops.libmngr.HopsLibraryMngr;
import se.sics.nstream.hops.libmngr.HopsLibraryMngrTestHelper;
import se.sics.nstream.hops.library.HopsLibraryProvider;
import se.sics.nstream.hops.library.HopsLibraryProviderTestHelper;
import se.sics.nstream.library.LibraryMngrComp;
import se.sics.nstream.library.LibraryMngrCompTestHelper;
import se.sics.nstream.library.LibraryProvider;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class LibTFSMHelper {

  public static OverlayIdFactory systemSetup(String applicationConfPath) {
    Properties props = System.getProperties();
    props.setProperty("config.file", applicationConfPath);

    TorrentIds.registerDefaults(1234);
    OverlayIdFactory oidFactory = overlaysSetup();
    fsmSetup();
    return oidFactory;
  }

  private static OverlayIdFactory overlaysSetup() {
    OverlayRegistry.initiate(new SystemOverlays.TypeFactory(), new SystemOverlays.Comparator());

    byte torrentOwnerId = 1;
    OverlayRegistry.registerPrefix(TorrentIds.TORRENT_OVERLAYS, torrentOwnerId);

    IdentifierFactory torrentBaseIdFactory = IdentifierRegistry.lookup(BasicIdentifiers.Values.OVERLAY.toString());
    return new OverlayIdFactory(torrentBaseIdFactory, TorrentIds.Types.TORRENT, torrentOwnerId);
  }

  private static void fsmSetup() {
    FSMIdRegistry.registerPrefix(LibTFSM.NAME, (byte) 1);
  }

  public static TestContext<LibraryMngrComp> getContext() {
    KAddress selfAdr = getAddress(0);
    LibraryProvider libProvider = new HopsLibraryProvider();
    LibraryMngrComp.Init init = new LibraryMngrComp.Init(selfAdr, libProvider);
    TestContext<LibraryMngrComp> context = Testkit.newTestContext(LibraryMngrComp.class, init);
    return context;
  }

  public static KAddress getAddress(int nodeId) {
    Identifier nId = BasicIdentifiers.nodeId(new BasicBuilders.IntBuilder(nodeId));
    KAddress adr;
    try {
      adr = NatAwareAddressImpl.open(new BasicAddress(InetAddress.getLocalHost(), 10000, nId));
    } catch (UnknownHostException ex) {
      throw new RuntimeException(ex);
    }
    return adr;
  }

  public static <DR extends Direct.Response, P extends Promise<DR>> Function<P, DR> promiseSuccessMapper(Class<P> promiseType) {
    return promiseSuccessMapper(promiseType, Result.success(true));
  }
  
  public static <DR extends Direct.Response, P extends Promise<DR>> Function<P, DR> promiseSuccessMapper(Class<P> promiseType, final Result r) {
    return new Function<P, DR>() {
      @Override
      public DR apply(P promise) {
        return promise.success(r);
      }
    };
  }
  
  public static <K extends KompicsEvent> Predicate<K> anyPredicate(Class<K> msgType) {
    return new Predicate<K>() {
      @Override
      public boolean apply(K t) {
        return true;
      }
    };
  }
  
  public static Predicate<LibraryMngrComp> inspectState(final OverlayId torrentId, final LibTStates expectedState) {
   return new Predicate<LibraryMngrComp>() {
    @Override
    public boolean apply(LibraryMngrComp lm) {
      LibraryProvider lp = LibraryMngrCompTestHelper.getLibraryProvider(lm);
      HopsLibraryMngr hlm = HopsLibraryProviderTestHelper.getHopsLibraryMngr(lp);
      MultiFSM mfsm = HopsLibraryMngrTestHelper.getFSM(hlm);
      return expectedState.equals(mfsm.getFSMState(LibTFSM.NAME, torrentId.baseId));
    }
  };
}
}
