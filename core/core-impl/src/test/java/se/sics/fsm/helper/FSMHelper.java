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
package se.sics.fsm.helper;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Properties;
import se.sics.cobweb.ReflectEvent;
import se.sics.kompics.Direct;
import se.sics.kompics.KompicsEvent;
import se.sics.kompics.Promise;
import se.sics.ktoolbox.nutil.fsm.ids.FSMIdRegistry;
import se.sics.ktoolbox.util.identifiable.BasicBuilders;
import se.sics.ktoolbox.util.identifiable.BasicIdentifiers;
import se.sics.ktoolbox.util.identifiable.Identifier;
import se.sics.ktoolbox.util.identifiable.IdentifierFactory;
import se.sics.ktoolbox.util.identifiable.IdentifierRegistry;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayIdFactory;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayRegistry;
import se.sics.ktoolbox.util.network.KAddress;
import se.sics.ktoolbox.util.network.basic.BasicAddress;
import se.sics.ktoolbox.util.network.basic.BasicContentMsg;
import se.sics.ktoolbox.util.network.nat.NatAwareAddressImpl;
import se.sics.ktoolbox.util.result.Result;
import se.sics.nstream.SystemOverlays;
import se.sics.nstream.TorrentIds;
import se.sics.nutil.ContentWrapper;
import se.sics.nutil.ContentWrapperHelper;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class FSMHelper {

  public static OverlayIdFactory systemSetup(String applicationConfPath, String[] fsmNames) {
    Properties props = System.getProperties();
    props.setProperty("config.file", applicationConfPath);

    TorrentIds.registerDefaults(1234);
    OverlayIdFactory oidFactory = overlaysSetup();
    fsmSetup(fsmNames);
    return oidFactory;
  }

  private static OverlayIdFactory overlaysSetup() {
    OverlayRegistry.initiate(new SystemOverlays.TypeFactory(), new SystemOverlays.Comparator());

    byte torrentOwnerId = 1;
    OverlayRegistry.registerPrefix(TorrentIds.TORRENT_OVERLAYS, torrentOwnerId);

    IdentifierFactory torrentBaseIdFactory = IdentifierRegistry.lookup(BasicIdentifiers.Values.OVERLAY.toString());
    return new OverlayIdFactory(torrentBaseIdFactory, TorrentIds.Types.TORRENT, torrentOwnerId);
  }

  private static void fsmSetup(String[] fsmNames) {
    if(fsmNames.length > 254) {
      throw new RuntimeException("exceeding byte size");
    }
    byte idx = (byte)1;
    for(String fsmName : fsmNames) {
      FSMIdRegistry.registerPrefix(fsmName, idx++);
    }
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

  public static <DR extends Direct.Response, P extends Promise<DR>> Function<P, DR> promiseSuccessMapper(
    Class<P> promiseType) {
    return promiseSuccessMapper(promiseType, Result.success(true));
  }

  public static <DR extends Direct.Response, P extends Promise<DR>> Function<P, DR> promiseSuccessMapper(
    Class<P> promiseType, final Result r) {
    return new Function<P, DR>() {
      @Override
      public DR apply(P promise) {
        return promise.success(r);
      }
    };
  }

  public static <K extends KompicsEvent> Predicate<K> anyEvent(Class<K> msgType) {
    return new Predicate<K>() {
      @Override
      public boolean apply(K t) {
        return true;
      }
    };
  }
  
    public static <K extends KompicsEvent> Predicate<K> anyEventAndPredicate(Class<K> msgType, final Predicate<Boolean> next) {
    return new Predicate<K>() {
      @Override
      public boolean apply(K t) {
        return next.apply(true);
      }
    };
  }
  
  public static <K extends Object> Predicate<BasicContentMsg> anyMsg(final Class<K> msgType) {
    return new Predicate<BasicContentMsg>() {
      @Override
      public boolean apply(BasicContentMsg msg) {
        Object baseContent = msg.getContent();
        if(baseContent instanceof ContentWrapper) {
            baseContent = ContentWrapperHelper.getBaseContent((ContentWrapper)baseContent, Object.class);
        }
        if(baseContent.getClass().isAssignableFrom(msgType)) {
          return true;
        }
        return false;
      }
    };
  }
  
  public static <K extends KompicsEvent> Predicate<ReflectEvent> anyReflectedEvent(final Class<K> msgType) {
    return new Predicate<ReflectEvent>() {
      @Override
      public boolean apply(ReflectEvent t) {
        return msgType.isInstance(t);
      }
    };
  }
}
