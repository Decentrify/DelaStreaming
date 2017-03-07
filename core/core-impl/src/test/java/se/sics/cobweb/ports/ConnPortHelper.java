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
package se.sics.cobweb.ports;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import java.util.LinkedList;
import java.util.List;
import org.javatuples.Triplet;
import se.sics.cobweb.conn.ConnPort;
import se.sics.cobweb.conn.event.ConnE;
import se.sics.cobweb.util.HandleId;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayId;
import se.sics.ktoolbox.util.network.KAddress;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class ConnPortHelper {
  public static List<Class> indication() {
    List<Class> events = new LinkedList<>();
    events.add(ConnE.SeederSample.class);
    events.add(ConnE.Connect2Request.class);
    events.add(ConnE.Connect1Accept.class);
    return events;
  }

  public static List<Class> request() {
    List<Class> events = new LinkedList<>();
    events.add(ConnE.Connect1Request.class);
    events.add(ConnE.Connect2Accept.class);
    return events;
  }
  
  public static Triplet seederSample(OverlayId torrentId, KAddress seederAdr) {
    ConnE.SeederSample msg = new ConnE.SeederSample(torrentId, seederAdr);
    Triplet result = Triplet.with(msg, false, ConnPort.class);
    return result;
  }

  
  public static Predicate<ConnE.Connect1Request> connectToSeeder(final HandleId handleId) {
    return new Predicate<ConnE.Connect1Request>() {
      @Override
      public boolean apply(ConnE.Connect1Request t) {
        return t.handleId.equals(handleId);
      }
    };
  }
  
  public static Predicate<ConnE.Connect1Accept> connectedSeeder(final HandleId handleId) {
    return new Predicate<ConnE.Connect1Accept>() {
      @Override
      public boolean apply(ConnE.Connect1Accept t) {
        return t.handleId.equals(handleId);
      }
    };
  }

  public static Triplet leecherConnect(OverlayId torrentId, HandleId leecherHandle, KAddress leecher) {
    ConnE.Connect2Request msg = new ConnE.Connect2Request(torrentId, leecherHandle, leecher);
    Triplet result = Triplet.with(msg, false, ConnPort.class);
    return result;
  }

  public static Function<ConnE.Connect1Request, ConnE.Connect1Accept> seederAccept() {
    return new Function<ConnE.Connect1Request, ConnE.Connect1Accept>() {
      @Override
      public ConnE.Connect1Accept apply(ConnE.Connect1Request req) {
        return req.accept();
      }
    };
  }
  
  public static Function<ConnE.Connect2Request, ConnE.Connect2Accept> leecherAccept() {
    return new Function<ConnE.Connect2Request, ConnE.Connect2Accept>() {
      @Override
      public ConnE.Connect2Accept apply(ConnE.Connect2Request req) {
        return req.accept();
      }
    };
  }
}
