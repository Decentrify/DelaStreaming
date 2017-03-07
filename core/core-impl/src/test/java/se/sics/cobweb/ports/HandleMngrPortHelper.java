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
import java.util.LinkedList;
import java.util.List;
import org.javatuples.Triplet;
import se.sics.cobweb.transfer.handlemngr.HandleMngrPort;
import se.sics.cobweb.transfer.handlemngr.event.HandleMngrE;
import se.sics.cobweb.util.HandleId;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayId;
import se.sics.ktoolbox.util.network.KAddress;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class HandleMngrPortHelper {

  public static List<Class> indication() {
    List<Class> events = new LinkedList<>();
    events.add(HandleMngrE.LeecherConnected.class);
    events.add(HandleMngrE.LeecherDisconnected.class);
    events.add(HandleMngrE.SeederConnected.class);
    events.add(HandleMngrE.SeederDisconnected.class);
    return events;
  }

  public static List<Class> request() {
    List<Class> events = new LinkedList<>();
    events.add(HandleMngrE.LeecherConnect.class);
    events.add(HandleMngrE.LeecherDisconnect.class);
    events.add(HandleMngrE.SeederConnect.class);
    events.add(HandleMngrE.SeederDisconnect.class);
    return events;
  }

  public static Triplet leecherConnect(OverlayId torrentId, HandleId seederHId, KAddress seederAdr) {
    HandleMngrE.LeecherConnect connect = new HandleMngrE.LeecherConnect(torrentId, seederHId, seederAdr);
    Triplet result = Triplet.with(connect, true, HandleMngrPort.class);
    return result;
  }

  public static Triplet seederConnect(OverlayId torrentId, HandleId leecherHId, KAddress leecherAdr) {
    HandleMngrE.SeederConnect msg = new HandleMngrE.SeederConnect(torrentId, leecherHId, leecherAdr);
    Triplet result = Triplet.with(msg, true, HandleMngrPort.class);
    return result;
  }

  public static Triplet leecherDisconnect(OverlayId torrentId, HandleId seederHId) {
    HandleMngrE.LeecherDisconnect connect = new HandleMngrE.LeecherDisconnect(torrentId, seederHId);
    Triplet result = Triplet.with(connect, true, HandleMngrPort.class);
    return result;
  }

  public static Triplet seederDisconnect(OverlayId torrentId, HandleId leecherHId) {
    HandleMngrE.SeederDisconnect msg = new HandleMngrE.SeederDisconnect(torrentId, leecherHId);
    Triplet result = Triplet.with(msg, true, HandleMngrPort.class);
    return result;
  }

  public static Function<HandleMngrE.LeecherConnect, HandleMngrE.LeecherConnected> leecherConnected() {
    return new Function<HandleMngrE.LeecherConnect, HandleMngrE.LeecherConnected>() {
      @Override
      public HandleMngrE.LeecherConnected apply(HandleMngrE.LeecherConnect req) {
        return req.success();
      }
    };
  }
  
  public static Function<HandleMngrE.SeederConnect, HandleMngrE.SeederConnected> seederConnected() {
    return new Function<HandleMngrE.SeederConnect, HandleMngrE.SeederConnected>() {
      @Override
      public HandleMngrE.SeederConnected apply(HandleMngrE.SeederConnect req) {
        return req.success();
      }
    };
  }
  
}
