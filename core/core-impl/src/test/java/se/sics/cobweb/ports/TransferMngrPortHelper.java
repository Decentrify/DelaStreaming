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

import com.google.common.base.Optional;
import java.util.LinkedList;
import java.util.List;
import org.javatuples.Triplet;
import se.sics.cobweb.transfer.mngr.TransferMngrPort;
import se.sics.cobweb.transfer.mngr.event.TransferMngrE;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayId;

/**
 *
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class TransferMngrPortHelper {

  public static List<Class> request() {
    List<Class> events = new LinkedList<>();
    events.add(TransferMngrE.SetupReq.class);
    events.add(TransferMngrE.StartReq.class);
    events.add(TransferMngrE.StopReq.class);
    return events;
  }

  public static List<Class> indication() {
    List<Class> events = new LinkedList<>();
    events.add(TransferMngrE.SetupSuccess.class);
    events.add(TransferMngrE.StartSuccess.class);
    events.add(TransferMngrE.Failed.class);
    events.add(TransferMngrE.StopSuccess.class);
    return events;
  }
  
  public static Triplet setupTorrent(OverlayId torrentId, Optional torrent) {
    TransferMngrE.SetupReq msg = new TransferMngrE.SetupReq(torrentId, torrent);
    Triplet result = Triplet.with(msg, true, TransferMngrPort.class);
    return result;
  }
  
  public static Triplet startTorrent(OverlayId torrentId) {
    TransferMngrE.StartReq msg = new TransferMngrE.StartReq(torrentId);
    Triplet result = Triplet.with(msg, true, TransferMngrPort.class);
    return result;
  }
}
