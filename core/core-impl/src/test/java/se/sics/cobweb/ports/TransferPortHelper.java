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

import java.util.LinkedList;
import java.util.List;
import org.javatuples.Triplet;
import se.sics.cobweb.transfer.instance.TransferPort;
import se.sics.cobweb.transfer.mngr.event.TransferE;
import se.sics.cobweb.util.FileId;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayId;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class TransferPortHelper {

  public static List<Class> indication() {
    List<Class> events = new LinkedList<>();
    events.add(TransferE.SeederStarted.class);
    events.add(TransferE.LeecherStarted.class);
    events.add(TransferE.LeecherCompleted.class);
    return events;
  }

  public static List<Class> request() {
    List<Class> events = new LinkedList<>();
    return events;
  }

  public static Triplet leecherStartFile(OverlayId torrentId, FileId fileId) {
    TransferE.LeecherStarted msg = new TransferE.LeecherStarted(torrentId, fileId);
    Triplet result = Triplet.with(msg, false, TransferPort.class);
    return result;
  }
}
