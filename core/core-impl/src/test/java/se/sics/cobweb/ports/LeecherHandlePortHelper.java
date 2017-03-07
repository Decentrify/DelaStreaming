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
import se.sics.cobweb.transfer.handle.LeecherHandlePort;
import se.sics.cobweb.transfer.handle.event.LeecherHandleE;
import se.sics.cobweb.util.HandleId;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayId;
import se.sics.nstream.util.BlockDetails;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class LeecherHandlePortHelper {

  public static List<Class> request() {
    List<Class> events = new LinkedList<>();
    events.add(LeecherHandleE.SetupResp.class);
    events.add(LeecherHandleE.Download.class);
    events.add(LeecherHandleE.ShutdownAck.class);
    return events;
  }

  public static List<Class> indication() {
    List<Class> events = new LinkedList<>();
    events.add(LeecherHandleE.SetupReq.class);
    events.add(LeecherHandleE.Completed.class);
    events.add(LeecherHandleE.Shutdown.class);
    return events;
  }

  public static Function<LeecherHandleE.SetupReq, LeecherHandleE.SetupResp> setup(final BlockDetails defaultBlock,
    final boolean withHashes) {
    return new Function<LeecherHandleE.SetupReq, LeecherHandleE.SetupResp>() {
      @Override
      public LeecherHandleE.SetupResp apply(LeecherHandleE.SetupReq req) {
        return req.answer(defaultBlock, withHashes);
      }
    };
  }
  
  public static Triplet download(OverlayId torrentId, HandleId leecherHId) {
    LeecherHandleE.Download msg = new LeecherHandleE.Download(torrentId, leecherHId, null, null, 0);
    Triplet result = Triplet.with(msg, true, LeecherHandlePort.class);
    return result;
  }
}
