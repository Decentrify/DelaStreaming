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
package se.sics.cobweb.transfer.handle.event;

import se.sics.cobweb.overlord.handle.SeederHandleOverlordFSMEvent;
import se.sics.cobweb.transfer.handle.util.SeederActivityReport;
import se.sics.cobweb.util.HandleEvent;
import se.sics.cobweb.util.HandleId;
import se.sics.ktoolbox.util.identifiable.BasicIdentifiers;
import se.sics.ktoolbox.util.identifiable.Identifier;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayId;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class SeederHandleCtrlE {
  public static class Ready extends HandleEvent.Base implements SeederHandleOverlordFSMEvent {

    public Ready(OverlayId torrentId, HandleId handleId) {
      super(BasicIdentifiers.eventId(), torrentId, handleId);
    }

    @Override
    public HandleEvent.Base withHandleId(HandleId handleId) {
      throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Identifier getSHOId() {
      return handleId;
    }
  }
  
  public static class Shutdown extends HandleEvent.Request<ShutdownAck> {
    public Shutdown(OverlayId torrentId, HandleId handleId) {
      super(BasicIdentifiers.eventId(), torrentId, handleId);
    }
    
    public ShutdownAck success() {
      return new ShutdownAck(this);
    }
  }
  
  public static class ShutdownAck extends HandleEvent.Response {

    public ShutdownAck(Shutdown req) {
      super(req);
    }
  }
  
  public static class Report extends HandleEvent.Base {
    public final SeederActivityReport report;

    public Report(OverlayId torrentId, HandleId handleId, SeederActivityReport report) {
      super(BasicIdentifiers.eventId(), torrentId, handleId);
      this.report = report;
    }

    @Override
    public HandleEvent.Base withHandleId(HandleId handleId) {
      throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
  }
}
