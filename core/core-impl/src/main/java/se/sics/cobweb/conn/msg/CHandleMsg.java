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
package se.sics.cobweb.conn.msg;

import se.sics.cobweb.conn.CLeecherHandleFSMEvent;
import se.sics.cobweb.conn.CSeederHandleFSMEvent;
import se.sics.cobweb.util.HandleEvent;
import se.sics.cobweb.util.HandleId;
import se.sics.ktoolbox.util.identifiable.BasicIdentifiers;
import se.sics.ktoolbox.util.identifiable.Identifier;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayId;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class CHandleMsg {
  public static class ConnectToSeeder extends HandleEvent.Base implements CSeederHandleFSMEvent {
    public ConnectToSeeder(OverlayId torrentId, HandleId handleId) {
      super(BasicIdentifiers.eventId(), torrentId, handleId);
    }

    @Override
    public ConnectToSeeder withHandleId(HandleId handleId) {
      return new ConnectToSeeder(torrentId, handleId);
    }

    @Override
    public Identifier cshFSMId() {
      return handleId;
    }
    
    @Override
    public String toString() {
      return "ConnectToSeeder" + handleId;
    }
    
    public Connected success() {
      return new Connected(this);
    }
  }
  
  public static class Connected extends HandleEvent.Base implements CLeecherHandleFSMEvent {
    protected Connected(OverlayId torrentId, HandleId handleId) {
      super(BasicIdentifiers.eventId(), torrentId, handleId);
    }
    
    private Connected(ConnectToSeeder req) {
      super(req.eventId, req.torrentId, req.handleId);
    }

    @Override
    public Connected withHandleId(HandleId handleId) {
      return new Connected(torrentId, handleId);
    }

    @Override
    public Identifier clhFSMId() {
      return handleId;
    }
    
    @Override
    public String toString() {
      return "Connected" + handleId;
    }
  }
}
