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
package se.sics.silk.r2torrent.torrent.msg;

import se.sics.kompics.util.Identifier;
import se.sics.ktoolbox.util.identifiable.BasicIdentifiers;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayId;
import se.sics.silk.event.SilkEvent;
import se.sics.silk.r2torrent.torrent.R1MetadataGet;
import se.sics.silk.r2torrent.torrent.R1MetadataServe;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class R1MetadataMsgs {
  public static class Get extends SilkEvent.E4 implements R1MetadataServe.Msg {
    Get(Identifier msgId, OverlayId torrentId, Identifier fileId) {
      super(msgId, torrentId, fileId);
    }
    
    public Get(OverlayId torrentId, Identifier fileId) {
      this(BasicIdentifiers.msgId(), torrentId, fileId);
    }
    
    public Serve answer() {
      return new Serve(this);
    }
  }
  
  public static class Serve extends SilkEvent.E4 implements R1MetadataGet.Msg {
    Serve(Identifier msgId, OverlayId torrentId, Identifier fileId) {
      super(msgId, torrentId, fileId);
    }
    
    public Serve(Get req) {
      super(req.eventId, req.torrentId, req.fileId);
    }
  }
}
