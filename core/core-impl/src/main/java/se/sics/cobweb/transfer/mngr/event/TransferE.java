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
package se.sics.cobweb.transfer.mngr.event;

import se.sics.cobweb.util.FileId;
import se.sics.cobweb.util.TorrentEvent;
import se.sics.ktoolbox.util.identifiable.BasicIdentifiers;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayId;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class TransferE {
    public static class SeederStarted extends TorrentEvent.Base {
    public final FileId fileId;
    
    public SeederStarted(OverlayId torrentId, FileId fileId) {
      super(BasicIdentifiers.eventId(), torrentId);
      this.fileId = fileId;
    }
  }
    
  public static class LeecherStarted extends TorrentEvent.Base {
    public final FileId fileId;
    
    public LeecherStarted(OverlayId torrentId, FileId fileId) {
      super(BasicIdentifiers.eventId(), torrentId);
      this.fileId = fileId;
    }
  }
  
  public static class LeecherCompleted extends TorrentEvent.Base {
    public final FileId fileId;
    
    public LeecherCompleted(OverlayId torrentId, FileId fileId) {
      super(BasicIdentifiers.eventId(), torrentId);
      this.fileId = fileId;
    }
  }
}
