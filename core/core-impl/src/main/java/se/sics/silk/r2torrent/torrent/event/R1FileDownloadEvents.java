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
package se.sics.silk.r2torrent.torrent.event;

import se.sics.kompics.util.Identifier;
import se.sics.ktoolbox.util.identifiable.BasicIdentifiers;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayId;
import se.sics.ktoolbox.util.network.KAddress;
import se.sics.silk.event.SilkEvent;
import se.sics.silk.r2torrent.torrent.R1FileDownload;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class R1FileDownloadEvents {

  public static class Start extends SilkEvent.E4 implements R1FileDownload.CtrlEvent {

    public Start(OverlayId torrentId, Identifier fileId) {
      super(BasicIdentifiers.eventId(), torrentId, fileId);
    }
  }

  public static class Close extends SilkEvent.E4 implements R1FileDownload.CtrlEvent {
    
    public Close(OverlayId torrentId, Identifier fileId) {
      super(BasicIdentifiers.eventId(), torrentId, fileId);
    }
  }
  
  public static class Pause extends SilkEvent.E4 implements R1FileDownload.CtrlEvent {
    
    public Pause(OverlayId torrentId, Identifier fileId) {
      super(BasicIdentifiers.eventId(), torrentId, fileId);
    }
  }
  
  public static class Resume extends SilkEvent.E4 implements R1FileDownload.CtrlEvent {
    
    public Resume(OverlayId torrentId, Identifier fileId) {
      super(BasicIdentifiers.eventId(), torrentId, fileId);
    }
  }
  
  public static class Indication extends SilkEvent.E4 {
    public final R1FileDownload.States state;
    public Indication(OverlayId torrentId, Identifier fileId, R1FileDownload.States state) {
      super(BasicIdentifiers.eventId(), torrentId, fileId);
      this.state = state;
    }
  }
  
  public static class Connect extends SilkEvent.E4 implements R1FileDownload.CtrlEvent {
    public final KAddress seeder;
    public Connect(OverlayId torrentId, Identifier fileId, KAddress seeder) {
      super(BasicIdentifiers.eventId(), torrentId, fileId);
      this.seeder = seeder;
    }
  }
  
  public static class Disconnect extends SilkEvent.E4 implements R1FileDownload.CtrlEvent {
    public final Identifier seederId;
    public Disconnect(OverlayId torrentId, Identifier fileId, Identifier seederId) {
      super(BasicIdentifiers.eventId(), torrentId, fileId);
      this.seederId = seederId;
    }
  }
  
  public static class Disconnected extends SilkEvent.E4 implements R1FileDownload.CtrlEvent {
    public final Identifier seederId;
    public Disconnected(OverlayId torrentId, Identifier fileId, Identifier seederId) {
      super(BasicIdentifiers.eventId(), torrentId, fileId);
      this.seederId = seederId;
    }
  }
}
