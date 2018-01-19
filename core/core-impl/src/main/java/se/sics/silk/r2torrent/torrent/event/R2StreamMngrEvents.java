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
import se.sics.nstream.StreamId;
import se.sics.nstream.storage.durable.util.MyStream;
import se.sics.silk.event.SilkEvent;
import se.sics.silk.r2torrent.torrent.R1StreamCtrlEvent;
import se.sics.silk.r2torrent.torrent.R2StreamMngr;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class R2StreamMngrEvents {

  public static class Open extends SilkEvent.E4 implements R2StreamMngr.R1StreamEvent {

    public final StreamId streamId;
    public final MyStream stream;

    public Open(OverlayId torrentId, Identifier fileId, StreamId streamId, MyStream stream) {
      super(BasicIdentifiers.eventId(), torrentId, fileId);
      this.streamId = streamId;
      this.stream = stream;
    }

    public OpenSucc success() {
      return new OpenSucc(this);
    }
  }

  public static class OpenSucc extends SilkEvent.E4 implements R1StreamCtrlEvent {

    public OpenSucc(Open req) {
      super(req.eventId, req.torrentId, req.fileId);
    }
  }

  public static class Close extends SilkEvent.E4 implements R2StreamMngr.R1StreamEvent {
    public final StreamId streamId;
    
    public Close(OverlayId torrentId, Identifier fileId, StreamId streamId) {
      super(BasicIdentifiers.eventId(), torrentId, fileId);
      this.streamId = streamId;
    }

    public CloseAck ack() {
      return new CloseAck(this);
    }
  }

  public static class CloseAck extends SilkEvent.E4 implements R1StreamCtrlEvent {

    public CloseAck(Close req) {
      super(req.eventId, req.torrentId, req.fileId);
    }
  }
}
