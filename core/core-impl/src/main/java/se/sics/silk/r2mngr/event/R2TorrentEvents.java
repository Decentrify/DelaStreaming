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
package se.sics.silk.r2mngr.event;

import se.sics.kompics.util.Identifiable;
import se.sics.kompics.util.Identifier;
import se.sics.ktoolbox.util.identifiable.BasicIdentifiers;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayId;
import se.sics.silk.r2mngr.R2Torrent;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class R2TorrentEvents {

  public static abstract class Base implements R2Torrent.Event, Identifiable {

    public final Identifier eventId;
    public final OverlayId torrentId;

    Base(Identifier eventId, OverlayId torrentId) {
      this.eventId = eventId;
      this.torrentId = torrentId;
    }

    @Override
    public Identifier getId() {
      return eventId;
    }

    @Override
    public Identifier getR2TorrentFSMId() {
      return torrentId.baseId;
    }
  }

  public static class GetMeta extends Base {

    public GetMeta(OverlayId torrentId) {
      super(BasicIdentifiers.eventId(), torrentId);
    }
  }

  public static class ServeMeta extends Base {

    public ServeMeta(OverlayId torrentId) {
      super(BasicIdentifiers.eventId(), torrentId);
    }
  }

  public static class Hashing extends Base {

    public Hashing(OverlayId torrentId) {
      super(BasicIdentifiers.eventId(), torrentId);
    }
  }

  public static class Download extends Base {

    public Download(OverlayId torrentId) {
      super(BasicIdentifiers.eventId(), torrentId);
    }
  }

  public static class DownloadSlotReq extends Base {
    public final int fileNr;
    public final int reqBlockSlots;

    public DownloadSlotReq(OverlayId torrentId, int fileNr, int reqBlockSlots) {
      super(BasicIdentifiers.eventId(), torrentId);
      this.fileNr = fileNr;
      this.reqBlockSlots = reqBlockSlots;
    }
    
    public DownloadSlotResp answer(int accBlockSlots) {
      return new DownloadSlotResp(this, fileNr, accBlockSlots);
    }
  }

  public static class DownloadSlotResp extends Base {
    public final int fileNr;
    public final int accBlockSlots;
    
    DownloadSlotResp(DownloadSlotReq req, int fileNr, int accBlockSlots) {
      super(req.eventId, req.torrentId);
      this.fileNr = fileNr;
      this.accBlockSlots = accBlockSlots;
    }
  }

  public static class Upload extends Base {

    public Upload(OverlayId torrentId) {
      super(BasicIdentifiers.eventId(), torrentId);
    }
  }
  
  public static class UploadSlotReq extends Base {
    public final int fileNr;
    public final int reqBlockSlots;

    public UploadSlotReq(OverlayId torrentId, int fileNr, int reqBlockSlots) {
      super(BasicIdentifiers.eventId(), torrentId);
      this.fileNr = fileNr;
      this.reqBlockSlots = reqBlockSlots;
    }
    
    public UploadSlotResp answer(int accBlockSlots) {
      return new UploadSlotResp(this, fileNr, accBlockSlots);
    }
  }

  public static class UploadSlotResp extends Base {
    public final int fileNr;
    public final int accBlockSlots;
    
    UploadSlotResp(UploadSlotReq req, int fileNr, int accBlockSlots) {
      super(req.eventId, req.torrentId);
      this.fileNr = fileNr;
      this.accBlockSlots = accBlockSlots;
    }
  }
  
  public static class Stop extends Base {
    public Stop(OverlayId torrentId) {
      super(BasicIdentifiers.eventId(), torrentId);
    }
  }
}
