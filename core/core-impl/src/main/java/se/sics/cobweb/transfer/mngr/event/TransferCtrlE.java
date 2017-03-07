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

import se.sics.cobweb.transfer.instance.TransferFSMEvent;
import se.sics.cobweb.transfer.mngr.TransferMngrFSMEvent;
import se.sics.cobweb.util.TorrentEvent;
import se.sics.ktoolbox.util.identifiable.BasicIdentifiers;
import se.sics.ktoolbox.util.identifiable.Identifier;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayId;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class TransferCtrlE {
  
  public static class Start extends TorrentEvent.Base implements TransferMngrFSMEvent, TransferFSMEvent {

    public Start(OverlayId torrentId) {
      super(BasicIdentifiers.eventId(), torrentId);
    }

    @Override
    public String toString() {
      return "Start<" + torrentId + "," + eventId + ">";
    }
    
    @Override
    public Identifier getTransferMngrFSMId() {
      return torrentId.baseId;
    }

    @Override
    public Identifier getTransferFSMId() {
      return torrentId.baseId;
    }
  }
  
  public static class CleanReq extends TorrentEvent.Request<CleanResp> implements TransferMngrFSMEvent, TransferFSMEvent {

    public CleanReq(OverlayId torrentId) {
      super(BasicIdentifiers.eventId(), torrentId);
    }

    @Override
    public String toString() {
      return "CleanReq<" + torrentId + "," + eventId + ">";
    }

    public CleanResp complete() {
      return new CleanResp(this);
    }

    @Override
    public Identifier getTransferMngrFSMId() {
      return torrentId.baseId;
    }

    @Override
    public Identifier getTransferFSMId() {
      return torrentId.baseId;
    }
  }

  public static class CleanResp extends TorrentEvent.Response implements TransferMngrFSMEvent, TransferFSMEvent {

    public CleanResp(CleanReq req) {
      super(req);
    }

    @Override
    public String toString() {
      return "CleanResp<" + torrentId + "," + eventId + ">";
    }

    @Override
    public Identifier getTransferMngrFSMId() {
      return torrentId.baseId;
    }

    @Override
    public Identifier getTransferFSMId() {
      return torrentId.baseId;
    }
  }
}
