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

import com.google.common.base.Optional;
import se.sics.cobweb.transfer.mngr.TransferMngrFSMEvent;
import se.sics.cobweb.util.TorrentEvent;
import se.sics.ktoolbox.util.Either;
import se.sics.ktoolbox.util.identifiable.BasicIdentifiers;
import se.sics.ktoolbox.util.identifiable.Identifier;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayId;
import se.sics.nstream.transfer.MyTorrent;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class TransferMngrE {
  public static class SetupReq extends TorrentEvent.Request<SetupSuccess> implements TransferMngrFSMEvent {
    public final Optional<MyTorrent.Manifest> torrent;
    public SetupReq(OverlayId torrentId, Optional<MyTorrent.Manifest> torrent) {
      super(BasicIdentifiers.eventId(), torrentId);
      this.torrent = torrent;
    }

    @Override
    public String toString() {
      return "SetupReq<" + torrentId + "," + eventId + ">";
    }

    public SetupSuccess success() {
      return new SetupSuccess(this);
    }

    @Override
    public Identifier getTransferMngrFSMId() {
      return torrentId.baseId;
    }
  }

  public static class SetupSuccess extends TorrentEvent.Response implements TransferMngrFSMEvent {

    protected SetupSuccess(SetupReq req) {
      super(req);
    }

    @Override
    public String toString() {
      return "SetupSuccess<" + torrentId + "," + eventId + ">";
    }

    @Override
    public Identifier getTransferMngrFSMId() {
      return torrentId.baseId;
    }
  }

  public static class StartReq extends TorrentEvent.Request<Indication> implements TransferMngrFSMEvent {

    public StartReq(OverlayId torrentId) {
      super(BasicIdentifiers.eventId(), torrentId);
    }

    public StartSuccess success() {
      return new StartSuccess(this);
    }

    public Failed failed(Optional runningFault, Optional cleaningFault) {
      return new Failed(this, runningFault, cleaningFault);
    }

    @Override
    public String toString() {
      return "StartReq<" + torrentId + "," + eventId + ">";
    }

    @Override
    public Identifier getTransferMngrFSMId() {
      return torrentId.baseId;
    }
  }

  public static class StopReq extends TorrentEvent.Request<StopSuccess> implements TransferMngrFSMEvent {

    public StopReq(OverlayId torrentId) {
      super(BasicIdentifiers.eventId(), torrentId);
    }

    @Override
    public String toString() {
      return "StopReq<" + torrentId + "," + eventId + ">";
    }

    public StopSuccess success() {
      return new StopSuccess(this);
    }
    
    public Failed failed(Optional runningFault, Optional cleaningFault) {
      return new Failed(this, runningFault, cleaningFault);
    }

    @Override
    public Identifier getTransferMngrFSMId() {
      return torrentId.baseId;
    }
  }

  public static abstract class Indication extends TorrentEvent.Response implements TransferMngrFSMEvent {

    protected Indication(TorrentEvent.Request req) {
      super(req);
    }

    @Override
    public Identifier getTransferMngrFSMId() {
      return torrentId.baseId;
    }
  }

  public static final class StartSuccess extends Indication {

    public StartSuccess(StartReq req) {
      super(req);
    }

    @Override
    public String toString() {
      return "StartSuccess<" + torrentId + "," + eventId + ">";
    }
  }

  public static class StopSuccess extends Indication {

    public StopSuccess(StopReq req) {
      super(req);
    }

    @Override
    public String toString() {
      return "StopSuccess<" + torrentId + "," + eventId + ">";
    }

    @Override
    public Identifier getTransferMngrFSMId() {
      return torrentId.baseId;
    }
  }
  
  public static final class Failed extends Indication {

    public final Optional<Either<Throwable, String>> runningFault;
    public final Optional<Either<Throwable, String>> cleaningFault;

    public Failed(TorrentEvent.Request req, Optional runningFault, Optional cleaningFault) {
      super(req);
      this.runningFault = runningFault;
      this.cleaningFault = cleaningFault;
    }

    @Override
    public String toString() {
      return "Failed<" + torrentId + "," + eventId + ">";
    }
  }
}
