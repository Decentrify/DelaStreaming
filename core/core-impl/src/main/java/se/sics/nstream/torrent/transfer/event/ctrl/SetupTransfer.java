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
package se.sics.nstream.torrent.transfer.event.ctrl;

import se.sics.kompics.Direct;
import se.sics.ktoolbox.nutil.fsm.FSMEvent;
import se.sics.ktoolbox.util.identifiable.BasicIdentifiers;
import se.sics.ktoolbox.util.identifiable.Identifier;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayId;
import se.sics.ktoolbox.util.overlays.OverlayEvent;
import se.sics.ktoolbox.util.result.Result;
import se.sics.nstream.transfer.MyTorrent;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class SetupTransfer {

  public static class Request extends Direct.Request<Response> implements OverlayEvent, FSMEvent {

    public final Identifier eventId;
    public final OverlayId torrentId;
    public final MyTorrent torrent;

    public Request(OverlayId torrentId, MyTorrent torrent) {
      eventId = BasicIdentifiers.eventId();
      this.torrentId = torrentId;
      this.torrent = torrent;
    }

    @Override
    public Identifier getId() {
      return eventId;
    }

    @Override
    public OverlayId overlayId() {
      return torrentId;
    }

    public Response complete(Result<Boolean> result) {
      return new Response(this, result);
    }

    @Override
    public Identifier getBaseId() {
      return torrentId.baseId;
    }
  }

  public static class Response implements Direct.Response, OverlayEvent, FSMEvent {

    public final Request req;
    public final Result<Boolean> result;

    private Response(Request req, Result<Boolean> result) {
      this.req = req;
      this.result = result;
    }

    @Override
    public Identifier getId() {
      return req.getId();
    }

    @Override
    public OverlayId overlayId() {
      return req.overlayId();
    }
    
    @Override
    public Identifier getBaseId() {
      return req.getBaseId();
    }
  }
}
