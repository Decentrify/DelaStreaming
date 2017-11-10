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
package se.sics.nstream.torrent.event;

import java.util.List;
import se.sics.kompics.Direct;
import se.sics.kompics.Promise;
import se.sics.kompics.util.Identifier;
import se.sics.ktoolbox.util.identifiable.BasicIdentifiers;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayId;
import se.sics.ktoolbox.util.network.KAddress;
import se.sics.ktoolbox.util.overlays.OverlayEvent;
import se.sics.ktoolbox.util.result.Result;
import se.sics.nstream.library.restart.LibTFSMEvent;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class StartTorrent {

  public static class Request extends Promise<Response> implements OverlayEvent, LibTFSMEvent {

    public final Identifier eventId;
    public final OverlayId torrentId;
    public final List<KAddress> partners;

    public Request(OverlayId torrentId, List<KAddress> partners) {
      this.eventId = BasicIdentifiers.eventId();
      this.torrentId = torrentId;
      this.partners = partners;
    }

    @Override
    public Identifier getId() {
      return eventId;
    }

    @Override
    public OverlayId overlayId() {
      return torrentId;
    }

    @Override
    public Response success(Result r) {
      return new Response(this, r);
    }

    @Override
    public Identifier getLibTFSMId() {
      return torrentId.baseId;
    }

    @Override
    public Response fail(Result r) {
      throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
  }

  public static class Response implements Direct.Response, OverlayEvent, LibTFSMEvent {

    public final Identifier eventId;
    public final OverlayId torrentId;
    public final Result<Boolean> result;

    private Response(Request req, Result<Boolean> result) {
      this.eventId = req.eventId;
      this.torrentId = req.torrentId;
      this.result = result;
    }

    @Override
    public Identifier getId() {
      return eventId;
    }

    @Override
    public OverlayId overlayId() {
      return torrentId;
    }

    @Override
    public Identifier getLibTFSMId() {
      return torrentId.baseId;
    }
  }
}
