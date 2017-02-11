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
package se.sics.nstream.storage.durable.events;

import se.sics.kompics.Direct;
import se.sics.ktoolbox.nutil.fsm.FSMEvent;
import se.sics.ktoolbox.util.identifiable.BasicIdentifiers;
import se.sics.ktoolbox.util.identifiable.Identifiable;
import se.sics.ktoolbox.util.identifiable.Identifier;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayId;

/**
 *
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class DEndpointDisconnect {

  public static class Request extends Direct.Request<Success> implements Identifiable, FSMEvent {

    public final Identifier eventId;
    public final OverlayId torrentId;
    public final Identifier endpointId;

    public Request(OverlayId torrentId, Identifier endpointId) {
      this.eventId = BasicIdentifiers.eventId();
      this.torrentId = torrentId;
      this.endpointId = endpointId;
    }

    @Override
    public Identifier getId() {
      return eventId;
    }

    public Success success() {
      return new Success(this);
    }

    @Override
    public Identifier getBaseId() {
      return torrentId.baseId;
    }
  }

  public static class Success implements Direct.Response, Identifiable, FSMEvent {

    public final Request req;

    private Success(Request req) {
      this.req = req;
    }

    @Override
    public Identifier getId() {
      return req.getId();
    }

    @Override
    public Identifier getBaseId() {
      return req.getBaseId();
    }
  }
}
