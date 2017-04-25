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
import se.sics.kompics.Promise;
import se.sics.ktoolbox.nutil.fsm.api.FSMEvent;
import se.sics.ktoolbox.util.identifiable.BasicIdentifiers;
import se.sics.ktoolbox.util.identifiable.Identifiable;
import se.sics.ktoolbox.util.identifiable.Identifier;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayId;
import se.sics.ktoolbox.util.result.Result;
import se.sics.nstream.storage.durable.DurableStorageProvider;

/**
 *
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class DEndpoint {

  public static class Connect extends Promise<Indication> implements Identifiable, FSMEvent {

    public final Identifier eventId;
    public final OverlayId torrentId;
    public final Identifier endpointId;
    public final DurableStorageProvider endpointProvider;

    public Connect(OverlayId torrentId, Identifier endpointId, DurableStorageProvider endpointProvider) {
      this.eventId = BasicIdentifiers.eventId();
      this.torrentId = torrentId;
      this.endpointId = endpointId;
      this.endpointProvider = endpointProvider;
    }

    @Override
    public Identifier getId() {
      return eventId;
    }

    @Override
    public Indication success(Result r) {
      return new Success(this);
    }
    
    @Override
    public Indication fail(Result r) {
      return new Failed(this, r);
    }

    @Override
    public Identifier getFSMBaseId() {
      return torrentId.baseId;
    }
  }

  public static abstract class Indication implements Direct.Response, Identifiable, FSMEvent {

    public final Connect req;

    private Indication(Connect req) {
      this.req = req;
    }

    @Override
    public Identifier getId() {
      return req.getId();
    }

    @Override
    public Identifier getFSMBaseId() {
      return req.getFSMBaseId();
    }
  }
  
  public static class Success extends Indication {

    public Success(Connect req) {
      super(req);
    }
  }
  
  public static class Failed extends Indication {
    public final Throwable cause;
    public Failed(Connect req, Result r) {
      super(req);
      this.cause = r.getException();
    }
  }
  
  public static class Disconnect extends Direct.Request<Disconnected> implements Identifiable, FSMEvent {

    public final Identifier eventId;
    public final OverlayId torrentId;
    public final Identifier endpointId;

    public Disconnect(OverlayId torrentId, Identifier endpointId) {
      this.eventId = BasicIdentifiers.eventId();
      this.torrentId = torrentId;
      this.endpointId = endpointId;
    }

    @Override
    public Identifier getId() {
      return eventId;
    }

    public Disconnected success() {
      return new Disconnected(this);
    }

    @Override
    public Identifier getFSMBaseId() {
      return torrentId.baseId;
    }
  }

  public static class Disconnected implements Direct.Response, Identifiable, FSMEvent {

    public final Disconnect req;

    private Disconnected(Disconnect req) {
      this.req = req;
    }

    @Override
    public Identifier getId() {
      return req.getId();
    }

    @Override
    public Identifier getFSMBaseId() {
      return req.getFSMBaseId();
    }
  }
}
