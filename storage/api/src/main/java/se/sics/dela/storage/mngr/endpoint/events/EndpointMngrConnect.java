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
package se.sics.dela.storage.mngr.endpoint.events;

import se.sics.dela.storage.mngr.StorageProvider;
import se.sics.kompics.Direct;
import se.sics.kompics.Promise;
import se.sics.kompics.util.Identifier;
import se.sics.ktoolbox.util.result.Result;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class EndpointMngrConnect {
  public static class Request extends Promise<Indication> implements EndpointMngrEvent {

    public final Identifier eventId;
    public final Identifier clientId;
    public final Identifier endpointId;
    public final StorageProvider endpointProvider;

    public Request(Identifier eventId, Identifier clientId, Identifier endpointId, StorageProvider endpointProvider) {
      this.eventId = eventId;
      this.clientId = clientId;
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
      return new Failure(this, r.getException());
    }

    @Override
    public Identifier getEndpointId() {
      return endpointId;
    }

    @Override
    public Identifier getClientId() {
      return clientId;
    }
  }

  public static abstract class Indication implements Direct.Response, EndpointMngrEvent {

    public final Request req;

    private Indication(Request req) {
      this.req = req;
    }

    @Override
    public Identifier getId() {
      return req.getId();
    }

    @Override
    public Identifier getEndpointId() {
      return req.getEndpointId();
    }
    
    @Override
    public Identifier getClientId() {
      return req.clientId;
    }
  }

  public static class Success extends Indication {

    public Success(Request req) {
      super(req);
    }
  }

  public static class Failure extends Indication {

    public final Throwable cause;

    public Failure(Request req, Throwable cause) {
      super(req);
      this.cause = cause;
    }
  }
}
