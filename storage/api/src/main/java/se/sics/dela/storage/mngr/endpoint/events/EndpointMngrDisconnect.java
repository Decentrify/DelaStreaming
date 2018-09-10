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

import se.sics.kompics.Direct;
import se.sics.kompics.util.Identifier;
import se.sics.ktoolbox.util.identifiable.BasicIdentifiers;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class EndpointMngrDisconnect {
  public static class Request extends Direct.Request<Success> implements EndpointMngrEvent {

    public final Identifier eventId;
    public final Identifier clientId;
    public final Identifier endpointId;

    public Request(Identifier clientId, Identifier endpointId) {
      this.eventId = BasicIdentifiers.eventId();
      this.clientId = clientId;
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
    public Identifier getEndpointId() {
      return endpointId;
    }

    @Override
    public Identifier getClientId() {
      return clientId;
    }
  }

  public static class Success implements Direct.Response, EndpointMngrEvent {

    public final Request req;

    private Success(Request req) {
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
      return req.getClientId();
    }
  }
}
