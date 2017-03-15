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
package se.sics.nstream.hops.library.util;

import com.google.gson.JsonElement;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class MyStreamJSON {

  public static class Read {

    private String type;
    private JsonElement endpoint;
    private JsonElement resource;

    public Read() {
    }

    public String getType() {
      return type;
    }

    public void setType(String type) {
      this.type = type;
    }

    public JsonElement getEndpoint() {
      return endpoint;
    }

    public void setEndpoint(JsonElement endpoint) {
      this.endpoint = endpoint;
    }

    public JsonElement getResource() {
      return resource;
    }

    public void setResource(JsonElement resource) {
      this.resource = resource;
    }
  }
  
  public static class Write {
    private String type;
    private EndpointJSON endpoint;
    private ResourceJSON resource;

    public Write() {
    }

    public Write(String type, EndpointJSON endpoint, ResourceJSON resource) {
      this.type = type;
      this.endpoint = endpoint;
      this.resource = resource;
    }

    public String getType() {
      return type;
    }

    public void setType(String type) {
      this.type = type;
    }

    public EndpointJSON getEndpoint() {
      return endpoint;
    }

    public void setEndpoint(EndpointJSON endpoint) {
      this.endpoint = endpoint;
    }

    public ResourceJSON getResource() {
      return resource;
    }

    public void setResource(ResourceJSON resource) {
      this.resource = resource;
    }
  }
}
