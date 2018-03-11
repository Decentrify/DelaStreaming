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
package se.sics.nstream.torrent.tracking.tracker;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.List;
import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLSession;
import javax.ws.rs.ProcessingException;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

/**
 *
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class ClientWrapper<T extends Object> {
  private Client client;
  private final Class<T> respContentClass;
  private String target;
  private String path;
  private Entity payload;
  private String mediaType = MediaType.APPLICATION_JSON;
  
  private ClientWrapper(Client client, Class<T> respContentClass) {
    this.client = client;
    this.respContentClass = respContentClass;
    this.payload = Entity.entity("", mediaType);
  }
  
  public ClientWrapper setTarget(String target) {
    this.target = target;
    return this;
  }

  public ClientWrapper setPath(String path) {
    this.path = path;
    return this;
  }

  public ClientWrapper setMediaType(String mediaType) {
    this.mediaType = mediaType;
    return this;
  }

  public ClientWrapper setPayload(Object jsonObject) {
    this.payload = Entity.entity(jsonObject, mediaType);
    return this;
  }

  public T doGet() {
    performSanityCheck();
    try {
      WebTarget webTarget = client.target(target).path(path);
      Response response = webTarget.request(mediaType).get();
      return getResponse(response);
    } catch (ProcessingException ex) {
      throw new IllegalStateException(ex.getMessage());
    } finally {
      if (client != null) {
        client.close();
        client = null;
      }
    }
  }

  public List<T> doGetGenericType() {
    performSanityCheck();
    try {
      WebTarget webTarget = client.target(target).path(path);
      Response response = webTarget.request(mediaType).get();
      ParameterizedType parameterizedGenericType = new ParameterizedType() {
        @Override
        public Type[] getActualTypeArguments() {
          return new Type[]{respContentClass};
        }

        @Override
        public Type getRawType() {
          return List.class;
        }

        @Override
        public Type getOwnerType() {
          return List.class;
        }
      };
      GenericType<List<T>> type = new GenericType<List<T>>(parameterizedGenericType) {
      };
      Response.Status.Family status = response.getStatusInfo().getFamily();
      try {
        if (status == Response.Status.Family.INFORMATIONAL || status == Response.Status.Family.SUCCESSFUL) {
          List<T> content = response.readEntity(type);
          return content;
        } else {
          JsonResponse jsonRes = response.readEntity(JsonResponse.class);
          throw new IllegalStateException(jsonRes.getErrorMsg());
        }
      } catch (ProcessingException e) {
        throw new IllegalStateException(e.getMessage());
      }
    } finally {
      if (client != null) {
        client.close();
        client = null;
      }
    }
  }

  public T doPost() {
    performSanityCheck();
    try {
      WebTarget webTarget = client.target(target).path(path);
      Response response = webTarget.request(mediaType).post(payload);
      return getResponse(response);
    } catch (ProcessingException ex) {
      throw new IllegalStateException(ex.getMessage());
    } finally {
      if (client != null) {
        client.close();
        client = null;
      }
    }
  }

  public T doPut() {
    performSanityCheck();
    try {
      WebTarget webTarget = client.target(target).path(path);
      Response response = webTarget.request(mediaType).put(payload);
      return getResponse(response);
    } catch (ProcessingException ex) {
      throw new IllegalStateException(ex.getMessage());
    } finally {
      if (client != null) {
        client.close();
        client = null;
      }
    }
  }

  public T doDelete() {
    performSanityCheck();
    try {
      WebTarget webTarget = client.target(target).path(path);
      Response response = webTarget.request(mediaType).delete();
      return getResponse(response);
    } catch (ProcessingException ex) {
      throw new IllegalStateException(ex.getMessage());
    } finally {
      if (client != null) {
        client.close();
        client = null;
      }
    }
  }

    private T getResponse(Response response) {
    Response.Status.Family statusFamily = response.getStatusInfo().getFamily();
    if (response.getMediaType().getSubtype().equals(MediaType.APPLICATION_JSON_TYPE.getSubtype())) {
      try {
        if (statusFamily == Response.Status.Family.INFORMATIONAL || statusFamily == Response.Status.Family.SUCCESSFUL) {
          T content = response.readEntity(respContentClass);
          return content;
        } else {
          JsonResponse jsonRes = response.readEntity(JsonResponse.class);
          throw new IllegalStateException(jsonRes.getErrorMsg());
        }
      } catch (ProcessingException e) {
        throw new IllegalStateException(e.getMessage());
      }
    } else {
      throw new IllegalStateException("Cannot Connect To Server.");
    }

  }

  private void performSanityCheck() {
    if (client == null) {
      throw new IllegalStateException("Client not created.");
    }

    if (target == null || target.isEmpty()) {
      throw new IllegalStateException("Target not set.");
    }

    if (path == null || path.isEmpty()) {
      throw new IllegalStateException("Path not set.");
    }
  }

  public String getFullPath() {
    return this.target + "/" + this.path;
  }
  public static <T> ClientWrapper<T> httpsInstance(Class<T> resultClass) {
    Client client = ClientBuilder.newBuilder().hostnameVerifier(InsecureHostnameVerifier.INSTANCE).build();
    return new ClientWrapper(client, resultClass);
  }
  
  public static <T> ClientWrapper httpInstance(Class<T> resultClass) {
    Client client = ClientBuilder.newClient();
    return new ClientWrapper(client, resultClass);
  }
  
  private static class InsecureHostnameVerifier implements HostnameVerifier {

    static InsecureHostnameVerifier INSTANCE = new InsecureHostnameVerifier();

    InsecureHostnameVerifier() {
    }

    @Override
    public boolean verify(String string, SSLSession ssls) {
      return true;
    }
  }
}
