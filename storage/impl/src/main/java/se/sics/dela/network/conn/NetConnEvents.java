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
package se.sics.dela.network.conn;

import se.sics.dela.network.util.ChannelId;
import se.sics.kompics.Direct;
import se.sics.kompics.util.Identifiable;
import se.sics.kompics.util.Identifier;
import se.sics.ktoolbox.nutil.network.portsv2.SelectableEventV2;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class NetConnEvents {

  public static final String EVENT_TYPE = "NET_CONN_EVENTS";

  public static interface Base extends Identifiable, SelectableEventV2 {

    public Identifier partnerId();

    public Identifier channelId();

    public Identifier dataId();
  }

  public static abstract class Req extends Direct.Request<Resp> implements Base {

    public final Identifier eventId;
    public final ChannelId channelId;

    public Req(Identifier eventId, ChannelId channelId) {
      this.eventId = eventId;
      this.channelId = channelId;
    }

    @Override
    public Identifier channelId() {
      return channelId;
    }

    @Override
    public Identifier dataId() {
      return channelId.dataId();
    }

    @Override
    public Identifier getId() {
      return eventId;
    }

    @Override
    public String eventType() {
      return EVENT_TYPE;
    }
  }

  public static abstract class Resp implements Direct.Response, Base {

    public final Req req;

    public Resp(Req req) {
      this.req = req;
    }

    @Override
    public Identifier partnerId() {
      return req.partnerId();
    }

    @Override
    public Identifier channelId() {
      return req.channelId();
    }

    @Override
    public Identifier dataId() {
      return req.dataId();
    }

    @Override
    public Identifier getId() {
      return req.getId();
    }

    @Override
    public String eventType() {
      return EVENT_TYPE;
    }
  }

  public static class LedbatSenderCreate extends Req {

    public LedbatSenderCreate(Identifier eventId, ChannelId channelId) {
      super(eventId, channelId);
    }

    public LedbatSenderCreateAck ack() {
      return new LedbatSenderCreateAck(this);
    }

    @Override
    public String toString() {
      return "LedbatSenderCreate{}";
    }

    @Override
    public Identifier partnerId() {
      return channelId.receiverId();
    }
  }

  public static class LedbatSenderCreateAck extends Resp {

    public LedbatSenderCreateAck(LedbatSenderCreate req) {
      super(req);
    }

    @Override
    public String toString() {
      return "LedbatSenderCreateAck{}";
    }
  }

  public static class LedbatSenderKill extends Req {

    public LedbatSenderKill(Identifier eventId, ChannelId channelId) {
      super(eventId, channelId);
    }

    @Override
    public String toString() {
      return "LedbatSenderKill{}";
    }

    public LedbatSenderKillAck ack() {
      return new LedbatSenderKillAck(this);
    }

    @Override
    public Identifier partnerId() {
      return channelId.receiverId();
    }
  }

  public static class LedbatSenderKillAck extends Resp {

    public LedbatSenderKillAck(LedbatSenderKill req) {
      super(req);
    }

    @Override
    public String toString() {
      return "LedbatSenderKillAck{}";
    }
  }

  public static class LedbatReceiverCreate extends Req {

    public LedbatReceiverCreate(Identifier eventId, ChannelId channelId) {
      super(eventId, channelId);
    }

    public LedbatReceiverCreateAck ack() {
      return new LedbatReceiverCreateAck(this);
    }

    @Override
    public String toString() {
      return "LedbatReceiverCreate{}";
    }

    @Override
    public Identifier partnerId() {
      return channelId.senderId();
    }
  }

  public static class LedbatReceiverCreateAck extends Resp {

    public LedbatReceiverCreateAck(LedbatReceiverCreate req) {
      super(req);
    }

    @Override
    public String toString() {
      return "LedbatReceiverCreateAck{}";
    }
  }

  public static class LedbatReceiverKill extends Req {

    public LedbatReceiverKill(Identifier eventId, ChannelId channelId) {
      super(eventId, channelId);
    }

    @Override
    public String toString() {
      return "LedbatReceiverKill{}";
    }

    public LedbatReceiverKillAck ack() {
      return new LedbatReceiverKillAck(this);
    }

    @Override
    public Identifier partnerId() {
      return channelId.senderId();
    }
  }

  public static class LedbatReceiverKillAck extends Resp {

    public LedbatReceiverKillAck(LedbatReceiverKill req) {
      super(req);
    }

    @Override
    public String toString() {
      return "LedbatReceiverKill{}";
    }
  }
}
