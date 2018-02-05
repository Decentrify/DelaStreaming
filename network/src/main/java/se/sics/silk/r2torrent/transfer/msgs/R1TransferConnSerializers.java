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
package se.sics.silk.r2torrent.transfer.msgs;

import com.google.common.base.Optional;
import io.netty.buffer.ByteBuf;
import org.javatuples.Triplet;
import se.sics.kompics.network.netty.serialization.Serializer;
import se.sics.kompics.network.netty.serialization.Serializers;
import se.sics.kompics.util.Identifier;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayId;
import se.sics.silk.event.SilkEvent;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class R1TransferConnSerializers {

  public static abstract class Base implements Serializer {

    private final int id;

    Base(int id) {
      this.id = id;
    }

    @Override
    public int identifier() {
      return id;
    }

    public void toBin(Object o, ByteBuf buf) {
      SilkEvent.E4 obj = (SilkEvent.E4) o;
      Serializers.toBinary(obj.eventId, buf);
      Serializers.toBinary(obj.torrentId, buf);
      Serializers.toBinary(obj.fileId, buf);
    }

    public Triplet<Identifier, OverlayId, Identifier> fromBin(ByteBuf buf, Optional<Object> hint) {
      Identifier eventId = (Identifier)Serializers.fromBinary(buf, hint);
      OverlayId torrentId = (OverlayId)Serializers.fromBinary(buf, hint);
      Identifier fileId = (Identifier)Serializers.fromBinary(buf, hint);
      return Triplet.with(eventId, torrentId, fileId);
    }
  }
  
  public static class Connect extends Base {

    public Connect(int id) {
      super(id);
    }

    @Override
    public void toBinary(Object o, ByteBuf buf) {
      R1TransferConnMsgs.Connect obj = (R1TransferConnMsgs.Connect)o;
      toBin(o, buf);
    }

    @Override
    public Object fromBinary(ByteBuf buf, Optional<Object> hint) {
      Triplet<Identifier, OverlayId, Identifier> base = fromBin(buf, hint);
      return new R1TransferConnMsgs.Connect(base.getValue0(), base.getValue1(), base.getValue2());
    }
  }
  
  public static class ConnectAcc extends Base {

    public ConnectAcc(int id) {
      super(id);
    }

    @Override
    public void toBinary(Object o, ByteBuf buf) {
      R1TransferConnMsgs.ConnectAcc obj = (R1TransferConnMsgs.ConnectAcc)o;
      toBin(o, buf);
    }

    @Override
    public Object fromBinary(ByteBuf buf, Optional<Object> hint) {
      Triplet<Identifier, OverlayId, Identifier> base = fromBin(buf, hint);
      return new R1TransferConnMsgs.ConnectAcc(base.getValue0(), base.getValue1(), base.getValue2());
    }
  }
  
  public static class Disconnect extends Base {

    public Disconnect(int id) {
      super(id);
    }

    @Override
    public void toBinary(Object o, ByteBuf buf) {
      R1TransferConnMsgs.Disconnect obj = (R1TransferConnMsgs.Disconnect)o;
      toBin(o, buf);
    }

    @Override
    public Object fromBinary(ByteBuf buf, Optional<Object> hint) {
      Triplet<Identifier, OverlayId, Identifier> base = fromBin(buf, hint);
      return new R1TransferConnMsgs.Disconnect(base.getValue0(), base.getValue1(), base.getValue2());
    }
  }
  
  public static class Ping extends Base {

    public Ping(int id) {
      super(id);
    }

    @Override
    public void toBinary(Object o, ByteBuf buf) {
      R1TransferConnMsgs.Ping obj = (R1TransferConnMsgs.Ping)o;
      toBin(o, buf);
    }

    @Override
    public Object fromBinary(ByteBuf buf, Optional<Object> hint) {
      Triplet<Identifier, OverlayId, Identifier> base = fromBin(buf, hint);
      return new R1TransferConnMsgs.Ping(base.getValue0(), base.getValue1(), base.getValue2());
    }
  }
  
  public static class Pong extends Base {

    public Pong(int id) {
      super(id);
    }

    @Override
    public void toBinary(Object o, ByteBuf buf) {
      R1TransferConnMsgs.Pong obj = (R1TransferConnMsgs.Pong)o;
      toBin(o, buf);
    }

    @Override
    public Object fromBinary(ByteBuf buf, Optional<Object> hint) {
      Triplet<Identifier, OverlayId, Identifier> base = fromBin(buf, hint);
      return new R1TransferConnMsgs.Pong(base.getValue0(), base.getValue1(), base.getValue2());
    }
  }
}
