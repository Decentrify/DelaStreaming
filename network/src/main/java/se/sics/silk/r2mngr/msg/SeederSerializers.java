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
package se.sics.silk.r2mngr.msg;

import com.google.common.base.Optional;
import io.netty.buffer.ByteBuf;
import org.javatuples.Triplet;
import se.sics.kompics.network.netty.serialization.Serializer;
import se.sics.kompics.network.netty.serialization.Serializers;
import se.sics.kompics.util.Identifier;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class SeederSerializers {
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
      ConnSeederMsgs.Base obj = (ConnSeederMsgs.Base)o;
      Serializers.toBinary(obj.msgId, buf);
      Serializers.toBinary(obj.srcId, buf);
      Serializers.toBinary(obj.dstId, buf);
    }

    public Triplet<Identifier, Identifier, Identifier> fromBin(ByteBuf buf, Optional<Object> hint) {
      Identifier eventId = (Identifier)Serializers.fromBinary(buf, hint);
      Identifier srcId = (Identifier)Serializers.fromBinary(buf, hint);
      Identifier dstId = (Identifier)Serializers.fromBinary(buf, hint);
      return Triplet.with(eventId, srcId, dstId);
    }
  }
  
  public static class Connect extends Base {

    public Connect(int id) {
      super(id);
    }

    @Override
    public void toBinary(Object o, ByteBuf buf) {
      toBin(o, buf);
    }

    @Override
    public Object fromBinary(ByteBuf buf, Optional<Object> hint) {
      Triplet<Identifier, Identifier, Identifier> aux = fromBin(buf, hint);
      return new ConnSeederMsgs.Connect(aux.getValue0(), aux.getValue1(), aux.getValue2());
    }
  }

  public static class ConnectAcc extends Base {

    public ConnectAcc(int id) {
      super(id);
    }

    @Override
    public void toBinary(Object o, ByteBuf buf) {
      toBin(o, buf);
    }

    @Override
    public Object fromBinary(ByteBuf buf, Optional<Object> hint) {
      Triplet<Identifier, Identifier, Identifier> aux = fromBin(buf, hint);
      return new ConnSeederMsgs.ConnectAcc(aux.getValue0(), aux.getValue1(), aux.getValue2());
    }
  }

  public static class ConnectReject extends Base {

    public ConnectReject(int id) {
      super(id);
    }

    @Override
    public void toBinary(Object o, ByteBuf buf) {
      toBin(o, buf);
    }

    @Override
    public Object fromBinary(ByteBuf buf, Optional<Object> hint) {
      Triplet<Identifier, Identifier, Identifier> aux = fromBin(buf, hint);
      return new ConnSeederMsgs.ConnectRej(aux.getValue0(), aux.getValue1(), aux.getValue2());
    }
  }

  public static class Disconnect extends Base {

    public Disconnect(int id) {
      super(id);
    }

    @Override
    public void toBinary(Object o, ByteBuf buf) {
      toBin(o, buf);
    }

    @Override
    public Object fromBinary(ByteBuf buf, Optional<Object> hint) {
      Triplet<Identifier, Identifier, Identifier> aux = fromBin(buf, hint);
      return new ConnSeederMsgs.Disconnect(aux.getValue0(), aux.getValue1(), aux.getValue2());
    }
  }

  public static class DisconnectAck extends Base {

    public DisconnectAck(int id) {
      super(id);
    }

    @Override
    public void toBinary(Object o, ByteBuf buf) {
      toBin(o, buf);
    }

    @Override
    public Object fromBinary(ByteBuf buf, Optional<Object> hint) {
      Triplet<Identifier, Identifier, Identifier> aux = fromBin(buf, hint);
      return new ConnSeederMsgs.DisconnectAck(aux.getValue0(), aux.getValue1(), aux.getValue2());
    }
  }

  public static class Ping extends Base {

    public Ping(int id) {
      super(id);
    }

    @Override
    public void toBinary(Object o, ByteBuf buf) {
      toBin(o, buf);
    }

    @Override
    public Object fromBinary(ByteBuf buf, Optional<Object> hint) {
      Triplet<Identifier, Identifier, Identifier> aux = fromBin(buf, hint);
      return new ConnSeederMsgs.Ping(aux.getValue0(), aux.getValue1(), aux.getValue2());
    }
  }

  public static class Pong extends Base {

    public Pong(int id) {
      super(id);
    }

    @Override
    public void toBinary(Object o, ByteBuf buf) {
      toBin(o, buf);
    }

    @Override
    public Object fromBinary(ByteBuf buf, Optional<Object> hint) {
      Triplet<Identifier, Identifier, Identifier> aux = fromBin(buf, hint);
      return new ConnSeederMsgs.Pong(aux.getValue0(), aux.getValue1(), aux.getValue2());
    }
  }
}
