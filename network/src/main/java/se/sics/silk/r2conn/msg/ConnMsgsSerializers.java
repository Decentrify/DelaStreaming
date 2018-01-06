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
package se.sics.silk.r2conn.msg;

import se.sics.silk.r2conn.msg.R2ConnMsgs;
import com.google.common.base.Optional;
import io.netty.buffer.ByteBuf;
import se.sics.kompics.network.netty.serialization.Serializer;
import se.sics.kompics.network.netty.serialization.Serializers;
import se.sics.kompics.util.Identifier;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class ConnMsgsSerializers {
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
      R2ConnMsgs.Base obj = (R2ConnMsgs.Base)o;
      Serializers.toBinary(obj.msgId, buf);
    }

    public Identifier fromBin(ByteBuf buf, Optional<Object> hint) {
      Identifier msgId = (Identifier)Serializers.fromBinary(buf, hint);
      return msgId;
    }
  }
  
  public static class ConnectReq extends Base {

    public ConnectReq(int id) {
      super(id);
    }

    @Override
    public void toBinary(Object o, ByteBuf buf) {
      toBin(o, buf);
    }

    @Override
    public Object fromBinary(ByteBuf buf, Optional<Object> hint) {
      Identifier aux = fromBin(buf, hint);
      return new R2ConnMsgs.ConnectReq(aux);
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
      Identifier aux = fromBin(buf, hint);
      return new R2ConnMsgs.ConnectAcc(aux);
    }
  }

  public static class ConnectRej extends Base {

    public ConnectRej(int id) {
      super(id);
    }

    @Override
    public void toBinary(Object o, ByteBuf buf) {
      toBin(o, buf);
    }

    @Override
    public Object fromBinary(ByteBuf buf, Optional<Object> hint) {
      Identifier aux = fromBin(buf, hint);
      return new R2ConnMsgs.ConnectRej(aux);
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
      Identifier aux = fromBin(buf, hint);
      return new R2ConnMsgs.Disconnect(aux);
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
      Identifier aux = fromBin(buf, hint);
      return new R2ConnMsgs.DisconnectAck(aux);
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
      Identifier aux = fromBin(buf, hint);
      return new R2ConnMsgs.Ping(aux);
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
      Identifier aux = fromBin(buf, hint);
      return new R2ConnMsgs.Pong(aux);
    }
  }
}
