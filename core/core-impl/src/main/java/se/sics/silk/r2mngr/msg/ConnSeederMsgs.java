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

import se.sics.kompics.util.Identifier;
import se.sics.ktoolbox.util.identifiable.BasicIdentifiers;
import se.sics.silk.r2mngr.ConnSeeder;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class ConnSeederMsgs {

  public static class Base implements ConnSeeder.Event {

    public final Identifier msgId;
    public final Identifier srcId;
    public final Identifier dstId;

    public Base(Identifier msgId, Identifier srcId, Identifier dstId) {
      this.msgId = msgId;
      this.srcId = srcId;
      this.dstId = dstId;
    }

    @Override
    public Identifier getId() {
      return msgId;
    }

    @Override
    public Identifier getConnSeederFSMId() {
      return dstId;
    }
  }

  public static class Connect extends Base {

    Connect(Identifier msgId, Identifier srcId, Identifier dstId) {
      super(msgId, srcId, dstId);
    }

    public Connect(Identifier srcId, Identifier dstId) {
      this(BasicIdentifiers.msgId(), srcId, dstId);
    }

    public ConnectAcc accept() {
      return new ConnectAcc(msgId, srcId, dstId);
    }
  }

  public static class ConnectAcc extends Base {

    ConnectAcc(Identifier msgId, Identifier srcId, Identifier dstId) {
      super(msgId, srcId, dstId);
    }
  }

  public static class ConnectReject extends Base {

    ConnectReject(Identifier msgId, Identifier srcId, Identifier dstId) {
      super(msgId, srcId, dstId);
    }
  }

  public static class Disconnect extends Base {

    Disconnect(Identifier msgId, Identifier srcId, Identifier dstId) {
      super(msgId, srcId, dstId);
    }

    public Disconnect(Identifier srcId, Identifier dstId) {
      this(BasicIdentifiers.msgId(), srcId, dstId);
    }
    
    public DisconnectAck ack() {
      return new DisconnectAck(this);
    }
  }

  public static class DisconnectAck extends Base {

    DisconnectAck(Identifier msgId, Identifier srcId, Identifier dstId) {
      super(msgId, srcId, dstId);
    }
    
    private DisconnectAck(Disconnect req) {
      this(req.msgId, req.srcId, req.dstId);
    }
  }

  public static class Ping extends Base {

    public Ping(Identifier msgId, Identifier srcId, Identifier dstId) {
      super(msgId, srcId, dstId);
    }
  }

  public static class Pong extends Base {

    public Pong(Identifier msgId, Identifier srcId, Identifier dstId) {
      super(msgId, srcId, dstId);
    }
  }
}
