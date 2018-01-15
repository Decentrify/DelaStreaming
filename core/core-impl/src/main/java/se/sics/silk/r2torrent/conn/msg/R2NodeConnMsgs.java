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
package se.sics.silk.r2torrent.conn.msg;

import se.sics.kompics.util.Identifier;
import se.sics.ktoolbox.util.identifiable.BasicIdentifiers;
import se.sics.silk.r2torrent.conn.R2NodeLeecher;
import se.sics.silk.r2torrent.conn.R2NodeSeeder;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class R2NodeConnMsgs {

  public static abstract class Base implements R2NodeSeeder.Msg, R2NodeLeecher.Msg {

    public final Identifier msgId;

    Base(Identifier msgId) {
      this.msgId = msgId;
    }

    @Override
    public Identifier getId() {
      return msgId;
    }
  }

  public static class ConnectReq extends Base {
    ConnectReq(Identifier msgId) {
      super(msgId);
    }

    public ConnectReq() {
      this(BasicIdentifiers.msgId());
    }
    
    public ConnectAcc accept() {
      return new ConnectAcc(msgId);
    }
    
    public ConnectRej reject() {
      return new ConnectRej(msgId);
    }
  }

  public static class ConnectAcc extends Base {

    ConnectAcc(Identifier msgId) {
      super(msgId);
    }
  }

  public static class ConnectRej extends Base {

    ConnectRej(Identifier msgId) {
      super(msgId);
    }
  }

  public static class Disconnect extends Base {

    Disconnect(Identifier msgId) {
      super(msgId);
    }

    public Disconnect() {
      this(BasicIdentifiers.msgId());
    }
  }

  public static class Ping extends Base {

    Ping(Identifier msgId) {
      super(msgId);
    }
    
    public Ping() {
      this(BasicIdentifiers.msgId());
    }
    
    public Pong ack() {
      return new Pong(msgId);
    }
  }

  public static class Pong extends Base {

    public Pong(Identifier msgId) {
      super(msgId);
    }
  }
}
