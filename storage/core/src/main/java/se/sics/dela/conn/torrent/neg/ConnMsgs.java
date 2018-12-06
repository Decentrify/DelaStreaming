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
package se.sics.dela.conn.torrent.neg;

import se.sics.kompics.KompicsEvent;
import se.sics.kompics.util.Identifiable;
import se.sics.kompics.util.Identifier;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class ConnMsgs {
  private static abstract class Base implements KompicsEvent, Identifiable {
    public final Identifier msgId;
    public final Identifier connId;
    
    protected Base(Identifier msgId, Identifier connId) {
      this.msgId = msgId;
      this.connId = connId;
    }

    @Override
    public Identifier getId() {
      return msgId;
    }
  }

  public static class ConnReq extends Base {
    public ConnReq(Identifier msgId, Identifier torrentId) {
      super(msgId, torrentId);
    }
  }
  
  public static class ConnReply extends Base {
    public final TorrentConnReplyState state;
    public ConnReply(Identifier msgId, Identifier torrentId, TorrentConnReplyState state) {
      super(msgId, torrentId);
      this.state = state;
    }
  }
  
  public static class Heartbeat extends Base {
    
    public Heartbeat(Identifier msgId, Identifier connId) {
      super(msgId, connId);
    }
  }
  
  public static interface TorrentConnReplyState {
  }
}
