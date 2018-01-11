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
package se.sics.silk.r2torrent;

import se.sics.kompics.testing.Future;
import se.sics.silk.r2torrent.event.R1HashEvents;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class R1HashHelper {
  public static abstract class MyFuture<I extends R1HashEvents.E2, O extends R1HashEvents.E3> extends Future<I, O> {
    I req;
    @Override
    public boolean set(I req) {
      this.req = req;
      return true;
    }
  }
  
  public static Future<R1HashEvents.HashReq, R1HashEvents.HashSucc> transferHashSucc() {
    return new MyFuture<R1HashEvents.HashReq, R1HashEvents.HashSucc>() {
      @Override
      public R1HashEvents.HashSucc get() {
        return req.success();
      }
    };
  }

  public static Future<R1HashEvents.HashReq, R1HashEvents.HashFail> transferHashFail() {
    return new MyFuture<R1HashEvents.HashReq, R1HashEvents.HashFail>() {
      @Override
      public R1HashEvents.HashFail get() {
        return req.fail();
      }
    };
  }

  public static Future<R1HashEvents.HashStop, R1HashEvents.HashStopAck> transferHashClean() {
    return new MyFuture<R1HashEvents.HashStop, R1HashEvents.HashStopAck>() {

      @Override
      public R1HashEvents.HashStopAck get() {
        return req.ack();
      }
    };
  }
}
