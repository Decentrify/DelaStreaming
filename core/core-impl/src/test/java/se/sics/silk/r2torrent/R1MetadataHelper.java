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
import se.sics.silk.r2torrent.event.R1MetadataEvents;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class R1MetadataHelper {

  public static abstract class MyFuture<I extends R1MetadataEvents.E2, O extends R1MetadataEvents.E3> extends Future<I, O> {

    I req;

    @Override
    public boolean set(I req) {
      this.req = req;
      return true;
    }
  }
  
  public static Future<R1MetadataEvents.MetaGetReq, R1MetadataEvents.MetaGetSucc> transferMetaGetSucc() {
    return new MyFuture<R1MetadataEvents.MetaGetReq, R1MetadataEvents.MetaGetSucc>() {
      @Override
      public R1MetadataEvents.MetaGetSucc get() {
        return req.success();
      }
    };
  }

  public static Future<R1MetadataEvents.MetaGetReq, R1MetadataEvents.MetaGetFail> transferMetaGetFail() {
    return new MyFuture<R1MetadataEvents.MetaGetReq, R1MetadataEvents.MetaGetFail>() {
      @Override
      public R1MetadataEvents.MetaGetFail get() {
        return req.fail();
      }
    };
  }

  public static Future<R1MetadataEvents.MetaServeReq, R1MetadataEvents.MetaServeSucc> transferMetaServeSucc() {
    return new MyFuture<R1MetadataEvents.MetaServeReq, R1MetadataEvents.MetaServeSucc>() {
      @Override
      public R1MetadataEvents.MetaServeSucc get() {
        return req.success();
      }
    };
  }
  
  public static Future<R1MetadataEvents.MetaStop, R1MetadataEvents.MetaStopAck> transferMetaStop() {
    return new MyFuture<R1MetadataEvents.MetaStop, R1MetadataEvents.MetaStopAck>() {
      @Override
      public R1MetadataEvents.MetaStopAck get() {
        return req.ack();
      }
    };
  }
}
