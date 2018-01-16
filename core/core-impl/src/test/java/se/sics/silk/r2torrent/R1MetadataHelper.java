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

import se.sics.kompics.Port;
import se.sics.kompics.testing.Direction;
import se.sics.kompics.testing.Future;
import se.sics.kompics.testing.TestContext;
import se.sics.silk.r2torrent.event.R1MetadataGetEvents;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class R1MetadataHelper {

  public static abstract class MyFuture<I extends R1MetadataGetEvents.Req, O extends R1MetadataGetEvents.Ind> extends Future<I, O> {

    I req;

    @Override
    public boolean set(I req) {
      this.req = req;
      return true;
    }
  }
  
  public static Future<R1MetadataGetEvents.MetaGetReq, R1MetadataGetEvents.MetaGetSucc> transferMetaGetSucc() {
    return new MyFuture<R1MetadataGetEvents.MetaGetReq, R1MetadataGetEvents.MetaGetSucc>() {
      @Override
      public R1MetadataGetEvents.MetaGetSucc get() {
        return req.success();
      }
    };
  }

  public static Future<R1MetadataGetEvents.MetaGetReq, R1MetadataGetEvents.MetaGetFail> transferMetaGetFail() {
    return new MyFuture<R1MetadataGetEvents.MetaGetReq, R1MetadataGetEvents.MetaGetFail>() {
      @Override
      public R1MetadataGetEvents.MetaGetFail get() {
        return req.fail();
      }
    };
  }

  public static Future<R1MetadataGetEvents.MetaServeReq, R1MetadataGetEvents.MetaServeSucc> transferMetaServeSucc() {
    return new MyFuture<R1MetadataGetEvents.MetaServeReq, R1MetadataGetEvents.MetaServeSucc>() {
      @Override
      public R1MetadataGetEvents.MetaServeSucc get() {
        return req.success();
      }
    };
  }
  
  public static Future<R1MetadataGetEvents.MetaStop, R1MetadataGetEvents.MetaStopAck> transferMetaStop() {
    return new MyFuture<R1MetadataGetEvents.MetaStop, R1MetadataGetEvents.MetaStopAck>() {
      @Override
      public R1MetadataGetEvents.MetaStopAck get() {
        return req.ack();
      }
    };
  }

  public static TestContext mngrMetaGetReq(TestContext tc, Port expectP) {
    return tc.expect(R1MetadataGetEvents.MetaGetReq.class, expectP, Direction.OUT);
  }
}
