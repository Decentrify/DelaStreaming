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
package se.sics.silk.r2torrent.conn;

import se.sics.kompics.Port;
import se.sics.kompics.testing.Direction;
import se.sics.kompics.testing.Future;
import se.sics.kompics.testing.TestContext;
import se.sics.kompics.util.Identifier;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayId;
import se.sics.silk.FutureHelper;
import se.sics.silk.r2torrent.conn.event.R2NodeSeederEvents;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class R2NodeSeederHelper {

  public static TestContext nodeSeederConnReq(TestContext tc, Port expectP) {
    return tc.expect(R2NodeSeederEvents.ConnectReq.class, expectP, Direction.OUT);
  }
  
  public static TestContext nodeSeederConnSucc(TestContext tc, Port expectP, Port triggerP) {
    Future f = nodeConnSucc();
    return tc
      .answerRequest(R2NodeSeederEvents.ConnectReq.class, expectP, f)
      .trigger(f, triggerP);
  }
 
  public static TestContext nodeSeederConnSucc(TestContext tc, Port expectP) {
    return tc.expect(R2NodeSeederEvents.ConnectReq.class, expectP, Direction.OUT);
  }
  
  public static TestContext nodeSeederConnFail(TestContext tc, Port expectP, Port triggerP) {
    Future f = nodeConnFail();
    return tc
      .answerRequest(R2NodeSeederEvents.ConnectReq.class, expectP, f)
      .trigger(f, triggerP);
  }
  
  public static TestContext nodeSeederConnFail(TestContext tc, Port expectP) {
    return tc.expect(R2NodeSeederEvents.ConnectFail.class, expectP, Direction.OUT);
  }
  
  public static TestContext nodeSeederConnFail(TestContext tc, Port triggerP, R2NodeSeederEvents.ConnectFail ind) {
    return tc.trigger(ind, triggerP);
  }
  
  public static TestContext nodeSeederDisconnect(TestContext tc, Port expectP) {
    return tc.expect(R2NodeSeederEvents.Disconnect.class, expectP, Direction.OUT);
  }
  
  public static Future<R2NodeSeederEvents.ConnectReq, R2NodeSeederEvents.ConnectSucc> nodeConnSucc() {
    return new FutureHelper.BasicFuture<R2NodeSeederEvents.ConnectReq, R2NodeSeederEvents.ConnectSucc>() {
      @Override
      public R2NodeSeederEvents.ConnectSucc get() {
        return event.success();
      }
    };
  }
  
  public static Future<R2NodeSeederEvents.ConnectReq, R2NodeSeederEvents.ConnectFail> nodeConnFail() {
    return new FutureHelper.BasicFuture<R2NodeSeederEvents.ConnectReq, R2NodeSeederEvents.ConnectFail>() {
      @Override
      public R2NodeSeederEvents.ConnectFail get() {
        return event.fail();
      }
    };
  }
  
  public static R2NodeSeederEvents.ConnectFail nodeSeederConnFail(OverlayId torrentId, Identifier seederId) {
    return new R2NodeSeederEvents.ConnectFail(torrentId, seederId);
  }
}
