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
package se.sics.silk.r2torrent.conn.helper;

import com.google.common.base.Predicate;
import se.sics.kompics.Port;
import se.sics.kompics.testing.Direction;
import se.sics.kompics.testing.Future;
import se.sics.kompics.testing.TestContext;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayId;
import se.sics.ktoolbox.util.network.KAddress;
import se.sics.ktoolbox.util.network.basic.BasicContentMsg;
import se.sics.silk.FutureHelper;
import static se.sics.silk.MsgHelper.content;
import static se.sics.silk.MsgHelper.destination;
import static se.sics.silk.MsgHelper.msg;
import static se.sics.silk.MsgHelper.srcDst;
import se.sics.silk.r2torrent.conn.event.R2NodeLeecherEvents;
import static se.sics.silk.r2torrent.conn.helper.R2NodeConnHelper.nodeConnectReq;
import static se.sics.silk.r2torrent.conn.helper.R2NodeConnHelper.nodeDisconnect;
import se.sics.silk.r2torrent.conn.msg.R2NodeConnMsgs;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class R2NodeLeecherHelper {
  //**************************************************LOCAL*************************************************************
  public static TestContext nodeLeecherConnReqLoc(TestContext tc, Port expectP) {
    return tc.expect(R2NodeLeecherEvents.ConnectReq.class, expectP, Direction.OUT);
  }
  
  public static TestContext nodeLeecherConnSuccLoc(TestContext tc, Port expectP) {
    return tc.expect(R2NodeLeecherEvents.ConnectSucc.class, expectP, Direction.OUT);
  }
  
  public static TestContext nodeLeecherDisconnectLoc(TestContext tc, Port expectP) {
    return tc.expect(R2NodeLeecherEvents.Disconnect.class, expectP, Direction.OUT);
  }
  
  public static TestContext nodeLeecherConnSuccLoc(TestContext tc, Port expectP, Port triggerP) {
    Future f = nodeConnSuccLoc();
    return tc
      .answerRequest(R2NodeLeecherEvents.ConnectReq.class, expectP, f)
      .trigger(f, triggerP);
  }
  
  
  public static TestContext nodeLeecherConnFailLoc(TestContext tc, Port expectP, Port triggerP) {
    Future f = nodeConnFailLoc();
    return tc
      .answerRequest(R2NodeLeecherEvents.ConnectReq.class, expectP, f)
      .trigger(f, triggerP);
  }
  
  public static Future<R2NodeLeecherEvents.ConnectReq, R2NodeLeecherEvents.ConnectSucc> nodeConnSuccLoc() {
    return new FutureHelper.BasicFuture<R2NodeLeecherEvents.ConnectReq, R2NodeLeecherEvents.ConnectSucc>() {
      @Override
      public R2NodeLeecherEvents.ConnectSucc get() {
        return event.success();
      }
    };
  }
  
  public static Future<R2NodeLeecherEvents.ConnectReq, R2NodeLeecherEvents.ConnectFail> nodeConnFailLoc() {
    return new FutureHelper.BasicFuture<R2NodeLeecherEvents.ConnectReq, R2NodeLeecherEvents.ConnectFail>() {
      @Override
      public R2NodeLeecherEvents.ConnectFail get() {
        return event.fail();
      }
    };
  }
  
  public static TestContext nodeLeecherConnSuccLoc(TestContext tc, Port triggerP, Future f) {
    return tc.trigger(f, triggerP);
  }
  
  public static TestContext nodeLeecherConnReqLoc(TestContext tc, Port expectP, Future f) {
    return tc.answerRequest(R2NodeLeecherEvents.ConnectReq.class, expectP, f);
  }
  
  public static TestContext nodeLeecherConnFailLoc(TestContext tc, Port triggerP, Future f) {
    return tc.trigger(f, triggerP);
  }
  
  public static TestContext nodeLeecherConnFailLoc(TestContext tc, Port expectP) {
    return tc.expect(R2NodeLeecherEvents.ConnectFail.class, expectP, Direction.OUT);
  }
  
  public static TestContext nodeLeecherConnFailLoc(TestContext tc, Port triggerP, OverlayId torrentId, KAddress leecher) {
    R2NodeLeecherEvents.ConnectReq req = new R2NodeLeecherEvents.ConnectReq(torrentId, leecher.getId());
    return tc.trigger(req.fail(), triggerP);
  }
  
  public static TestContext nodeLeecherConnReqLoc(TestContext tc, Port triggerP, OverlayId torrentId, KAddress leecher) {
    R2NodeLeecherEvents.ConnectReq event = new R2NodeLeecherEvents.ConnectReq(torrentId, leecher.getId());
    return tc.trigger(event, triggerP);
  }
  
  public static TestContext nodeLeecherDisc(TestContext tc, Port triggerP, OverlayId torrentId, KAddress leecher) {
    R2NodeLeecherEvents.Disconnect event = new R2NodeLeecherEvents.Disconnect(torrentId, leecher.getId());
    return tc.trigger(event, triggerP);
  }
  //***************************************************NET**************************************************************
  public static TestContext nodeLeecherConnReqNet(TestContext tc, Port network, KAddress self, KAddress leecher) {
    BasicContentMsg m = msg(leecher, self, nodeConnectReq());
    return tc.trigger(m, network);
  }
  
  public static TestContext nodeLeecherConnSuccNet(TestContext tc, Port network, KAddress leecher) {
    Predicate p = msg(destination(leecher), content(R2NodeConnMsgs.ConnectAcc.class));
    return tc.expect(BasicContentMsg.class, p, network, Direction.OUT);
  }
  
  public static TestContext nodeLeecherPing(TestContext tc, Port network, KAddress self, KAddress leecher) {
    R2NodeConnMsgs.Ping c = new R2NodeConnMsgs.Ping();
    BasicContentMsg msg = msg(leecher, self, c);
    return tc.trigger(msg, network);
  }
  
  public static TestContext nodeLeecherPong(TestContext tc, Port network, KAddress self, KAddress leecher) {
    Predicate p = msg(srcDst(self, leecher), content(R2NodeConnMsgs.Pong.class));
    return tc.expect(BasicContentMsg.class, p, network, Direction.OUT);
  }
  
  public static TestContext nodeLeecherDisconnectNet(TestContext tc, Port network, KAddress self, KAddress leecher) {
    BasicContentMsg m = msg(leecher, self, nodeDisconnect());
    return tc.trigger(m, network);
  }
}
