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

import com.google.common.base.Predicate;
import se.sics.kompics.Port;
import se.sics.kompics.network.Msg;
import se.sics.kompics.testing.Direction;
import se.sics.kompics.testing.Future;
import se.sics.kompics.testing.TestContext;
import se.sics.kompics.timer.CancelPeriodicTimeout;
import se.sics.kompics.timer.SchedulePeriodicTimeout;
import se.sics.kompics.util.Identifier;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayId;
import se.sics.ktoolbox.util.network.KAddress;
import se.sics.ktoolbox.util.network.basic.BasicContentMsg;
import se.sics.silk.FutureHelper;
import se.sics.silk.MsgHelper;
import static se.sics.silk.MsgHelper.beMsg;
import static se.sics.silk.MsgHelper.destination;
import static se.sics.silk.MsgHelper.msg;
import se.sics.silk.PredicateHelper;
import se.sics.silk.r2torrent.conn.event.R2NodeSeederEvents;
import se.sics.silk.r2torrent.conn.event.R2NodeSeederTimeout;
import se.sics.silk.r2torrent.conn.msg.R2NodeConnMsgs;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class R2NodeSeederHelper {

  //*****************************************************LOCAL**********************************************************
  public static TestContext nodeSeederConnReqLoc(TestContext tc, Port expectP) {
    return tc.expect(R2NodeSeederEvents.ConnectReq.class, expectP, Direction.OUT);
  }
  
  public static TestContext nodeSeederConnReqLoc(TestContext tc, Port expectP, Future f) {
    return tc.answerRequest(R2NodeSeederEvents.ConnectReq.class, expectP, f);
  }
  
  public static TestContext nodeSeederConnReqLoc(TestContext tc, Port triggerP, R2NodeSeederEvents.ConnectReq req) {
    return tc.trigger(req, triggerP);
  }
  
  public static TestContext nodeSeederConnSuccLoc(TestContext tc, Port expectP) {
    return tc.expect(R2NodeSeederEvents.ConnectSucc.class, expectP, Direction.OUT);
  }
  
  public static TestContext nodeSeederConnSuccLoc(TestContext tc, Port triggerP, Future f) {
    return tc.trigger(f, triggerP);
  }
  
  public static TestContext nodeSeederConnSuccLoc(TestContext tc, Port expectP, Port triggerP) {
    Future f = nodeConnSuccLoc();
    return tc
      .answerRequest(R2NodeSeederEvents.ConnectReq.class, expectP, f)
      .trigger(f, triggerP);
  }
  
  public static TestContext nodeSeederConnFailLoc(TestContext tc, Port expectP, Port triggerP) {
    Future f = nodeConnFailLoc();
    return tc
      .answerRequest(R2NodeSeederEvents.ConnectReq.class, expectP, f)
      .trigger(f, triggerP);
  }
  
  public static TestContext nodeSeederConnFailLoc(TestContext tc, Port expectP, OverlayId torrentId) {
    Predicate p = new PredicateHelper.TorrentEPredicate<>(torrentId);
    return tc.expect(R2NodeSeederEvents.ConnectFail.class, p, expectP, Direction.OUT);
  }
  
  public static TestContext nodeSeederConnFailLoc(TestContext tc, Port triggerP, R2NodeSeederEvents.ConnectFail ind) {
    return tc.trigger(ind, triggerP);
  }
  
  public static TestContext nodeSeederConnFailLoc(TestContext tc, Port triggerP, Future f) {
    return tc.trigger(f, triggerP);
  }
  
  public static TestContext nodeSeederDisconnectLoc(TestContext tc, Port expectP) {
    return tc.expect(R2NodeSeederEvents.Disconnect.class, expectP, Direction.OUT);
  }
  
  public static TestContext nodeSeederDisconnectLoc(TestContext tc, Port triggerP, OverlayId torrentId, KAddress seeder) {
    R2NodeSeederEvents.Disconnect req = new R2NodeSeederEvents.Disconnect(torrentId, seeder.getId());
    return tc.trigger(req, triggerP);
  }
  
  public static Future<R2NodeSeederEvents.ConnectReq, R2NodeSeederEvents.ConnectSucc> nodeConnSuccLoc() {
    return new FutureHelper.BasicFuture<R2NodeSeederEvents.ConnectReq, R2NodeSeederEvents.ConnectSucc>() {
      @Override
      public R2NodeSeederEvents.ConnectSucc get() {
        return event.success();
      }
    };
  }
  public static Future<R2NodeSeederEvents.ConnectReq, R2NodeSeederEvents.ConnectFail> nodeConnFailLoc() {
    return new FutureHelper.BasicFuture<R2NodeSeederEvents.ConnectReq, R2NodeSeederEvents.ConnectFail>() {
      @Override
      public R2NodeSeederEvents.ConnectFail get() {
        return event.fail();
      }
    };
  }
  //***************************************************NET**************************************************************
  public static TestContext nodeSeederConnReqNet(TestContext tc, Port expectNetP, KAddress seeder) {
    Predicate p = beMsg(R2NodeConnMsgs.ConnectReq.class, destination(seeder));
    return tc.expect(BasicContentMsg.class, p, expectNetP, Direction.OUT);
  }
  
  public static TestContext nodeSeederConnSuccNet(TestContext tc, Port expectNetP, Port triggerNetP) {
    Future f = nodeSeederConnSuccNet();
    return tc
      .answerRequest(Msg.class, expectNetP, f)
      .trigger(f, triggerNetP);
  }
  
  public static TestContext nodeSeederConnSuccNet(TestContext tc, Port network, KAddress self, KAddress seeder) {
    R2NodeConnMsgs.ConnectReq req = new R2NodeConnMsgs.ConnectReq();
    Msg m = msg(seeder, self, req.accept());
    return tc.trigger(m, network);
  }
  
  public static TestContext nodeSeederConnRejNet(TestContext tc, Port expectNetP, Port triggerNetP) {
    Future f = nodeSeederConnRejNet();
    return tc
      .answerRequest(Msg.class, expectNetP, f)
      .trigger(f, triggerNetP);
  }
  
  public static TestContext nodeSeederConnRejNet(TestContext tc, Port network, KAddress self, KAddress seeder) {
    R2NodeConnMsgs.ConnectReq req = new R2NodeConnMsgs.ConnectReq();
    Msg m = msg(seeder, self, req.reject());
    return tc.trigger(m, network);
  }
  
  public static TestContext nodeSeederPingNet(TestContext tc, Port netP, KAddress seeder) {
    Predicate p = beMsg(R2NodeConnMsgs.Ping.class, destination(seeder));
    return tc.expect(Msg.class, p, netP, Direction.OUT);
  }
  
  public static TestContext nodeSeederPingNet(TestContext tc, Port netP) {
    Future f = nodeSeederPingPongNet();
    return tc
      .answerRequest(Msg.class, netP, f)
      .trigger(f, netP);
  }
  
  public static TestContext nodeSeederPongNet(TestContext tc, Port network, KAddress self, KAddress seeder) {
    R2NodeConnMsgs.Ping ping = new R2NodeConnMsgs.Ping();
    Msg m = msg(seeder, self, ping.ack());
    return tc.trigger(m, network);
  }
  public static TestContext nodeSeederDisconnectNet(TestContext tc, Port triggerNetP, KAddress self, KAddress seeder) {
    R2NodeConnMsgs.Disconnect disc = new R2NodeConnMsgs.Disconnect();
    Msg msg = msg(seeder, self, disc);
    return tc.trigger(msg, triggerNetP);
  }
  
  public static TestContext nodeSeederDisconnectNet(TestContext tc, Port expectedNetP, KAddress seeder) {
    Predicate p = beMsg(R2NodeConnMsgs.Disconnect.class, destination(seeder));
    return tc.expect(BasicContentMsg.class, p, expectedNetP, Direction.OUT);
  }
  
  public static Future<Msg, Msg> nodeSeederConnSuccNet() {
    return new FutureHelper.NetBEFuture<R2NodeConnMsgs.ConnectReq>(R2NodeConnMsgs.ConnectReq.class) {
      @Override
      public Msg get() {
        return msg.answer(content.accept());
      }
    };
  }
  
  public static Future<Msg, Msg> nodeSeederConnRejNet() {
    return new FutureHelper.NetBEFuture<R2NodeConnMsgs.ConnectReq>(R2NodeConnMsgs.ConnectReq.class) {
      @Override
      public Msg get() {
        return msg.answer(content.reject());
      }
    };
  }
  
  public static Future<Msg, Msg> nodeSeederPingPongNet() {
    return new FutureHelper.NetBEFuture<R2NodeConnMsgs.Ping>(R2NodeConnMsgs.Ping.class) {
      @Override
      public Msg get() {
        return msg.answer(content.ack());
      }
    };
  }
  //*************************************************TIMER**************************************************************
  public static TestContext nodeSeederScheduleTimer(TestContext tc, Port timerP) {
    return tc.expect(SchedulePeriodicTimeout.class, timerP, Direction.OUT);
  }
  
  public static TestContext nodeSeederCancelTimer(TestContext tc, Port timerP) {
    return tc.expect(CancelPeriodicTimeout.class, timerP, Direction.OUT);
  }
  
  public static TestContext nodeSeederPingTimer(TestContext tc, Port timerP, R2NodeSeederTimeout timeout) {
    return tc.trigger(timeout, timerP);
  }
  //********************************************************************************************************************
  
  public static R2NodeSeederEvents.ConnectReq nodeSeederConnReqLoc(OverlayId torrentId, KAddress seeder) {
    return new R2NodeSeederEvents.ConnectReq(torrentId, seeder);
  }
  
  public static R2NodeSeederEvents.ConnectFail nodeSeederConnFailLoc(OverlayId torrentId, Identifier seederId) {
    return new R2NodeSeederEvents.ConnectFail(torrentId, seederId);
  }
  
  public static R2NodeSeederEvents.Disconnect nodeSeederDisconnectLoc(OverlayId torrentId, Identifier seederId) {
    return new R2NodeSeederEvents.Disconnect(torrentId, seederId);
  }
  
  public static Msg nodeSeederDisconnectNet(KAddress src,  KAddress dst) {
    R2NodeConnMsgs.Disconnect content = new R2NodeConnMsgs.Disconnect();
    return MsgHelper.msg(src, dst, content);
  }
  
  public static Msg nodeSeederPingNet(KAddress src,  KAddress dst) {
    R2NodeConnMsgs.Ping content = new R2NodeConnMsgs.Ping();
    return MsgHelper.msg(src, dst, content);
  }
  
  public static R2NodeSeederTimeout nodeSeederPingTimeout(KAddress seeder) {
    SchedulePeriodicTimeout spt = new SchedulePeriodicTimeout(0, 0);
    return new R2NodeSeederTimeout(spt, seeder.getId());
  }
}
