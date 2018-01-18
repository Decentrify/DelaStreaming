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
package se.sics.silk;

import com.google.common.base.Predicate;
import java.util.LinkedList;
import java.util.List;
import se.sics.kompics.Port;
import se.sics.kompics.network.Msg;
import se.sics.kompics.testing.Direction;
import se.sics.kompics.testing.Future;
import se.sics.kompics.testing.TestContext;
import se.sics.kompics.timer.SchedulePeriodicTimeout;
import se.sics.kompics.util.Identifier;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayId;
import se.sics.ktoolbox.util.network.KAddress;
import se.sics.ktoolbox.util.network.basic.BasicContentMsg;
import se.sics.silk.FutureHelper.BasicFuture;
import static se.sics.silk.MsgHelper.msg;
import se.sics.silk.r2torrent.conn.event.R1TorrentSeederEvents;
import se.sics.silk.r2torrent.conn.event.R2NodeSeederEvents;
import se.sics.silk.r2torrent.conn.msg.R2NodeConnMsgs;
import se.sics.silk.r2torrent.torrent.event.R1HashEvents;
import se.sics.silk.r2torrent.torrent.event.R1MetadataGetEvents;
import se.sics.silk.r2torrent.torrent.event.R1MetadataServeEvents;
import se.sics.silk.r2torrent.torrent.event.R2TorrentCtrlEvents;
import se.sics.silk.r2torrent.torrent.msg.R1MetadataMsgs;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class TorrentTestHelper {

  //************************************************NETWORK_CONN********************************************************
  public static TestContext netNodeConnAcc(TestContext tc, Port network) {
    Future<Msg, Msg> f = netNodeConnAcc();
    tc = tc.answerRequest(Msg.class, network, f);
    tc = tc.trigger(f, network);
    return tc;
  }
  
  public static TestContext tNetNodeConnReq(TestContext tc, Port network, KAddress src, KAddress dst) {
    R2NodeConnMsgs.ConnectReq content = new R2NodeConnMsgs.ConnectReq();
    return tc.trigger(msg(src, dst, content), network);
  }
  
  public static TestContext eNetNodeConnAcc(TestContext tc, Port network) {
    Predicate payloadP = new PredicateHelper.ContentPredicate(R2NodeConnMsgs.ConnectAcc.class);
    Predicate p = new PredicateHelper.MsgPredicate(PredicateHelper.TRUE_P, payloadP);
    return tc.expect(BasicContentMsg.class, network, Direction.OUT);
  }
  
  //*****************************************************NODE_SEEDER****************************************************
  public static TestContext eNodeSeederConnSucc(TestContext tc, Port expectP) {
    return tc.expect(R2NodeSeederEvents.ConnectSucc.class, expectP, Direction.OUT);
  }

  //***************************************************TORRENT_SEEDER***************************************************
  public static TestContext eTorrentSeederConnReq(TestContext tc, Port expectP) {
    return tc.expect(R1TorrentSeederEvents.ConnectReq.class, expectP, Direction.OUT);
  }

  public static TestContext eTorrentSeederConnSucc(TestContext tc, Port expectP) {
    return tc.expect(R1TorrentSeederEvents.ConnectSucc.class, expectP, Direction.OUT);
  }

  public static TestContext torrentSeederConnSucc(TestContext tc, Port expectP, Port triggerP) {
    Future f = new BasicFuture<R1TorrentSeederEvents.ConnectReq, R1TorrentSeederEvents.ConnectSucc>() {
      @Override
      public R1TorrentSeederEvents.ConnectSucc get() {
        return event.success();
      }
    };
    return tc
      .answerRequest(R1TorrentSeederEvents.ConnectReq.class, expectP, f)
      .trigger(f, triggerP);
  }

  //***************************************************TIMER************************************************************
  public static TestContext eTimerSchedulePeriodicTimeout(TestContext tc, Port timerP) {
    return tc.expect(SchedulePeriodicTimeout.class, timerP, Direction.OUT);
  }

  //**********************************************NETWORK_METADATA******************************************************
  public static TestContext eNetMetadataGet(TestContext tc, Port network, KAddress src, KAddress dst) {
    Predicate msgP = new PredicateHelper.MsgSrcDstPredicate(src, dst);
    Predicate p = new PredicateHelper.MsgBEReqPredicate<>(R1MetadataMsgs.Get.class, msgP);
    return tc.expect(BasicContentMsg.class, p, network, Direction.OUT);
  }
  
  public static TestContext eNetMetadataServe(TestContext tc, Port network, KAddress src, KAddress dst) {
    Predicate payloadP = new PredicateHelper.ContentPredicate(R1MetadataMsgs.Serve.class);
    Predicate msgP = new PredicateHelper.MsgSrcDstPredicate(src, dst);
    Predicate p = new PredicateHelper.MsgPredicate(msgP, payloadP);
    return tc.expect(BasicContentMsg.class, p, network, Direction.OUT);
  }
  
  public static TestContext tNetMetadataGet(TestContext tc, Port network, KAddress src, KAddress dst, OverlayId torrentId, Identifier fileId) {
    R1MetadataMsgs.Get payload = new R1MetadataMsgs.Get(torrentId, fileId);
    BasicContentMsg msg = msg(src, dst, payload);
    return tc.trigger(msg, network);
  }
  
  public static TestContext tNetMetadataServe(TestContext tc, Port network, KAddress src, KAddress dst, OverlayId torrentId, Identifier fileId) {
    R1MetadataMsgs.Get payload = new R1MetadataMsgs.Get(torrentId, fileId);
    BasicContentMsg msg = msg(src, dst, payload.answer());
    return tc.trigger(msg, network);
  }
  
  public static TestContext netMetadata(TestContext tc, Port network) {
    Future f = new FutureHelper.NetBEFuture<R1MetadataMsgs.Get>(R1MetadataMsgs.Get.class) {

      @Override
      public Msg get() {
        return msg.answer(content.answer());
      }
    };
    return tc
      .answerRequest(BasicContentMsg.class, network, f)
      .trigger(f, network);
  }
  //*************************************************TORRENT CTRL********************************************************
  public static TestContext tCtrlMetaGetReq(TestContext tc, Port ctrlP, OverlayId torrentId, KAddress seeder) {
    List<KAddress> seeders = new LinkedList<>();
    seeders.add(seeder);
    R2TorrentCtrlEvents.MetaGetReq r = new R2TorrentCtrlEvents.MetaGetReq(torrentId, seeders);
    return tc.trigger(r, ctrlP);
  }
  
  public static TestContext tCtrlDownloadReq(TestContext tc, Port ctrlP, OverlayId torrentId) {
    R2TorrentCtrlEvents.Download r = new R2TorrentCtrlEvents.Download(torrentId);
    return tc.trigger(r, ctrlP);
  }
  
  public static TestContext tCtrlUploadReq(TestContext tc, Port ctrlP, OverlayId torrentId) {
    R2TorrentCtrlEvents.Upload r = new R2TorrentCtrlEvents.Upload(torrentId);
    return tc.trigger(r, ctrlP);
  }
  
  public static TestContext tCtrlStopReq(TestContext tc, Port ctrlP, OverlayId torrentId) {
    R2TorrentCtrlEvents.Stop r = new R2TorrentCtrlEvents.Stop(torrentId);
    return tc.trigger(r, ctrlP);
  }
  
  public static TestContext eCtrlMetaGetReq(TestContext tc, Port expectP) {
    return tc.expect(R2TorrentCtrlEvents.MetaGetReq.class, expectP, Direction.OUT);
  }

  public static TestContext eCtrlMetaGetSucc(TestContext tc, Port expectP) {
    return tc.expect(R2TorrentCtrlEvents.MetaGetSucc.class, expectP, Direction.OUT);
  }
  
  public static TestContext eCtrlBaseInfoInd(TestContext tc, Port expectP) {
    return tc.expect(R2TorrentCtrlEvents.TorrentBaseInfo.class, expectP, Direction.OUT);
  }
  
  public static TestContext eCtrlStopAck(TestContext tc, Port expectP) {
    return tc.expect(R2TorrentCtrlEvents.StopAck.class, expectP, Direction.OUT);
  }
  //************************************************METADATA_GET********************************************************
  public static TestContext tMetadataGetReq(TestContext tc, Port triggerP, OverlayId torrentId, Identifier fileId,
    KAddress seeder) {
    R1MetadataGetEvents.GetReq req = new R1MetadataGetEvents.GetReq(torrentId, fileId, seeder);
    return tc.trigger(req, triggerP);
  }

  public static TestContext tMetadataStop(TestContext tc, Port triggerP, OverlayId torrentId, Identifier fileId) {
    R1MetadataGetEvents.Stop req = new R1MetadataGetEvents.Stop(torrentId, fileId);
    return tc.trigger(req, triggerP);
  }

  public static TestContext eMetadataGetReq(TestContext tc, Port expectP) {
    return tc.expect(R1MetadataGetEvents.GetReq.class, expectP, Direction.OUT);
  }

  public static TestContext eMetadataGetSucc(TestContext tc, Port expectP) {
    return tc.expect(R1MetadataGetEvents.GetSucc.class, expectP, Direction.OUT);
  }

  public static TestContext eMetadataStopAck(TestContext tc, Port expectP) {
    return tc.expect(R1MetadataGetEvents.StopAck.class, expectP, Direction.OUT);
  }
  //***********************************************METADATA_SERVE*******************************************************
  public static TestContext tMetadataServeReq(TestContext tc, Port triggerP, OverlayId torrentId, Identifier fileId) {
    R1MetadataServeEvents.ServeReq req = new R1MetadataServeEvents.ServeReq(torrentId, fileId);
    return tc.trigger(req, triggerP);
  }
  
  public static TestContext tMetadataServeStop(TestContext tc, Port triggerP, OverlayId torrentId, Identifier fileId) {
    R1MetadataServeEvents.Stop req = new R1MetadataServeEvents.Stop(torrentId, fileId);
    return tc.trigger(req, triggerP);
  }
  
  public static TestContext eMetadataServeReq(TestContext tc, Port expectP) {
    return tc.expect(R1MetadataServeEvents.ServeReq.class, expectP, Direction.OUT);
  }
  
  public static TestContext eMetadataServeSucc(TestContext tc, Port expectP) {
    return tc.expect(R1MetadataServeEvents.ServeSucc.class, expectP, Direction.OUT);
  }
  
  public static TestContext eMetadataServeStop(TestContext tc, Port expectP) {
    return tc.expect(R1MetadataServeEvents.Stop.class, expectP, Direction.OUT);
  }
  
  public static TestContext eMetadataServeStopAck(TestContext tc, Port expectP) {
    return tc.expect(R1MetadataServeEvents.StopAck.class, expectP, Direction.OUT);
  }
  //*****************************************************HASH***********************************************************
  public static TestContext eHashSucc(TestContext tc, Port expectP) {
    return tc.expect(R1HashEvents.HashSucc.class, expectP, Direction.OUT);
  }
  
  public static TestContext eHashReq(TestContext tc, Port expectP) {
    return tc.expect(R1HashEvents.HashReq.class, expectP, Direction.OUT);
  }
  
  public static TestContext eHashStop(TestContext tc, Port expectP) {
    return tc.expect(R1HashEvents.HashStop.class, expectP, Direction.OUT);
  }
  
  public static TestContext eHashStopAck(TestContext tc, Port expectP) {
    return tc.expect(R1HashEvents.HashStopAck.class, expectP, Direction.OUT);
  }
  //**************************************************FUTURES***********************************************************
  public static Future<Msg, Msg> netNodeConnAcc() {
    return new FutureHelper.NetBEFuture<R2NodeConnMsgs.ConnectReq>(R2NodeConnMsgs.ConnectReq.class) {
      @Override
      public Msg get() {
        return msg.answer(content.accept());
      }
    };
  }
  
  public static Future<R1HashEvents.HashReq, R1HashEvents.HashSucc> gashSucc() {
    return new BasicFuture<R1HashEvents.HashReq, R1HashEvents.HashSucc>() {
      @Override
      public R1HashEvents.HashSucc get() {
        return event.success();
      }
    };
  }

  public static Future<R1HashEvents.HashReq, R1HashEvents.HashFail> hashFail() {
    return new BasicFuture<R1HashEvents.HashReq, R1HashEvents.HashFail>() {
      @Override
      public R1HashEvents.HashFail get() {
        return event.fail();
      }
    };
  }

  public static Future<R1HashEvents.HashStop, R1HashEvents.HashStopAck> hashStop() {
    return new BasicFuture<R1HashEvents.HashStop, R1HashEvents.HashStopAck>() {

      @Override
      public R1HashEvents.HashStopAck get() {
        return event.ack();
      }
    };
  }
}
