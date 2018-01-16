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

import se.sics.kompics.Port;
import se.sics.kompics.network.Msg;
import se.sics.kompics.testing.Direction;
import se.sics.kompics.testing.Future;
import se.sics.kompics.testing.TestContext;
import se.sics.kompics.timer.SchedulePeriodicTimeout;
import se.sics.silk.r2torrent.conn.event.R1TorrentSeederEvents;
import se.sics.silk.r2torrent.conn.event.R2NodeSeederEvents;
import se.sics.silk.r2torrent.conn.msg.R2NodeConnMsgs;
import se.sics.silk.r2torrent.event.R1MetadataGetEvents;
import se.sics.silk.r2torrent.event.R2TorrentCtrlEvents;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class TorrentTestHelper {
  //***************************************************NETWORK**********************************************************
  public static TestContext netNodeConnAcc(TestContext tc, Port network) {
    Future<Msg, Msg> f = netNodeConnAcc();
    tc = tc.answerRequest(Msg.class, network, f);
    tc = tc.trigger(f, network);
    return tc;
  }
  //*****************************************************NODE_SEEDER****************************************************
  public static TestContext nodeSeederConnSucc(TestContext tc, Port expectP) {
    return tc.expect(R2NodeSeederEvents.ConnectSucc.class, expectP, Direction.OUT);
  }
  //***************************************************TORRENT_SEEDER***************************************************
  public static TestContext torrentSeederConnReq(TestContext tc, Port expectP) {
    return tc.expect(R1TorrentSeederEvents.ConnectReq.class, expectP, Direction.OUT);
  }
  public static TestContext torrentSeederConnSucc(TestContext tc, Port expectP) {
    return tc.expect(R1TorrentSeederEvents.ConnectSucc.class, expectP, Direction.OUT);
  }
  //***************************************************TIMER************************************************************
  public static TestContext timerSchedulePeriodicTimeout(TestContext tc, Port timerP) {
    return tc.expect(SchedulePeriodicTimeout.class, timerP, Direction.OUT);
  }
  //************************************************METADATA_GET********************************************************
  public static TestContext metadataGetSucc(TestContext tc, Port expectP) {
    return tc.expect(R1MetadataGetEvents.MetaGetSucc.class, expectP, Direction.OUT);
  }
  //***************************************************TORRENT**********************************************************
  public static TestContext torrentMetaGetSucc(TestContext tc, Port expectP) {
    return tc.expect(R2TorrentCtrlEvents.MetaGetSucc.class, expectP, Direction.OUT);
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
}

