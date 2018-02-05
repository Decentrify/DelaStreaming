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
import java.util.UUID;
import se.sics.kompics.Port;
import se.sics.kompics.testing.Direction;
import se.sics.kompics.testing.Future;
import se.sics.kompics.testing.TestContext;
import se.sics.kompics.timer.CancelPeriodicTimeout;
import se.sics.kompics.timer.SchedulePeriodicTimeout;
import se.sics.ktoolbox.util.network.basic.BasicContentMsg;
import se.sics.nstream.storage.durable.events.DStreamConnect;
import se.sics.nstream.storage.durable.events.DStreamDisconnect;
import se.sics.silk.FutureHelper.BasicFuture;
import se.sics.silk.r2torrent.torrent.event.R1FileDownloadEvents;
import se.sics.silk.r2torrent.transfer.events.R1TransferSeederEvents;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class TorrentTestHelper {

  //***************************************************TIMER************************************************************
  public static TestContext eTimerSchedulePeriodicTimeout(TestContext tc, Port timerP) {
    return tc.expect(SchedulePeriodicTimeout.class, timerP, Direction.OUT);
  }

  //*****************************************************FILE***********************************************************

  public static TestContext tFileOpen(TestContext tc, Port triggerP, R1FileDownloadEvents.Start event) {
    return tc.trigger(event, triggerP);
  }

  public static TestContext tStreamOpened(TestContext tc, Port triggerP, DStreamConnect.Request event) {
    return tc.trigger(event, triggerP);
  }

  public static TestContext tFileClose(TestContext tc, Port triggerP, R1FileDownloadEvents.Close event) {
    return tc.trigger(event, triggerP);
  }

  public static TestContext tStreamClosed(TestContext tc, Port triggerP, DStreamDisconnect.Request event) {
    return tc.trigger(event, triggerP);
  }

  public static TestContext tFileConnect(TestContext tc, Port triggerP, R1FileDownloadEvents.Connect req) {
    return tc.trigger(req, triggerP);
  }

  public static TestContext eStreamCloseReq(TestContext tc, Port expectP) {
    return tc.expect(DStreamDisconnect.Request.class, expectP, Direction.OUT);
  }

  public static TestContext streamOpenSucc(TestContext tc, Port streamCtrl, int filePos) {
    Future f = new FutureHelper.BasicFuture<DStreamConnect.Request, DStreamConnect.Success>() {

      @Override
      public DStreamConnect.Success get() {
        return event.success(filePos);
      }
    };
    return tc
      .answerRequest(DStreamConnect.Request.class, streamCtrl, f)
      .trigger(f, streamCtrl);
  }

  public static TestContext streamCloseSucc(TestContext tc, Port streamCtrl) {
    Future f = new FutureHelper.BasicFuture<DStreamDisconnect.Request, DStreamDisconnect.Success>() {

      @Override
      public DStreamDisconnect.Success get() {
        return event.success();
      }
    };
    return tc
      .answerRequest(DStreamDisconnect.Request.class, streamCtrl, f)
      .trigger(f, streamCtrl);
  }
  
  //**********************************************TRANSFER SEEDER*******************************************************
  public static TestContext transferSeederConnectSucc(TestContext tc, Port expectP, Port triggerP) {
    Future f = new BasicFuture<R1TransferSeederEvents.Connect, R1TransferSeederEvents.Connected>() {

      @Override
      public R1TransferSeederEvents.Connected get() {
        return event.success();
      }
    };
    return tc
      .answerRequest(R1TransferSeederEvents.Connect.class, expectP, f)
      .trigger(f, triggerP);
  }
  
  //**********************************************STORAGE STREAMS*******************************************************
  public static TestContext storageStreamConnected(TestContext tc, Port expectP, Port triggerP) {
    Future f = new FutureHelper.BasicFuture<DStreamConnect.Request, DStreamConnect.Success>() {

      @Override
      public DStreamConnect.Success get() {
        return event.success(0);
      }
    };
    tc = tc
      .answerRequest(DStreamConnect.Request.class, expectP, f) //1
      .trigger(f, triggerP);//2
    return tc;
  }
  
  public static TestContext storageStreamDisconnected(TestContext tc, Port expectP, Port triggerP) {
    Future f = new BasicFuture<DStreamDisconnect.Request, DStreamDisconnect.Success>() {

      @Override
      public DStreamDisconnect.Success get() {
        return event.success();
      }
    };
    return tc
      .answerRequest(DStreamDisconnect.Request.class, expectP, f)
      .trigger(f, triggerP);
  }

  public static TestContext eNetPayload(TestContext tc, Class payloadType, Port network) {
    Predicate payloadP = new PredicateHelper.ContentPredicate(payloadType);
    Predicate p = new PredicateHelper.MsgPredicate(PredicateHelper.TRUE_P, payloadP);
    return tc.expect(BasicContentMsg.class, network, Direction.OUT);
  }
  
  public static TestContext eSchedulePeriodicTimer(TestContext tc, Class payloadType, Port timer) {
    return tc.expect(SchedulePeriodicTimeout.class, timer, Direction.OUT);
  }

  public static TestContext eSchedulePeriodicTimer(TestContext tc, Class payloadType, Port timer, 
    CancelPeriodicTimerPredicate cp) {
    Predicate p = new Predicate<SchedulePeriodicTimeout>() {
      @Override
      public boolean apply(SchedulePeriodicTimeout t) {
        cp.setTimerId(t.getTimeoutEvent().getTimeoutId());
        return payloadType.isAssignableFrom(t.getTimeoutEvent().getClass());
      }
    };
    return tc.expect(SchedulePeriodicTimeout.class, p, timer, Direction.OUT);
  }
  
  public static TestContext eCancelPeriodicTimer(TestContext tc, Port timer) {
    return tc.expect(CancelPeriodicTimeout.class, timer, Direction.OUT);
  }
  
  public static TestContext eCancelPeriodicTimer(TestContext tc, Port timer, CancelPeriodicTimerPredicate cp) {
    return tc.expect(CancelPeriodicTimeout.class, cp, timer, Direction.OUT);
  }
  
  public static class CancelPeriodicTimerPredicate implements Predicate<CancelPeriodicTimeout> {
    private UUID timerId;
    
    public void setTimerId(UUID timerId) {
      this.timerId = timerId;
    }

    @Override
    public boolean apply(CancelPeriodicTimeout timeout) {
      return timeout.getTimeoutId().equals(timerId);
    }
  }
}
