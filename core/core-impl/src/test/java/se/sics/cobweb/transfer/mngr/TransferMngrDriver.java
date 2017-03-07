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
package se.sics.cobweb.transfer.mngr;

import java.util.LinkedList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.cobweb.transfer.mngr.event.TransferCtrlE;
import se.sics.cobweb.transfer.mngr.event.TransferMngrE;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Handler;
import se.sics.kompics.KompicsEvent;
import se.sics.kompics.Negative;
import se.sics.kompics.Positive;
import se.sics.kompics.Start;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class TransferMngrDriver extends ComponentDefinition {

  private static final Logger LOG = LoggerFactory.getLogger(TransferMngrDriver.class);

  private final Positive<TransferMngrPort> transferMngr = requires(TransferMngrPort.class);
  private final Negative<TransferCtrlPort> transferCtrl = provides(TransferCtrlPort.class);
  private final LinkedList<KompicsEvent> transferMngrEvents;
  
  public TransferMngrDriver(Init init) {
    LOG.info("initiating");
    transferMngrEvents = init.transferMngrEvents;

    subscribe(handleStart, control);
    subscribe(handleSetupSuccess, transferMngr);
    subscribe(handleStartSuccess, transferMngr);
    subscribe(handleStopSuccess, transferMngr);
    subscribe(handleClean, transferCtrl);
  }

  Handler handleStart = new Handler<Start>() {

    @Override
    public void handle(Start event) {
      LOG.info("starting");
      if(!transferMngrEvents.isEmpty()) {
        trigger(transferMngrEvents.removeFirst(), transferMngr);
      }
    }
  };
  
  Handler handleSetupSuccess = new Handler<TransferMngrE.SetupSuccess>() {
    @Override
    public void handle(TransferMngrE.SetupSuccess event) {
      LOG.info("{}", event);
      if(!transferMngrEvents.isEmpty()) {
        trigger(transferMngrEvents.removeFirst(), transferMngr);
      }
    }
  };
  
  Handler handleStartSuccess = new Handler<TransferMngrE.StartSuccess>() {
    @Override
    public void handle(TransferMngrE.StartSuccess event) {
      LOG.info("{}", event);
      if(!transferMngrEvents.isEmpty()) {
        trigger(transferMngrEvents.removeFirst(), transferMngr);
      }
    }
  };
  
  Handler handleStopSuccess = new Handler<TransferMngrE.StopSuccess>() {
    @Override
    public void handle(TransferMngrE.StopSuccess event) {
      LOG.info("{}", event);
      if(!transferMngrEvents.isEmpty()) {
        trigger(transferMngrEvents.removeFirst(), transferMngr);
      }
    }
  };
  
  Handler handleClean = new Handler<TransferCtrlE.CleanReq>() {
    @Override
    public void handle(TransferCtrlE.CleanReq req) {
      LOG.info("{}", req);
      answer(req, req.complete());
    }
  };

  public static class Init extends se.sics.kompics.Init<TransferMngrDriver> {
    public final LinkedList<KompicsEvent> transferMngrEvents;
    
    public Init(LinkedList<KompicsEvent> connMngrEvents) {
      this.transferMngrEvents = connMngrEvents;
    }
  }
}