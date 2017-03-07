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
package se.sics.cobweb.mngr;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.cobweb.conn.ConnComp;
import se.sics.cobweb.conn.ConnPort;
import se.sics.cobweb.overlord.OverlordCreator;
import se.sics.cobweb.overlord.conn.api.ConnectionDecider;
import se.sics.cobweb.transfer.handle.LeecherHandlePort;
import se.sics.cobweb.transfer.handle.SeederHandlePort;
import se.sics.cobweb.transfer.handlemngr.HandleMngrComp;
import se.sics.cobweb.transfer.handlemngr.HandleMngrPort;
import se.sics.cobweb.transfer.instance.TransferPort;
import se.sics.cobweb.transfer.mngr.TransferMngrComp;
import se.sics.cobweb.transfer.mngr.TransferMngrPort;
import se.sics.kompics.Component;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Handler;
import se.sics.kompics.Negative;
import se.sics.kompics.Positive;
import se.sics.kompics.Start;
import se.sics.kompics.network.Network;
import se.sics.kompics.timer.Timer;
import se.sics.ktoolbox.croupier.CroupierPort;
import se.sics.ktoolbox.util.network.KAddress;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class SpiderComp extends ComponentDefinition {

  private static final Logger LOG = LoggerFactory.getLogger(SpiderComp.class);
  private final String logPrefix;

  private final Negative<SpiderCtrlPort> ctrlPort = provides(SpiderCtrlPort.class);
  private final Negative<TransferMngrPort> transferMngrPort = provides(TransferMngrPort.class);
  private final Positive<Network> networkPort = requires(Network.class);
  private final Positive<Timer> timerPort = requires(Timer.class);
  private final Positive<CroupierPort> croupierPort = requires(CroupierPort.class);

  private Component connComp;
  private Component handleMngrComp;
  private Component transferMngrComp;
  private Component overlordComp;

  private final Init init;

  public SpiderComp(Init init) {
    logPrefix = "<nid:" + init.selfAdr.getId() + ">";
    this.init = init;
    connect();
    subscribe(handleStart, control);
    subscribe(handleSetup, ctrlPort);
  }

  //********************************************************************************************************************
  Handler handleStart = new Handler<Start>() {
    @Override
    public void handle(Start event) {
      LOG.info("{}starting", logPrefix);
      
    }
  };

  @Override
  public void tearDown() {
    LOG.info("{}tear down", logPrefix);
  }

  Handler handleSetup = new Handler<SpiderCtrlE.SetupReq>() {
    @Override
    public void handle(SpiderCtrlE.SetupReq req) {
      LOG.info("{}setup", logPrefix);
//      connect();
//      start();
      answer(req, req.success());
    }
  };

  private void connect() {
    connComp = init.connCompCreator.create(proxy, init.selfAdr);
    init.connCompCreator.connect(proxy, connComp, networkPort, timerPort, croupierPort);

    handleMngrComp = init.handleMngrCompCreator.create(proxy, init.selfAdr);
    init.handleMngrCompCreator.connect(proxy, handleMngrComp, networkPort);

    transferMngrComp = init.transferMngrCompCreator.create(proxy, init.selfAdr);
    init.transferMngrCompCreator.connect(proxy, transferMngrComp, handleMngrComp.getPositive(LeecherHandlePort.class),
      handleMngrComp.getPositive(SeederHandlePort.class), transferMngrPort);

    overlordComp = init.overlordCreator.create(proxy, init.selfAdr, init.connSeederSideDecider,
      init.connLeecherSideDecider);
    init.overlordCreator.connect(proxy, overlordComp, connComp.getPositive(ConnPort.class), handleMngrComp.getPositive(
      HandleMngrPort.class), transferMngrComp.getPositive(TransferPort.class));
  }

  private void start() {
    init.connCompCreator.start(proxy, connComp);
    init.handleMngrCompCreator.start(proxy, handleMngrComp);
    init.transferMngrCompCreator.start(proxy, transferMngrComp);
    init.overlordCreator.start(proxy, overlordComp);
  }

  public static class Init extends se.sics.kompics.Init<SpiderComp> {

    public final KAddress selfAdr;
    public final ConnectionDecider.SeederSide connSeederSideDecider;
    public final ConnectionDecider.LeecherSide connLeecherSideDecider;
    public final ConnComp.Creator connCompCreator;
    public final HandleMngrComp.Creator handleMngrCompCreator;
    public final TransferMngrComp.Creator transferMngrCompCreator;
    public final OverlordCreator overlordCreator;

    public Init(KAddress selfAdr, ConnectionDecider.SeederSide connSeederSideDecider,
      ConnectionDecider.LeecherSide connLeecherSideDecider, ConnComp.Creator connCompCreator,
      HandleMngrComp.Creator handleMngrCompCreator, TransferMngrComp.Creator transferMngrCompCreator,
      OverlordCreator overlordCreator) {
      this.selfAdr = selfAdr;
      this.overlordCreator = overlordCreator;
      this.connLeecherSideDecider = connLeecherSideDecider;
      this.connSeederSideDecider = connSeederSideDecider;
      this.transferMngrCompCreator = transferMngrCompCreator;
      this.connCompCreator = connCompCreator;
      this.handleMngrCompCreator = handleMngrCompCreator;
    }
  }
}
