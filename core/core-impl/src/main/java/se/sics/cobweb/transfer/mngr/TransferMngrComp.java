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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.cobweb.OverlayComp;
import se.sics.cobweb.transfer.TransferComp;
import se.sics.cobweb.transfer.handle.LeecherHandlePort;
import se.sics.cobweb.transfer.handle.SeederHandlePort;
import se.sics.cobweb.transfer.instance.TransferPort;
import se.sics.cobweb.transfer.mngr.event.TransferFaultE;
import se.sics.kompics.Component;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.ComponentProxy;
import se.sics.kompics.Fault;
import se.sics.kompics.Handler;
import se.sics.kompics.Negative;
import se.sics.kompics.Positive;
import se.sics.kompics.Start;
import se.sics.ktoolbox.nutil.fsm.MultiFSM;
import se.sics.ktoolbox.nutil.fsm.api.FSMException;
import se.sics.ktoolbox.nutil.fsm.genericsetup.OnFSMExceptionAction;
import se.sics.ktoolbox.util.Either;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayId;
import se.sics.ktoolbox.util.network.KAddress;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class TransferMngrComp extends ComponentDefinition {

  private static final Logger LOG = LoggerFactory.getLogger(TransferMngrComp.class);
  private final String logPrefix;

  //***************************************EXTERNALLY CONNECT TO********************************************************
  private final Negative<TransferMngrPort> transferMngrPort = provides(TransferMngrPort.class);
  private final Negative<TransferPort> transferPort = provides(TransferPort.class);
  private final Positive<LeecherHandlePort> leecherHandlePort = requires(LeecherHandlePort.class);
  private final Positive<SeederHandlePort> seederHandlePort = requires(SeederHandlePort.class);
  //**********************************************INTERNAL**************************************************************
  private final Positive<TransferCtrlPort> transferCtrlPort = requires(TransferCtrlPort.class);
  //********************************************************************************************************************
  private TransferMngrExternal es;
  private MultiFSM transferFSMs;

  public TransferMngrComp(Init init) {
    logPrefix = "<nid:" + init.selfAdr.getId() + ">";
    LOG.info("{}initiating...", logPrefix);

    createTransferFSM(init);
    subscribe(handleStart, control);
  }

  private void createTransferFSM(Init init) {
    TransferMngrInternal.Builder isb = new TransferMngrInternal.Builder();
    es = new TransferMngrExternal(init, transferCtrlPort, transferPort, leecherHandlePort, seederHandlePort);
    OnFSMExceptionAction oexa = new OnFSMExceptionAction() {
      @Override
      public void handle(FSMException ex) {
        throw new RuntimeException(ex);
      }
    };
    try {
      transferFSMs = TransferMngrFSM.multifsm(es, isb, oexa);
      transferFSMs.setProxy(proxy);
    } catch (FSMException ex) {
      throw new RuntimeException(ex);
    }
  }
  //********************************************************************************************************************
  Handler handleStart = new Handler<Start>() {
    @Override
    public void handle(Start event) {
      LOG.info("{}starting", logPrefix);
      transferFSMs.setupHandlers();
    }
  };

  @Override
  public void tearDown() {
    LOG.info("{}tear down", logPrefix);
    if (!transferFSMs.isEmpty()) {
      throw new RuntimeException("tearing down without cleaning");
    }
  }

  @Override
  public Fault.ResolveAction handleFault(Fault fault) {
    OverlayId torrentId = ((OverlayComp) fault.getSource()).overlayId();
    LOG.warn("{}transfer comp:{} fault:{}", new Object[]{logPrefix, torrentId, fault.getCause()});
    trigger(new TransferFaultE(torrentId, Either.left(fault.getCause())), onSelf);
    return Fault.ResolveAction.RESOLVED;
  }

  //*************************************INTROSPECTION - TESTING********************************************************
  protected boolean isEmpty() {
    return transferFSMs.isEmpty();
  }

  protected MultiFSM getTransferFSM() {
    return transferFSMs;
  }

  public static class Init extends se.sics.kompics.Init<TransferMngrComp> {

    public final KAddress selfAdr;
    public final TransferComp.Creator transferCreator;

    public Init(KAddress selfAdr, TransferComp.Creator transferCreator) {
      this.selfAdr = selfAdr;
      this.transferCreator = transferCreator;
    }
  }

  public static interface Creator {

    public Component create(ComponentProxy proxy, KAddress selfAdr);

    public void connect(ComponentProxy proxy, Component transferMngrComp, Positive<LeecherHandlePort> leecherHandlePort,
      Positive<SeederHandlePort> seederHandlePort, Negative<TransferMngrPort> transferMngrPort);

    public void start(ComponentProxy proxy, Component transferMngrComp);
  }

  public static final DefaultCreator DEFAULT_CREATOR = new DefaultCreator();
  public static class DefaultCreator implements Creator {

    @Override
    public Component create(ComponentProxy proxy, KAddress selfAdr) {
      TransferMngrComp.Init transferMngrInit = new TransferMngrComp.Init(selfAdr, TransferComp.DEFAULT_CREATOR);
      Component transferMngrComp = proxy.create(TransferMngrComp.class, transferMngrInit);
      return transferMngrComp;
    }

    @Override
    public void connect(ComponentProxy proxy, Component transferMngrComp, Positive<LeecherHandlePort> leecherHandlePort,
      Positive<SeederHandlePort> seederHandlePort, Negative<TransferMngrPort> transferMngrPort) {
      proxy.connect(transferMngrComp.getNegative(LeecherHandlePort.class), leecherHandlePort);
      proxy.connect(transferMngrComp.getNegative(SeederHandlePort.class), seederHandlePort);
      proxy.connect(transferMngrPort, transferMngrComp.getPositive(TransferMngrPort.class));
    }

    @Override
    public void start(ComponentProxy proxy, Component transferMngrComp) {
      proxy.trigger(Start.event, transferMngrComp.control());
    }

  }
}
