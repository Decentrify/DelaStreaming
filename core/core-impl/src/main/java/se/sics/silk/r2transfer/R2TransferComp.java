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
package se.sics.silk.r2transfer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.ComponentProxy;
import se.sics.kompics.Handler;
import se.sics.kompics.Negative;
import se.sics.kompics.Positive;
import se.sics.kompics.Start;
import se.sics.kompics.fsm.FSMException;
import se.sics.kompics.fsm.FSMStateName;
import se.sics.kompics.fsm.MultiFSM;
import se.sics.kompics.fsm.OnFSMExceptionAction;
import se.sics.kompics.fsm.id.FSMIdentifierFactory;
import se.sics.kompics.util.Identifier;
import se.sics.ktoolbox.util.network.KAddress;
import se.sics.silk.r2conn.R2ConnLeecherPort;
import se.sics.silk.r2conn.R2ConnSeederPort;
import se.sics.silk.r2torrent.R2TorrentTransferPort;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class R2TransferComp extends ComponentDefinition {

  private static final Logger LOG = LoggerFactory.getLogger(R2TransferComp.class);
  private String logPrefix;

  private Ports ports;
  private MultiFSM metadataMngr;
  private MultiFSM hashMngr;
  private R1Metadata.ES metadatMngrES;
  private R1Hash.ES hashMngrES;

  public R2TransferComp(Init init) {
    logPrefix = "<" + init.selfAdr.getId() + ">";
    ports = new Ports(proxy);
    subscribe(handleStart, control);
    setupFSM(init);
  }

  private void setupFSM(Init init) {
    metadatMngrES = new R1Metadata.ES(ports);
    hashMngrES = new R1Hash.ES(ports);

    metadatMngrES.setProxy(proxy);
    hashMngrES.setProxy(proxy);
    try {
      OnFSMExceptionAction oexa = new OnFSMExceptionAction() {
        @Override
        public void handle(FSMException ex) {
          throw new RuntimeException(ex);
        }
      };
      FSMIdentifierFactory fsmIdFactory = config().getValue(FSMIdentifierFactory.CONFIG_KEY, FSMIdentifierFactory.class);
      metadataMngr = R1Metadata.FSM.multifsm(fsmIdFactory, metadatMngrES, oexa);
      hashMngr = R1Hash.FSM.multifsm(fsmIdFactory, hashMngrES, oexa);
    } catch (FSMException ex) {
      throw new RuntimeException(ex);
    }
  }

  Handler handleStart = new Handler<Start>() {

    @Override
    public void handle(Start event) {
      LOG.info("{}starting", logPrefix);
      metadataMngr.setupHandlers();
      hashMngr.setupHandlers();
    }
  };

  //******************************************TESTING HELPERS***********************************************************
  FSMStateName getMetadataState(Identifier baseId) {
    return metadataMngr.getFSMState(baseId);
  }

  boolean activeMetadataFSM(Identifier baseId) {
    return metadataMngr.activeFSM(baseId);
  }

  FSMStateName getHashState(Identifier baseId) {
    return hashMngr.getFSMState(baseId);
  }

  boolean activeHashFSM(Identifier baseId) {
    return hashMngr.activeFSM(baseId);
  }
  //********************************************************************************************************************

  public static class Ports {

    public final Negative<R2TorrentTransferPort> transfer;
    public final Positive<R2ConnLeecherPort> leecher;
    public final Positive<R2ConnSeederPort> seeder;

    public Ports(ComponentProxy proxy) {
      transfer = proxy.provides(R2TorrentTransferPort.class);
      leecher = proxy.requires(R2ConnLeecherPort.class);
      seeder = proxy.requires(R2ConnSeederPort.class);
    }
  }

  public static class Init extends se.sics.kompics.Init<R2TransferComp> {

    public final KAddress selfAdr;
    public final int retries = 5;
    public final long retryInterval = 1000;

    public Init(KAddress selfAdr) {
      this.selfAdr = selfAdr;
    }
  }
}
