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
package se.sics.silk.r2torrent;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.kompics.Channel;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.ComponentProxy;
import se.sics.kompics.Handler;
import se.sics.kompics.Negative;
import se.sics.kompics.Positive;
import se.sics.kompics.Start;
import se.sics.kompics.fsm.FSMException;
import se.sics.kompics.fsm.FSMInternalState;
import se.sics.kompics.fsm.FSMStateName;
import se.sics.kompics.fsm.MultiFSM;
import se.sics.kompics.fsm.OnFSMExceptionAction;
import se.sics.kompics.fsm.id.FSMIdentifierFactory;
import se.sics.kompics.network.Network;
import se.sics.kompics.timer.Timer;
import se.sics.kompics.util.Identifier;
import se.sics.ktoolbox.util.network.KAddress;
import se.sics.silk.r2torrent.conn.R1TorrentLeecher;
import se.sics.silk.r2torrent.conn.R1TorrentSeeder;
import se.sics.silk.r2torrent.conn.R2NodeLeecher;
import se.sics.silk.r2torrent.conn.R2NodeSeeder;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class R2TorrentComp extends ComponentDefinition {

  private static final Logger LOG = LoggerFactory.getLogger(R2TorrentComp.class);
  private String logPrefix;

  private Ports ports;
  private MultiFSM nodeSeeders;
  private MultiFSM nodeLeechers;
  private MultiFSM torrentSeeders;
  private MultiFSM torrentLeechers;
  private MultiFSM torrents;
  private MultiFSM metadataMngr;
  private MultiFSM hashMngr;
  private R2Torrent.ES torrentES;
  private R1MetadataGet.ES metadatMngrES;
  private R1Hash.ES hashMngrES;
  private R2NodeSeeder.ES nodeSeederES;
  private R2NodeLeecher.ES nodeLeecherES;
  private R1TorrentSeeder.ES torrentSeederES;
  private R1TorrentLeecher.ES torrentLeecherES;

  public R2TorrentComp(Init init) {
    logPrefix = "<" + init.selfAdr.getId() + ">";
    ports = new Ports(proxy);
    subscribe(handleStart, control);
    setupFSM(init);
  }

  private void setupFSM(Init init) {
    nodeSeederES = new R2NodeSeeder.ES(ports, init.selfAdr);
    nodeLeecherES = new R2NodeLeecher.ES(ports, init.selfAdr);
    torrentSeederES = new R1TorrentSeeder.ES(ports);
    torrentLeecherES = new R1TorrentLeecher.ES(ports);
    torrentES = new R2Torrent.ES(ports);
    metadatMngrES = new R1MetadataGet.ES(ports);
    hashMngrES = new R1Hash.ES(ports);

    nodeSeederES.setProxy(proxy);
    nodeLeecherES.setProxy(proxy);
    torrentSeederES.setProxy(proxy);
    torrentLeecherES.setProxy(proxy);
    torrentES.setProxy(proxy);
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
      nodeSeeders = R2NodeSeeder.FSM.multifsm(fsmIdFactory, nodeSeederES, oexa);
      nodeLeechers = R2NodeLeecher.FSM.multifsm(fsmIdFactory, nodeLeecherES, oexa);
      torrentSeeders = R1TorrentSeeder.FSM.multifsm(fsmIdFactory, torrentSeederES, oexa);
      torrentLeechers = R1TorrentLeecher.FSM.multifsm(fsmIdFactory, torrentLeecherES, oexa);
      torrents = R2Torrent.FSM.multifsm(fsmIdFactory, torrentES, oexa);
      metadataMngr = R1MetadataGet.FSM.multifsm(fsmIdFactory, metadatMngrES, oexa);
      hashMngr = R1Hash.FSM.multifsm(fsmIdFactory, hashMngrES, oexa);
    } catch (FSMException ex) {
      throw new RuntimeException(ex);
    }
  }

  Handler handleStart = new Handler<Start>() {

    @Override
    public void handle(Start event) {
      LOG.info("{}starting", logPrefix);
      nodeSeeders.setupHandlers();
      nodeLeechers.setupHandlers();
      torrentSeeders.setupHandlers();
      torrentLeechers.setupHandlers();
      torrents.setupHandlers();
      metadataMngr.setupHandlers();
      hashMngr.setupHandlers();
    }
  };

  //******************************************TESTING HELPERS***********************************************************
  FSMInternalState getConnSeederIS(Identifier baseId) {
    return nodeSeeders.getFSMInternalState(baseId);
  }

  FSMStateName getConnSeederState(Identifier baseId) {
    return nodeSeeders.getFSMState(baseId);
  }

  boolean activeSeederFSM(Identifier baseId) {
    return nodeSeeders.activeFSM(baseId);
  }
  
  FSMStateName getConnLeecherState(Identifier baseId) {
    return nodeLeechers.getFSMState(baseId);
  }
  
  boolean activeLeecherFSM(Identifier baseId) {
    return nodeLeechers.activeFSM(baseId);
  }
  
  FSMStateName getTorrentState(Identifier baseId) {
    return torrents.getFSMState(baseId);
  }

  boolean activeTorrentFSM(Identifier baseId) {
    return torrents.activeFSM(baseId);
  }

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

    public final Negative<R2TorrentPort> loopbackSend;
    public final Positive<R2TorrentPort> loopbackSubscribe;
    public final Negative<R2TorrentCtrlPort> ctrl;
    public final Positive<Network> network;
    public final Positive<Timer> timer;

    public Ports(ComponentProxy proxy) {
      loopbackSend = proxy.provides(R2TorrentPort.class);
      loopbackSubscribe = proxy.requires(R2TorrentPort.class);
      proxy.connect(loopbackSend.getPair(), loopbackSubscribe.getPair(), Channel.TWO_WAY);
      ctrl = proxy.provides(R2TorrentCtrlPort.class);
      network = proxy.requires(Network.class);
      timer = proxy.requires(Timer.class);
    }
  }

  public static class Init extends se.sics.kompics.Init<R2TorrentComp> {

    public final KAddress selfAdr;

    public Init(KAddress selfAdr) {
      this.selfAdr = selfAdr;
    }
  }
}
