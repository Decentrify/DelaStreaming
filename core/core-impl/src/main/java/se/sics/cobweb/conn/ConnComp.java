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
package se.sics.cobweb.conn;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.cobweb.conn.event.ConnE;
import se.sics.kompics.Component;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.ComponentProxy;
import se.sics.kompics.Handler;
import se.sics.kompics.Negative;
import se.sics.kompics.Positive;
import se.sics.kompics.Start;
import se.sics.kompics.network.Network;
import se.sics.kompics.timer.Timer;
import se.sics.ktoolbox.croupier.CroupierPort;
import se.sics.ktoolbox.croupier.event.CroupierSample;
import se.sics.ktoolbox.nutil.fsm.MultiFSM;
import se.sics.ktoolbox.nutil.fsm.api.FSMException;
import se.sics.ktoolbox.nutil.fsm.genericsetup.OnFSMExceptionAction;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayId;
import se.sics.ktoolbox.util.network.KAddress;
import se.sics.ktoolbox.util.other.Container;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class ConnComp extends ComponentDefinition {

  private static final Logger LOG = LoggerFactory.getLogger(ConnComp.class);
  private final String logPrefix;

  private final Negative<ConnPort> conn = provides(ConnPort.class);
  private final Positive<Network> network = requires(Network.class);
  private final Positive<Timer> timer = requires(Timer.class);
  private final Positive<CroupierPort> croupier = requires(CroupierPort.class);
  //********************************************************************************************************************
  private final KAddress selfAdr;
  //********************************************************************************************************************
  private MultiFSM leecherFSMs;
  private MultiFSM seederFSMs;

  public ConnComp(Init init) {
    this.selfAdr = init.selfAdr;
    logPrefix = "<nid:" + selfAdr.getId() + ">";
    LOG.info("{}initiating...", logPrefix);

    createLeecherConnFSM();
    createSeederConnFSM();
    subscribe(handleStart, control);
    subscribe(handleSamples, croupier);
  }

  private void createLeecherConnFSM() {
    CLeecherHandleInternal.Builder leecherInternal = new CLeecherHandleInternal.Builder();
    CLeecherHandleExternal leecherExternal = new CLeecherHandleExternal(selfAdr, network, conn);
    OnFSMExceptionAction oexa = new OnFSMExceptionAction() {
      @Override
      public void handle(FSMException ex) {
        throw new RuntimeException(ex);
      }
    };
    try {
      leecherFSMs = CLeecherHandleFSM.multifsm(leecherExternal, leecherInternal, oexa);
      leecherFSMs.setProxy(proxy);
      leecherFSMs.setupHandlers();
    } catch (FSMException ex) {
      throw new RuntimeException(ex);
    }
  }

  private void createSeederConnFSM() {
    CSeederHandleInternal.Builder isb = new CSeederHandleInternal.Builder();
    CSeederHandleExternal es = new CSeederHandleExternal(selfAdr, conn, network);
    OnFSMExceptionAction oexa = new OnFSMExceptionAction() {
      @Override
      public void handle(FSMException ex) {
        throw new RuntimeException(ex);
      }
    };
    try {
      seederFSMs = CSeederHandleFSM.multifsm(es, isb, oexa);
      seederFSMs.setProxy(proxy);
      seederFSMs.setupHandlers();
    } catch (FSMException ex) {
      throw new RuntimeException(ex);
    }
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
  //********************************************************************************************************************
  Handler handleSamples = new Handler<CroupierSample<ConnHandleView>>() {
    @Override
    public void handle(CroupierSample<ConnHandleView> sample) {
      LOG.trace("{}{}", logPrefix, sample);
      for (Container<KAddress, ConnHandleView> peer : sample.publicSample.values()) {
        OverlayId torrentId = peer.getContent().torrentId;
        KAddress seederAdr = peer.getSource();
        trigger(new ConnE.SeederSample(torrentId, seederAdr), conn);
      }
    }
  };

  //******************************************INTROSPECTION - TESTING***************************************************
  protected MultiFSM getLeecherFSM() {
    return leecherFSMs;
  }

  protected MultiFSM getSeederFSM() {
    return seederFSMs;
  }

  public static class Init extends se.sics.kompics.Init<ConnComp> {

    public final KAddress selfAdr;

    public Init(KAddress selfAdr) {
      this.selfAdr = selfAdr;
    }
  }

  public static interface Creator {

    public Component create(ComponentProxy proxy, KAddress selfAdr);

    public void connect(ComponentProxy proxy, Component connComp, Positive<Network> networkPort,
      Positive<Timer> timerPort, Positive<CroupierPort> croupierPort);

    public void start(ComponentProxy proxy, Component connComp);
  }

  public static final DefaultCreator DEFAULT_CREATOR = new DefaultCreator();
  
  public static class DefaultCreator implements Creator {

    @Override
    public Component create(ComponentProxy proxy, KAddress selfAdr) {
      ConnComp.Init init = new ConnComp.Init(selfAdr);
      Component connComp = proxy.create(ConnComp.class, init);
      return connComp;
    }

    @Override
    public void connect(ComponentProxy proxy, Component connComp, Positive<Network> networkPort,
      Positive<Timer> timerPort, Positive<CroupierPort> croupierPort) {
      proxy.connect(connComp.getNegative(Network.class), networkPort);
      proxy.connect(connComp.getNegative(Timer.class), timerPort);
      proxy.connect(connComp.getNegative(CroupierPort.class), croupierPort);
    }

    @Override
    public void start(ComponentProxy proxy, Component connComp) {
      proxy.trigger(Start.event, connComp.control());
    }

  }
}
