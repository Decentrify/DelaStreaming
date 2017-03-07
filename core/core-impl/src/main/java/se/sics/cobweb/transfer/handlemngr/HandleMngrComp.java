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
package se.sics.cobweb.transfer.handlemngr;

import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.cobweb.transfer.handle.LeecherHandleComp;
import se.sics.cobweb.transfer.handle.LeecherHandleCtrlPort;
import se.sics.cobweb.transfer.handle.LeecherHandlePort;
import se.sics.cobweb.transfer.handle.SeederHandleComp;
import se.sics.cobweb.transfer.handle.SeederHandleCtrlPort;
import se.sics.cobweb.transfer.handle.SeederHandlePort;
import se.sics.cobweb.transfer.handlemngr.event.HandleMngrE;
import se.sics.cobweb.util.EventHandleIdExtractor;
import se.sics.cobweb.util.HandleId;
import se.sics.cobweb.util.MsgHandleIdExtractor;
import se.sics.kompics.Component;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.ComponentProxy;
import se.sics.kompics.Handler;
import se.sics.kompics.Kill;
import se.sics.kompics.Negative;
import se.sics.kompics.Positive;
import se.sics.kompics.Start;
import se.sics.kompics.network.Network;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayId;
import se.sics.ktoolbox.util.network.KAddress;
import se.sics.ktoolbox.util.network.ports.One2NChannel;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class HandleMngrComp extends ComponentDefinition {

  private static final Logger LOG = LoggerFactory.getLogger(HandleMngrComp.class);
  private final String logPrefix;

  private static final int STARTING_ACTIVE_BLOCKS = 10;

  //********************************************************************************************************************
  private final Negative<HandleMngrPort> handleMngrPort = provides(HandleMngrPort.class);
  private final Negative<LeecherHandlePort> leecherHandlePort = provides(LeecherHandlePort.class);
  private final Negative<LeecherHandleCtrlPort> leecherHandleCtrlPort = provides(LeecherHandleCtrlPort.class);
  private final Negative<SeederHandlePort> seederHandlePort = provides(SeederHandlePort.class);
  private final Negative<SeederHandleCtrlPort> seederHandleCtrlPort = provides(SeederHandleCtrlPort.class);
  private final Positive<Network> networkPort = requires(Network.class);
  //********************************************************************************************************************
  private final One2NChannel<LeecherHandlePort> leecherHandle;
  private final One2NChannel<LeecherHandleCtrlPort> leecherHandleCtrl;
  private final One2NChannel<SeederHandlePort> seederHandle;
  private final One2NChannel<SeederHandleCtrlPort> seederHandleCtrl;
  private final One2NChannel<Network> network;
  //********************************************************************************************************************
  private final KAddress selfAdr;
  private final LeecherHandleCreator leecherHandleCreator;
  private final SeederHandleCreator seederHandleCreator;

  private final Map<HandleId, Component> handleComps = new HashMap<>();

  public HandleMngrComp(Init init) {
    selfAdr = init.selfAdr;
    logPrefix = "<pid:" + init.selfAdr.getId() + ">";

    leecherHandleCreator = init.leecherHandleCreator;
    seederHandleCreator = init.seederHandleCreator;

    leecherHandle = One2NChannel.getChannel(logPrefix + "handle-leecher", leecherHandlePort,
      new EventHandleIdExtractor());
    leecherHandleCtrl = One2NChannel.getChannel(logPrefix + "handleCtrl-leecher", leecherHandleCtrlPort,
      new EventHandleIdExtractor());
    seederHandle = One2NChannel.getChannel(logPrefix + "handle-seeder", seederHandlePort, new EventHandleIdExtractor());
    seederHandleCtrl = One2NChannel.getChannel(logPrefix + "handleCtrl-seeder", seederHandleCtrlPort,
      new EventHandleIdExtractor());
    network = One2NChannel.getChannel(logPrefix + "handle-network", networkPort, new MsgHandleIdExtractor());

    subscribe(handleStart, control);
    subscribe(handleLeecherConnect, handleMngrPort);
    subscribe(handleLeecherDisconnect, handleMngrPort);
    subscribe(handleSeederConnect, handleMngrPort);
    subscribe(handleSeederDisconnect, handleMngrPort);
  }

  Handler handleStart = new Handler<Start>() {
    @Override
    public void handle(Start event) {
      LOG.info("{}start", logPrefix);
    }
  };

  Handler handleLeecherConnect = new Handler<HandleMngrE.LeecherConnect>() {
    @Override
    public void handle(HandleMngrE.LeecherConnect req) {
      LOG.info("{}{}", logPrefix, req);
      connectLeecherHandle(req.torrentId, req.handleId, req.seederAdr);
      answer(req, req.success());
    }
  };

  Handler handleLeecherDisconnect = new Handler<HandleMngrE.LeecherDisconnect>() {
    @Override
    public void handle(HandleMngrE.LeecherDisconnect req) {
      LOG.info("{}{}", logPrefix, req);
      disconnectLeecherHandle(req.torrentId, req.handleId);
      answer(req, req.success());
    }
  };

  Handler handleSeederConnect = new Handler<HandleMngrE.SeederConnect>() {
    @Override
    public void handle(HandleMngrE.SeederConnect req) {
      LOG.info("{}{}", logPrefix, req);
      connectSeederHandle(req.torrentId, req.handleId, req.leecherAdr);
      answer(req, req.success());
    }
  };

  Handler handleSeederDisconnect = new Handler<HandleMngrE.SeederDisconnect>() {
    @Override
    public void handle(HandleMngrE.SeederDisconnect req) {
      LOG.info("{}{}", logPrefix, req);
      disconnectSeederHandle(req.torrentId, req.handleId);
      answer(req, req.success());
    }
  };

  public void connectLeecherHandle(OverlayId torrentId, HandleId handleId, KAddress seederAdr) {
    Component comp = leecherHandleCreator.
      connect(proxy, torrentId, handleId, selfAdr, seederAdr, STARTING_ACTIVE_BLOCKS);
    leecherHandle.addChannel(handleId, comp.getPositive(LeecherHandlePort.class));
    leecherHandleCtrl.addChannel(handleId, comp.getPositive(LeecherHandleCtrlPort.class));
    network.addChannel(handleId, comp.getNegative(Network.class));
    handleComps.put(handleId, comp);
    trigger(Start.event, comp.control());
  }

  public void disconnectLeecherHandle(OverlayId torrentId, HandleId handleId) {
    Component comp = handleComps.remove(handleId);
    leecherHandle.removeChannel(handleId, comp.getPositive(LeecherHandlePort.class));
    leecherHandleCtrl.removeChannel(handleId, comp.getPositive(LeecherHandleCtrlPort.class));
    network.removeChannel(handleId, comp.getNegative(Network.class));
    trigger(Kill.event, comp.control());
  }

  public void connectSeederHandle(OverlayId torrentId, HandleId handleId, KAddress seederAdr) {
    Component comp = seederHandleCreator.connect(proxy, torrentId, handleId, selfAdr, seederAdr);
    seederHandle.addChannel(handleId, comp.getPositive(SeederHandlePort.class));
    seederHandleCtrl.addChannel(handleId, comp.getPositive(SeederHandleCtrlPort.class));
    network.addChannel(handleId, comp.getNegative(Network.class));
    handleComps.put(handleId, comp);
    trigger(Start.event, comp.control());
  }

  public void disconnectSeederHandle(OverlayId torrentId, HandleId handleId) {
    Component comp = handleComps.remove(handleId);
    seederHandle.removeChannel(handleId, comp.getPositive(SeederHandlePort.class));
    seederHandleCtrl.removeChannel(handleId, comp.getPositive(SeederHandleCtrlPort.class));
    network.removeChannel(handleId, comp.getNegative(Network.class));
    trigger(Kill.event, comp.control());
  }

  //******************************************INTROSPECTION - TEST******************************************************
  protected int size() {
    return handleComps.size();
  }

  public static class Init extends se.sics.kompics.Init<HandleMngrComp> {

    public final KAddress selfAdr;
    public final LeecherHandleCreator leecherHandleCreator;
    public final SeederHandleCreator seederHandleCreator;

    public Init(KAddress selfAdr, LeecherHandleCreator leecherHandleCreator,
      SeederHandleCreator seederHandleCreator) {
      this.selfAdr = selfAdr;
      this.leecherHandleCreator = leecherHandleCreator;
      this.seederHandleCreator = seederHandleCreator;
    }
  }

  public static interface Creator {

    public Component create(ComponentProxy proxy, KAddress selfAdr);

    public void connect(ComponentProxy proxy, Component handleMngrComp, Positive<Network> networkPort);

    public void start(ComponentProxy proxy, Component handleMngrComp);
  }

  public static final DefaultCreator DEFAULT_CREATOR = new DefaultCreator();

  public static class DefaultCreator implements Creator {

    @Override
    public Component create(ComponentProxy proxy, KAddress selfAdr) {
      HandleMngrComp.Init handleMngrInit = new HandleMngrComp.Init(selfAdr, LeecherHandleComp.DEFAULT_CREATOR, SeederHandleComp.DEFAULT_CREATOR);
      Component handleMngrComp = proxy.create(HandleMngrComp.class, handleMngrInit);
      return handleMngrComp;
    }

    @Override
    public void connect(ComponentProxy proxy, Component handleMngrComp, Positive<Network> networkPort) {
      proxy.connect(handleMngrComp.getNegative(Network.class), networkPort);
    }

    @Override
    public void start(ComponentProxy proxy, Component handleMngrComp) {
      proxy.trigger(Start.event, handleMngrComp.control());
    }

  }
}
