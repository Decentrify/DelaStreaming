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
package se.sics.cobweb.overlord;

import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.cobweb.conn.ConnPort;
import se.sics.cobweb.conn.event.ConnE;
import se.sics.cobweb.overlord.conn.api.ConnectionDecider;
import se.sics.cobweb.overlord.conn.api.SeederState;
import se.sics.cobweb.overlord.conn.api.TorrentState;
import se.sics.cobweb.overlord.conn.impl.LocalLeechersViewImpl;
import se.sics.cobweb.overlord.conn.impl.LocalSeedersViewImpl;
import se.sics.cobweb.overlord.handle.LeecherHandleOverlordExternal;
import se.sics.cobweb.overlord.handle.LeecherHandleOverlordFSM;
import se.sics.cobweb.overlord.handle.LeecherHandleOverlordInternal;
import se.sics.cobweb.overlord.handle.SeederHandleOverlordExternal;
import se.sics.cobweb.overlord.handle.SeederHandleOverlordFSM;
import se.sics.cobweb.overlord.handle.SeederHandleOverlordInternal;
import se.sics.cobweb.transfer.handle.LeecherHandleCtrlPort;
import se.sics.cobweb.transfer.handle.SeederHandleCtrlPort;
import se.sics.cobweb.transfer.handlemngr.HandleMngrPort;
import se.sics.cobweb.transfer.instance.TransferPort;
import se.sics.cobweb.transfer.mngr.event.TransferE;
import se.sics.cobweb.util.HandleId;
import se.sics.kompics.Component;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.ComponentProxy;
import se.sics.kompics.Handler;
import se.sics.kompics.Positive;
import se.sics.kompics.Start;
import se.sics.ktoolbox.nutil.fsm.MultiFSM;
import se.sics.ktoolbox.nutil.fsm.api.FSMException;
import se.sics.ktoolbox.nutil.fsm.genericsetup.OnFSMExceptionAction;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayId;
import se.sics.ktoolbox.util.network.KAddress;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class OverlordComp extends ComponentDefinition {

  private static final Logger LOG = LoggerFactory.getLogger(OverlordComp.class);
  private final String logPrefix;

  private final Positive<ConnPort> connPort = requires(ConnPort.class);
  private final Positive<HandleMngrPort> handleMngrPort = requires(HandleMngrPort.class);
  private final Positive<LeecherHandleCtrlPort> leecherHandleCtrlPort = requires(LeecherHandleCtrlPort.class);
  private final Positive<SeederHandleCtrlPort> seederHandleCtrlPort = requires(SeederHandleCtrlPort.class);
  private final Positive<TransferPort> transferPort = requires(TransferPort.class);

  private MultiFSM leecherHandleFSMs;
  private MultiFSM seederHandleFSMs;
  private LocalLeechersViewImpl leecherSideView;
  private LocalSeedersViewImpl seederSideView;
  private ConnectionDecider.SeederSide seederSideDecider;
  private ConnectionDecider.LeecherSide leecherSideDecider;

  public OverlordComp(Init init) {
    logPrefix = "<nid:" + init.selfAdr.getId() + ">";
    LOG.info("{}init", logPrefix);

    seederSideDecider = init.seederSideDecider;
    leecherSideDecider = init.leecherSideDecider;
    createOLeecherConnFSM(init);
    createOSeederConnFSM(init);

    subscribe(handleStart, control);
    subscribe(handleSeederSample, connPort);
    subscribe(handleSeederFileStarted, transferPort);
    subscribe(handleLeecherFileStarted, transferPort);
    subscribe(handleLeecherFileCompleted, transferPort);
  }

  private void createOLeecherConnFSM(Init init) {
    leecherSideView = new LocalLeechersViewImpl();
    LeecherHandleOverlordInternal.Builder leecherInternal = new LeecherHandleOverlordInternal.Builder();
    LeecherHandleOverlordExternal leecherExternal = new LeecherHandleOverlordExternal(init.selfAdr,
      leecherSideDecider, leecherSideView, handleMngrPort, leecherHandleCtrlPort);
    OnFSMExceptionAction oexa = new OnFSMExceptionAction() {
      @Override
      public void handle(FSMException ex) {
        throw new RuntimeException(ex);
      }
    };
    try {
      leecherHandleFSMs = LeecherHandleOverlordFSM.multifsm(leecherExternal, leecherInternal, oexa);
      leecherHandleFSMs.setProxy(proxy);
      leecherHandleFSMs.setupHandlers();
    } catch (FSMException ex) {
      throw new RuntimeException(ex);
    }
  }

  private void createOSeederConnFSM(Init init) {
    seederSideView = new LocalSeedersViewImpl();
    SeederHandleOverlordInternal.Builder isb = new SeederHandleOverlordInternal.Builder();
    SeederHandleOverlordExternal es = new SeederHandleOverlordExternal(seederSideDecider, seederSideView,
      leecherSideView, handleMngrPort, seederHandleCtrlPort);
    OnFSMExceptionAction oexa = new OnFSMExceptionAction() {
      @Override
      public void handle(FSMException ex) {
        throw new RuntimeException(ex);
      }
    };
    try {
      seederHandleFSMs = SeederHandleOverlordFSM.multifsm(es, isb, oexa);
      seederHandleFSMs.setProxy(proxy);
      seederHandleFSMs.setupHandlers();
    } catch (FSMException ex) {
      throw new RuntimeException(ex);
    }
  }

  //********************************************************************************************************************
  Handler handleStart = new Handler<Start>() {
    @Override
    public void handle(Start event) {
      LOG.info("{}start", logPrefix);
    }
  };

  @Override
  public void tearDown() {
    LOG.info("{}tear down", logPrefix);
  }
  //********************************************************************************************************************
  Handler handleSeederSample = new Handler<ConnE.SeederSample>() {
    @Override
    public void handle(ConnE.SeederSample event) {
      LOG.trace("{}{}", logPrefix, event);
      Set<HandleId> handles = leecherSideDecider.canConnect(seederSideView, leecherSideView, getSeederState(
        event.seederAdr), getTorrentState(event.torrentId));
      for (HandleId handle : handles) {
        ConnE.Connect1Request req = new ConnE.Connect1Request(event.torrentId, handle, event.seederAdr);
        trigger(req, connPort);
      }
    }
  };

  //***********************************************  !! TEMP  !!  ******************************************************
  Handler handleLeecherFileStarted = new Handler<TransferE.LeecherStarted>() {
    @Override
    public void handle(TransferE.LeecherStarted event) {
      LOG.trace("{}{}", logPrefix, event);
      leecherSideView.newFile(event.torrentId, event.fileId);
    }
  };
  
  Handler handleSeederFileStarted = new Handler<TransferE.SeederStarted>() {
    @Override
    public void handle(TransferE.SeederStarted event) {
      LOG.trace("{}{}", logPrefix, event);
      seederSideView.newFile(event.torrentId, event.fileId);
    }
  };

  Handler handleLeecherFileCompleted = new Handler<TransferE.LeecherCompleted>() {
    @Override
    public void handle(TransferE.LeecherCompleted event) {
      LOG.trace("{}{}", logPrefix, event);
      leecherSideView.completeFile(event.torrentId, event.fileId);
    }
  };

  private static SeederState getSeederState(KAddress leecherAdr) {
    return new SeederState(leecherAdr);
  }

  private static TorrentState getTorrentState(OverlayId torrentId) {
    return new TorrentState(torrentId);
  }
  
  //*******************************************  !! INTROSPECTION _ TEST  !!  ******************************************
  protected MultiFSM getLeecherHandleFSMs() {
    return leecherHandleFSMs;
  }
  
  protected MultiFSM getSeederHandleFSMs() {
    return seederHandleFSMs;
  }

  public static class Init extends se.sics.kompics.Init<OverlordComp> {

    public final KAddress selfAdr;
    public final ConnectionDecider.SeederSide seederSideDecider;
    public final ConnectionDecider.LeecherSide leecherSideDecider;

    public Init(KAddress selfAdr, ConnectionDecider.SeederSide seederSideDecider,
      ConnectionDecider.LeecherSide leecherSideDecider) {
      this.selfAdr = selfAdr;
      this.seederSideDecider = seederSideDecider;
      this.leecherSideDecider = leecherSideDecider;
    }
  }
  
  public static final DefaultCreator DEFAULT_CREATOR = new DefaultCreator();
  public static class DefaultCreator implements OverlordCreator {

    @Override
    public Component create(ComponentProxy proxy, KAddress selfAdr, ConnectionDecider.SeederSide seederSideDecider, ConnectionDecider.LeecherSide leecherSideDecider) {
      Init init = new Init(selfAdr, seederSideDecider, leecherSideDecider);
      return proxy.create(OverlordComp.class, init);
    }

    @Override
    public void start(ComponentProxy proxy, Component overlord) {
      proxy.trigger(Start.event, overlord.control());
    }

    @Override
    public void connect(ComponentProxy proxy, Component overlord, Positive<ConnPort> connPort, Positive<HandleMngrPort> handleMngrPort, Positive<TransferPort> transferPort) {
      proxy.connect(overlord.getNegative(ConnPort.class), connPort);
      proxy.connect(overlord.getNegative(HandleMngrPort.class), handleMngrPort);
      proxy.connect(overlord.getNegative(TransferPort.class), transferPort);
    }
  }
}
