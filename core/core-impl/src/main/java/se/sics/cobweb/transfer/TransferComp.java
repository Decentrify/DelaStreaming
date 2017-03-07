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
package se.sics.cobweb.transfer;

import com.google.common.base.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.cobweb.OverlayComp;
import se.sics.cobweb.transfer.event.TransferLocalE;
import se.sics.cobweb.transfer.handle.LeecherHandlePort;
import se.sics.cobweb.transfer.handle.SeederHandlePort;
import se.sics.cobweb.transfer.instance.TransferPort;
import se.sics.cobweb.transfer.mngr.TransferCtrlPort;
import se.sics.cobweb.transfer.mngr.event.TransferCtrlE;
import se.sics.kompics.Component;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.ComponentProxy;
import se.sics.kompics.Handler;
import se.sics.kompics.Negative;
import se.sics.kompics.Positive;
import se.sics.kompics.Start;
import se.sics.ktoolbox.nutil.fsm.MultiFSM;
import se.sics.ktoolbox.nutil.fsm.api.FSMException;
import se.sics.ktoolbox.nutil.fsm.genericsetup.OnFSMExceptionAction;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayId;
import se.sics.ktoolbox.util.network.KAddress;
import se.sics.nstream.torrent.resourceMngr.ResourceMngrPort;
import se.sics.nstream.transfer.MyTorrent;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class TransferComp extends ComponentDefinition implements OverlayComp {

  private static final Logger LOG = LoggerFactory.getLogger(TransferComp.class);
  private final String logPrefix;

  private final Negative<TransferPort> transferPort = provides(TransferPort.class);
  private final Positive<LeecherHandlePort> leecherHandlePort = requires(LeecherHandlePort.class);
  private final Positive<SeederHandlePort> seederHandlePort = requires(SeederHandlePort.class);
  private final Positive<ResourceMngrPort> resourceMngr = requires(ResourceMngrPort.class);
  //****************************************INTERNAL - DO NOT CONNECT***************************************************
  private final Negative<TransferCtrlPort> transferCtrlPort = provides(TransferCtrlPort.class);
  //********************************************************************************************************************
  private final KAddress selfAdr;
  private final OverlayId torrentId;
  //********************************************************************************************************************
  private MultiFSM leecherFSMs;
  private MultiFSM seederFSMs;
  //********************************************************************************************************************
  private final Optional<MyTorrent.Manifest> torrent;

  public TransferComp(Init init) {
    this.selfAdr = init.selfAdr;
    this.torrentId = init.torrentId;
    logPrefix = "<nid:" + selfAdr.getId() + "," + torrentId + ">";
    LOG.info("{}initiating...", logPrefix);

    this.torrent = init.torrent;
    
    createLeecherTransferFSM();
    createSeederTransferFSM();
    subscribe(handleStart, control);
    subscribe(handleClean, transferCtrlPort);
  }

  private void createLeecherTransferFSM() {
    TransferLeecherInternal.Builder leecherInternal = new TransferLeecherInternal.Builder();
    TransferLeecherExternal leecherExternal = new TransferLeecherExternal(torrentId, transferPort);
    OnFSMExceptionAction oexa = new OnFSMExceptionAction() {
      @Override
      public void handle(FSMException ex) {
        throw new RuntimeException(ex);
      }
    };
    try {
      leecherFSMs = TransferLeecherFSM.multifsm(leecherExternal, leecherInternal, oexa);
      leecherFSMs.setProxy(proxy);
      leecherFSMs.setupHandlers();
    } catch (FSMException ex) {
      throw new RuntimeException(ex);
    }
  }

  private void createSeederTransferFSM() {
    TransferSeederInternal.Builder isb = new TransferSeederInternal.Builder();
    TransferSeederExternal es = new TransferSeederExternal(torrentId, transferPort);
    OnFSMExceptionAction oexa = new OnFSMExceptionAction() {
      @Override
      public void handle(FSMException ex) {
        throw new RuntimeException(ex);
      }
    };
    try {
      seederFSMs = TransferSeederFSM.multifsm(es, isb, oexa);
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
      if(torrent.isPresent()) {
        trigger(new TransferLocalE.SeederStart(torrentId, torrent.get()), onSelf);
      } else{
      
        trigger(new TransferLocalE.LeecherStart(torrentId), onSelf);
        
      }
    }
  };

  @Override
  public void tearDown() {
    LOG.info("{}tear down", logPrefix);
  }

  //********************************************************************************************************************
  Handler handleClean = new Handler<TransferCtrlE.CleanReq>() {
    @Override
    public void handle(TransferCtrlE.CleanReq req) {
      LOG.info("{}cleaning", logPrefix);
      answer(req, req.getResponse());
    }
  };

  @Override
  public OverlayId overlayId() {
    return torrentId;
  }

  //******************************************INTROSPECTION - TESTING***************************************************
  protected MultiFSM getLeecherFSM() {
    return leecherFSMs;
  }

  protected MultiFSM getSeederFSM() {
    return seederFSMs;
  }

  public static class Init extends se.sics.kompics.Init<TransferComp> {

    public final KAddress selfAdr;
    public final OverlayId torrentId;
    public final Optional<MyTorrent.Manifest> torrent;

    public Init(KAddress selfAdr, OverlayId torrentId, Optional torrent) {
      this.selfAdr = selfAdr;
      this.torrentId = torrentId;
      this.torrent = torrent;
    }
  }

  public interface Creator {

    public Component create(ComponentProxy proxy, KAddress selfAdr, OverlayId torrentId,
      Optional<MyTorrent.Manifest> torrent);
  }

  public static final DefaultCreator DEFAULT_CREATOR = new DefaultCreator();

  public static class DefaultCreator implements Creator {

    @Override
    public Component create(ComponentProxy proxy, KAddress selfAdr, OverlayId torrentId,
      Optional<MyTorrent.Manifest> torrent) {
      TransferComp.Init init = new TransferComp.Init(selfAdr, torrentId, torrent);
      return proxy.create(TransferComp.class, init);
    }
  }
}
