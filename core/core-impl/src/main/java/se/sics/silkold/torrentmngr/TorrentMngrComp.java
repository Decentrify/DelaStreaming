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
package se.sics.silkold.torrentmngr;

import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.kompics.Channel;
import se.sics.kompics.Component;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Handler;
import se.sics.kompics.KompicsEvent;
import se.sics.kompics.Negative;
import se.sics.kompics.Positive;
import se.sics.kompics.Start;
import se.sics.kompics.Stopped;
import se.sics.kompics.fsm.FSMException;
import se.sics.kompics.fsm.MultiFSM;
import se.sics.kompics.fsm.OnFSMExceptionAction;
import se.sics.kompics.fsm.id.FSMIdentifierFactory;
import se.sics.kompics.network.Network;
import se.sics.kompics.timer.Timer;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayId;
import se.sics.ktoolbox.util.idextractor.EventOverlayIdExtractor;
import se.sics.ktoolbox.util.idextractor.MsgOverlayIdExtractor;
import se.sics.ktoolbox.util.network.KAddress;
import se.sics.ktoolbox.util.network.ports.One2NChannel;
import se.sics.nstream.storage.durable.DStoragePort;
import se.sics.nstream.storage.durable.DStreamControlPort;
import se.sics.nstream.torrent.tracking.TorrentStatusPort;
import se.sics.nstream.torrent.transfer.TransferCtrlPort;
import se.sics.nutil.network.bestEffort.BestEffortNetworkComp;
import se.sics.silkold.resourcemngr.ResourceMngrComp;
import se.sics.silkold.resourcemngr.ResourceMngrPort;
import se.sics.silkold.torrent.TorrentComp;
import se.sics.silkold.torrent.TorrentMngrPort;

/**
 *
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class TorrentMngrComp extends ComponentDefinition {

  private static final Logger LOG = LoggerFactory.getLogger(TorrentMngrComp.class);
  private String logPrefix;

  //*********************************************EXTERNAL***************************************************************
  private final Positive<Timer> timerPort = requires(Timer.class);
  private final Positive<Network> networkPort = requires(Network.class);
  private final Negative<TorrentMngrPort> torrentMngrPort = provides(TorrentMngrPort.class);
  private final Negative<TransferCtrlPort> transferCtrlPort = provides(TransferCtrlPort.class);
  private final Negative<TorrentStatusPort> torrentStatusPort = provides(TorrentStatusPort.class);
  //*********************************************INTERNAL***************************************************************
  private final Positive<DStreamControlPort> streamControlPort = requires(DStreamControlPort.class);
  private final Positive<DStoragePort> storagePort = requires(DStoragePort.class);
  //used for listening to some of the forwarded events in this comp
  private final Positive<TorrentStatusPort> torrentStatusAuxPort = requires(TorrentStatusPort.class);
  //********************************************************************************************************************
  private One2NChannel networkChannel;
  private One2NChannel transferCtrlChannel;
  private One2NChannel reportChannel;
  //**************************************************************************
  private Component resourceMngrComp;
  private Component wheelNetworkComp;
  //**************************************************************************
  private TorrentMngrExternal es;
  private MultiFSM fsm;

  public TorrentMngrComp(Init init) {
    logPrefix = "<nid:" + init.selfAdr.getId() + ">";
    LOG.info("{}initiating...", logPrefix);

    //used for listening to some of the forwarded events in this comp
    connect((Positive) (torrentStatusPort.getPair()), (Negative) (torrentStatusAuxPort.getPair()), Channel.TWO_WAY);

    setupFSM(init);

    subscribe(handleStart, control);
    subscribe(handleStopped, control);
  }

  private void setupFSM(Init init) {
    es = new TorrentMngrExternal(init.selfAdr, new Connectors());
    es.setProxy(proxy);
    try {
      OnFSMExceptionAction oexa = new OnFSMExceptionAction() {
        @Override
        public void handle(FSMException ex) {
          throw new RuntimeException(ex);
        }
      };
      FSMIdentifierFactory fsmIdFactory = config().getValue(FSMIdentifierFactory.CONFIG_KEY, FSMIdentifierFactory.class);
      fsm = TorrentMngrFSM.multifsm(fsmIdFactory, es, oexa);
    } catch (FSMException ex) {
      throw new RuntimeException(ex);
    }
  }

  Handler handleStart = new Handler<Start>() {
    @Override
    public void handle(Start event) {
      LOG.info("{}starting", logPrefix);

      baseSetup();
      baseStart();
      fsm.setupHandlers();
    }
  };

  private void baseSetup() {
    ResourceMngrComp.Init rmInit = new ResourceMngrComp.Init(es.getSelfAdr().getId());
    resourceMngrComp = create(ResourceMngrComp.class, rmInit);
    connect(resourceMngrComp.getNegative(DStreamControlPort.class), streamControlPort, Channel.TWO_WAY);

    BestEffortNetworkComp.Init whInit = new BestEffortNetworkComp.Init(es.getSelfAdr(), es.getSelfAdr().getId());
    wheelNetworkComp = create(BestEffortNetworkComp.class, whInit);
    connect(wheelNetworkComp.getNegative(Timer.class), timerPort, Channel.TWO_WAY);
    connect(wheelNetworkComp.getNegative(Network.class), networkPort, Channel.TWO_WAY);

    networkChannel = One2NChannel.getChannel(logPrefix + "torrent", wheelNetworkComp.getPositive(Network.class), 
      new MsgOverlayIdExtractor());
    transferCtrlChannel = One2NChannel.getChannel(logPrefix + "transferCtrl", transferCtrlPort,
      new EventOverlayIdExtractor());
    reportChannel = One2NChannel.getChannel("hopsTorrentMngrReport", torrentStatusPort, new EventOverlayIdExtractor());

  }

  private void baseStart() {
    trigger(Start.event, resourceMngrComp.control());
    trigger(Start.event, wheelNetworkComp.control());
  }

  Handler handleStopped = new Handler<Stopped>() {
    @Override
    public void handle(Stopped event) {
      Optional<KompicsEvent> stopResp = es.stoppedComp(event.component.id());
      if (stopResp.isPresent()) {
        trigger(stopResp.get(), onSelf);
      }
    }
  };

  //********************************************************************************************************************

  public class Connectors {

    public final Function<TorrentMngrInternal, Component> torrentCompConn
      = new Function<TorrentMngrInternal, Component>() {
        @Override
        public Component apply(TorrentMngrInternal is) {
          TorrentComp.Init init = new TorrentComp.Init(es.getSelfAdr(), is.getTorrentId(), is.getPartners());
          Component torrentComp = create(TorrentComp.class, init);
          connect(torrentComp.getNegative(Timer.class), timerPort, Channel.TWO_WAY);
          networkChannel.addChannel(is.getTorrentId(), torrentComp.getNegative(Network.class));
          connect(torrentComp.getNegative(ResourceMngrPort.class), resourceMngrComp.getPositive(ResourceMngrPort.class),
            Channel.TWO_WAY);
          connect(torrentComp.getNegative(DStoragePort.class), storagePort, Channel.TWO_WAY);
          transferCtrlChannel.addChannel(is.getTorrentId(), torrentComp.getPositive(TransferCtrlPort.class));
          reportChannel.addChannel(is.getTorrentId(), torrentComp.getPositive(TorrentStatusPort.class));
          return torrentComp;
        }
      };

    public final Consumer<TorrentMngrInternal> torrentCompDisc
      = new Consumer<TorrentMngrInternal>() {
        @Override
        public void accept(TorrentMngrInternal is) {
          OverlayId torrentId = is.getTorrentId();
          Component torrentComp = is.getTorrentComp();

          disconnect(torrentComp.getNegative(Timer.class), timerPort);
          networkChannel.removeChannel(torrentId, torrentComp.getNegative(Network.class));
          disconnect(torrentComp.getNegative(ResourceMngrPort.class), resourceMngrComp.getPositive(
              ResourceMngrPort.class));
          disconnect(torrentComp.getNegative(DStoragePort.class), storagePort);
          transferCtrlChannel.removeChannel(torrentId, torrentComp.getPositive(TransferCtrlPort.class));
          reportChannel.removeChannel(torrentId, torrentComp.getPositive(TorrentStatusPort.class));
        }
      };
  }

  public static class Init extends se.sics.kompics.Init<TorrentMngrComp> {

    public final KAddress selfAdr;

    public Init(KAddress selfAdr) {
      this.selfAdr = selfAdr;
    }
  }
}
