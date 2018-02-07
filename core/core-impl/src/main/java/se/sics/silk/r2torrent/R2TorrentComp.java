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
import se.sics.kompics.Component;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.ComponentProxy;
import se.sics.kompics.Handler;
import se.sics.kompics.Kill;
import se.sics.kompics.KompicsEvent;
import se.sics.kompics.Negative;
import se.sics.kompics.Positive;
import se.sics.kompics.Start;
import se.sics.kompics.fsm.FSMException;
import se.sics.kompics.fsm.MultiFSM;
import se.sics.kompics.fsm.OnFSMExceptionAction;
import se.sics.kompics.fsm.id.FSMIdentifierFactory;
import se.sics.kompics.network.Network;
import se.sics.kompics.timer.Timer;
import se.sics.kompics.util.Identifier;
import se.sics.ktoolbox.util.network.KAddress;
import se.sics.ktoolbox.util.network.basic.BasicContentMsg;
import se.sics.ktoolbox.util.network.ports.ChannelIdExtractor;
import se.sics.ktoolbox.util.network.ports.One2NChannel;
import se.sics.nstream.storage.durable.DEndpointCtrlPort;
import se.sics.nstream.storage.durable.DStoragePort;
import se.sics.nstream.storage.durable.DStreamControlPort;
import se.sics.nutil.network.bestEffort.event.BestEffortMsg;
import se.sics.silk.SelfPort;
import se.sics.silk.r2torrent.torrent.R1FileDownload;
import se.sics.silk.r2torrent.torrent.R1FileDownloadCtrl;
import se.sics.silk.r2torrent.torrent.R1FileUpload;
import se.sics.silk.r2torrent.torrent.R1FileUploadCtrl;
import se.sics.silk.r2torrent.torrent.R1Torrent;
import se.sics.silk.r2torrent.torrent.R1TorrentConnComp;
import se.sics.silk.r2torrent.torrent.R1TorrentConnPort;
import se.sics.silk.r2torrent.torrent.R1TorrentCtrlPort;
import se.sics.silk.r2torrent.torrent.util.R1TorrentDetails;
import se.sics.silk.r2torrent.transfer.R1DownloadComp;
import se.sics.silk.r2torrent.transfer.R1DownloadPort;
import se.sics.silk.r2torrent.transfer.R1TransferLeecher;
import se.sics.silk.r2torrent.transfer.R1TransferLeecherCtrl;
import se.sics.silk.r2torrent.transfer.R1TransferSeeder;
import se.sics.silk.r2torrent.transfer.R1TransferSeederCtrl;
import se.sics.silk.r2torrent.transfer.R1UploadComp;
import se.sics.silk.r2torrent.transfer.R1UploadPort;
import se.sics.silk.r2torrent.transfer.events.R1DownloadEvent;
import se.sics.silk.r2torrent.transfer.events.R1TransferMsg;
import se.sics.silk.r2torrent.transfer.events.R1UploadEvent;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class R2TorrentComp extends ComponentDefinition {

  private static final Logger LOG = LoggerFactory.getLogger(R2TorrentComp.class);
  private String logPrefix;

  private final Ports ports;
  private Component overlayComp;
  
  private MultiFSM torrents;
  private MultiFSM fileUpload;
  private MultiFSM fileDownload;
  private MultiFSM transferSeeder;
  private MultiFSM transferLeecher;

  private R1Torrent.ES torrentES;
  private R1FileUpload.ES fileUploadES;
  private R1FileDownload.ES fileDownloadES;
  private R1TransferSeeder.ES transferSeederES;
  private R1TransferLeecher.ES transferLeecherES;

  public R2TorrentComp(Init init) {
    logPrefix = "<" + init.selfAdr.getId() + ">";
    ports = new Ports(proxy);
    subscribe(handleStart, control);
    setupComp(init);
    setupFSM(init);
  }

  private void setupComp(Init init) {
    R1TorrentConnComp.Init overlayCompInit = new R1TorrentConnComp.Init();
    overlayComp = create(R1TorrentConnComp.class, overlayCompInit);
    connect(ports.timer, overlayComp.getNegative(Timer.class), Channel.TWO_WAY);
    connect(ports.torrentConnReq.getPair(), overlayComp.getPositive(R1TorrentConnPort.class), Channel.TWO_WAY);
  }
  
  private void setupFSM(Init init) {
    R1TorrentDetails.Mngr mngr = new R1TorrentDetails.Mngr();
    torrentES = new R1Torrent.ES(mngr);
    fileUploadES = new R1FileUpload.ES(init.selfAdr, mngr);
    fileDownloadES = new R1FileDownload.ES(init.selfAdr, mngr);
    transferSeederES = new R1TransferSeeder.ES(init.selfAdr);
    transferLeecherES = new R1TransferLeecher.ES(init.selfAdr, mngr);

    torrentES.setProxy(proxy);
    fileUploadES.setProxy(proxy);
    fileDownloadES.setProxy(proxy);
    transferSeederES.setProxy(proxy);
    transferLeecherES.setProxy(proxy);

    torrentES.setPorts(ports);
    fileUploadES.setPorts(ports);
    fileDownloadES.setPorts(ports);
    transferSeederES.setPorts(ports);
    transferLeecherES.setPorts(ports);
    try {
      OnFSMExceptionAction oexa = new OnFSMExceptionAction() {
        @Override
        public void handle(FSMException ex) {
          throw new RuntimeException(ex);
        }
      };
      FSMIdentifierFactory fsmIdFactory = config().getValue(FSMIdentifierFactory.CONFIG_KEY, FSMIdentifierFactory.class);
      torrents = R1Torrent.FSM.multifsm(fsmIdFactory, torrentES, oexa);
      fileUpload = R1FileUpload.FSM.multifsm(fsmIdFactory, fileUploadES, oexa);
      fileDownload = R1FileDownload.FSM.multifsm(fsmIdFactory, fileDownloadES, oexa);
      transferSeeder = R1TransferSeeder.FSM.multifsm(fsmIdFactory, transferSeederES, oexa);
      transferLeecher = R1TransferLeecher.FSM.multifsm(fsmIdFactory, transferLeecherES, oexa);
    } catch (FSMException ex) {
      throw new RuntimeException(ex);
    }
  }

  Handler handleStart = new Handler<Start>() {

    @Override
    public void handle(Start event) {
      LOG.info("{}starting", logPrefix);
      torrents.setupHandlers();
      fileUpload.setupHandlers();
      fileDownload.setupHandlers();
      transferSeeder.setupHandlers();
      transferLeecher.setupHandlers();
    }
  };
  
  @Override
  public void tearDown() {
    killComp();
  }
  
  private void killComp() {
    trigger(Kill.event, overlayComp.control());
    disconnect(ports.timer, overlayComp.getNegative(Timer.class));
    disconnect(ports.torrentConnReq.getPair(), overlayComp.getPositive(R1TorrentConnPort.class));
  }
  //********************************************************************************************************************

  public static class Ports {

    //**************************************************EXTERNAL********************************************************
    public final Positive<Network> network;
    public final Positive<Timer> timer;
    public final Positive<DEndpointCtrlPort> endpointCtrl;
    public final Positive<DStreamControlPort> streamCtrl;
    public final Positive<DStoragePort> storage;
    //**************************************************INTERNAL********************************************************
    public final Negative<SelfPort> loopbackPos;
    public final Positive<SelfPort> loopbackSubscribe;
    public final Negative<R1TorrentCtrlPort> torrentCtrl;
    public final Positive<R1TorrentConnPort> torrentConnReq;
    public final Positive<R1FileUploadCtrl> fileUploadCtrlReq;
    public final Negative<R1FileUploadCtrl> fileUploadCtrlProv;
    public final Positive<R1FileDownloadCtrl> fileDownloadCtrlReq;
    public final Negative<R1FileDownloadCtrl> fileDownloadCtrlProv;
    public final Positive<R1DownloadPort> transferDownload;
    public final Positive<R1UploadPort> transferUpload;
    public final Positive<R1TransferSeederCtrl> transferSeederCtrlReq;
    public final Negative<R1TransferSeederCtrl> transferSeederCtrlProv;
    public final Positive<R1TransferLeecherCtrl> transferLeecherCtrlReq;
    public final Negative<R1TransferLeecherCtrl> transferLeecherCtrlProv;

    public final One2NChannel<R1UploadPort> transferUploadC;
    public final One2NChannel<R1DownloadPort> transferDownloadC;
    public final One2NChannel<Network> netTransferUploadC;
    public final One2NChannel<Network> netTransferDownloadC;

    public Ports(ComponentProxy proxy) {
      loopbackPos = proxy.provides(SelfPort.class);
      loopbackSubscribe = proxy.requires(SelfPort.class);
      proxy.connect(proxy.getPositive(SelfPort.class), proxy.getNegative(SelfPort.class), Channel.TWO_WAY);

      network = proxy.requires(Network.class);
      timer = proxy.requires(Timer.class);
      endpointCtrl = proxy.requires(DEndpointCtrlPort.class);
      streamCtrl = proxy.requires(DStreamControlPort.class);
      storage = proxy.requires(DStoragePort.class);

      torrentCtrl = proxy.provides(R1TorrentCtrlPort.class);
      torrentConnReq = proxy.requires(R1TorrentConnPort.class);
      fileUploadCtrlReq = proxy.requires(R1FileUploadCtrl.class);
      fileUploadCtrlProv = proxy.provides(R1FileUploadCtrl.class);
      proxy.connect(proxy.getPositive(R1FileUploadCtrl.class), proxy.getNegative(R1FileUploadCtrl.class),
        Channel.TWO_WAY);
      fileDownloadCtrlReq = proxy.requires(R1FileDownloadCtrl.class);
      fileDownloadCtrlProv = proxy.provides(R1FileDownloadCtrl.class);
      proxy.connect(proxy.getPositive(R1FileDownloadCtrl.class), proxy.getNegative(R1FileDownloadCtrl.class),
        Channel.TWO_WAY);
      transferDownload = proxy.requires(R1DownloadPort.class);
      transferUpload = proxy.requires(R1UploadPort.class);
      transferSeederCtrlReq = proxy.requires(R1TransferSeederCtrl.class);
      transferSeederCtrlProv = proxy.provides(R1TransferSeederCtrl.class);
      proxy.connect(proxy.getPositive(R1TransferSeederCtrl.class), proxy.getNegative(R1TransferSeederCtrl.class),
        Channel.TWO_WAY);
      transferLeecherCtrlReq = proxy.requires(R1TransferLeecherCtrl.class);
      transferLeecherCtrlProv = proxy.provides(R1TransferLeecherCtrl.class);
      proxy.connect(proxy.getPositive(R1TransferLeecherCtrl.class), proxy.getNegative(R1TransferLeecherCtrl.class),
        Channel.TWO_WAY);

      transferDownloadC = One2NChannel.
        getChannel("r1-torrent-file-download-ctrl", (Negative) transferDownload.getPair(),
          downloadCompIdExtractor());
      transferUploadC = One2NChannel.getChannel("r1-torrent-file-upload-ctrl", (Negative) transferUpload.getPair(),
        uploadCompIdExtractor());
      netTransferDownloadC = One2NChannel.getChannel("r1-torrent-transfer-download-network", network,
        netDownloadCompIdExtractor());
      netTransferUploadC = One2NChannel.getChannel("r1-torrent-transfer-upload-network", network,
        netUploadCompIdExtractor());
    }
  }

  public static class Init extends se.sics.kompics.Init<R2TorrentComp> {

    public final KAddress selfAdr;

    public Init(KAddress selfAdr) {
      this.selfAdr = selfAdr;
    }
  }

  public static ChannelIdExtractor downloadCompIdExtractor() {
    return new ChannelIdExtractor<KompicsEvent, Identifier>(KompicsEvent.class) {

      @Override
      public Identifier getValue(KompicsEvent event) {
        if (event instanceof R1DownloadEvent) {
          R1DownloadEvent e = (R1DownloadEvent) event;
          return R1DownloadComp.baseId(e.torrentId(), e.fileId(), e.nodeId());
        } else {
          return null;
        }
      }
    };
  }

  public static ChannelIdExtractor netDownloadCompIdExtractor() {
    return new ChannelIdExtractor<BasicContentMsg, Identifier>(BasicContentMsg.class) {

      @Override
      public Identifier getValue(BasicContentMsg msg) {
        KAddress seeder = msg.getSource();
        if (msg.getContent() instanceof R1TransferMsg.Dwnl) {
          R1TransferMsg.Dwnl payload = (R1TransferMsg.Dwnl) msg.getContent();
          return R1DownloadComp.baseId(payload.torrentId(), payload.fileId(), seeder.getId());
        } else if (msg.getContent() instanceof BestEffortMsg.Request) {
          BestEffortMsg.Request be = (BestEffortMsg.Request) msg.getContent();
          if (be.extractValue() instanceof R1TransferMsg.Dwnl) {
            R1TransferMsg.Dwnl payload = (R1TransferMsg.Dwnl) be.content;
            return R1DownloadComp.baseId(payload.torrentId(), payload.fileId(), seeder.getId());
          }
        }
        return null;
      }
    };
  }

  public static ChannelIdExtractor uploadCompIdExtractor() {
    return new ChannelIdExtractor<KompicsEvent, Identifier>(KompicsEvent.class) {

      @Override
      public Identifier getValue(KompicsEvent event) {
        if (event instanceof R1UploadEvent) {
          R1UploadEvent e = (R1UploadEvent) event;
          return R1UploadComp.baseId(e.torrentId(), e.fileId(), e.nodeId());
        } else {
          return null;
        }
      }
    };
  }

  public static ChannelIdExtractor netUploadCompIdExtractor() {
    return new ChannelIdExtractor<BasicContentMsg, Identifier>(BasicContentMsg.class) {

      @Override
      public Identifier getValue(BasicContentMsg msg) {
        KAddress leecher = msg.getSource();
        if (msg.getContent() instanceof R1TransferMsg.Upld) {
          R1TransferMsg.Upld payload = (R1TransferMsg.Upld) msg.getContent();
          return R1UploadComp.baseId(payload.torrentId(), payload.fileId(), leecher.getId());
        }
        return null;
      }
    };
  }
}
