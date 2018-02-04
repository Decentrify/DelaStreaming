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
import se.sics.nstream.storage.durable.DStoragePort;
import se.sics.nstream.storage.durable.DStreamControlPort;
import se.sics.nutil.network.bestEffort.event.BestEffortMsg;
import se.sics.silk.SelfPort;
import se.sics.silk.r2torrent.conn.R1TorrentLeecher;
import se.sics.silk.r2torrent.conn.R1TorrentSeeder;
import se.sics.silk.r2torrent.conn.R2NodeLeecher;
import se.sics.silk.r2torrent.conn.R2NodeSeeder;
import se.sics.silk.r2torrent.torrent.R1Hash;
import se.sics.silk.r2torrent.torrent.R1MetadataGet;
import se.sics.silk.r2torrent.torrent.R1MetadataServe;
import se.sics.silk.r2torrent.torrent.R2Torrent;
import se.sics.silk.r2torrent.transfer.R1DownloadComp;
import se.sics.silk.r2torrent.transfer.R1DownloadPort;
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
  private MultiFSM nodeSeeders;
  private MultiFSM nodeLeechers;
  private MultiFSM torrentSeeders;
  private MultiFSM torrentLeechers;
  private MultiFSM torrents;
  private MultiFSM metadataGet;
  private MultiFSM metadataServe;
  private MultiFSM hashMngr;

  private MultiFSM transferSeeders;

  private R2Torrent.ES torrentES;
  private R1MetadataGet.ES metadataGetES;
  private R1MetadataServe.ES metadataServeES;
  private R1Hash.ES hashMngrES;
  private R2NodeSeeder.ES nodeSeederES;
  private R2NodeLeecher.ES nodeLeecherES;
  private R1TorrentSeeder.ES torrentSeederES;
  private R1TorrentLeecher.ES torrentLeecherES;

  private R1TransferSeeder.ES transferSeederES;

  public R2TorrentComp(Init init) {
    logPrefix = "<" + init.selfAdr.getId() + ">";
    ports = new Ports(proxy);
    subscribe(handleStart, control);
    setupFSM(init);
  }

  private void setupFSM(Init init) {
    nodeSeederES = new R2NodeSeeder.ES(init.selfAdr);
    nodeLeecherES = new R2NodeLeecher.ES(init.selfAdr);
    torrentSeederES = new R1TorrentSeeder.ES();
    torrentLeecherES = new R1TorrentLeecher.ES();
    torrentES = new R2Torrent.ES();
    metadataGetES = new R1MetadataGet.ES(init.selfAdr);
    metadataServeES = new R1MetadataServe.ES();
    hashMngrES = new R1Hash.ES(ports);

    transferSeederES = new R1TransferSeeder.ES(init.selfAdr);

    nodeSeederES.setProxy(proxy);
    nodeLeecherES.setProxy(proxy);
    torrentSeederES.setProxy(proxy);
    torrentLeecherES.setProxy(proxy);
    torrentES.setProxy(proxy);
    metadataGetES.setProxy(proxy);
    metadataServeES.setProxy(proxy);
    hashMngrES.setProxy(proxy);
    transferSeederES.setProxy(proxy);

    nodeLeecherES.setPorts(ports);
    nodeSeederES.setPorts(ports);
    torrentLeecherES.setPorts(ports);
    torrentSeederES.setPorts(ports);
    torrentES.setPorts(ports);
    metadataGetES.setPorts(ports);
    metadataServeES.setPorts(ports);
    transferSeederES.setPorts(ports);
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
      metadataGet = R1MetadataGet.FSM.multifsm(fsmIdFactory, metadataGetES, oexa);
      metadataServe = R1MetadataServe.FSM.multifsm(fsmIdFactory, metadataServeES, oexa);
      hashMngr = R1Hash.FSM.multifsm(fsmIdFactory, hashMngrES, oexa);
      transferSeeders = R1TransferSeeder.FSM.multifsm(fsmIdFactory, transferSeederES, oexa);
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
      metadataGet.setupHandlers();
      metadataServe.setupHandlers();
      hashMngr.setupHandlers();
      transferSeeders.setupHandlers();
    }
  };
  //********************************************************************************************************************

  public static class Ports {

    public final Negative<SelfPort> loopbackPos;
    public final Positive<SelfPort> loopbackSubscribe;
    public final Negative<R2TorrentCtrlPort> ctrl;
    public final Positive<DStreamControlPort> streamCtrl;
    public final Positive<R1UploadPort> upload;
    public final Positive<Network> network;
    public final Positive<Timer> timer;
    public final Positive<DStoragePort> storage;
    
    public final Positive download;
    public final Positive transferSeederCtrlPos;
    public final Negative transferSeederCtrlNeg;
    public final One2NChannel<R1UploadPort> uploadC;
    public final One2NChannel<Network> netUploadC;

    public final One2NChannel<Network> netDownloadC;
    public final One2NChannel<R1DownloadPort> downloadC;

    public Ports(ComponentProxy proxy) {
      loopbackPos = proxy.provides(SelfPort.class);
      loopbackSubscribe = proxy.requires(SelfPort.class);
      proxy.connect(proxy.getPositive(SelfPort.class), proxy.getNegative(SelfPort.class), Channel.TWO_WAY);
      ctrl = proxy.provides(R2TorrentCtrlPort.class);
      streamCtrl = proxy.requires(DStreamControlPort.class);
      //clean ports
      upload = proxy.requires(R1UploadPort.class);
      network = proxy.requires(Network.class); 
      timer = proxy.requires(Timer.class);
      storage = proxy.requires(DStoragePort.class);
      uploadC = One2NChannel.getChannel("r1-torrent-file-upload-ctrl", (Negative) upload.getPair(),
        uploadCompIdExtractor());
      netUploadC = One2NChannel.getChannel("r1-torrent-file-upload-network", network, netUploadCompIdExtractor());

      download = proxy.requires(R1DownloadPort.class);
      transferSeederCtrlPos = proxy.requires(R1TransferSeederCtrl.class);
      transferSeederCtrlNeg = proxy.provides(R1TransferSeederCtrl.class);
      proxy.connect(proxy.getPositive(R1TransferSeederCtrl.class), proxy.getNegative(R1TransferSeederCtrl.class), 
        Channel.TWO_WAY);
      
      netDownloadC = One2NChannel.getChannel("r1-torrent-transfer-download-network", network,
        netDownloadCompIdExtractor());
      downloadC = One2NChannel.getChannel("r1-torrent-file-download-ctrl", (Negative)download.getPair(), 
        downloadCompIdExtractor());
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
