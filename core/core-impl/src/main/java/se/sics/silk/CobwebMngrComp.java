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
package se.sics.silk;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.kompics.Channel;
import se.sics.kompics.Component;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Handler;
import se.sics.kompics.Negative;
import se.sics.kompics.Positive;
import se.sics.kompics.Start;
import se.sics.kompics.network.Network;
import se.sics.kompics.timer.Timer;
import se.sics.ktoolbox.util.network.KAddress;
import se.sics.nstream.storage.durable.DStoragePort;
import se.sics.nstream.storage.durable.DStreamControlPort;
import se.sics.nstream.torrent.TorrentMngrComp;
import se.sics.nstream.torrent.TorrentMngrPort;
import se.sics.nstream.torrent.tracking.TorrentStatusPort;
import se.sics.nstream.torrent.transfer.TransferCtrlPort;
import se.sics.silk.overlord.OverlordComp;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class CobwebMngrComp extends ComponentDefinition {

  private static final Logger LOG = LoggerFactory.getLogger(CobwebMngrComp.class);
  private String logPrefix;

  //******************************************CONNECTIONS***************************************************************
  private final Positive<Timer> timerPort = requires(Timer.class);
  private final Positive<Network> networkPort = requires(Network.class);
  private final Positive<DStreamControlPort> streamControlPort = requires(DStreamControlPort.class);
  private final Positive<DStoragePort> storagePort = requires(DStoragePort.class);

  private final Negative<TorrentMngrPort> torrentMngrPort = provides(TorrentMngrPort.class);
  private final Negative<TransferCtrlPort> transferCtrlPort = provides(TransferCtrlPort.class);
  private final Negative<TorrentStatusPort> torrentStatusPort = provides(TorrentStatusPort.class);
  //********************************************************************************************************************
  private Component torrentMngrComp;
  private Component overlordComp;
  //********************************************************************************************************************
  private final KAddress selfAdr;

  public CobwebMngrComp(Init init) {
    this.selfAdr = init.selfAdr;
    this.logPrefix = this.selfAdr.getId().toString();

    subscribe(handleStart, control);

    setupTorrentMngr();
    setupOverlordComp();
  }

  Handler handleStart = new Handler<Start>() {

    @Override
    public void handle(Start event) {
      LOG.info("{}starting", logPrefix);
    }
  };

  private void setupTorrentMngr() {
    torrentMngrComp = create(TorrentMngrComp.class, new TorrentMngrComp.Init(selfAdr));
    connect(torrentMngrComp.getNegative(Timer.class), timerPort, Channel.TWO_WAY);
    connect(torrentMngrComp.getNegative(Network.class), networkPort, Channel.TWO_WAY);
    connect(torrentMngrComp.getNegative(DStreamControlPort.class), streamControlPort, Channel.TWO_WAY);
    connect(torrentMngrComp.getNegative(DStoragePort.class), storagePort, Channel.TWO_WAY);

    connect(torrentMngrPort, torrentMngrComp.getPositive(TorrentMngrPort.class), Channel.TWO_WAY);
    connect(transferCtrlPort, torrentMngrComp.getPositive(TransferCtrlPort.class), Channel.TWO_WAY);
  }

  private void setupOverlordComp() {
    overlordComp = create(OverlordComp.class, new OverlordComp.Init());
    connect(torrentStatusPort, overlordComp.getPositive(TorrentStatusPort.class), Channel.TWO_WAY);
    connect(overlordComp.getNegative(TorrentStatusPort.class), torrentMngrComp.getPositive(TorrentStatusPort.class),
      Channel.TWO_WAY);
  }

  public static class Init extends se.sics.kompics.Init<CobwebMngrComp> {

    public final KAddress selfAdr;

    public Init(KAddress selfAdr) {
      this.selfAdr = selfAdr;
    }
  }
}
