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
package se.sics.silk.r2mngr;

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
import se.sics.silk.r2conn.R2ConnComp;
import se.sics.silk.r2torrent.R2TorrentComp;
import se.sics.silk.r2torrent.R2TorrentCtrlPort;
import se.sics.silk.r2torrent.R2TorrentTransferPort;
import se.sics.silk.r2transfer.R2TransferComp;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class R2MngrComp extends ComponentDefinition {

  Positive<Network> network = requires(Network.class);
  Positive<Timer> timer = requires(Timer.class);
  Negative<R2TorrentCtrlPort> torrentCtrl = provides(R2TorrentCtrlPort.class);
  
  private Component connMngrComp;
  private Component torrentMngrComp;
  private Component transferMngrComp;
  
  //*********************************************TESTING PURPOSES*******************************************************
  R2ConnComp connMngr;
  R2TorrentComp torrentMngr;
  R2TransferComp transferMngr;
  //********************************************************************************************************************
  public R2MngrComp(Init init) {
    subscribe(handleStart, control);
    
    connMngrComp = create(R2ConnComp.class, new R2ConnComp.Init(init.selfAdr));
    connect(connMngrComp.getNegative(Network.class), network, Channel.TWO_WAY);
    connect(connMngrComp.getNegative(Timer.class), timer, Channel.TWO_WAY);
    torrentMngrComp = create(R2TorrentComp.class, new R2TorrentComp.Init(init.selfAdr));
    connect(torrentCtrl, torrentMngrComp.getPositive(R2TorrentCtrlPort.class), Channel.TWO_WAY);
    transferMngrComp = create(R2TransferComp.class, new R2TransferComp.Init(init.selfAdr));
    connect(torrentMngrComp.getNegative(R2TorrentTransferPort.class), transferMngrComp.getPositive(R2TorrentTransferPort.class), Channel.TWO_WAY);
  
    connMngr = (R2ConnComp)connMngrComp.getComponent();
    torrentMngr = (R2TorrentComp)torrentMngrComp.getComponent();
    transferMngr = (R2TransferComp)transferMngrComp.getComponent();
  }

  Handler handleStart = new Handler<Start>() {
    @Override
    public void handle(Start event) {
    }
  };

  public static class Init extends se.sics.kompics.Init<R2MngrComp> {

    public final KAddress selfAdr;

    public Init(KAddress selfAdr) {
      this.selfAdr = selfAdr;
    }
  }
}
