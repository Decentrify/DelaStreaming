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
import se.sics.kompics.Handler;
import se.sics.kompics.Kill;
import se.sics.kompics.Negative;
import se.sics.kompics.Positive;
import se.sics.kompics.Start;
import se.sics.kompics.network.Network;
import se.sics.kompics.timer.Timer;
import se.sics.ktoolbox.util.network.KAddress;
import se.sics.nutil.network.bestEffort.BestEffortNetworkComp;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class R2ExtendedNetworkComp extends ComponentDefinition {
  private static final Logger LOG = LoggerFactory.getLogger(R2ExtendedNetworkComp.class);
  
  Positive<Timer> timer = requires(Timer.class);
  Positive<Network> baseNetwork = requires(Network.class);
  Negative<Network> extendedNetwork = provides(Network.class);

  private Component beNetComp;

  public R2ExtendedNetworkComp(Init init) {
    connectComp(init);
    subscribe(handleStart, control);
  }

  Handler handleStart = new Handler<Start>() {

    @Override
    public void handle(Start event) {
      LOG.info("starting");
    }
  };

  @Override
  public void tearDown() {
    trigger(Kill.event, beNetComp.control());
    disconnect(timer, beNetComp.getNegative(Timer.class));
    disconnect(baseNetwork, beNetComp.getNegative(Network.class));
  }

  private void connectComp(Init init) {
    BestEffortNetworkComp.Init beNetCompInit = new BestEffortNetworkComp.Init(init.selfAdr, init.selfAdr.getId());
    beNetComp = create(BestEffortNetworkComp.class, beNetCompInit);
    connect(timer, beNetComp.getNegative(Timer.class), Channel.TWO_WAY);
    connect(baseNetwork, beNetComp.getNegative(Network.class), Channel.TWO_WAY);
    connect(extendedNetwork, beNetComp.getPositive(Network.class), Channel.TWO_WAY);
  }

  public static class Init extends se.sics.kompics.Init<R2ExtendedNetworkComp> {

    public KAddress selfAdr;

    public Init(KAddress selfAdr) {
      this.selfAdr = selfAdr;
    }
  }
}
