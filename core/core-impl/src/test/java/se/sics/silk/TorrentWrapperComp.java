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
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.ComponentProxy;
import se.sics.kompics.Handler;
import se.sics.kompics.Start;
import se.sics.kompics.config.Config;
import se.sics.kompics.fsm.MultiFSM;
import se.sics.ktoolbox.util.network.KAddress;
import se.sics.silk.r2torrent.R2TorrentComp;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class TorrentWrapperComp extends ComponentDefinition {

  private static final Logger LOG = LoggerFactory.getLogger(TorrentWrapperComp.class);
  private String logPrefix;

  private R2TorrentComp.Ports ports;
  public MultiFSM fsm;

  public TorrentWrapperComp(Init init) {
    logPrefix = "<" + init.selfAdr.getId() + ">";
    ports = new R2TorrentComp.Ports(proxy);
    subscribe(handleStart, control);
    setupFSM(init);
  }

  private void setupFSM(Init init) {
    fsm = init.setup.setupFSM(proxy, config(), ports);
  }

  Handler handleStart = new Handler<Start>() {

    @Override
    public void handle(Start event) {
      LOG.info("{}starting", logPrefix);
      fsm.setupHandlers();
    }
  };

  public static class Init extends se.sics.kompics.Init<TorrentWrapperComp> {

    public final Setup setup;
    public final KAddress selfAdr;

    public Init(KAddress selfAdr, Setup setup) {
      this.selfAdr = selfAdr;
      this.setup = setup;
    }
  }
  
  public static interface Setup {
    public MultiFSM setupFSM(ComponentProxy proxy, Config config, R2TorrentComp.Ports ports);
  }
}