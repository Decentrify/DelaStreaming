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
package se.sics.silk.r2torrent.conn;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Handler;
import se.sics.kompics.Start;
import se.sics.kompics.fsm.FSMException;
import se.sics.kompics.fsm.MultiFSM;
import se.sics.kompics.fsm.OnFSMExceptionAction;
import se.sics.kompics.fsm.id.FSMIdentifierFactory;
import se.sics.ktoolbox.util.network.KAddress;
import se.sics.silk.r2torrent.R2TorrentComp;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class R1TorrentSeederAuxComp extends ComponentDefinition {

  private static final Logger LOG = LoggerFactory.getLogger(R1TorrentSeederAuxComp.class);
  private String logPrefix;

  private R2TorrentComp.Ports ports;
  private MultiFSM torrentSeeders;
  private R1TorrentSeeder.ES torrentSeedersES;

  public R1TorrentSeederAuxComp(Init init) {
    logPrefix = "<" + init.selfAdr.getId() + ">";
    ports = new R2TorrentComp.Ports(proxy);
    subscribe(handleStart, control);
    setupFSM(init);
  }

  private void setupFSM(Init init) {
    torrentSeedersES = new R1TorrentSeeder.ES(ports);

    torrentSeedersES.setProxy(proxy);
    try {
      OnFSMExceptionAction oexa = new OnFSMExceptionAction() {
        @Override
        public void handle(FSMException ex) {
          throw new RuntimeException(ex);
        }
      };
      FSMIdentifierFactory fsmIdFactory = config().getValue(FSMIdentifierFactory.CONFIG_KEY, FSMIdentifierFactory.class);
      torrentSeeders = R1TorrentSeeder.FSM.multifsm(fsmIdFactory, torrentSeedersES, oexa);
    } catch (FSMException ex) {
      throw new RuntimeException(ex);
    }
  }

  Handler handleStart = new Handler<Start>() {

    @Override
    public void handle(Start event) {
      LOG.info("{}starting", logPrefix);
      torrentSeeders.setupHandlers();
    }
  };

  //******************************************TESTING HELPERS***********************************************************
  
  //********************************************************************************************************************

  public static class Init extends se.sics.kompics.Init<R1TorrentSeederAuxComp> {

    public final KAddress selfAdr;

    public Init(KAddress selfAdr) {
      this.selfAdr = selfAdr;
    }
  }
}
