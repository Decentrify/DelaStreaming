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
package se.sics.nstream.hops.libmngr;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.kompics.ComponentProxy;
import se.sics.kompics.config.Config;
import se.sics.ktoolbox.nutil.fsm.MultiFSM;
import se.sics.ktoolbox.nutil.fsm.genericsetup.OnFSMExceptionAction;
import se.sics.ktoolbox.util.network.KAddress;
import se.sics.nstream.hops.library.Details;
import se.sics.nstream.hops.library.HopsLibraryKConfig;
import se.sics.nstream.library.Library;
import se.sics.nstream.library.endpointmngr.EndpointIdRegistry;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class HopsLibraryMngr {

  private static final Logger LOG = LoggerFactory.getLogger(HopsLibraryMngr.class);
  private String logPrefix = "";

  private final Config config;
  private final KAddress selfAdr;
  private final MultiFSM fsm;
  private final Library library;

  public HopsLibraryMngr(OnFSMExceptionAction oexa, ComponentProxy proxy, Config config, String logPrefix,
    KAddress selfAdr) {
    this.logPrefix = logPrefix;
    this.config = config;
    this.selfAdr = selfAdr;
    this.library = new Library(config.getValue("library.summary", String.class));

    HopsLibraryKConfig hopsLibraryConfig = new HopsLibraryKConfig(config);

    if (hopsLibraryConfig.baseEndpointType.equals(Details.Types.DISK)) {
      this.fsm = LocalLibTorrentFSM.getFSM(
        new LocalLibTorrentFSM.LibTExternal(selfAdr, library,new EndpointIdRegistry()), oexa);
    } else {
      throw new RuntimeException("not ready");
    }
    fsm.setProxy(proxy);
  }

  public void start() {
    //not sure when the provided ports are set, but for sure they are set after Start event. Ports are not set in constructor
    //TODO Alex - might lose some msg between Start and process of Start
    fsm.setupHandlers();
  }

  public void close() {
  }

}
