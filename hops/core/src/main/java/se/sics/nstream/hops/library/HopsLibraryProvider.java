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
package se.sics.nstream.hops.library;

import java.util.ArrayList;
import java.util.List;
import se.sics.kompics.ComponentProxy;
import se.sics.kompics.PortType;
import se.sics.kompics.config.Config;
import se.sics.kompics.fsm.OnFSMExceptionAction;
import se.sics.ktoolbox.util.network.KAddress;
import se.sics.nstream.hops.libmngr.HopsLibraryMngr;
import se.sics.nstream.library.LibraryProvider;

/**
 *
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class HopsLibraryProvider implements LibraryProvider {
    private HopsHelperMngr hopsHelper;
    private HopsLibraryMngr hopsTorrent;
    
    @Override
    public List<Class<PortType>> providesPorts() {
        List ports = new ArrayList();
        ports.add(HopsHelperPort.class);
        ports.add(HopsTorrentPort.class);
        return ports;
    }
    
    @Override
    public void create(ComponentProxy proxy, Config config, String logPrefix, KAddress selfAdr, OnFSMExceptionAction oexa) {
        hopsHelper = new HopsHelperMngr(proxy, logPrefix);
        hopsTorrent = new HopsLibraryMngr(oexa, proxy, config, logPrefix, selfAdr);
    }
    
    @Override
    public void start() {
        hopsHelper.start();
        hopsTorrent.start();
    }
    
    @Override
    public void close() {
        hopsHelper.close();
        hopsTorrent.close();
    }
    //********************************INTROSPECTION METHODS FOR TESTING*************************************************
    protected HopsLibraryMngr getHopsLibraryMngr() {
      return hopsTorrent;
    }
}
