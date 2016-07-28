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
package se.sics.nstream.library;

import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.gvod.stream.report.ReportPort;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Handler;
import se.sics.kompics.Negative;
import se.sics.kompics.PortType;
import se.sics.kompics.Positive;
import se.sics.kompics.Start;
import se.sics.ktoolbox.util.network.KAddress;
import se.sics.nstream.library.event.system.SystemAddressEvent;
import se.sics.nstream.transfer.TransferMngrPort;
import se.sics.nstream.util.CoreExtPorts;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class LibraryMngrComp extends ComponentDefinition {

    private static final Logger LOG = LoggerFactory.getLogger(LibraryMngrComp.class);
    private String logPrefix = "";

    //***************************CONNECTIONS************************************
    private final CoreExtPorts extPorts;
    //**********************EXTERNAL_CONNECT_TO*********************************
    Negative<SystemPort> systemPort = provides(SystemPort.class);
    List<Negative> providedPorts = new ArrayList<>();
    //*******************INTERNAL_DO_NOT_CONNECT_TO*****************************
    Negative<TransferMngrPort> transferMngrPort = provides(TransferMngrPort.class);
    Positive<ReportPort> reportPort = requires(ReportPort.class);
    //**************************EXTERNAL_STATE**********************************
    private final KAddress selfAdr;
    //**************************INTERNAL_STATE**********************************
    private final TorrentProvider torrentProvider;

    public LibraryMngrComp(Init init) {
        LOG.info("{}initiating...", logPrefix);

        extPorts = init.extPorts;
        selfAdr = init.selfAddress;
        torrentProvider = init.torrentProvider;
        setupTorrentProvider();

        subscribe(handleStart, control);
        subscribe(handleSystemAddress, systemPort);
    }

    private void setupTorrentProvider() {
        for (Class<PortType> r : torrentProvider.providesPorts()) {
            providedPorts.add(provides(r));
        }
        torrentProvider.create(proxy, config(), logPrefix, selfAdr, extPorts);
    }

    Handler handleStart = new Handler<Start>() {
        @Override
        public void handle(Start event) {
            LOG.info("{}starting...", logPrefix);
            torrentProvider.start();
        }
    };

    @Override
    public void tearDown() {
        torrentProvider.close();
    }

    Handler handleSystemAddress = new Handler<SystemAddressEvent.Request>() {
        @Override
        public void handle(SystemAddressEvent.Request req) {
            LOG.trace("{}received:{}", logPrefix, req);
            answer(req, req.success(selfAdr));
        }
    };

    public static class Init extends se.sics.kompics.Init<LibraryMngrComp> {

        public final CoreExtPorts extPorts;
        public final KAddress selfAddress;
        public final TorrentProvider torrentProvider;

        public Init(CoreExtPorts extPorts, KAddress selfAddress, TorrentProvider torrentProvider) {
            this.extPorts = extPorts;
            this.selfAddress = selfAddress;
            this.torrentProvider = torrentProvider;
        }
    }
}
