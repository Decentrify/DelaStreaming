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
import se.sics.kompics.Channel;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Handler;
import se.sics.kompics.Negative;
import se.sics.kompics.PortType;
import se.sics.kompics.Positive;
import se.sics.kompics.Start;
import se.sics.kompics.fsm.FSMException;
import se.sics.kompics.fsm.OnFSMExceptionAction;
import se.sics.ktoolbox.util.network.KAddress;
import se.sics.nstream.library.event.system.SystemAddressEvent;
import se.sics.nstream.library.restart.TorrentRestartPort;
import se.sics.nstream.storage.durable.DEndpointCtrlPort;
import se.sics.silkold.torrent.TorrentMngrPort;
import se.sics.nstream.torrent.tracking.TorrentStatusPort;
import se.sics.nstream.torrent.transfer.TransferCtrlPort;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class LibraryMngrComp extends ComponentDefinition {

    private static final Logger LOG = LoggerFactory.getLogger(LibraryMngrComp.class);
    private String logPrefix = "";

    //***************************CONNECTIONS************************************
    //**********************EXTERNAL_CONNECT_TO*********************************
    Positive<DEndpointCtrlPort> endpointControlPort = requires(DEndpointCtrlPort.class);
    Positive<TorrentMngrPort> torrentMngrPort = requires(TorrentMngrPort.class);
    Positive<TransferCtrlPort> transferCtrl = requires(TransferCtrlPort.class);
    Positive<TorrentStatusPort> reportPort = requires(TorrentStatusPort.class);
    Positive<TorrentRestartPort> restartPort1 = requires(TorrentRestartPort.class);
    Negative<TorrentRestartPort> restartPort2 = provides(TorrentRestartPort.class);
    
    Negative<SystemPort> systemPort = provides(SystemPort.class);
    List<Negative> providedPorts = new ArrayList<>();
    //**************************EXTERNAL_STATE**********************************
    private final KAddress selfAdr;
    //**************************INTERNAL_STATE**********************************
    private final LibraryProvider torrentProvider;

    public LibraryMngrComp(Init init) {
        LOG.info("{}initiating...", logPrefix);

        selfAdr = init.selfAddress;
        torrentProvider = init.libraryProvider;
        setupLibraryProvider();
        LOG.info("{}initiated", logPrefix);
        
        connect(proxy.getPositive(TorrentRestartPort.class), proxy.getNegative(TorrentRestartPort.class), Channel.TWO_WAY);

        subscribe(handleStart, control);
        subscribe(handleSystemAddress, systemPort);
    }

    private void setupLibraryProvider() {
        for (Class<PortType> r : torrentProvider.providesPorts()) {
            providedPorts.add(provides(r));
        }
        OnFSMExceptionAction oexa = new OnFSMExceptionAction() {

          @Override
          public void handle(FSMException ex) {
            throw new RuntimeException(ex);
          }
        };
        torrentProvider.create(proxy, config(), logPrefix, selfAdr, oexa);
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

        public final KAddress selfAddress;
        public final LibraryProvider libraryProvider;

        public Init(KAddress selfAddress, LibraryProvider libraryProvider) {
            this.selfAddress = selfAddress;
            this.libraryProvider = libraryProvider;
        }
    }
    
    //********************************INTROSPECTION METHODS FOR TESTING*************************************************
    protected LibraryProvider getLibraryProvider() {
      return torrentProvider;
    }
}
