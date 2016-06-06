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
package se.sics.gvod.mngr;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.gvod.core.VoDPort;
import se.sics.gvod.mngr.event.TorrentDownloadEvent;
import se.sics.gvod.mngr.event.LibraryContentsEvent;
import se.sics.gvod.mngr.event.LibraryElementEvent;
import se.sics.gvod.mngr.event.TorrentStopEvent;
import se.sics.gvod.mngr.event.TorrentUploadEvent;
import se.sics.gvod.mngr.event.VideoPlayEvent;
import se.sics.gvod.mngr.event.VideoStopEvent;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Handler;
import se.sics.kompics.Negative;
import se.sics.kompics.Positive;
import se.sics.kompics.Start;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class VoDMngrComp extends ComponentDefinition {
    private static final Logger LOG = LoggerFactory.getLogger(VoDMngrComp.class);
    private String logPrefix;
    
    //***************************CONNECTIONS************************************
    Negative<LibraryPort> libraryPort = provides(LibraryPort.class);
    Negative<TorrentPort> torrentPort = provides(TorrentPort.class);
    Negative<VideoPort> videoPort = provides(VideoPort.class);
    //**************************INTERNAL_STATE**********************************
    private final LibraryMngr libMngr;
    
    public VoDMngrComp(Init init) {
        LOG.info("{}initiating...", logPrefix);
        
        libMngr = new LibraryMngr(null, null);
    
        subscribe(handleStart, control);
        subscribe(handleLibraryContent, libraryPort);
        subscribe(handleLibraryElement, libraryPort);
        subscribe(handleTorrentUpload, torrentPort);
        subscribe(handleTorrentDownload, torrentPort);
        subscribe(handleTorrentStop, torrentPort);
        subscribe(handleVideoPlay, videoPort);
        subscribe(handleVideoStop, videoPort);
    }
    
    Handler handleStart = new Handler<Start>() {
        @Override
        public void handle(Start event) {
            LOG.info("{}starting...", logPrefix);
        }
    };
    
    Handler handleLibraryContent = new Handler<LibraryContentsEvent.Request>() {
        @Override
        public void handle(LibraryContentsEvent.Request req) {
            LOG.trace("{}received:{}", logPrefix, req);
        }
    };
    
     Handler handleLibraryElement = new Handler<LibraryElementEvent.Request>() {
        @Override
        public void handle(LibraryElementEvent.Request req) {
            LOG.trace("{}received:{}", logPrefix, req);
        }
    };
    
    Handler handleTorrentUpload = new Handler<TorrentUploadEvent.Request>() {
        @Override
        public void handle(TorrentUploadEvent.Request req) {
            LOG.trace("{}received:{}", logPrefix, req);
        }
    };
    
    Handler handleTorrentDownload = new Handler<TorrentDownloadEvent.Request>() {
        @Override
        public void handle(TorrentDownloadEvent.Request req) {
            LOG.trace("{}received:{}", logPrefix, req);
        }
    };
    
    Handler handleTorrentStop = new Handler<TorrentStopEvent.Request>() {
        @Override
        public void handle(TorrentStopEvent.Request req) {
            LOG.trace("{}received:{}", logPrefix, req);
        }
    };
    
    Handler handleVideoPlay = new Handler<VideoPlayEvent.Request>() {
        @Override
        public void handle(VideoPlayEvent.Request event) {
            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        }
    };
    
    Handler handleVideoStop = new Handler<VideoStopEvent.Request>() {
        @Override
        public void handle(VideoStopEvent.Request event) {
            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        }
    };
    
    public static class Init extends se.sics.kompics.Init<VoDMngrComp> {
    }
}
