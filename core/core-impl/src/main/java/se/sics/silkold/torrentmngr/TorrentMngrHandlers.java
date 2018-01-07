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
package se.sics.silkold.torrentmngr;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.kompics.Component;
import se.sics.kompics.Kill;
import se.sics.kompics.Start;
import se.sics.kompics.Stop;
import se.sics.kompics.fsm.FSMBasicStateNames;
import se.sics.kompics.fsm.FSMStateName;
import se.sics.kompics.fsm.handler.FSMBasicEventHandler;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayId;
import se.sics.ktoolbox.util.result.Result;
import se.sics.silkold.torrentmngr.event.StartTorrent;
import se.sics.silkold.torrentmngr.event.StopTorrent;
import se.sics.nstream.torrent.status.event.TorrentReady;
import se.sics.silkold.torrentmngr.event.StoppedTorrentComp;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class TorrentMngrHandlers {

  private static final Logger LOG = LoggerFactory.getLogger(TorrentMngrHandlers.class);

  static FSMBasicEventHandler startTorrent
    = new FSMBasicEventHandler<TorrentMngrExternal, TorrentMngrInternal, StartTorrent.Request>() {
      @Override
      public FSMStateName handle(FSMStateName state, TorrentMngrExternal es, TorrentMngrInternal is,
        StartTorrent.Request req) {
        is.setStartReq(req);
        OverlayId torrentId = is.getTorrentId();
        LOG.info("<{},{}>starting torrent", new Object[]{es.getSelfAdr(), torrentId});
        Component torrentComp = es.connectors.torrentCompConn.apply(is);
        is.setTorrentComp(torrentComp);
        es.torrentComp(torrentId, torrentComp.id());
        es.getProxy().trigger(Start.event, torrentComp.control());
        return TorrentMngrStates.PREPARE_COMP;
      }
    };

  static FSMBasicEventHandler torrentReady
    = new FSMBasicEventHandler<TorrentMngrExternal, TorrentMngrInternal, TorrentReady>() {
      @Override
      public FSMStateName handle(FSMStateName state, TorrentMngrExternal es, TorrentMngrInternal is,
        TorrentReady resp) {
        StartTorrent.Request req = is.getStartReq();
        es.getProxy().answer(req, req.success(Result.success(true)));
        return TorrentMngrStates.READY;
      }
    };

  static FSMBasicEventHandler stopTorrent
    = new FSMBasicEventHandler<TorrentMngrExternal, TorrentMngrInternal, StopTorrent.Request>() {
      @Override
      public FSMStateName handle(FSMStateName state, TorrentMngrExternal es, TorrentMngrInternal is,
        StopTorrent.Request req) {
        LOG.info("<{},{}>stopping torrent", new Object[]{es.getSelfAdr(), is.getTorrentId()});
        is.setStopReq(req);
        es.getProxy().trigger(Stop.event, is.getTorrentComp().control());
        return TorrentMngrStates.STOPPING;
      }
    };

  static FSMBasicEventHandler stoppedTorrentComp
    = new FSMBasicEventHandler<TorrentMngrExternal, TorrentMngrInternal, StoppedTorrentComp>() {
      @Override
      public FSMStateName handle(FSMStateName state, TorrentMngrExternal es, TorrentMngrInternal is,
        StoppedTorrentComp resp) {
        es.removeTorrentComp(resp.compId);
        es.connectors.torrentCompDisc.accept(is);
        es.getProxy().trigger(Kill.event, is.getTorrentComp().control());
        StopTorrent.Request req = is.getStopReq();
        es.getProxy().answer(req, req.success());
        return FSMBasicStateNames.FINAL;
      }
    };
}
