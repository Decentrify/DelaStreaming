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
package se.sics.silk.torrentmngr;

import java.util.List;
import se.sics.kompics.Component;
import se.sics.kompics.fsm.FSMInternalState;
import se.sics.kompics.fsm.FSMInternalStateBuilder;
import se.sics.kompics.fsm.id.FSMIdentifier;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayId;
import se.sics.ktoolbox.util.network.KAddress;
import se.sics.silk.torrentmngr.event.StartTorrent;
import se.sics.silk.torrentmngr.event.StopTorrent;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class TorrentMngrInternal implements FSMInternalState {

  private final FSMIdentifier fsmId;
  private OverlayId torrentId;
  private List<KAddress> partners;
  
  private Component torrentComp;
  private StartTorrent.Request startReq;
  private StopTorrent.Request stopReq;

  public TorrentMngrInternal(FSMIdentifier fsmId) {
    this.fsmId = fsmId;
  }

  @Override
  public FSMIdentifier getFSMId() {
    return fsmId;
  }
  
  public void setStartReq(StartTorrent.Request req) {
    this.startReq = req;
    this.torrentId = req.torrentId;
    this.partners = req.partners;
  }

  public OverlayId getTorrentId() {
    return torrentId;
  }

  public List<KAddress> getPartners() {
    return partners;
  }

  public StartTorrent.Request getStartReq() {
    return startReq;
  }

  public void setStopReq(StopTorrent.Request req) {
    this.stopReq = req;
  }
  
  public StopTorrent.Request getStopReq() {
    return stopReq;
  }

  public Component getTorrentComp() {
    return torrentComp;
  }

  public void setTorrentComp(Component torrentComp) {
    this.torrentComp = torrentComp;
  }
  
  public static class Builder implements FSMInternalStateBuilder {
    @Override
    public FSMInternalState newState(FSMIdentifier fsmId) {
      return new TorrentMngrInternal(fsmId);
    }
  }
}
