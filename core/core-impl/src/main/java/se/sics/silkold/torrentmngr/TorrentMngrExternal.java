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

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import se.sics.kompics.ComponentProxy;
import se.sics.kompics.KompicsEvent;
import se.sics.kompics.fsm.FSMExternalState;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayId;
import se.sics.ktoolbox.util.network.KAddress;
import se.sics.silkold.torrentmngr.event.StoppedTorrentComp;

/**
 *
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class TorrentMngrExternal implements FSMExternalState {

  public final KAddress selfAdr;
  public final TorrentMngrComp.Connectors connectors;
  private ComponentProxy proxy;
  private final Map<UUID, OverlayId> torrentComp = new HashMap<>();

  public TorrentMngrExternal(KAddress selfAdr, TorrentMngrComp.Connectors connectors) {
    this.selfAdr = selfAdr;
    this.connectors = connectors;
  }

  @Override
  public void setProxy(ComponentProxy proxy) {
    this.proxy = proxy;
  }

  @Override
  public ComponentProxy getProxy() {
    return proxy;
  }

  public KAddress getSelfAdr() {
    return selfAdr;
  }
  
  public void torrentComp(OverlayId torrentId, UUID compId) {
    torrentComp.put(compId, torrentId);
  }
  
  public Optional<KompicsEvent> stoppedComp(UUID compId) {
    OverlayId torrentId = torrentComp.get(compId);
    if(torrentId != null) {
      KompicsEvent resp = new StoppedTorrentComp(torrentId, compId);
      return Optional.of(resp);
    }
    return Optional.empty();
  }
  
  public void removeTorrentComp(UUID compId) {
    torrentComp.remove(compId);
  }
}
