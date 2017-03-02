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
package se.sics.nstream.hops.libmngr.fsm;

import se.sics.kompics.ComponentProxy;
import se.sics.kompics.Positive;
import se.sics.ktoolbox.nutil.fsm.api.FSMExternalState;
import se.sics.ktoolbox.util.network.KAddress;
import se.sics.nstream.hops.library.Details;
import se.sics.nstream.library.Library;
import se.sics.nstream.library.endpointmngr.EndpointIdRegistry;
import se.sics.nstream.storage.durable.DEndpointCtrlPort;
import se.sics.nstream.torrent.TorrentMngrPort;
import se.sics.nstream.torrent.tracking.TorrentStatusPort;
import se.sics.nstream.torrent.transfer.TransferCtrlPort;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class LibTExternal implements FSMExternalState {

  private ComponentProxy proxy;
  public final KAddress selfAdr;
  public final Library library;
  public final EndpointIdRegistry endpointIdRegistry;
  public final Details.Types fsmType;

  public LibTExternal(KAddress selfAdr, Library library, EndpointIdRegistry endpointIdRegistry,
    Details.Types fsmType) {
    this.selfAdr = selfAdr;
    this.library = library;
    this.endpointIdRegistry = endpointIdRegistry;
    this.fsmType = fsmType;
  }

  @Override
  public void setProxy(ComponentProxy proxy) {
    this.proxy = proxy;
  }

  @Override
  public ComponentProxy getProxy() {
    return proxy;
  }

  public Positive endpointPort() {
    return proxy.getNegative(DEndpointCtrlPort.class).getPair();
  }

  public Positive torrentMngrPort() {
    return proxy.getNegative(TorrentMngrPort.class).getPair();
  }

  public Positive transferCtrlPort() {
    return proxy.getNegative(TransferCtrlPort.class).getPair();
  }

  public Positive torrentStatusPort() {
    return proxy.getNegative(TorrentStatusPort.class).getPair();
  }
}
