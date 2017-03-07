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
package se.sics.cobweb.overlord.handle;

import se.sics.cobweb.conn.event.ConnE;
import se.sics.cobweb.overlord.conn.api.ConnectionDecider;
import se.sics.cobweb.overlord.conn.impl.LocalLeechersViewImpl;
import se.sics.cobweb.transfer.handle.LeecherHandleCtrlPort;
import se.sics.cobweb.transfer.handlemngr.HandleMngrPort;
import se.sics.cobweb.transfer.handlemngr.event.HandleMngrE;
import se.sics.kompics.ComponentProxy;
import se.sics.kompics.Positive;
import se.sics.ktoolbox.nutil.fsm.api.FSMExternalState;
import se.sics.ktoolbox.util.network.KAddress;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class LeecherHandleOverlordExternal implements FSMExternalState {

  private ComponentProxy proxy;

  public final KAddress selfAdr;
  public final ConnectionDecider.LeecherSide decider;
  public final LocalLeechersViewImpl view;

  public final Positive<HandleMngrPort> handleMngrPort;
  public final Positive<LeecherHandleCtrlPort> leecherHandleCtrlPort;

  public LeecherHandleOverlordExternal(KAddress selfAdr, ConnectionDecider.LeecherSide decider, LocalLeechersViewImpl view,
    Positive<HandleMngrPort> handleMngrPort, Positive<LeecherHandleCtrlPort> leecherHandleCtrlPort) {
    this.selfAdr = selfAdr;
    this.decider = decider;
    this.view = view;
    this.handleMngrPort = handleMngrPort;
    this.leecherHandleCtrlPort = leecherHandleCtrlPort;
  }

  @Override
  public void setProxy(ComponentProxy proxy) {
    this.proxy = proxy;
  }

  @Override
  public ComponentProxy getProxy() {
    return proxy;
  }

  public void triggerSetupHandle(ConnE.Connect1Accept event) {
     HandleMngrE.LeecherConnect req = new HandleMngrE.LeecherConnect(event.torrentId, event.handleId, event.seederAdr);
     proxy.trigger(req, handleMngrPort);
  }
}
