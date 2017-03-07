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
package se.sics.cobweb.overlord;

import se.sics.cobweb.conn.ConnPort;
import se.sics.cobweb.overlord.conn.api.ConnectionDecider;
import se.sics.cobweb.transfer.handlemngr.HandleMngrPort;
import se.sics.cobweb.transfer.instance.TransferPort;
import se.sics.kompics.Component;
import se.sics.kompics.ComponentProxy;
import se.sics.kompics.Positive;
import se.sics.ktoolbox.util.network.KAddress;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public interface OverlordCreator {

  public Component create(ComponentProxy proxy, KAddress selfAdr, ConnectionDecider.SeederSide seederSideDecider,
    ConnectionDecider.LeecherSide leecherSideDecider);

  public void connect(ComponentProxy proxy, Component overlord, Positive<ConnPort> connPort,
    Positive<HandleMngrPort> handleMngrPort, Positive<TransferPort> transferPort);

  public void start(ComponentProxy proxy, Component overlord);
}
