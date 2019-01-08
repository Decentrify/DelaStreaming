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
package se.sics.dela.workers.ctrl.util;

import se.sics.dela.network.ledbat.LedbatMsg;
import se.sics.kompics.util.Identifier;
import se.sics.ktoolbox.nutil.network.portsv2.MsgIdExtractorV2;
import se.sics.ktoolbox.nutil.nxcomp.NxStackId;
import se.sics.ktoolbox.util.network.KAddress;
import se.sics.ktoolbox.util.network.KContentMsg;
import se.sics.ktoolbox.util.network.KHeader;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class NxStackMsgIdExtractors {
  public static class Source implements MsgIdExtractorV2<LedbatMsg> {
    private final Identifier netConnStackId;
    
    public Source(Identifier netConnStackId) {
      this.netConnStackId = netConnStackId;
    }

    @Override
    public NxStackId getValue(KContentMsg<KAddress, KHeader<KAddress>, LedbatMsg> msg) {
      return new NxStackId(netConnStackId, msg.getHeader().getSource().getId());
    }
  }
}
