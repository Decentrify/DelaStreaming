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
package se.sics.cobweb.transfer;

import se.sics.cobweb.util.HandleEvent;
import se.sics.kompics.KompicsEvent;
import se.sics.ktoolbox.util.network.KContentMsg;
import se.sics.ktoolbox.util.network.ports.ChannelFilter;
import se.sics.nutil.ContentWrapper;
import se.sics.nutil.ContentWrapperHelper;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class TransferFilters {
  public static class Timeout implements ChannelFilter {

    @Override
    public boolean filter(KompicsEvent event) {
      return event instanceof HandleEvent.Timeout;
    }
  }

  public static class Network implements ChannelFilter {

    @Override
    public boolean filter(KompicsEvent event) {
      if(!(event instanceof KContentMsg)) {
        return false;
      }
      Object baseContent = ((KContentMsg) event).getContent();
      if (baseContent instanceof ContentWrapper) {
        baseContent = ContentWrapperHelper.getBaseContent((ContentWrapper) baseContent);
      }
      return baseContent instanceof TransferEvent.Base;
    }
  }
}
