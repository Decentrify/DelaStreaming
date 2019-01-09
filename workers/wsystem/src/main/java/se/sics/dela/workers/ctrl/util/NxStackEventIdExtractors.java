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

import se.sics.dela.network.ledbat.LedbatEvent;
import se.sics.kompics.util.Identifier;
import se.sics.ktoolbox.nutil.network.portsv2.EventIdExtractorV2;
import se.sics.ktoolbox.nutil.nxcomp.NxStackId;
import se.sics.ktoolbox.nutil.timer.TimerProxyImpl;
import se.sics.ktoolbox.util.identifiable.basic.PairIdentifier;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class NxStackEventIdExtractors {

  public static class TimerProxy implements EventIdExtractorV2<TimerProxyImpl.Timeout> {

    private final Identifier stackId;

    public TimerProxy(Identifier stackId) {
      this.stackId = stackId;
    }

    @Override
    public NxStackId getValue(TimerProxyImpl.Timeout timeout) {
      return new NxStackId(stackId, timeout.timerProxyId());
    }
  }

  public static class LedbatData implements EventIdExtractorV2<LedbatEvent> {

    private final Identifier stackId;

    public LedbatData(Identifier stackId) {
      this.stackId = stackId;
    }
    
    @Override
    public Identifier getValue(LedbatEvent event) {
      return new NxStackId(stackId, event.dataId());
    }
  }
  
  public static class LedbatSenderRivulet implements EventIdExtractorV2<LedbatEvent> {

    private final Identifier stackId;

    public LedbatSenderRivulet(Identifier stackId) {
      this.stackId = stackId;
    }
    
    @Override
    public Identifier getValue(LedbatEvent event) {
      return new NxStackId(stackId, ((PairIdentifier)event.rivuletId()).id2);
    }
  }
  
  public static class LedbatReceiverRivulet implements EventIdExtractorV2<LedbatEvent> {

    private final Identifier stackId;

    public LedbatReceiverRivulet(Identifier stackId) {
      this.stackId = stackId;
    }
    
    @Override
    public Identifier getValue(LedbatEvent event) {
      return new NxStackId(stackId, ((PairIdentifier)event.rivuletId()).id1);
    }
  }
}
