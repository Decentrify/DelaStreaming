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
package se.sics.silk;

import se.sics.kompics.KompicsEvent;
import se.sics.kompics.network.Msg;
import se.sics.kompics.testing.Future;
import se.sics.ktoolbox.util.network.basic.BasicContentMsg;
import se.sics.nutil.network.bestEffort.event.BestEffortMsg;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class FutureHelper {
  public static abstract class NetBEFuture<C extends KompicsEvent> extends Future<Msg, Msg> {

    private Class<C> contentType;
    protected BasicContentMsg msg;
    protected BestEffortMsg.Request wrap;
    protected C content;

    protected NetBEFuture(Class<C> contentType) {
      this.contentType = contentType;
    }

    @Override
    public boolean set(Msg r) {
      if (!(r instanceof BasicContentMsg)) {
        return false;
      }
      this.msg = (BasicContentMsg) r;
      if (!(msg.getContent() instanceof BestEffortMsg.Request)) {
        return false;
      }
      wrap = (BestEffortMsg.Request) msg.getContent();
      if (!(contentType.isAssignableFrom(wrap.extractValue().getClass()))) {
        return false;
      }
      this.content = (C) wrap.extractValue();
      return true;
    }
  }
  
  public static abstract class BasicFuture<I extends KompicsEvent, O extends KompicsEvent> extends Future<I, O> {

    protected I event;

    @Override
    public boolean set(I event) {
      this.event = event;
      return true;
    }
  }
}
