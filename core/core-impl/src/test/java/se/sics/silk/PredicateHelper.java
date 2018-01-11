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

import com.google.common.base.Predicate;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayId;
import se.sics.ktoolbox.util.network.KAddress;
import se.sics.ktoolbox.util.network.basic.BasicContentMsg;
import se.sics.nutil.network.bestEffort.event.BestEffortMsg;
import se.sics.silk.event.SilkEvent;

/**
 *
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class PredicateHelper {
  
  public static final Predicate TRUE_P = new Predicate<Object>() {
    @Override
    public boolean apply(Object t) {
      return true;
    }
  };

  public static class MsgBEReqPredicate<C extends Object> implements Predicate<BasicContentMsg> {

    private final Class<C> contentType;
    private final Predicate<BasicContentMsg> msgP;
    private final Predicate<C> contentP;

    public MsgBEReqPredicate(Class<C> contentType, Predicate<BasicContentMsg> msgP, Predicate<C> contentP) {
      this.contentType = contentType;
      this.msgP = msgP;
      this.contentP = contentP;
    }

    @Override
    public boolean apply(BasicContentMsg msg) {
      if (!msgP.apply(msg)) {
        return false;
      }
      if (msg.extractValue() instanceof BestEffortMsg.Request) {
        BestEffortMsg.Request wrapper = (BestEffortMsg.Request) msg.extractValue();
        if (contentType.isAssignableFrom(wrapper.extractValue().getClass())) {
          C content = (C) wrapper.extractValue();
          return contentP.apply(content);
        }
      }
      return false;
    }
  }

  public static class MsgDstPredicate implements Predicate<BasicContentMsg> {

    private final KAddress dst;

    public MsgDstPredicate(KAddress dst) {
      this.dst = dst;
    }

    @Override
    public boolean apply(BasicContentMsg msg) {
      return msg.getDestination().equals(dst);
    }
  }

  public static class TorrentEPredicate<C extends SilkEvent.TorrentEvent> implements Predicate<C> {

    private final OverlayId torrentId;

    public TorrentEPredicate(OverlayId torrentId) {
      this.torrentId = torrentId;
    }

    @Override
    public boolean apply(C t) {
      return t.torrentId().equals(torrentId);
    }
  }
}
