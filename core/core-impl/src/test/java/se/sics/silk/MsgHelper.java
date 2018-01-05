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
import se.sics.kompics.network.Msg;
import se.sics.kompics.network.Transport;
import se.sics.ktoolbox.util.network.KAddress;
import se.sics.ktoolbox.util.network.basic.BasicContentMsg;
import se.sics.ktoolbox.util.network.basic.BasicHeader;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class MsgHelper {
  public static <C extends Object> Predicate<Msg> incNetP(KAddress leecher, Class<C> contentType) {
    return (Msg m) -> {
      if (!(m instanceof BasicContentMsg)) {
        return false;
      }
      BasicContentMsg msg = (BasicContentMsg) m;
      if(!(contentType.isAssignableFrom(msg.getContent().getClass()))) {
        return false;
      }
      return msg.getDestination().equals(leecher);
    };
  }
  
  public static <C extends Object> BasicContentMsg msg(KAddress src, KAddress dst, C content) {
    BasicHeader header = new BasicHeader(src, dst, Transport.UDP);
    BasicContentMsg msg = new BasicContentMsg(header, content);
    return msg;
  }
}
