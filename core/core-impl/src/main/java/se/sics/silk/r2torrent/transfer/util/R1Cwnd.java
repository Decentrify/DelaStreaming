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
package se.sics.silk.r2torrent.transfer.util;

import java.util.HashSet;
import java.util.Set;
import se.sics.kompics.util.Identifier;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class R1Cwnd {
  private final Set<Identifier> pendingMsgs = new HashSet<>();
  private int size;
  
  public R1Cwnd(int size) {
    this.size = size;
  }
  public boolean canSend() {
    return pendingMsgs.size() < size;
  }
  
  public void send(Identifier msgId) {
    pendingMsgs.add(msgId);
  }
  
  public boolean timeout(Identifier msgId) {
    return pendingMsgs.remove(msgId);
  }
  
  public boolean receive(Identifier msgId) {
    return pendingMsgs.remove(msgId);
  }
}
