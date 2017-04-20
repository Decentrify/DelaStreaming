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
package se.sics.nstream.torrent.conn;

import se.sics.ktoolbox.util.identifiable.Identifier;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class ConnectionId implements Identifier {
  public final Identifier overlayBaseId;
  public final Identifier peerId;
  
  public ConnectionId(Identifier overlayBaseId, Identifier peerId) {
    this.overlayBaseId = overlayBaseId;
    this.peerId = peerId;
  }
  @Override
  public int partition(int nrPartitions) {
    if(nrPartitions > Integer.MAX_VALUE / 2) {
      throw new RuntimeException("fix this");
    }
    int partition = (overlayBaseId.partition(nrPartitions) + peerId.partition(nrPartitions)) % nrPartitions;
    return partition;
  }

  @Override
  public int compareTo(Identifier o) {
    int result;
    ConnectionId that = (ConnectionId) o;
    result = this.overlayBaseId.compareTo(that.overlayBaseId);
    if(result == 0) {
      result = this.peerId.compareTo(that.peerId);
    }
    return result;
  }
}
