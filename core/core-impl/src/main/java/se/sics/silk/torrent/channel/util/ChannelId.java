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
package se.sics.silk.torrent.channel.util;

import com.google.common.primitives.Ints;
import java.util.Objects;
import se.sics.kompics.util.Identifier;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayId;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class ChannelId implements Identifier {

  public final OverlayId torrentId;
  public final int channel;

  public ChannelId(OverlayId torrentId, int channel) {
    this.torrentId = torrentId;
    this.channel = channel;
  }

  @Override
  public int partition(int nrPartitions) {
    return torrentId.partition(nrPartitions);
  }

  @Override
  public int compareTo(Identifier o) {
    ChannelId that = (ChannelId) o;
    int result = this.torrentId.compareTo(that.torrentId);
    if (result != 0) {
      return result;
    }
    result = Ints.compare(this.channel, that.channel);
    return result;
  }

  @Override
  public int hashCode() {
    int hash = 3;
    hash = 89 * hash + Objects.hashCode(this.torrentId);
    hash = 89 * hash + this.channel;
    return hash;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    final ChannelId other = (ChannelId) obj;
    if (!Objects.equals(this.torrentId, other.torrentId)) {
      return false;
    }
    if (this.channel != other.channel) {
      return false;
    }
    return true;
  }
  
  
}
