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
package se.sics.cobweb.util;

import com.google.common.primitives.Ints;
import java.util.Objects;
import se.sics.ktoolbox.util.identifiable.Identifier;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class FileId implements Identifier {

  public final Identifier torrentBaseId;
  public final int file;

  public FileId(Identifier torrentBaseId, int file) {
    this.torrentBaseId = torrentBaseId;
    this.file = file;
  }
  
  public HandleId leecher(Identifier peerId) {
    return new HandleId(torrentBaseId, file, peerId, true);
  }
  
  public HandleId seeder(Identifier peerId) {
    return new HandleId(torrentBaseId, file, peerId, false);
  }

  @Override
  public String toString() {
    return "<tId:" + torrentBaseId + ",f:" + file + ">";
  }

  @Override
  public int partition(int nrPartitions) {
    assert nrPartitions < Integer.MAX_VALUE / 2;
    int aux1 = torrentBaseId.partition(nrPartitions);
    int aux2 = file % nrPartitions;
    int partition = (aux1 + aux2) % nrPartitions;
    return partition;
  }

  @Override
  public int hashCode() {
    int hash = 3;
    hash = 79 * hash + Objects.hashCode(this.torrentBaseId);
    hash = 79 * hash + this.file;
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
    final FileId other = (FileId) obj;
    if (!Objects.equals(this.torrentBaseId, other.torrentBaseId)) {
      return false;
    }
    if (this.file != other.file) {
      return false;
    }
    return true;
  }

  @Override
  public int compareTo(Identifier o) {
    FileId that = (FileId) o;
    int result = this.torrentBaseId.compareTo(that.torrentBaseId);
    if (result != 0) {
      return result;
    }
    result = Ints.compare(this.file, that.file);
    if (result != 0) {
      return result;
    }
    return result;
  }
}
