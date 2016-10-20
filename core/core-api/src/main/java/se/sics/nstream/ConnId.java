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
package se.sics.nstream;

import java.util.Objects;
import se.sics.ktoolbox.util.identifiable.Identifier;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class ConnId implements Identifier {

    public final FileId fileId;
    public final Identifier peerId;
    //refers to target handling component, if it is of type leecher or seeder
    public final boolean leecher;

    ConnId(FileId fileId, Identifier peerId, boolean leecher) {
        this.fileId = fileId;
        this.peerId = peerId;
        this.leecher = leecher;
    }
    
    public ConnId reverse() {
        return new ConnId(fileId, peerId, !leecher);
    }

    @Override
    public int partition(int nrPartitions) {
        assert nrPartitions < Integer.MAX_VALUE / 2;
        int aux1 = fileId.partition(nrPartitions);
        int aux2 = peerId.partition(nrPartitions);
        int aux3 = leecher ? 0 : 1;
        int partition = (aux1 + aux2 + aux3) % nrPartitions;
        return partition;
    }

    @Override
    public int compareTo(Identifier o) {
        ConnId that = (ConnId)o;
        int result = fileId.compareTo(that.fileId);
        if(result != 0) {
            return result;
        }
        result = peerId.compareTo(that.peerId);
        if(result != 0) {
            return result;
        }
        if(leecher == that.leecher) {
            return 0;
        }
        result = leecher ? -1 : 1;
        return result;
    }

    @Override
    public int hashCode() {
        int hash = 3;
        hash = 71 * hash + Objects.hashCode(this.fileId);
        hash = 71 * hash + Objects.hashCode(this.peerId);
        hash = 71 * hash + (this.leecher ? 1 : 0);
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
        final ConnId other = (ConnId) obj;
        if (!Objects.equals(this.fileId, other.fileId)) {
            return false;
        }
        if (!Objects.equals(this.peerId, other.peerId)) {
            return false;
        }
        if (this.leecher != other.leecher) {
            return false;
        }
        return true;
    }
    
    @Override
    public String toString() {
        return fileId.toString() + ",p:" + peerId.toString() + ":" + (leecher ? "leecher" : "seeder");
    }
}
