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
public class StreamId implements Identifier {
    public final Identifier endpointId;
    public final FileId fileId;
    
    StreamId(Identifier endpointId, FileId fileId) {
        this.endpointId = endpointId;
        this.fileId = fileId;
    } 
    
    public StreamId withFile(FileId fileId) {
        return new StreamId(endpointId, fileId);
    }

    @Override
    public int partition(int nrPartitions) {
        assert nrPartitions < Integer.MAX_VALUE / 2;
        int aux1 = endpointId.partition(nrPartitions);
        int aux2 = endpointId.partition(nrPartitions);
        int partition = (aux1 + aux2) % nrPartitions;
        return partition;
    }

    @Override
    public int compareTo(Identifier o) {
        StreamId that = (StreamId)o;
        int result = endpointId.compareTo(that.endpointId);
        if(result != 0) {
            return result;
        }
        result = fileId.compareTo(that.fileId);
        return result;
    }

    @Override
    public int hashCode() {
        int hash = 3;
        hash = 41 * hash + Objects.hashCode(this.endpointId);
        hash = 41 * hash + Objects.hashCode(this.fileId);
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
        final StreamId other = (StreamId) obj;
        if (!Objects.equals(this.endpointId, other.endpointId)) {
            return false;
        }
        if (!Objects.equals(this.fileId, other.fileId)) {
            return false;
        }
        return true;
    }
    
    @Override
    public String toString() {
        return "e:" + endpointId.toString() + "," + fileId.toString();
    }
}
