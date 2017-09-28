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

import com.google.common.primitives.Ints;
import java.util.Objects;
import se.sics.kompics.id.Identifier;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayId;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class FileId implements Identifier {
    public final OverlayId torrentId;
    public final int fileNr;
    
    FileId(OverlayId torrentId, int fileNr) {
        this.torrentId = torrentId;
        this.fileNr = fileNr;
    }
    @Override
    public int partition(int nrPartitions) {
        assert nrPartitions < Integer.MAX_VALUE / 2;
        int aux1 = torrentId.partition(nrPartitions);
        int aux2 = fileNr % nrPartitions;
        int partition = (aux1 + aux2) % nrPartitions;
        return partition;
    }

    @Override
    public int compareTo(Identifier o) {
        FileId that = (FileId)o;
        int result = torrentId.compareTo(that.torrentId);
        if(result != 0) {
            return result;
        }
        return Ints.compare(fileNr, that.fileNr);
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 53 * hash + Objects.hashCode(this.torrentId);
        hash = 53 * hash + this.fileNr;
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
        if (!Objects.equals(this.torrentId, other.torrentId)) {
            return false;
        }
        if (this.fileNr != other.fileNr) {
            return false;
        }
        return true;
    }
    
    @Override
    public String toString() {
        return "t:" + torrentId.toString() + ",f:" + fileNr; 
    }
}
