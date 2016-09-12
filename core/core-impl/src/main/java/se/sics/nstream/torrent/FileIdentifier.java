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
package se.sics.nstream.torrent;

import java.util.Objects;
import se.sics.ktoolbox.util.identifiable.Identifier;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class FileIdentifier implements Identifier {

    public final Identifier overlayId;
    public final int fileId;

    public FileIdentifier(Identifier overlayId, int fileId) {
        this.overlayId = overlayId;
        this.fileId = fileId;
    }

    @Override
    public int partition(int nrPartitions) {
        assert nrPartitions < Integer.MAX_VALUE / 2;
        int overlayPartition = overlayId.partition(nrPartitions);
        int filePartition = fileId % nrPartitions;
        return (overlayPartition + filePartition) % 2;
    }

    @Override
    public int compareTo(Identifier o) {
        FileIdentifier that = (FileIdentifier) o;
        int aux = this.overlayId.compareTo(that.overlayId);
        if (aux == 0) {
            return Integer.compare(this.fileId, that.fileId);
        } else {
            return aux;
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final FileIdentifier that = (FileIdentifier) obj;
        if (!Objects.equals(this.overlayId, that.overlayId)) {
            return false;
        }
        if (this.fileId != that.fileId) {
            return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 97 * hash + Objects.hashCode(this.overlayId);
        hash = 97 * hash + this.fileId;
        return hash;
    }

    @Override 
    public String toString() {
        return "<oid:" + overlayId.toString() + ",fid:" + fileId +">";
    }
}
