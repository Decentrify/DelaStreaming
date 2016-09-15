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
package se.sics.nstream.torrent.util;

import java.util.Objects;
import se.sics.ktoolbox.util.identifiable.Identifier;
import se.sics.nstream.torrent.FileIdentifier;

/**
 *
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class TorrentConnId implements Identifier {
    public final Identifier targetId;
    public final FileIdentifier fileId;
    public final boolean leecher;
    
    public TorrentConnId(Identifier targetId, FileIdentifier fileId, boolean leecher) {
        this.targetId = targetId;
        this.fileId = fileId;
        this.leecher = leecher;
    }
    
    public TorrentConnId reverse() {
        return new TorrentConnId(targetId, fileId, !leecher);
    }
    
    @Override
    public int hashCode() {
        int hash = 3;
        hash = 79 * hash + Objects.hashCode(this.targetId);
        hash = 79 * hash + Objects.hashCode(this.fileId);
        hash = 79 * hash + (this.leecher ? 1 : 0);
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
        final TorrentConnId other = (TorrentConnId) obj;
        if (!Objects.equals(this.targetId, other.targetId)) {
            return false;
        }
        if (!Objects.equals(this.fileId, other.fileId)) {
            return false;
        }
        if (this.leecher != other.leecher) {
            return false;
        }
        return true;
    }

    @Override
    public int partition(int nrPartitions) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public int compareTo(Identifier o) {
        TorrentConnId that = (TorrentConnId)o;
        int result = this.fileId.compareTo(that.fileId);
        if(result != 0) {
            return result;
        }
        result = this.targetId.compareTo(that.targetId);
        return result;
    }
    
    @Override
    public String toString() {
        return "<nid:" + targetId + ",fid:" + fileId + ">";
    }
}
