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

import java.util.Map;
import java.util.Set;
import org.javatuples.Pair;
import se.sics.nstream.FileId;
import se.sics.nstream.transfer.MyTorrent;

/**
 *
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class DataReport {
    public final MyTorrent torrent;
    //<MaxSize, CurrentSize>
    public final Pair<Long, Long> totalSize;
    public final Map<FileId, Pair<Long, Long>> ongoing;
    public final Set<FileId> completed;
    public final Set<FileId> pending;
    
    public DataReport(MyTorrent torrent, Pair<Long, Long> totalSize, Set<FileId> completed, Map<FileId, Pair<Long, Long>> ongoing, Set<FileId> pending) {
        this.torrent = torrent;
        this.totalSize = totalSize;
        this.completed = completed;
        this.ongoing = ongoing;
        this.pending = pending;
    }
}
