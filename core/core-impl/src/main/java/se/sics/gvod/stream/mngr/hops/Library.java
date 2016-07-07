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
package se.sics.gvod.stream.mngr.hops;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.javatuples.Pair;
import se.sics.gvod.mngr.util.ElementSummary;
import se.sics.gvod.mngr.util.FileInfo;
import se.sics.gvod.mngr.util.TorrentInfo;
import se.sics.ktoolbox.hdfs.HopsResource;
import se.sics.ktoolbox.util.identifiable.Identifier;

/**
 *
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class Library {
    private Map<Identifier, HopsResource> hopsResources = new HashMap<>();
    private Map<Identifier, Pair<FileInfo, TorrentInfo>> libraryContents = new HashMap<>();

    public boolean containsTorrent(Identifier torrentId) {
        return libraryContents.containsKey(torrentId);
    }

    public Pair<FileInfo, TorrentInfo> getElement(Identifier torrentId) {
        return libraryContents.get(torrentId);
    }

    public void addTorrent(Identifier torrentId, FileInfo fileInfo, TorrentInfo torrentInfo) {
        libraryContents.put(torrentId, Pair.with(fileInfo, torrentInfo));
    }

    public Pair<FileInfo, TorrentInfo> removeTorrent(Identifier torrentId) {
        return libraryContents.remove(torrentId);
    }

    public List<ElementSummary> getSummary() {
        List<ElementSummary> summary = new ArrayList<>();
        for (Map.Entry<Identifier, Pair<FileInfo, TorrentInfo>> e : libraryContents.entrySet()) {
            ElementSummary es = new ElementSummary(e.getValue().getValue0().name, e.getKey(), e.getValue().getValue1().getStatus());
            summary.add(es);
        }
        return summary;
    }
}
