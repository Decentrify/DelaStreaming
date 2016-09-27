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
package se.sics.nstream.hops.library;

import se.sics.ktoolbox.util.identifiable.Identifier;
import se.sics.nstream.hops.hdfs.HDFSResource;

/**
 *
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class TorrentSummary {

    private String stringId;
    private String torrentName;
    private String endpointId;
    private String torrentDir;
    private String manifest;
    
    public TorrentSummary() {}
    
    private TorrentSummary(String stringId, String torrentName, String endpointId, String torrentDir, String manifest) {
        this.stringId = stringId;
        this.torrentName = torrentName;
        this.endpointId = endpointId;
        this.torrentDir = torrentDir;
        this.manifest = manifest;
    }

    public String getStringId() {
        return stringId;
    }

    public void setStringId(String stringId) {
        this.stringId = stringId;
    }

    public String getTorrentName() {
        return torrentName;
    }

    public void setTorrentName(String torrentName) {
        this.torrentName = torrentName;
    }

    public String getEndpointId() {
        return endpointId;
    }

    public void setEndpointId(String endpointId) {
        this.endpointId = endpointId;
    }

    public String getTorrentDir() {
        return torrentDir;
    }

    public void setTorrentDir(String torrentDir) {
        this.torrentDir = torrentDir;
    }

    public String getManifest() {
        return manifest;
    }

    public void setManifest(String manifest) {
        this.manifest = manifest;
    }
    
    public HDFSResource getManifestResource() {
        throw new RuntimeException("fix me");
//        Identifier manifestId = StringByteId.instance(stringId + "0");
//        HDFSResource manifestResource = new HDFSResource(torrentDir, manifest, manifestId); 
//        return manifestResource;
    }

    public static TorrentSummary getSummary(Identifier torrentId, String torrentName, Library.Torrent torrent) {
        String sTorrentId = torrentId.toString();
        String sEndpointId = torrent.hdfsEndpoint.endpointId.toString();
        String torrentDir = torrent.manifest.dirPath;
        String manifest = torrent.manifest.fileName;
        return new TorrentSummary(sTorrentId, torrentName, sEndpointId, torrentDir, manifest);
    }
}
