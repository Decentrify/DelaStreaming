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
package se.sics.nstream.library.disk;

import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class HDFSLibrarySummaryJSON {
    private List<TorrentJSON> torrents = new ArrayList<>();

    public List<TorrentJSON> getTorrents() {
        return torrents;
    }
    
    public void setTorrents(List<TorrentJSON> torrents) {
        this.torrents = torrents;
    }
    
    public void addTorrent(TorrentJSON torrent) {
        torrents.add(torrent);
    }
    
    public boolean isEmpty() {
        return torrents.isEmpty();
    }

    public static class HDFSEndpointJSON {
        private String url;
        private String user;

        public HDFSEndpointJSON() {}
        
        public String getUrl() {
            return url;
        }

        public void setUrl(String url) {
            this.url = url;
        }

        public String getUser() {
            return user;
        }

        public void setUser(String user) {
            this.user = user;
        }
    }
    
    public static class TorrentJSON {
        private Integer projectId;
        private String torrentName;
        private String torrentStatus;
        private String baseId;
        private HDFSEndpointJSON endpoint;
        private String dirPath;

        public TorrentJSON(){}

        public Integer getProjectId() {
          return projectId;
        }

        public void setProjectId(Integer projectId) {
          this.projectId = projectId;
        }
        
        public String getTorrentName() {
            return torrentName;
        }

        public void setTorrentName(String torrentName) {
            this.torrentName = torrentName;
        }

        public String getTorrentStatus() {
            return torrentStatus;
        }

        public void setTorrentStatus(String torrentStatus) {
            this.torrentStatus = torrentStatus;
        }
        
        public String getDirPath() {
            return dirPath;
        }

        public void setDirPath(String dirPath) {
            this.dirPath = dirPath;
        }
        
        public String getBaseId() {
            return baseId;
        }

        public void setBaseId(String baseId) {
            this.baseId = baseId;
        }

        public HDFSEndpointJSON getEndpoint() {
            return endpoint;
        }

        public void setEndpoint(HDFSEndpointJSON endpoint) {
            this.endpoint = endpoint;
        }
    }
}