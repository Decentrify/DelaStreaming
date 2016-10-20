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
package se.sics.nstream.hops.library.state;

import java.util.Map;

/**
 *
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class HopsLibrarySummary {
    private Map<String, Dataset> torrents;

    public Map<String, Dataset> getTorrents() {
        return torrents;
    }

    public void setTorrents(Map<String, Dataset> torrents) {
        this.torrents = torrents;
    }
    
    public static class Dataset {
        private String hopsUser;
        private String hopsIp;
        private int hopsPort;
        private String datasetPath;

        public String getHopsUser() {
            return hopsUser;
        }

        public void setHopsUser(String hopsUser) {
            this.hopsUser = hopsUser;
        }

        public String getHopsIp() {
            return hopsIp;
        }

        public void setHopsIp(String hopsIp) {
            this.hopsIp = hopsIp;
        }

        public int getHopsPort() {
            return hopsPort;
        }

        public void setHopsPort(int hopsPort) {
            this.hopsPort = hopsPort;
        }

        public String getDatasetPath() {
            return datasetPath;
        }

        public void setDatasetPath(String datasetPath) {
            this.datasetPath = datasetPath;
        }
    }
}
