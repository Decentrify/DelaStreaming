/*
 * Copyright (C) 2009 Swedish Institute of Computer Science (SICS) Copyright (C)
 * 2009 Royal Institute of Technology (KTH)
 *
 * KompicsToolbox is free software; you can redistribute it and/or
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
package se.sics.nstream.hops.storage.hdfs;

import java.io.File;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import se.sics.nstream.storage.durable.util.StreamEndpoint;
import se.sics.nstream.storage.durable.util.StreamEndpoint;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class HDFSEndpoint implements StreamEndpoint {

    public static final String HOPS_URL = "fs.defaultFS";
    public final Configuration hdfsConfig;
    public final String hopsURL;
    public final String user;

    public HDFSEndpoint(Configuration hdfsConfig, String user) {
        this.hdfsConfig = hdfsConfig;
        this.user = user;
        this.hopsURL = hdfsConfig.get(HOPS_URL);
    }

    @Override
    public String getEndpointName() {
        return hopsURL + "_" + user;
    }

    public static HDFSEndpoint getBasic(String user, String hopsIp, int hopsPort) {
        String hopsURL = "hdfs://" + hopsIp + ":" + hopsPort;
        return getBasic(hopsURL, user);
    }
    
    public static HDFSEndpoint getBasic(String hopsURL, String user) {
        Configuration hdfsConfig = new Configuration();
        hdfsConfig.set(HOPS_URL, hopsURL);
        return new HDFSEndpoint(hdfsConfig, user);
    }
    
    public static HDFSEndpoint getXML(String hdfsXMLPath, String user) {
        Configuration hdfsConfig = new Configuration();
        File confFile = new File(hdfsXMLPath);
        if (!confFile.exists()) {
            throw new RuntimeException("conf file does not exist");
        }
        hdfsConfig.addResource(new Path(hdfsXMLPath));
        return new HDFSEndpoint(hdfsConfig, user);
    }
}
