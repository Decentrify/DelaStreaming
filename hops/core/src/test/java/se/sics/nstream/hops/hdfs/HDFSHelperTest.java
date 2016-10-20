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
package se.sics.nstream.hops.hdfs;

import java.util.Random;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.Test;
import se.sics.ktoolbox.util.profiling.KProfiler;

/**
 *
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class HDFSHelperTest {

    @Test
    public void simpleAppend() throws InterruptedException {
        HDFSEndpoint endpoint = HDFSEndpoint.getBasic("glassfish", "bbc1.sics.se", 26801);
        HDFSResource resource = new HDFSResource("/experiment/download/", "test");
        Random rand = new Random(123);
        byte[] data;

        UserGroupInformation ugi = UserGroupInformation.createRemoteUser("glassfish");
        HDFSHelper.delete(ugi, endpoint, resource);
        HDFSHelper.simpleCreate(ugi, endpoint, resource);

        KProfiler kp = new KProfiler(KProfiler.Type.LOG);
        for (int i = 0; i < 100; i++) {
            data = new byte[1024 * 1024];
            rand.nextBytes(data);
            kp.start("hdfs", "append");
            HDFSHelper.append(ugi, endpoint, resource, data);
            kp.end();
        }
        HDFSHelper.delete(ugi, endpoint, resource);
    }
}
