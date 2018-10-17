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
package se.sics.dela.storage.hdfs;

import java.util.function.Supplier;
import org.apache.hadoop.security.UserGroupInformation;
import static se.sics.dela.storage.hdfs.HDFSHelper.doAs;
import static se.sics.dela.storage.hdfs.HDFSHelper.simpleCreate;
import se.sics.ktoolbox.util.trysf.Try;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class HDFSHelperTest {
  HDFSEndpoint endpoint = HDFSEndpoint.getBasic("glassfish", "10.0.2.15", 30201);
  HDFSResource resource = new HDFSResource("/test", "file1");
  UserGroupInformation ugi = UserGroupInformation.createRemoteUser(endpoint.user);
  
  Supplier<Try<Boolean>> createFile = simpleCreate(endpoint, resource);
  Try<Boolean> result = doAs(createFile, ugi);
}
