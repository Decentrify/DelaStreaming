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
package se.sics.nstream.hops.library.util;

import org.junit.Assert;
import org.junit.Test;
import se.sics.nstream.hops.storage.hdfs.HDFSEndpoint;
import se.sics.nstream.hops.storage.hdfs.HDFSResource;
import se.sics.nstream.storage.durable.util.MyStream;


/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class LibrarySummaryHelperTest {
  @Test
  public void hdfsTest() {
    MyStream writeStream = new MyStream(HDFSEndpoint.getBasic("http://bbc1.sics.se:12345", "user1"), new HDFSResource("/my/directory", "manifest.json"));
    String s = LibrarySummaryHelper.streamToJSON(writeStream);
    System.out.println(s);
    MyStream readStream  = LibrarySummaryHelper.streamFromJSON(s);
    Assert.assertTrue(true);
  }
}
