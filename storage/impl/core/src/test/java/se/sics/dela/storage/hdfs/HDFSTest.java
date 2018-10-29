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

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.junit.Test;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class HDFSTest {

//  @Test
  public void testDFSClosing1() throws IOException {
    Configuration conf = new Configuration();
    conf.set("fs.defaultFS", "hdfs://10.0.2.15:8020");
    Path p = new Path("/test");
    DistributedFileSystem dfs1 = (DistributedFileSystem) FileSystem.newInstance(conf);
    if(!dfs1.isFile(p)) {
      throw new RuntimeException("fix test - file does not exist");
    }
    //open 1 - dfs1
    try (FSDataInputStream in1 = dfs1.open(p)) {
      System.err.println("op1");
    }
    //open 2 - dfs1
    try (FSDataInputStream in1 = dfs1.open(p)) {
      System.err.println("op2");
    }
    //open 3 - different dfs and close dfs
    try (DistributedFileSystem dfs2 = (DistributedFileSystem) FileSystem.newInstance(conf);
      FSDataInputStream in2 = dfs2.open(p)) {
      System.err.println("op3");
    } finally {
      System.err.println("close");
    }
    //open 4 - dfs1
    try (FSDataInputStream in1 = dfs1.open(p)) {
      System.err.println("op4");
    }
    dfs1.close();
  }

//  @Test
  public void testDFSClosing2() throws IOException {
    Configuration conf = new Configuration();
    conf.set("fs.defaultFS", "hdfs://10.0.2.15:8020");
    Path p = new Path("/test");
    DistributedFileSystem dfs1 = (DistributedFileSystem) FileSystem.newInstance(conf);
    if(!dfs1.isFile(p)) {
      throw new RuntimeException("fix test - file does not exist");
    }
    //open 1 - dfs1
    FSDataInputStream in1 = dfs1.open(p);
    System.err.println("op1");
    in1.close();
    //open 2 - dfs1
    FSDataInputStream in2 = dfs1.open(p);
    System.err.println("op2");
    in2.close();
    //open 3 - different dfs and close dfs
    DistributedFileSystem dfs2 = (DistributedFileSystem) FileSystem.newInstance(conf);
    FSDataInputStream in3 = dfs2.open(p);
    System.err.println("op3");
    in3.close();
    dfs2.close();
    System.err.println("close2");
    //open 4 - dfs1
    FSDataInputStream in4 = dfs1.open(p);
    System.err.println("op4");
    in4.close();
    dfs1.close();  
  }
}
