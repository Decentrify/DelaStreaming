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
package se.sics.gvod.stream.system.hops;

import java.io.IOException;
import java.util.Random;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import se.sics.ktoolbox.util.managedStore.core.util.HashUtil;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class SetupExperiment {

    final static int pieceSize = 1024;
    final static int piecesPerBlock = 1024;
    final static int nrBlocks = 40;
    final static long fileSize = piecesPerBlock * pieceSize * nrBlocks + pieceSize * 100;

    public static void main(String[] args) throws IOException, HashUtil.HashBuilderException {
        String hopsURL = "bbc1.sics.se:26801";
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", hopsURL);
        DistributedFileSystem fs = (DistributedFileSystem) FileSystem.get(conf);

        String path = "/experiment";
        if (!fs.isDirectory(new Path(path))) {
            fs.mkdirs(new Path(path));
        } else {
            fs.delete(new Path(path), true);
            fs.mkdirs(new Path(path));
        }
        String uploadDirPath = path + "/upload";
        fs.mkdirs(new Path(uploadDirPath));
        String downloadDirPath = path + "/download";
        fs.mkdirs(new Path(downloadDirPath));

        String dataFile = uploadDirPath + "/file";
        Random rand = new Random(1234);
        try (FSDataOutputStream out = fs.create(new Path(dataFile))) {
            for (int i = 0; i < fileSize / pieceSize; i++) {
                byte[] data = new byte[1024];
                rand.nextBytes(data);
                out.write(data);
                out.flush();
            }
            System.err.println("created file - expected:"  + fileSize + " created:" + out.size());
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
        fs.close();
    }
}
