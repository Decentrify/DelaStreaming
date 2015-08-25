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
package se.sics.gvod.core.libraryMngr;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;
import org.apache.commons.io.filefilter.FileFileFilter;
import org.javatuples.Pair;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import se.sics.gvod.core.util.FileStatus;

/**
 * @author Alex Ormenisan <aaor@sics.se>
 */
public class LibraryMngrTest {
    static String testResourcePath = "./src/test/resources/libraryTest/";
    
    @BeforeClass
    public static void setup() {
        File file = new File(testResourcePath);
        file.mkdirs();
    }

    @Test
    public void testStatusFile1() throws IOException {
        clean1();

        //*********setup *******************************************************
        File videoData1 = new File(testResourcePath + "video1.mp4");
        videoData1.createNewFile();
        File videoData2 = new File(testResourcePath + "video2.bla.mp4");
        videoData2.createNewFile();

        File statusFile = new File(testResourcePath + "status.file");
        statusFile.createNewFile();
        BufferedWriter bw = new BufferedWriter(new FileWriter(testResourcePath + "status.file"));
        bw.write("");
        bw.close();
        //**********************************************************************

        LibraryMngr libMngr;
        Map<String, Pair<FileStatus, Integer>> libFiles;
        
        libMngr = new LibraryMngr(testResourcePath);
        libMngr.loadLibrary();
        libFiles = libMngr.getLibrary();
        
        Assert.assertEquals(2, libFiles.size());
        Assert.assertEquals(FileStatus.NONE, libFiles.get("video1.mp4").getValue0());
        Assert.assertEquals(FileStatus.NONE, libFiles.get("video2.bla.mp4").getValue0());

        File videoHash2 = new File(testResourcePath + "video2.bla.hash");
        videoHash2.createNewFile();
        Assert.assertTrue(libMngr.pendingDownload("video3.mp4"));
        Assert.assertTrue(libMngr.startDownload("video3.mp4", 10));
        Assert.assertTrue(libMngr.pendingUpload("video2.bla.mp4"));
        Assert.assertTrue(libMngr.upload("video2.bla.mp4", 11));
        
        Assert.assertEquals(3, libFiles.size());
        Assert.assertEquals(FileStatus.NONE, libFiles.get("video1.mp4").getValue0());
        Assert.assertEquals(FileStatus.UPLOADING, libFiles.get("video2.bla.mp4").getValue0());
        Assert.assertEquals(FileStatus.DOWNLOADING, libFiles.get("video3.mp4").getValue0());
        
        libMngr = new LibraryMngr(testResourcePath);
        libMngr.loadLibrary();
        libFiles = libMngr.getLibrary();

        Assert.assertEquals(2, libFiles.size());
        Assert.assertEquals(FileStatus.NONE, libFiles.get("video1.mp4").getValue0());
        Assert.assertEquals(FileStatus.UPLOADING, libFiles.get("video2.bla.mp4").getValue0());
        Assert.assertEquals(new Integer(11), libFiles.get("video2.bla.mp4").getValue1());
        
        clean1();
    }

    public void clean1() {
        File clean;
        clean = new File(testResourcePath + "video1.mp4");
        clean.delete();
        clean = new File(testResourcePath + "video2.mp4");
        clean.delete();
        clean = new File(testResourcePath + "video2.hash");
        clean.delete();
        clean = new File(testResourcePath + "status.file");
        clean.delete();
    }

    @Test
    public void testStatusFile2() throws IOException {
        clean2();
        
        //*********setup *******************************************************
        File videoData1 = new File(testResourcePath + "video1.mp4");
        videoData1.createNewFile();
        File videoData2 = new File(testResourcePath + "video2.mp4");
        videoData2.createNewFile();
        File videoData3 = new File(testResourcePath + "video3.mp4");
        videoData3.createNewFile();
        File videoHash3 = new File(testResourcePath + "video3.hash");
        videoHash3.createNewFile();

        File statusFile = new File(testResourcePath + "status.file");
        statusFile.createNewFile();
        BufferedWriter bw = new BufferedWriter(new FileWriter(testResourcePath + "status.file"));
        bw.write("video1.mp4:UPLOADING:10\n");
        bw.write("video2.mp4:DOWNLOADING:11\n");
        bw.write("video3.mp4:UPLOADING:12\n");
        bw.close();
        //**********************************************************************

        LibraryMngr libMngr = new LibraryMngr(testResourcePath);
        libMngr.loadLibrary();
        Map<String, Pair<FileStatus, Integer>> libFiles;

        libFiles = libMngr.getLibrary();
        Assert.assertEquals(2, libFiles.size());
        Assert.assertEquals(FileStatus.NONE, libFiles.get("video1.mp4").getValue0());
        Assert.assertEquals(FileStatus.UPLOADING, libFiles.get("video3.mp4").getValue0());
        Assert.assertEquals(new Integer(12), libFiles.get("video3.mp4").getValue1());
        clean2();
    }
    
    public void clean2() {
        File clean;
        clean = new File(testResourcePath + "video1.mp4");
        clean.delete();
        clean = new File(testResourcePath + "video2.mp4");
        clean.delete();
        clean = new File(testResourcePath + "video3.mp4");
        clean.delete();
        clean = new File(testResourcePath + "video3.hash");
        clean.delete();
        clean = new File(testResourcePath + "status.file");
        clean.delete();
    }
    
    @Test
    public void testAcceptedFiles() throws IOException {
        File file;
        file = new File(testResourcePath + "file.mp4");
        file.createNewFile();
        Assert.assertTrue(LibraryMngr.videoFilter.accept(file));
        file.delete();
        
        file = new File(testResourcePath + "file.mkv");
        file.createNewFile();
        Assert.assertTrue(LibraryMngr.videoFilter.accept(file));
        file.delete();
        
        file = new File(testResourcePath + "file.mp1");
        file.createNewFile();
        Assert.assertFalse(LibraryMngr.videoFilter.accept(file));
        file.delete();
    }
}
