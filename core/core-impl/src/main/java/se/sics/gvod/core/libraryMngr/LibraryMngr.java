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

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileFilter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.StringTokenizer;
import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.gvod.core.VoDComp;
import se.sics.gvod.core.util.FileStatus;

/**
 * @author Alex Ormenisan <aaor@sics.se>
 */
public class LibraryMngr {

    private static final Logger LOG = LoggerFactory.getLogger(VoDComp.class);
    private static final String STATUS_FILE = "status.file";

    private static final FileFilter mp4Filter = new FileFilter() {
        @Override
        public boolean accept(File file) {
            if (!file.isFile()) {
                return false;
            }
            return file.getName().endsWith(".mp4");
        }
    };
    private static final FileFilter statusFilter = new FileFilter() {
        @Override
        public boolean accept(File file) {
            if (!file.isFile()) {
                return false;
            }
            return file.getName().equals(STATUS_FILE);
        }
    };

    private final String libPath;
    private final Map<String, Pair<FileStatus, Integer>> fileMap;

    public LibraryMngr(String libPath) {
        this.libPath = libPath;
        this.fileMap = new HashMap<String, Pair<FileStatus, Integer>>();
    }

    public Map<String, Pair<FileStatus, Integer>> getLibrary() {
        return fileMap;
    }

    public void loadLibrary() {
        checkLibraryDir();
        reloadFiles();
        checkStatusFile();
        readStatusFile();
        writeStatusFile(); //re-write it after cleanup
    }

    public void reloadLibrary() {
        reloadFiles();
    }

    public boolean pendingUpload(String file) {
        Pair<FileStatus, Integer> fileStatus = fileMap.get(file);
        if (fileStatus == null) {
            return false;
        }
        if (!fileStatus.getValue0().equals(FileStatus.NONE)) {
            return false;
        }
        fileMap.put(file, Pair.with(FileStatus.PENDING_UPLOAD, (Integer) null));
        writeStatusFile();
        return true;
    }

    public boolean upload(String file, int overlayId) {
        Pair<FileStatus, Integer> fileStatus = fileMap.get(file);
        if (fileStatus == null) {
            return false;
        }
        if (!fileStatus.getValue0().equals(FileStatus.PENDING_UPLOAD)) {
            return false;
        }
        fileMap.put(file, Pair.with(FileStatus.UPLOADING, overlayId));
        writeStatusFile();
        return true;
    }

    public boolean pendingDownload(String file) {
        Pair<FileStatus, Integer> fileStatus = fileMap.get(file);
        if (fileStatus != null) {
            return false;
        }

        fileMap.put(file, Pair.with(FileStatus.PENDING_DOWNLOAD, (Integer) null));
        writeStatusFile();
        return true;

    }

    public boolean startDownload(String file, Integer overlayId) {
        Pair<FileStatus, Integer> fileStatus = fileMap.get(file);
        if (fileStatus == null || !fileStatus.getValue0().equals(FileStatus.PENDING_DOWNLOAD)) {
            return false;
        }
        fileMap.put(file, Pair.with(FileStatus.DOWNLOADING, overlayId));
        writeStatusFile();
        return true;
    }

//    public boolean finishDownload(String file) {
//        FileStatus fileStatus = fileMap.get(file);
//        if (fileStatus == null) {
//            return false;
//        }
//        if (!fileStatus.equals(FileStatus.DOWNLOADING)) {
//            return false;
//        }
//        fileMap.put(file, FileStatus.UPLOADING);
//        writeStatusFile();
//        return true;
//    }
    private void checkLibraryDir() {
        File dir = new File(libPath);
        if (!dir.isDirectory()) {
            dir.mkdirs();
            if (!dir.isDirectory()) {
                LOG.error("library path is invalid");
                throw new RuntimeException("library path is invalid");
            }
        }
    }

    private void reloadFiles() {
        File libDir = new File(libPath);
        for (File file : libDir.listFiles(mp4Filter)) {
            LOG.info("library - loading video: {}", file.getName());
            if (!fileMap.containsKey(file.getName())) {
                fileMap.put(file.getName(), Pair.with(FileStatus.NONE, (Integer) null));
            }
        }
    }

    private void checkStatusFile() {
        File libDir = new File(libPath);
        File[] files = libDir.listFiles(statusFilter);
        if (files.length == 0) {
            File statusFile = new File(libPath + File.separator + STATUS_FILE);
            try {
                statusFile.createNewFile();
            } catch (IOException ex) {
                LOG.error("could not create status check file");
                throw new RuntimeException("could not create status check file");
            }
            return;
        }
        if (files.length > 1) {
            LOG.error("too many status check files");
            throw new RuntimeException("too many status check files");
        }
    }

    private void readStatusFile() {
        FileReader fr = null;
        BufferedReader br = null;
        try {
            fr = new FileReader(libPath + File.separator + STATUS_FILE);
            br = new BufferedReader(fr);
            String line;
            while ((line = br.readLine()) != null) {
                StringTokenizer st = new StringTokenizer(line, ":");
                String fileName = st.nextToken();
                FileStatus fileStatus = FileStatus.valueOf(st.nextToken());
                Integer overlayId = null;
                if (st.hasMoreElements()) {
                    overlayId = Integer.parseInt(st.nextToken());
                }
                checkStatus(fileName, fileStatus, overlayId);
            }
        } catch (FileNotFoundException ex) {
            LOG.error("could not find status check file - should not get here");
            throw new RuntimeException("could not find status check file - should not get here", ex);
        } catch (IOException ex) {
            LOG.error("IO problem on read status check file");
            throw new RuntimeException("IO problem on read status check file", ex);
        } catch (IllegalArgumentException ex) {
            LOG.error("bad file status");
            throw new RuntimeException("bad file status", ex);
        } catch (NoSuchElementException ex) {
            LOG.error("bad status format");
            throw new RuntimeException("bad status format", ex);
        } finally {
            try {
                if (br != null) {
                    br.close();
                } else if (fr != null) {
                    fr.close();
                }
            } catch (IOException ex) {
                LOG.error("error closing status file - read");
                throw new RuntimeException("error closing status file - read", ex);
            }
        }
    }

    private void checkStatus(String fileName, FileStatus fileStatus, Integer overlayId) {
        if (fileStatus.equals(FileStatus.NONE) || fileStatus.equals(FileStatus.PENDING_UPLOAD) || fileStatus.equals(FileStatus.PENDING_DOWNLOAD)) {
            //do nothing
        } else if (fileStatus.equals(FileStatus.UPLOADING)) {
            if (fileMap.containsKey(fileName)) {
                StringTokenizer fileST = new StringTokenizer(fileName, ".");
                if (fileST.countTokens() != 2) {
                    LOG.error("bad file name");
                    throw new RuntimeException("bad file name");
                }
                File hashFile = new File(libPath + File.separator + fileST.nextToken() + ".hash");
                if (hashFile.exists()) {
                    fileMap.put(fileName, Pair.with(fileStatus, overlayId));
                }
            }
        } else if (fileStatus.equals(FileStatus.DOWNLOADING)) {
            //TODO hash check - continue
            //for the moment we delete and start anew
            File dataFile = new File(libPath + File.separator + fileName);
            dataFile.delete();
            StringTokenizer fileST = new StringTokenizer(fileName, ".");
            if (fileST.countTokens() != 2) {
                LOG.error("bad file name");
                throw new RuntimeException("bad file name");
            }
            File hashFile = new File(libPath + File.separator + fileST.nextToken() + ".hash");
            hashFile.delete();
            fileMap.remove(fileName);
        } else {
            LOG.error("logic error - introduced new FileStatus:" + fileStatus.toString() + " and did not add it to the checkStatusFile");
            throw new RuntimeException("logic error - introduced new FileStatus:" + fileStatus.toString() + " and did not add it to the checkStatusFile");
        }
    }

    private void writeStatusFile() {
        BufferedWriter bw = null;
        FileWriter fw = null;
        try {
            fw = new FileWriter(libPath + File.separator + STATUS_FILE);
            bw = new BufferedWriter(fw);
            for (Map.Entry<String, Pair<FileStatus, Integer>> fileStatus : fileMap.entrySet()) {
                if (!fileStatus.getValue().equals(FileStatus.NONE)) {
                    bw.write(fileStatus.getKey());
                    bw.write(":");
                    bw.write(fileStatus.getValue().getValue0().toString());
                    if (fileStatus.getValue().getValue1() != null) {
                        bw.write(":");
                        bw.write(fileStatus.getValue().getValue1().toString());
                    }
                    bw.write("\n");
                }
            }
        } catch (IOException ex) {
            LOG.error("IO problem on write status check file");
            throw new RuntimeException("IO problem on write status check file");
        } finally {
            try {
                if (bw != null) {
                    bw.close();
                } else if (fw != null) {
                    fw.close();
                }
            } catch (IOException ex) {
                LOG.error("error closing status file - write");
                throw new RuntimeException("error closing status file - write", ex);
            }
        }
    }
}
