///*
// * Copyright (C) 2009 Swedish Institute of Computer Science (SICS) Copyright (C)
// * 2009 Royal Institute of Technology (KTH)
// *
// * GVoD is free software; you can redistribute it and/or
// * modify it under the terms of the GNU General Public License
// * as published by the Free Software Foundation; either version 2
// * of the License, or (at your option) any later version.
// *
// * This program is distributed in the hope that it will be useful,
// * but WITHOUT ANY WARRANTY; without even the implied warranty of
// * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// * GNU General Public License for more details.
// *
// * You should have received a copy of the GNU General Public License
// * along with this program; if not, write to the Free Software
// * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
// */
//package se.sics.gvod.core.libraryMngr;
//
//import java.io.BufferedReader;
//import java.io.BufferedWriter;
//import java.io.File;
//import java.io.FileFilter;
//import java.io.FileNotFoundException;
//import java.io.FileReader;
//import java.io.FileWriter;
//import java.io.IOException;
//import java.util.HashMap;
//import java.util.Map;
//import java.util.NoSuchElementException;
//import java.util.StringTokenizer;
//import org.javatuples.Pair;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//import se.sics.gvod.core.VoDComp;
//import se.sics.gvod.core.util.FileStatus;
//import se.sics.ktoolbox.util.identifiable.Identifier;
//import se.sics.ktoolbox.util.identifiable.basic.IntIdentifier;
//import se.sics.ktoolbox.util.identifiable.basic.OverlayIdentifier;
//
///**
// * @author Alex Ormenisan <aaor@sics.se>
// */
//public class LibraryMngr {
//
//    private static final Logger LOG = LoggerFactory.getLogger(VoDComp.class);
//    private static final String STATUS_FILE = "status.file";
//
//    static enum AcceptedVideos {
//
//        MP4(".mp4"), MKV(".mkv");
//
//        private String extension;
//
//        private AcceptedVideos(String extension) {
//            this.extension = extension;
//        }
//    }
//    static final FileFilter videoFilter = new FileFilter() {
//        @Override
//        public boolean accept(File file) {
//            if (!file.isFile()) {
//                return false;
//            }
//            for (AcceptedVideos extension : AcceptedVideos.values()) {
//                if (file.getName().endsWith(extension.extension)) {
//                    return true;
//                }
//            }
//            return false;
//        }
//    };
//    private static final FileFilter statusFilter = new FileFilter() {
//        @Override
//        public boolean accept(File file) {
//            if (!file.isFile()) {
//                return false;
//            }
//            return file.getName().equals(STATUS_FILE);
//        }
//    };
//
//    private final String libPath;
//    private final Map<String, Pair<FileStatus, Identifier>> fileMap = new HashMap<>();
//
//    public LibraryMngr(String libPath) {
//        this.libPath = libPath;
//    }
//
//    public Map<String, Pair<FileStatus, Identifier>> getLibrary() {
//        return fileMap;
//    }
//
//    public void loadLibrary() {
//        checkLibraryDir();
//        reloadFiles();
//        checkStatusFile();
//        readStatusFile();
//        writeStatusFile(); //re-write it after cleanup
//    }
//
//    public void reloadLibrary() {
//        reloadFiles();
//    }
//
//    public boolean pendingUpload(String file) {
//        Pair<FileStatus, Identifier> fileStatus = fileMap.get(file);
//        if (fileStatus == null) {
//            return false;
//        }
//        if (!fileStatus.getValue0().equals(FileStatus.NONE)) {
//            return false;
//        }
//        fileMap.put(file, Pair.with(FileStatus.PENDING_UPLOAD, (Identifier) null));
//        writeStatusFile();
//        return true;
//    }
//
//    public boolean upload(String file, Identifier overlayId) {
//        Pair<FileStatus, Identifier> fileStatus = fileMap.get(file);
//        if (fileStatus == null) {
//            return false;
//        }
//        if (!fileStatus.getValue0().equals(FileStatus.PENDING_UPLOAD)) {
//            return false;
//        }
//        fileMap.put(file, Pair.with(FileStatus.UPLOADING, overlayId));
//        writeStatusFile();
//        return true;
//    }
//
//    public boolean pendingDownload(String file) {
//        Pair<FileStatus, Identifier> fileStatus = fileMap.get(file);
//        if (fileStatus != null) {
//            return false;
//        }
//
//        fileMap.put(file, Pair.with(FileStatus.PENDING_DOWNLOAD, (Identifier) null));
//        writeStatusFile();
//        return true;
//
//    }
//
//    public boolean startDownload(String file, Identifier overlayId) {
//        Pair<FileStatus, Identifier> fileStatus = fileMap.get(file);
//        if (fileStatus == null || !fileStatus.getValue0().equals(FileStatus.PENDING_DOWNLOAD)) {
//            return false;
//        }
//        fileMap.put(file, Pair.with(FileStatus.DOWNLOADING, overlayId));
//        writeStatusFile();
//        return true;
//    }
//
////    public boolean finishDownload(String file) {
////        FileStatus fileStatus = fileMap.get(file);
////        if (fileStatus == null) {
////            return false;
////        }
////        if (!fileStatus.equals(FileStatus.DOWNLOADING)) {
////            return false;
////        }
////        fileMap.put(file, FileStatus.UPLOADING);
////        writeStatusFile();
////        return true;
////    }
//    private void checkLibraryDir() {
//        File dir = new File(libPath);
//        if (!dir.isDirectory()) {
//            dir.mkdirs();
//            if (!dir.isDirectory()) {
//                LOG.error("library path is invalid");
//                throw new RuntimeException("library path is invalid");
//            }
//        }
//    }
//
//    private void reloadFiles() {
//        File libDir = new File(libPath);
//        for (File file : libDir.listFiles(videoFilter)) {
//            LOG.info("library - loading video: {}", file.getName());
//            if (!fileMap.containsKey(file.getName())) {
//                fileMap.put(file.getName(), Pair.with(FileStatus.NONE, (Identifier) null));
//            }
//        }
//    }
//
//    private void checkStatusFile() {
//        File libDir = new File(libPath);
//        File[] files = libDir.listFiles(statusFilter);
//        if (files.length == 0) {
//            File statusFile = new File(libPath + File.separator + STATUS_FILE);
//            try {
//                statusFile.createNewFile();
//            } catch (IOException ex) {
//                LOG.error("could not create status check file");
//                throw new RuntimeException("could not create status check file");
//            }
//            return;
//        }
//        if (files.length > 1) {
//            LOG.error("too many status check files");
//            throw new RuntimeException("too many status check files");
//        }
//    }
//
//    private void readStatusFile() {
//        FileReader fr = null;
//        BufferedReader br = null;
//        try {
//            fr = new FileReader(libPath + File.separator + STATUS_FILE);
//            br = new BufferedReader(fr);
//            String line;
//            while ((line = br.readLine()) != null) {
//                StringTokenizer st = new StringTokenizer(line, ":");
//                String fileName = st.nextToken();
//                FileStatus fileStatus = FileStatus.valueOf(st.nextToken());
//                Identifier overlayId = null;
//                if (st.hasMoreElements()) {
//                    overlayId = new IntIdentifier(Integer.parseInt(st.nextToken()));
//                }
//                checkStatus(fileName, fileStatus, overlayId);
//            }
//        } catch (FileNotFoundException ex) {
//            LOG.error("could not find status check file - should not get here");
//            throw new RuntimeException("could not find status check file - should not get here", ex);
//        } catch (IOException ex) {
//            LOG.error("IO problem on read status check file");
//            throw new RuntimeException("IO problem on read status check file", ex);
//        } catch (IllegalArgumentException ex) {
//            LOG.error("bad file status");
//            throw new RuntimeException("bad file status", ex);
//        } catch (NoSuchElementException ex) {
//            LOG.error("bad status format");
//            throw new RuntimeException("bad status format", ex);
//        } finally {
//            try {
//                if (br != null) {
//                    br.close();
//                } else if (fr != null) {
//                    fr.close();
//                }
//            } catch (IOException ex) {
//                LOG.error("error closing status file - read");
//                throw new RuntimeException("error closing status file - read", ex);
//            }
//        }
//    }
//
//    private void checkStatus(String fileName, FileStatus fileStatus, Identifier overlayId) {
//        File dataFile, hashFile;
//        String fileNoExt;
//        try {
//            fileNoExt = LibraryUtil.removeExtension(fileName);
//        } catch (IllegalArgumentException ex) {
//            LOG.error("bad file name:{}", fileName);
//            throw ex;
//        }
//        switch (fileStatus) {
//            case NONE:
//                break;
//            case PENDING_UPLOAD:
//                break;
//            case PENDING_DOWNLOAD:
//                break;
//            case DOWNLOADING:
//                //TODO hash check - continue
//                //for the moment we delete and start anew
//                dataFile = new File(libPath + File.separator + fileName);
//                dataFile.delete();
//                hashFile = new File(libPath + File.separator + fileNoExt + ".hash");
//                hashFile.delete();
//                fileMap.remove(fileName);
//                break;
//            case UPLOADING:
//                hashFile = new File(libPath + File.separator + fileNoExt + ".hash");
//                if (hashFile.exists()) {
//                    fileMap.put(fileName, Pair.with(fileStatus, overlayId));
//                } else {
//                    LOG.warn("no hash:{} file for uploading file:{}", hashFile, fileNoExt);
//                }
//                break;
//            default:
//                LOG.error("logic error - introduced new FileStatus:" + fileStatus.toString() + " and did not add it to the checkStatusFile");
//                throw new RuntimeException("logic error - introduced new FileStatus:" + fileStatus.toString() + " and did not add it to the checkStatusFile");
//        }
//    }
//
//    private void writeStatusFile() {
//        BufferedWriter bw = null;
//        FileWriter fw = null;
//        try {
//            fw = new FileWriter(libPath + File.separator + STATUS_FILE);
//            bw = new BufferedWriter(fw);
//            for (Map.Entry<String, Pair<FileStatus, Identifier>> fileStatus : fileMap.entrySet()) {
//                switch (fileStatus.getValue().getValue0()) {
//                    case NONE:
//                    case PENDING_DOWNLOAD:
//                    case PENDING_UPLOAD:
//                        continue;
//                    default:
//                        bw.write(fileStatus.getKey());
//                        bw.write(":");
//                        bw.write(fileStatus.getValue().getValue0().toString());
//                        if (fileStatus.getValue().getValue0() == null) {
//                            LOG.error("write status file - library logic error - missing overlay");
//                            throw new RuntimeException("write status file - library logic error - missing overlay");
//                        }
//                        bw.write(":");
//                        bw.write(((OverlayIdentifier)fileStatus.getValue().getValue1()).getInt());
//                        bw.write("\n");
//                }
//            }
//        } catch (IOException ex) {
//            LOG.error("IO problem on write status check file");
//            throw new RuntimeException("IO problem on write status check file");
//        } finally {
//            try {
//                if (bw != null) {
//                    bw.close();
//                } else if (fw != null) {
//                    fw.close();
//                }
//            } catch (IOException ex) {
//                LOG.error("error closing status file - write");
//                throw new RuntimeException("error closing status file - write", ex);
//            }
//        }
//    }
//}
