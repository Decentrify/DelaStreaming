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
//package se.sics.gvod.mngr;
//
//import com.google.common.base.Optional;
//import com.google.common.collect.HashBasedTable;
//import com.google.common.collect.Table;
//import java.util.ArrayList;
//import java.util.HashMap;
//import java.util.List;
//import java.util.Map;
//import java.util.TreeMap;
//import org.javatuples.Pair;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//import se.sics.gvod.mngr.util.FileInfo;
//import se.sics.gvod.mngr.util.LibraryElementSummary;
//import se.sics.gvod.mngr.util.TorrentStatus;
//import se.sics.ktoolbox.util.BasicOpResult;
//import se.sics.ktoolbox.util.identifiable.Identifier;
//import se.sics.ktoolbox.util.managedStore.resources.FileResourceMngr;
//import se.sics.ktoolbox.util.managedStore.resources.FileResourceRegistry;
//
///**
// * @author Alex Ormenisan <aaor@kth.se>
// */
//public class LibraryMngr {
//
//    private static final Logger LOG = LoggerFactory.getLogger(LibraryMngrOld.class);
//    private static final String STATUS_FILE = "status.file";
//
//    //String keys are file uri
//    private final Map<String, FileInfo> libFiles = new HashMap<>();
//    private final HashBasedTable<String, Identifier, TorrentStatus> torrents = HashBasedTable.create();
//
//    public LibraryMngr() {
//    }
//
//    public boolean containsFile(String uri) {
//        return libFiles.containsKey(uri);
//    }
//    
//    public boolean containsTorrent(String uri, Identifier overlayId) {
//        return torrents.contains(uri, overlayId);
//    }
//            
//    public BasicOpResult addElement(FileInfo file) {
//        if (file.shortDescription.contains(":") || file.shortDescription.contains("\n")) {
//            LOG.warn("file with uri:{} illegal character in description", file.uri);
//            return BasicOpResult.createFail("file with uri:" + file.uri + " illegal character in description");
//        }
//        if (libFiles.containsKey(file.uri)) {
//            LOG.warn("file with uri:{} already present in library", file.uri);
//            return BasicOpResult.createFail("file with uri:" + file.uri + " already present in library");
//        }
//        LOG.info("adding element file with uri:{}", file.uri);
//        libFiles.put(file.uri, file);
//        return BasicOpResult.success;
//    }
//
//    public BasicOpResult removeElement(String uri, boolean removeFile) {
//        if (torrents.containsRow(uri)) {
//            LOG.warn("file with uri:{} is still streaming", uri);
//            return BasicOpResult.createFail("file with uri:" + uri + " is still streaming");
//        }
//        LOG.info("removing element file with uri:{}", uri);
//        FileInfo file = libFiles.remove(uri);
//        if (file == null) {
//            return BasicOpResult.success;
//        }
//        if (removeFile) {
//            FileResourceMngr frMngr = FileResourceRegistry.getMngr(file.fileType);
//            return frMngr.delete(uri);
//        }
//        return BasicOpResult.success;
//    }
//
//    public BasicOpResult upload(String uri, Identifier overlayId) {
//        if (!libFiles.containsKey(uri)) {
//            LOG.warn("file with uri:{} is not part of the library", uri);
//            return BasicOpResult.createFail("file with uri:" + uri + " is not part of the library");
//        }
//        if (torrents.containsRow(uri)) {
//            LOG.warn("file with uri:{} already streaming", uri);
//            return BasicOpResult.createFail("file with uri:" + uri + " already streaming");
//        }
//        if (torrents.containsColumn(overlayId)) {
//            LOG.warn("file with overlayId:{} already streaming", overlayId);
//            return BasicOpResult.createFail("file with overlayId:" + overlayId + " already streaming");
//        }
//        LOG.info("uploading file with uri:{} overlayId:{}", uri, overlayId);
//        torrents.put(uri, overlayId, TorrentStatus.UPLOADING);
//        return BasicOpResult.success;
//    }
//
//    public BasicOpResult download(String uri, Identifier overlayId) {
//        if (!libFiles.containsKey(uri)) {
//            LOG.warn("file with uri:{} is not part of the library", uri);
//            return BasicOpResult.createFail("file with uri:" + uri + " is not part of the library");
//        }
//        if (torrents.containsRow(uri)) {
//            LOG.warn("file with uri:{} already streaming", uri);
//            return BasicOpResult.createFail("file with uri:" + uri + " already streaming");
//        }
//        if (torrents.containsColumn(overlayId)) {
//            LOG.warn("file with overlayId:{} already streaming", overlayId);
//            return BasicOpResult.createFail("file with overlayId:" + overlayId + " already streaming");
//        }
//        LOG.info("downloading file with uri:{} overlayId:{}", uri, overlayId);
//        torrents.put(uri, overlayId, TorrentStatus.DOWNLOADING);
//        return BasicOpResult.success;
//    }
//
//    public BasicOpResult stop(String uri, Identifier overlayId) {
//        LOG.info("stopping file with uri:{} overlayId:{}", uri, overlayId);
//        torrents.remove(uri, overlayId);
//        return BasicOpResult.success;
//    }
//
//    //TODO Alex - I know... looks a bit weird ... fix later
//    public List<LibraryElementSummary> getContents() {
//        List<LibraryElementSummary> result = new ArrayList<>();
//        for (Table.Cell<String, Identifier, TorrentStatus> cell : torrents.cellSet()) {
//            String fileName = libFiles.get(cell.getRowKey()).name;
//            LibraryElementSummary les = new LibraryElementSummary(cell.getRowKey(), fileName, cell.getValue(), Optional.of(cell.getColumnKey()));
//            result.add(les);
//        }
//        
//        for (Map.Entry<String, FileInfo> file : libFiles.entrySet()) {
//            if (!torrents.containsRow(file.getKey())) {
//                LibraryElementSummary les = new LibraryElementSummary(file.getKey(), file.getValue().name, TorrentStatus.NONE, Optional.fromNullable((Identifier)null));
//                result.add(les);
//            }
//        }
//        return result;
//    }
//
////    private void checkStatusFile() {
////        File appDir = new File(appDirPath);
////        if (!appDir.isDirectory()) {
////            LOG.error("missing appDir:{}", appDirPath);
////            throw new RuntimeException("missing appDir:" + appDirPath);
////        }
////        File libFile = new File(appDirPath + File.separator + STATUS_FILE);
////        if (libFile.isFile()) {
////            try {
////                libFile.createNewFile();
////            } catch (IOException ex) {
////                LOG.error("status file io error");
////                throw new RuntimeException(ex);
////            }
////        }
////    }
////
////    private void readStatusFile() {
////        try (BufferedReader br = new BufferedReader(new FileReader(appDirPath + File.separator + STATUS_FILE))) {
////            String line;
////            while ((line = br.readLine()) != null) {
////                StringTokenizer st = new StringTokenizer(line, ":");
////                String fileName = st.nextToken();
////                String fileUri = st.nextToken();
////                TorrentStatus fileStatus = TorrentStatus.valueOf(st.nextToken());
////                Identifier overlayId = null;
////                if (!fileStatus.equals(TorrentStatus.NONE)) {
////                    overlayId = new OverlayIdentifier(Ints.toByteArray(Integer.parseInt(st.nextToken())));
////                }
////                String fileDescription = st.nextToken();
////            }
////        } catch (FileNotFoundException ex) {
////            LOG.error("could not find status check file - should not get here");
////            throw new RuntimeException("could not find status check file - should not get here", ex);
////        } catch (IOException ex) {
////            LOG.error("IO problem on read status check file");
////            throw new RuntimeException("IO problem on read status check file", ex);
////        } catch (IllegalArgumentException ex) {
////            LOG.error("bad file status");
////            throw new RuntimeException("bad file status", ex);
////        } catch (NoSuchElementException ex) {
////            LOG.error("bad status format");
////            throw new RuntimeException("bad status format", ex);
////        }
////    }
////
////    private void writeStatusFile() {
////        try (BufferedWriter bw = new BufferedWriter(new FileWriter(appDirPath + File.separator + STATUS_FILE))) {
////            for (Map.Entry<String, Pair<FileInfo, Pair<Identifier, TorrentStatus>>> file : libFiles.entrySet()) {
////                bw.write(file.getValue().getValue0().name);
////                bw.write(":");
////                bw.write(file.getValue().getValue0().uri);
////                bw.write(":");
////                bw.write(file.getValue().getValue1().getValue1().toString());
////                if (file.getValue().getValue0() == null) {
////                    LOG.error("write status file - library logic error - missing overlay");
////                    throw new RuntimeException("write status file - library logic error - missing overlay");
////                }
////                bw.write(":");
////                bw.write(((OverlayIdentifier) file.getValue().getValue1().getValue0()).getInt());
////                bw.write(":");
////                bw.write(file.getValue().getValue0().shortDescription);
////                bw.write("\n");
////            }
////        } catch (IOException ex) {
////            LOG.error("IO problem on write status check file");
////            throw new RuntimeException("IO problem on write status check file");
////        }
////    }
//}
