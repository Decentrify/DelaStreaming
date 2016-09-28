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
package se.sics.nstream.hops.library;

import com.google.gson.Gson;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class TorrentList {

    private static final Logger LOG = LoggerFactory.getLogger(TorrentList.class);
    private String logPrefix;

    private final File torrentList;
    private final Content content;

    public TorrentList(File torrentList) {
        this.torrentList = torrentList;
        this.content = readContent();
    }

    private Content readContent() {
        try {
            Scanner fileScanner = new Scanner(torrentList);
            String jsonContent = fileScanner.useDelimiter("\\Z").next();
            Gson gson = new Gson();
            Content c = gson.fromJson(jsonContent, Content.class);
            return c;
        } catch (FileNotFoundException ex) {
            throw new RuntimeException("torrent file");
        }
    }
    
    private void writeContent(Content content) {
        Gson gson = new Gson();
        String jsonContent = gson.toJson(content);

        try (FileWriter fileWriter = new FileWriter(torrentList)) {
            fileWriter.write(jsonContent);
        } catch (IOException ex) {
            throw new RuntimeException("torrent file");
        }
    }
    
    public void write(TorrentSummary torrent, String endpointAddress, String user) {
        content.addTorrent(torrent, endpointAddress, user);
        writeContent(content);
    }

    public static TorrentList readTorrentList(String torrentListPath) {
        File torrentListFile = new File(torrentListPath);
        if (!torrentListFile.isFile()) {
            LOG.info("no torrent list file detected");
            try {
                if (!torrentListFile.createNewFile()) {
                    throw new RuntimeException("could not create torrent file");
                }
                createEmptyTorrentList(torrentListFile);
            } catch (IOException ex) {
                throw new RuntimeException("could not create torrent file");
            }
        }
        TorrentList torrentList = new TorrentList(torrentListFile);
        return torrentList;
    }

    private static void createEmptyTorrentList(File torrentListFile) throws IOException {
        Content emptyContent = new Content();
        Gson gson = new Gson();
        String jsonContent = gson.toJson(emptyContent);

        try (FileWriter fileWriter = new FileWriter(torrentListFile)) {
            fileWriter.write(jsonContent);
        }
    }

    public static class Content {

        private Map<String, TorrentSummary> files = new HashMap<>();

        public Content() {
        }
        
        public void addTorrent(TorrentSummary torrent, String endpointAddress, String user) {
            if(files.containsKey(torrent.getStringId())) {
                throw new RuntimeException("duplicate");
            }
            files.put(torrent.getStringId(), torrent);
        }

        public Map<String, TorrentSummary> getFiles() {
            return files;
        }

        public void setFiles(Map<String, TorrentSummary> files) {
            this.files = files;
        }
    }
}
