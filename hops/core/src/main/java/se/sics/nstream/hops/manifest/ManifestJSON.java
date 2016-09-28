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
package se.sics.nstream.hops.manifest;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.javatuples.Pair;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayId;
import se.sics.ktoolbox.util.managedStore.core.util.HashUtil;
import se.sics.nstream.FileId;
import se.sics.nstream.TorrentIds;
import se.sics.nstream.util.BlockDetails;
import se.sics.nstream.util.BlockHelper;
import se.sics.nstream.util.FileBaseDetails;

/**
 *
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class ManifestJSON {

    private String datasetName;

    private String datasetDescription;

    private String creatorEmail;

    private String creatorDate;

    private boolean kafkaSupport;

    private List<FileInfoJSON> fileInfos;

    private List<String> metaDataJsons;

    public ManifestJSON() {
    }

    public String getDatasetName() {
        return datasetName;
    }

    public void setDatasetName(String datasetName) {
        this.datasetName = datasetName;
    }

    public String getDatasetDescription() {
        return datasetDescription;
    }

    public void setDatasetDescription(String datasetDescription) {
        this.datasetDescription = datasetDescription;
    }

    public String getCreatorEmail() {
        return creatorEmail;
    }

    public void setCreatorEmail(String creatorEmail) {
        this.creatorEmail = creatorEmail;
    }

    public String getCreatorDate() {
        return creatorDate;
    }

    public void setCreatorDate(String creatorDate) {
        this.creatorDate = creatorDate;
    }

    public boolean isKafkaSupport() {
        return kafkaSupport;
    }

    public void setKafkaSupport(boolean kafkaSupport) {
        this.kafkaSupport = kafkaSupport;
    }

    public List<FileInfoJSON> getFileInfos() {
        return fileInfos;
    }

    public void setFileInfos(List<FileInfoJSON> fileInfos) {
        this.fileInfos = fileInfos;
    }

    public List<String> getMetaDataJsons() {
        return metaDataJsons;
    }

    public void setMetaDataJsons(List<String> metaDataJsons) {
        this.metaDataJsons = metaDataJsons;
    }

    public static Pair<Map<String, FileId>, Map<FileId, FileBaseDetails>> getBaseDetails(OverlayId torrentId, ManifestJSON manifest, BlockDetails defaultBlock) {
        Map<String, FileId> nameToId = new HashMap<>();
        Map<FileId, FileBaseDetails> baseDetails = new HashMap<>();
        int fileNr = 0;
        for(FileInfoJSON fileInfo : manifest.fileInfos) {
            fileNr++; //start from 1 - 0 is for the definition of the torrent
            FileId fileId = TorrentIds.fileId(torrentId, fileNr);
            nameToId.put(fileInfo.getFileName(), fileId);
            String hashAlg = HashUtil.getAlgName(HashUtil.SHA);
            Pair<Integer, BlockDetails> fileDetails = BlockHelper.getFileDetails(fileInfo.getLength(), defaultBlock);
            FileBaseDetails fbd = new FileBaseDetails(fileInfo.getLength(), fileDetails.getValue0(), defaultBlock, fileDetails.getValue1(), hashAlg);
            baseDetails.put(fileId, fbd);
        }
        return Pair.with(nameToId, baseDetails);
    }
}
