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
 * GNU General Public License for more defLastBlock.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
 */
package se.sics.nstream.hops.library;

import com.google.common.base.Optional;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.javatuples.Pair;
import se.sics.gvod.mngr.util.ElementSummary;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayId;
import se.sics.nstream.FileId;
import se.sics.nstream.StreamId;
import se.sics.nstream.TorrentIds;
import se.sics.nstream.hops.HopsFED;
import se.sics.nstream.hops.hdfs.HDFSEndpoint;
import se.sics.nstream.hops.hdfs.HDFSResource;
import se.sics.nstream.hops.kafka.KafkaEndpoint;
import se.sics.nstream.hops.kafka.KafkaResource;
import se.sics.nstream.library.util.TorrentStatus;
import se.sics.nstream.transfer.MyTorrent;
import se.sics.nstream.transfer.MyTorrent.Manifest;
import se.sics.nstream.util.FileBaseDetails;
import se.sics.nstream.util.FileExtendedDetails;
import se.sics.nstream.util.MyStream;

/**
 *
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class Library {

    private final Map<OverlayId, Pair<String, TorrentStatus>> torrentStatus = new HashMap<>();
    private final Map<OverlayId, Pair<String, Torrent>> manifests = new HashMap<>();
    private final Map<OverlayId, Pair<String, TorrentBuilder>> pendingManifests = new HashMap<>();
    private final TorrentList torrentList;

    public Library(TorrentList torrentList) {
        this.torrentList = torrentList;
    }

    public boolean containsTorrent(OverlayId torrentId) {
        return torrentStatus.containsKey(torrentId);
    }

    public TorrentStatus getStatus(OverlayId torrentId) {
        Pair<String, TorrentStatus> status = torrentStatus.get(torrentId);
        if (status == null) {
            status = Pair.with("", TorrentStatus.NONE);
        }
        return status.getValue1();
    }

    public void destroyed(OverlayId torrentId) {
        remove(torrentId);
        Pair<String, TorrentStatus> status = torrentStatus.remove(torrentId);
        status = Pair.with(status.getValue0(), TorrentStatus.DESTROYED);
        torrentStatus.put(torrentId, status);
    }

    public void upload(OverlayId torrentId, String torrentName, Torrent torrent) {
        torrentStatus.put(torrentId, Pair.with(torrentName, TorrentStatus.UPLOADING));
        manifests.put(torrentId, Pair.with(torrentName, torrent));
//        torrentList.write(TorrentSummary.getSummary(torrentId, torrentName, torrent), torrent.hdfsEndpoint.hopsURL, torrent.hdfsEndpoint.user);
    }

    public void download1(OverlayId torrentId, String torrentName, TorrentBuilder torrentBuilder) {
        torrentStatus.put(torrentId, Pair.with(torrentName, TorrentStatus.DOWNLOAD_1));
        pendingManifests.put(torrentId, Pair.with(torrentName, torrentBuilder));
    }

    public byte[] download2(OverlayId torrentId, Manifest manifest, Map<String, FileId> nameToId, Map<FileId, FileBaseDetails> base) {
        Pair<String, TorrentStatus> ts = torrentStatus.get(torrentId);
        Pair<String, TorrentBuilder> torrentBuilder = pendingManifests.get(torrentId);
        torrentBuilder.getValue1().setManifest(manifest);
        torrentBuilder.getValue1().torrentBuilder.setBase(nameToId, base);
        torrentStatus.put(torrentId, Pair.with(ts.getValue0(), TorrentStatus.DOWNLOAD_2));
        return manifest.manifestByte;
    }

    public MyTorrent download3(OverlayId torrentId, HDFSEndpoint hdfsEndpoint, Optional<KafkaEndpoint> ke,
            Map<String, HDFSResource> hdfsDetails, Map<String, KafkaResource> kafkaDetails) {
        Pair<String, TorrentStatus> status = torrentStatus.remove(torrentId);
        String torrentName = status.getValue0();
        status = Pair.with(status.getValue0(), TorrentStatus.DOWNLOADING);
        torrentStatus.put(torrentId, status);

        Pair<String, TorrentBuilder> torrentBuilder = pendingManifests.remove(torrentId);
        Map<String, FileId> fileNameToId = torrentBuilder.getValue1().torrentBuilder.getNameToId();
        KafkaEndpoint kafkaEndpoint = ke.isPresent() ? ke.get() : null;
        Map<FileId, FileExtendedDetails> fileExtendedDetails = processExtendedDetails(hdfsEndpoint, kafkaEndpoint, fileNameToId, hdfsDetails, kafkaDetails);
        torrentBuilder.getValue1().torrentBuilder.setExtended(fileExtendedDetails);
        Torrent torrent = torrentBuilder.getValue1().build();
        manifests.put(torrentId, Pair.with(torrentBuilder.getValue0(), torrent));
//        torrentList.write(TorrentSummary.getSummary(torrentId, torrentName, torrent), torrent.hdfsEndpoint.hopsURL, torrent.hdfsEndpoint.user);
        return torrent.torrent;
    }

    private Map<FileId, FileExtendedDetails> processExtendedDetails(HDFSEndpoint hdfsEndpoint, KafkaEndpoint kafkaEndpoint, Map<String, FileId> nameToId,
            Map<String, HDFSResource> hdfsDetails, Map<String, KafkaResource> kafkaDetails) {

        Map<FileId, FileExtendedDetails> result = new HashMap<>();
        for (Map.Entry<String, FileId> file : nameToId.entrySet()) {
            HDFSResource hdfsResource = hdfsDetails.get(file.getKey());
            if (hdfsResource == null) {
                throw new RuntimeException("no file extended details");
            }
            StreamId hdfsStreamId = TorrentIds.streamId(HopsStorageProvider.hdfsIdentifier, file.getValue());
            MyStream hdfsStream = new MyStream(hdfsStreamId, hdfsEndpoint, hdfsResource);

            Optional<MyStream> kafkaStream = Optional.absent();
            KafkaResource kafkaResource = kafkaDetails.get(file.getKey());
            if (kafkaResource != null) {
                StreamId kafkaStreamId = TorrentIds.streamId(HopsStorageProvider.kafkaIdentifier, file.getValue());
                kafkaStream = Optional.of(new MyStream(kafkaStreamId, kafkaEndpoint, kafkaResource));
                
            }
            result.put(file.getValue(), new HopsFED(hdfsStream, kafkaStream));
        }
        return result;
    }

    public Map<FileId, FileExtendedDetails> getExtendedDetails(OverlayId torrentId) {
        Pair<String, Torrent> aux = manifests.get(torrentId);
        return aux.getValue1().torrent.extended;
    }

    public Pair<String, Torrent> getTorrent(OverlayId torrentId) {
        return manifests.get(torrentId);
    }

    public Pair<String, TorrentBuilder> getTorrentBuilder(OverlayId torrentId) {
        return pendingManifests.get(torrentId);
    }

//    public Optional<TorrentDetails> getTorrentDetails(Identifier torrentId) {
//        return Optional.fromNullable(download3.get(torrentId));
//    }
//
//    public Triplet<String, HDFSEndpoint, HDFSResource> getManifest(Identifier torrentId) {
//        return manifests.get(torrentId);
//    }
    public void finishDownload(OverlayId torrentId) {
        Pair<String, TorrentStatus> status = torrentStatus.remove(torrentId);
        status = Pair.with(status.getValue0(), TorrentStatus.UPLOADING);
        torrentStatus.put(torrentId, status);
    }

    private void remove(OverlayId torrentId) {
        manifests.remove(torrentId);
        pendingManifests.remove(torrentId);
    }

    public List<ElementSummary> getSummary() {
        List<ElementSummary> summary = new ArrayList<>();
        for (Map.Entry<OverlayId, Pair<String, TorrentStatus>> e : torrentStatus.entrySet()) {
            ElementSummary es = new ElementSummary(e.getValue().getValue0(), e.getKey(), e.getValue().getValue1());
            summary.add(es);
        }
        return summary;
    }

    public List<ElementSummary> getSummary(int projectId) {
        List<ElementSummary> summary = new ArrayList<>();
//        for (Map.Entry<Identifier, Pair<FileInfo, TorrentInfo>> e : libraryContents.entrySet()) {
//            HopsResource hopsResource = hopsResources.get(e.getKey());
//            if (hopsResource.projectId == projectId) {
//                ElementSummary es = new ElementSummary(e.getValue().getValue0().name, e.getKey(), e.getValue().getValue1().getStatus());
//                summary.add(es);
//            }
//        }
        return summary;
    }

    public static class Torrent {

        public final HDFSEndpoint hdfsEndpoint;
        public final HDFSResource manifest;
        public final MyTorrent torrent;

        public Torrent(HDFSEndpoint hdfsEndpoint, HDFSResource manifest, MyTorrent torrent) {
            this.hdfsEndpoint = hdfsEndpoint;
            this.manifest = manifest;
            this.torrent = torrent;
        }
    }

    public static class TorrentBuilder {

        public final HDFSEndpoint hdfsEndpoint;
        public final HDFSResource manifestResource;
        private MyTorrent.Builder torrentBuilder;

        public TorrentBuilder(HDFSEndpoint hdfsEndpoint, HDFSResource manifest) {
            this.hdfsEndpoint = hdfsEndpoint;
            this.manifestResource = manifest;
        }

        public void setManifest(Manifest manifest) {
            torrentBuilder = new MyTorrent.Builder(manifest.getDef());
            torrentBuilder.manifestBuilder.addBlocks(manifest.manifestBlocks);
        }

        public Torrent build() {
            return new Torrent(hdfsEndpoint, manifestResource, torrentBuilder.build());
        }
    }
}
