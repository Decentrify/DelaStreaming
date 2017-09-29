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
package se.sics.nstream.hops.libmngr;

import java.util.HashMap;
import java.util.Map;
import org.javatuples.Pair;
import se.sics.kompics.id.Identifier;
import se.sics.ktoolbox.util.Either;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayId;
import se.sics.nstream.FileId;
import se.sics.nstream.StreamId;
import se.sics.nstream.hops.manifest.ManifestHelper;
import se.sics.nstream.hops.manifest.ManifestJSON;
import se.sics.nstream.storage.durable.util.FileExtendedDetails;
import se.sics.nstream.storage.durable.util.MyStream;
import se.sics.nstream.storage.durable.util.StreamEndpoint;
import se.sics.nstream.transfer.MyTorrent;
import se.sics.nstream.util.FileBaseDetails;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class TorrentBuilder {

  private Map<String, Identifier> endpointNameToId;
  private Map<Identifier, StreamEndpoint> endpoints;
  private Pair<StreamId, MyStream> manifestStream;
  private MyTorrent.Manifest manifest;
  private Map<String, FileId> fileNameToId;
  private Map<FileId, FileBaseDetails> baseDetails;
  private Map<FileId, FileExtendedDetails> extendedDetails = new HashMap<>();

  public TorrentBuilder() {
  }

  public void setManifestStream(StreamId streamId, MyStream stream) {
    manifestStream = Pair.with(streamId, stream);
  }

  public Pair<StreamId, MyStream> getManifestStream() {
    return manifestStream;
  }

  public void setEndpoints(Pair<Map<String, Identifier>, Map<Identifier, StreamEndpoint>> setup) {
    this.endpointNameToId = setup.getValue0();
    this.endpoints = setup.getValue1();
  }

  public void setManifest(OverlayId torrentId, Either<MyTorrent.Manifest, ManifestJSON> auxManifest) {
    ManifestJSON manifestJSON;
    if (auxManifest.isLeft()) {
      manifest = auxManifest.getLeft();
      manifestJSON = ManifestHelper.getManifestJSON(manifest.manifestByte);
    } else {
      manifestJSON = auxManifest.getRight();
      manifest = ManifestHelper.getManifest(manifestJSON);
    }
    Pair< Map< String, FileId>, Map<FileId, FileBaseDetails>> aux = ManifestHelper.getBaseDetails(torrentId, manifestJSON,
      MyTorrent.defaultDataBlock);
    fileNameToId = aux.getValue0();
    baseDetails = aux.getValue1();
  }

  public Map<String, FileId> getFiles() {
    return fileNameToId;
  }

  public void setExtendedDetails(Map<FileId, FileExtendedDetails> extendedDetails) {
    this.extendedDetails = extendedDetails;
  }

  public MyTorrent getTorrent() {
    if (endpointNameToId == null || endpoints == null || manifest == null
      || fileNameToId == null || baseDetails == null) {
      throw new RuntimeException("cleanup - todo");
    }

    return new MyTorrent(manifest, fileNameToId, baseDetails, extendedDetails);
  }
}
