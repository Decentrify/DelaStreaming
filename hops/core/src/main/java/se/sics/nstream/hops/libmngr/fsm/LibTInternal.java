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
package se.sics.nstream.hops.libmngr.fsm;

import com.google.common.base.Optional;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import org.javatuples.Pair;
import se.sics.kompics.Promise;
import se.sics.ktoolbox.nutil.fsm.api.FSMException;
import se.sics.ktoolbox.nutil.fsm.api.FSMInternalState;
import se.sics.ktoolbox.nutil.fsm.api.FSMInternalStateBuilder;
import se.sics.ktoolbox.nutil.fsm.ids.FSMId;
import se.sics.ktoolbox.util.Either;
import se.sics.ktoolbox.util.identifiable.Identifier;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayId;
import se.sics.ktoolbox.util.network.KAddress;
import se.sics.nstream.FileId;
import se.sics.nstream.StreamId;
import se.sics.nstream.hops.kafka.KafkaEndpoint;
import se.sics.nstream.hops.kafka.KafkaResource;
import se.sics.nstream.hops.libmngr.TorrentBuilder;
import se.sics.nstream.hops.library.event.core.HopsTorrentDownloadEvent;
import se.sics.nstream.hops.library.event.core.HopsTorrentStopEvent;
import se.sics.nstream.hops.library.event.core.HopsTorrentUploadEvent;
import se.sics.nstream.hops.manifest.ManifestJSON;
import se.sics.nstream.library.event.torrent.TorrentExtendedStatusEvent;
import se.sics.nstream.library.restart.TorrentRestart;
import se.sics.nstream.storage.durable.util.FileExtendedDetails;
import se.sics.nstream.storage.durable.util.MyStream;
import se.sics.nstream.storage.durable.util.StreamEndpoint;
import se.sics.nstream.transfer.MyTorrent;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class LibTInternal implements FSMInternalState {

  public final FSMId fsmId;
  public final String fsmName;

  //intermediary state
  public final ActiveRequest ar;
  private TorrentState torrentState;
  public final AuxState auxState = new AuxState();
  //final state
  private OverlayId torrentId;
  public final LibTEndpointRegistry storageRegistry = new LibTEndpointRegistry();
  private List<KAddress> partners;
  private MyTorrent torrent;
  
  public Optional<TorrentExtendedStatusEvent.Request> statusReq;

  public LibTInternal(FSMId fsmId) {
    this.fsmId = fsmId;
    this.fsmName = LibTFSM.NAME;
    this.ar = new ActiveRequest();
  }

  @Override
  public FSMId getFSMId() {
    return fsmId;
  }

  public void setUpload(HopsTorrentUploadEvent.Request req) throws FSMException {
    ar.setActive(req);
    this.torrentId = req.torrentId;
    this.partners = new LinkedList<>();
    this.torrentState = new TSetup(torrentId, false, false);
  }

  public void setUploadRestart(TorrentRestart.UpldReq req) throws FSMException {
    ar.setActive(req);
    this.torrentId = req.torrentId;
    this.partners = req.partners;
    this.torrentState = new TSetup(torrentId, false, false);
  }

  public void setDownload(HopsTorrentDownloadEvent.StartRequest req) throws FSMException {
    ar.setActive(req);
    this.torrentId = req.torrentId;
    this.partners = req.partners;
    this.torrentState = new TSetup(torrentId, true, true);
  }

  public void setDownloadAdvance(HopsTorrentDownloadEvent.AdvanceRequest req) throws FSMException {
    ar.setActive(req);
    auxState.setKafkaEndpointName(req.kafkaEndpoint);
    auxState.setKafkaDetails(req.kafkaDetails);
  }

  public void setDownloadRestart(TorrentRestart.DwldReq req) throws FSMException {
    ar.setActive(req);
    this.torrentId = req.torrentId;
    this.partners = req.partners;
    this.torrentState = new TSetup(torrentId, false, true);
  }

  public void setStop(HopsTorrentStopEvent.Request req) throws FSMException {
    ar.setActive(req);
  }

  public void advanceTransfer() {
    torrentState = ((TSetup) torrentState).finish();
  }

  public TSetup getSetupState() {
    return (TSetup) torrentState;
  }

  public TComplete getCompleteState() {
    return (TComplete) torrentState;
  }

  public OverlayId getTorrentId() {
    return torrentId;
  }

  public List<KAddress> getPartners() {
    return partners;
  }

  public static class ActiveRequest {

    //either one
    private Optional<Promise> activeRequest = Optional.absent();

    private ActiveRequest() {
    }

    private void setActive(Promise req) throws FSMException {
      if (activeRequest.isPresent()) {
        throw new FSMException("concurrent pending requests");
      }
      this.activeRequest = Optional.of(req);
    }

    public void reset() {
      activeRequest = Optional.absent();
    }

    public Optional<Promise> active() {
      return activeRequest;
    }

    public boolean isStopping() {
      if (activeRequest.isPresent() && HopsTorrentStopEvent.Request.class.isAssignableFrom(activeRequest.get().
        getClass())) {
        return true;
      }
      return false;
    }
  }

  public static interface TorrentState {
  }

  public static class TSetup implements TorrentState {

    private final OverlayId torrentId;
    private final TorrentBuilder torrentBuilder;
    private final boolean downloadSetup;
    private final boolean download;

    private TSetup(OverlayId torrentId, boolean downloadSetup, boolean download) {
      this.torrentId = torrentId;
      this.torrentBuilder = new TorrentBuilder();
      this.downloadSetup = downloadSetup;
      this.download = download;
    }

    public boolean isDownloadSetup() {
      return downloadSetup;
    }

    public void storageSetupComplete(Pair<Map<String, Identifier>, Map<Identifier, StreamEndpoint>> setup) {
      torrentBuilder.setEndpoints(setup);
    }

    public void setManifestStream(StreamId manifestStreamId, MyStream manifestStream) {
      torrentBuilder.setManifestStream(manifestStreamId, manifestStream);
    }

    public MyStream getManifestStream() {
      return torrentBuilder.getManifestStream().getValue1();
    }

    public StreamId getManifestStreamId() {
      return torrentBuilder.getManifestStream().getValue0();
    }

    public void setManifest(Either<MyTorrent.Manifest, ManifestJSON> manifest) {
      torrentBuilder.setManifest(torrentId, manifest);
    }

    public Map<String, FileId> getFiles() {
      return torrentBuilder.getFiles();
    }

    public void setDetails(Map<FileId, FileExtendedDetails> extendedDetails) {
      torrentBuilder.setExtendedDetails(extendedDetails);
    }

    private TComplete finish() {
      return new TComplete(torrentBuilder.getTorrent(), getManifestStream(), download);
    }
  }

  public static class TComplete implements TorrentState {

    public final MyTorrent torrent;
    public final MyStream manifestStream;
    public final boolean download;

    private TComplete(MyTorrent torrent, MyStream manifestStream, boolean download) {
      this.torrent = torrent;
      this.manifestStream = manifestStream;
      this.download = download;
    }
  }

  public static class AuxState {

    private String kafkaEndpointName;
    private Map<String, KafkaResource> kafkaDetails;

    private void setKafkaDetails(Map<String, KafkaResource> kafkaDetails) {
      this.kafkaDetails = kafkaDetails;
    }

    public Map<String, KafkaResource> getKafkaDetails() {
      return kafkaDetails;
    }

    public String getKafkaEndpointName() {
      return kafkaEndpointName;
    }

    private void setKafkaEndpointName(Optional<KafkaEndpoint> kafkaEndpoint) {
      if (kafkaEndpoint.isPresent()) {
        this.kafkaEndpointName = kafkaEndpoint.get().getEndpointName();
      }
    }
  }

  public static class Builder implements FSMInternalStateBuilder {

    @Override
    public FSMInternalState newState(FSMId fsmId) {
      return new LibTInternal(fsmId);
    }
  }
}
