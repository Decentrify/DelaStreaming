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
import se.sics.kompics.ComponentProxy;
import se.sics.ktoolbox.nutil.fsm.api.FSMInternalState;
import se.sics.ktoolbox.nutil.fsm.api.FSMInternalStateBuilder;
import se.sics.ktoolbox.nutil.fsm.ids.FSMId;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayId;
import se.sics.ktoolbox.util.network.KAddress;
import se.sics.ktoolbox.util.result.Result;
import se.sics.nstream.hops.libmngr.EndpointRegistration;
import se.sics.nstream.hops.libmngr.TorrentBuilder;
import se.sics.nstream.hops.library.event.core.HopsTorrentDownloadEvent;
import se.sics.nstream.hops.library.event.core.HopsTorrentStopEvent;
import se.sics.nstream.hops.library.event.core.HopsTorrentUploadEvent;
import se.sics.nstream.library.Library;
import se.sics.nstream.library.restart.TorrentRestart;
import se.sics.nstream.transfer.MyTorrent;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class LibTInternal implements FSMInternalState {

  public final FSMId fsmId;
  public final String fsmName;
  //either
  private Optional<HopsTorrentUploadEvent.Request> uploadReq = Optional.absent();
  private Optional<TorrentRestart.UpldReq> uRestartReq = Optional.absent();
  private Optional<HopsTorrentDownloadEvent.StartRequest> downloadReq = Optional.absent();
  private Optional<HopsTorrentDownloadEvent.AdvanceRequest> advanceReq = Optional.absent();
  private Optional<TorrentRestart.DwldReq> dRestartReq = Optional.absent();
  //
  private HopsTorrentStopEvent.Request stopReq;
  //
  private OverlayId torrentId;
  private List<KAddress> partners;
  public final EndpointRegistration endpointRegistration = new EndpointRegistration();
  private TorrentBuilder torrentBuilder;
  private MyTorrent torrent;

  public LibTInternal(FSMId fsmId) {
    this.fsmId = fsmId;
    this.fsmName = LibTFSM.NAME;
  }

  public void setUploadInit(HopsTorrentUploadEvent.Request req) {
    this.uploadReq = Optional.of(req);
    this.torrentId = req.torrentId;
    this.partners = new LinkedList<>();
  }

  public void setUploadRestartInit(TorrentRestart.UpldReq req) {
    this.uRestartReq = Optional.of(req);
    this.torrentId = req.torrentId;
    this.partners = req.partners;
  }

  public void setDownloadInit(HopsTorrentDownloadEvent.StartRequest req) {
    this.downloadReq = Optional.of(req);
    this.torrentId = req.torrentId;
    this.partners = req.partners;
  }
  
  public void setDownloadAdvance(HopsTorrentDownloadEvent.AdvanceRequest req) {
    this.advanceReq = Optional.of(req);
  }

  public void setDownloadRestartInit(TorrentRestart.DwldReq req) {
    this.dRestartReq = Optional.of(req);
    this.torrentId = req.torrentId;
    this.partners = req.partners;
  }

  public HopsTorrentUploadEvent.Request getUploadReq() {
    return uploadReq.get();
  }

  public void finishUploadReq() {
    uploadReq = Optional.absent();
  }

  public boolean isDownload() {
    return downloadReq.isPresent();
  }
  
  public boolean isDownloadRestart() {
    return dRestartReq.isPresent();
  }
  
  public HopsTorrentDownloadEvent.StartRequest getDownloadReq() {
    return downloadReq.get();
  }

  public void finishDownloadReq() {
    downloadReq = Optional.absent();
  }

  public HopsTorrentStopEvent.Request getStopReq() {
    return stopReq;
  }

  public void setStopReq(HopsTorrentStopEvent.Request stopReq) {
    this.stopReq = stopReq;
  }

  public OverlayId getTorrentId() {
    return torrentId;
  }

  public List<KAddress> getPartners() {
    return partners;
  }

  public TorrentBuilder getTorrentBuilder() {
    return torrentBuilder;
  }

  public void setTorrentBuilder(TorrentBuilder torrentBuilder) {
    this.torrentBuilder = torrentBuilder;
  }

  public MyTorrent getTorrent() {
    return torrent;
  }

  public void setTorrent(MyTorrent torrent) {
    this.torrent = torrent;
  }
  
  public void reqFailed(ComponentProxy proxy) {
    if (downloadReq.isPresent()) {
      proxy.answer(downloadReq.get(), downloadReq.get().
        failed(Result.logicalFail("concurrent stop event with download:" + torrentId)));
      downloadReq = Optional.absent();
    } else if (advanceReq.isPresent()) {
      proxy.answer(advanceReq.get(), advanceReq.get().
        fail(Result.logicalFail("concurrent stop event with download:" + torrentId)));
      advanceReq = Optional.absent();
    } else if (dRestartReq.isPresent()) {
      dRestartReq.get().failed();
      dRestartReq = Optional.absent();
    } else if (uploadReq.isPresent()) {
      proxy.answer(uploadReq.get(), uploadReq.get().
        failed(Result.logicalFail("concurrent stop event with upload:" + torrentId)));
      uploadReq = Optional.absent();
    } else {
      uRestartReq.get().failed();
      uRestartReq = Optional.absent();
    }
  }
  
  public LibTStates reqSuccess(ComponentProxy proxy, Library library) {
    if (downloadReq.isPresent()) {
      proxy.answer(downloadReq.get(), downloadReq.get().success(Result.success(true)));
      library.download(torrentId, torrentBuilder.getManifestStream().getValue1());
      downloadReq = Optional.absent();
      return LibTStates.DOWNLOADING;
    } else if (advanceReq.isPresent()) {
      proxy.answer(advanceReq.get(), advanceReq.get().success(Result.success(true)));
      library.download(torrentId, torrentBuilder.getManifestStream().getValue1());
      advanceReq = Optional.absent();
      return LibTStates.DOWNLOADING;
    } else if (dRestartReq.isPresent()) {
      dRestartReq.get().success();
      library.download(torrentId, torrentBuilder.getManifestStream().getValue1());
      dRestartReq = Optional.absent();
      return LibTStates.DOWNLOADING;
    }
    if (uploadReq.isPresent()) {
      proxy.answer(getUploadReq(), getUploadReq().success(Result.success(true)));
      library.upload(torrentId, torrentBuilder.getManifestStream().getValue1());
      uploadReq = Optional.absent();
      return LibTStates.UPLOADING;
    } else {
      uRestartReq.get().success();
      library.upload(torrentId, torrentBuilder.getManifestStream().getValue1());
      uRestartReq = Optional.absent();
      return LibTStates.UPLOADING;
    }
  }
  
  public static class Builder implements FSMInternalStateBuilder {

    @Override
    public FSMInternalState newState(FSMId fsmId) {
      return new LibTInternal(fsmId);
    }
  }
}
