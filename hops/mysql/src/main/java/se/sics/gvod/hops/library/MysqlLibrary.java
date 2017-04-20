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
package se.sics.gvod.hops.library;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import se.sics.gvod.hops.library.dao.TorrentDAO;
import se.sics.kompics.config.Config;
import se.sics.ktoolbox.util.identifiable.BasicBuilders;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayId;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayIdFactory;
import se.sics.ktoolbox.util.network.KAddress;
import se.sics.nstream.hops.library.LibraryCtrl;
import se.sics.nstream.hops.library.Torrent;
import se.sics.nstream.hops.library.util.LibrarySummaryHelper;
import se.sics.nstream.library.util.TorrentState;
import se.sics.nstream.storage.durable.util.MyStream;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class MysqlLibrary implements LibraryCtrl {

  private final Config config;
  private final OverlayIdFactory torrentIdFactory;
  private EntityManagerFactory emf;
  private Map<OverlayId, Torrent> torrents = new HashMap<>();
  private Map<OverlayId, TorrentDAO> tdaos;

  public MysqlLibrary(OverlayIdFactory torrentIdFactory, Config config) {
    this.config = config;
    this.torrentIdFactory = torrentIdFactory;
  }

  @Override
  public Map<OverlayId, Torrent> start() {
    emf = PersistenceMngr.getEMF(config);
    return readTorrents();
  }

  @Override
  public void stop() {
    emf.close();
  }

  private Map<OverlayId, Torrent> readTorrents() {
    EntityManager em = emf.createEntityManager();
    try {
      List<TorrentDAO> ts = em.createNamedQuery("dela.findAll", TorrentDAO.class).getResultList();
      Map<OverlayId, Torrent> readTorrents = new HashMap<>();
      tdaos = new HashMap<>();
      for (TorrentDAO t : ts) {
        OverlayId tId = torrentIdFactory.id(new BasicBuilders.StringBuilder(t.getId()));
        Torrent tt = new Torrent(t.getPid(), t.getDid(), t.getName(), TorrentState.valueOf(t.getStatus()));
        tt.setManifestStream(LibrarySummaryHelper.streamFromJSON(t.getStream()));
        tt.setPartners(LibrarySummaryHelper.partnersFromJSON(t.getPartners()));
        readTorrents.put(tId, tt);
        tdaos.put(tId, t);
      }
      return readTorrents;
    } finally {
      em.close();
    }
  }

  private void delete(TorrentDAO torrent) {
    EntityManager em = emf.createEntityManager();
    try {
      em.getTransaction().begin();
      em.remove(torrent);
      em.getTransaction().commit();
    } finally {
      em.close();
    }
  }

  private TorrentDAO merge(TorrentDAO torrent) {
    EntityManager em = emf.createEntityManager();
    try {
      em.getTransaction().begin();
      TorrentDAO res = em.merge(torrent);
      em.getTransaction().commit();
      return res;
    } finally {
      em.close();
    }
  }

  @Override
  public Map<OverlayId, Torrent> getTorrents() {
    return torrents;
  }

  @Override
  public boolean containsTorrent(OverlayId tId) {
    return torrents.containsKey(tId);
  }

  @Override
  public TorrentState stateOf(OverlayId tId) {
    Torrent t = torrents.get(tId);
    if (t == null) {
      return TorrentState.NONE;
    }
    return t.getTorrentStatus();
  }

  @Override
  public void prepareUpload(OverlayId torrentId, Integer projectId, Integer datasetId, String torrentName) {
    Torrent torrent = new Torrent(projectId, datasetId, torrentName, TorrentState.PREPARE_UPLOAD);
    torrents.put(torrentId, torrent);

    TorrentDAO tdao = new TorrentDAO(torrentId.baseId.toString());
    tdao.setPid(projectId);
    tdao.setDid(datasetId);
    tdao.setName(torrentName);
    tdao.setStatus(TorrentState.PREPARE_UPLOAD.name());
    tdaos.put(torrentId, tdao);
    merge(tdao);
  }

  @Override
  public void upload(OverlayId torrentId, MyStream manifestStream) {
    Torrent torrent = torrents.get(torrentId);
    torrent.setTorrentStatus(TorrentState.UPLOADING);
    torrent.setManifestStream(manifestStream);

    TorrentDAO tdao = tdaos.get(torrentId);
    tdao.setStatus(TorrentState.UPLOADING.name());
    tdao.setStream(LibrarySummaryHelper.streamToJSON(manifestStream));
    tdao = merge(tdao);
    tdaos.put(torrentId, tdao);
  }

  @Override
  public void prepareDownload(OverlayId torrentId, Integer projectId, Integer datasetId, String torrentName,
    List<KAddress> partners) {

    Torrent torrent = new Torrent(projectId, datasetId, torrentName, TorrentState.PREPARE_DOWNLOAD);
    torrent.setPartners(partners);
    torrents.put(torrentId, torrent);

    TorrentDAO tdao = new TorrentDAO(torrentId.baseId.toString());
    tdao.setPid(projectId);
    tdao.setDid(datasetId);
    tdao.setName(torrentName);
    tdao.setStatus(TorrentState.PREPARE_DOWNLOAD.name());
    tdao.setPartners(LibrarySummaryHelper.partnersToJSON(partners));
    tdaos.put(torrentId, tdao);
    merge(tdao);
  }

  @Override
  public void download(OverlayId torrentId, MyStream manifestStream) {
    Torrent torrent = torrents.get(torrentId);
    torrent.setTorrentStatus(TorrentState.DOWNLOADING);
    torrent.setManifestStream(manifestStream);

    TorrentDAO tdao = tdaos.get(torrentId);
    tdao.setStatus(TorrentState.DOWNLOADING.name());
    tdao.setStream(LibrarySummaryHelper.streamToJSON(manifestStream));
  }

  @Override
  public void finishDownload(OverlayId torrentId) {
    Torrent torrent = torrents.get(torrentId);
    torrent.setTorrentStatus(TorrentState.UPLOADING);

    TorrentDAO tdao = tdaos.get(torrentId);
    tdao.setStatus(TorrentState.UPLOADING.name());
    tdao = merge(tdao);
    tdaos.put(torrentId, tdao);
  }

  @Override
  public void killing(OverlayId torrentId) {
    torrents.get(torrentId).setTorrentStatus(TorrentState.KILLING);

    TorrentDAO tdao = tdaos.get(torrentId);
    tdao.setStatus(TorrentState.KILLING.name());
    tdao = merge(tdao);
    tdaos.put(torrentId, tdao);
  }

  @Override
  public void killed(OverlayId torrentId) {
    torrents.remove(torrentId);
    TorrentDAO tdao = tdaos.remove(torrentId);
    if (tdao != null) {
      delete(tdao);
    }
  }
}