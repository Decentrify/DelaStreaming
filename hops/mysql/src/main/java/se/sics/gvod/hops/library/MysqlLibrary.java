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
import se.sics.gvod.hops.api.LibraryCtrl;
import se.sics.gvod.hops.api.Torrent;
import se.sics.gvod.hops.library.dao.TorrentDAO;
import se.sics.ktoolbox.util.identifiable.BasicBuilders;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayId;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayIdFactory;
import se.sics.ktoolbox.util.network.KAddress;
import se.sics.nstream.library.util.TorrentState;
import se.sics.nstream.storage.durable.util.MyStream;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class MysqlLibrary implements LibraryCtrl {

  private final OverlayIdFactory torrentIdFactory;
  private EntityManager em;
  private Map<OverlayId, Torrent> torrents;
  private Map<OverlayId, TorrentDAO> tdaos;

  public MysqlLibrary(OverlayIdFactory torrentIdFactory) {
    this.torrentIdFactory = torrentIdFactory;
  }

  public void start() {
    em = PersistenceMngr.INSTANCE.getEntityManager();
    readTorrents();
  }

  public void stop() {
    em.close();
    PersistenceMngr.INSTANCE.close();
  }

  private void readTorrents() {
    List<TorrentDAO> ts = (List<TorrentDAO>) em.createNamedQuery("Dela.findAll").getResultList();
    torrents = new HashMap<>();
    tdaos = new HashMap<>();
    for (TorrentDAO t : ts) {
      OverlayId tId = torrentIdFactory.id(new BasicBuilders.StringBuilder(t.getId()));
      Torrent tt = new Torrent(t.getPid(), t.getDid(), t.getName(), TorrentState.valueOf(t.getStatus()));
      torrents.put(tId, tt);
      tdaos.put(tId, t);
    }
  }

  private void persist(TorrentDAO torrent) {
    em.getTransaction().begin();
    em.persist(torrent);
    em.getTransaction().commit();
  }

  private void delete(TorrentDAO torrent) {
    em.getTransaction().begin();
    em.remove(torrent);
    em.getTransaction().commit();
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
    persist(tdao);
  }

  @Override
  public void upload(OverlayId torrentId, MyStream manifestStream) {
    Torrent torrent = torrents.get(torrentId);
    torrent.setTorrentStatus(TorrentState.UPLOADING);
    torrent.setManifestStream(manifestStream);

    TorrentDAO tdao = tdaos.get(torrentId);
    tdao.setStatus(TorrentState.UPLOADING.name());
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
    tdaos.put(torrentId, tdao);
    persist(tdao);
  }

  @Override
  public void download(OverlayId torrentId, MyStream manifestStream) {
    Torrent torrent = torrents.get(torrentId);
    torrent.setTorrentStatus(TorrentState.DOWNLOADING);
    torrent.setManifestStream(manifestStream);

    TorrentDAO tdao = tdaos.get(torrentId);
    tdao.setStatus(TorrentState.DOWNLOADING.name());
  }

  @Override
  public void finishDownload(OverlayId torrentId) {
    Torrent torrent = torrents.get(torrentId);
    torrent.setTorrentStatus(TorrentState.UPLOADING);

    TorrentDAO tdao = tdaos.get(torrentId);
    tdao.setStatus(TorrentState.UPLOADING.name());
  }

  @Override
  public void killing(OverlayId torrentId) {
    torrents.get(torrentId).setTorrentStatus(TorrentState.KILLING);
    tdaos.get(torrentId).setStatus(TorrentState.KILLING.name());
  }

  @Override
  public void killed(OverlayId torrentId) {
    torrents.remove(torrentId);
    delete(tdaos.remove(torrentId));
  }
}
