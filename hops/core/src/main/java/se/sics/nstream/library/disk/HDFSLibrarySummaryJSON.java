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
package se.sics.nstream.library.disk;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import org.javatuples.Pair;
import se.sics.gvod.hops.api.Torrent;
import se.sics.ktoolbox.util.identifiable.BasicBuilders;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayId;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayIdFactory;
import se.sics.ktoolbox.util.network.KAddress;
import se.sics.nstream.hops.hdfs.HDFSEndpoint;
import se.sics.nstream.hops.hdfs.HDFSResource;
import se.sics.nstream.library.util.TorrentState;
import se.sics.nstream.storage.durable.util.MyStream;
import se.sics.nstream.transfer.MyTorrent;

/**
 *
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class HDFSLibrarySummaryJSON {

  private List<TorrentJSON> torrents = new ArrayList<>();

  public List<TorrentJSON> getTorrents() {
    return torrents;
  }

  public void setTorrents(List<TorrentJSON> torrents) {
    this.torrents = torrents;
  }

  public void addTorrent(TorrentJSON torrent) {
    torrents.add(torrent);
  }

  public boolean isEmpty() {
    return torrents.isEmpty();
  }

  public static class HDFSEndpointJSON {

    private String url;
    private String user;

    public HDFSEndpointJSON() {
    }
    
    public HDFSEndpointJSON(String url, String user) {
      this.url = url;
      this.user = user;
    }
    
    public String getUrl() {
      return url;
    }

    public void setUrl(String url) {
      this.url = url;
    }

    public String getUser() {
      return user;
    }

    public void setUser(String user) {
      this.user = user;
    }
  }

  public static class TorrentJSON {

    private Integer projectId;
    private Integer datasetId;
    private String torrentName;
    private String torrentStatus;
    private String baseId;
    private HDFSEndpointJSON endpoint;
    private String dirPath;
    private List<AddressJSON> partners;

    public TorrentJSON() {
    }
    
    public TorrentJSON(Integer projectId, Integer datasetId, String torrentName, String torrentStatus, String baseId) {
      this.projectId = projectId;
      this.datasetId = datasetId;
      this.torrentName = torrentName;
      this.torrentStatus = torrentStatus;
      this.baseId = baseId;
    }

    public Integer getProjectId() {
      return projectId;
    }

    public void setProjectId(Integer projectId) {
      this.projectId = projectId;
    }

    public Integer getDatasetId() {
      return datasetId;
    }

    public void setDatasetId(Integer datasetId) {
      this.datasetId = datasetId;
    }
    
    public String getTorrentName() {
      return torrentName;
    }

    public void setTorrentName(String torrentName) {
      this.torrentName = torrentName;
    }

    public String getTorrentStatus() {
      return torrentStatus;
    }

    public void setTorrentStatus(String torrentStatus) {
      this.torrentStatus = torrentStatus;
    }

    public String getDirPath() {
      return dirPath;
    }

    public void setDirPath(String dirPath) {
      this.dirPath = dirPath;
    }

    public String getBaseId() {
      return baseId;
    }

    public void setBaseId(String baseId) {
      this.baseId = baseId;
    }

    public HDFSEndpointJSON getEndpoint() {
      return endpoint;
    }

    public void setEndpoint(HDFSEndpointJSON endpoint) {
      this.endpoint = endpoint;
    }

    public List<AddressJSON> getPartners() {
      return partners;
    }

    public void setPartners(List<AddressJSON> partners) {
      this.partners = partners;
    }

    public static TorrentJSON toJSON(OverlayId tId, Torrent t) {
      HDFSLibrarySummaryJSON.TorrentJSON torrentJSON = new HDFSLibrarySummaryJSON.TorrentJSON(t.projectId, t.datasetId,
        t.torrentName, t.getTorrentStatus().toString(), tId.baseId.toString());
      //endpoint
      HDFSEndpoint hdfsEndpoint = (HDFSEndpoint) t.getManifestStream().endpoint;
      HDFSEndpointJSON endpoint = new HDFSEndpointJSON(hdfsEndpoint.hopsURL, hdfsEndpoint.user);
      torrentJSON.setEndpoint(endpoint);
      //resource
      HDFSResource hdfsResource = (HDFSResource) t.getManifestStream().resource;
      torrentJSON.setDirPath(hdfsResource.dirPath);
      List<AddressJSON> partners = new LinkedList<>();
      for (KAddress p : t.getPartners()) {
        partners.add(AddressJSON.toJSON(p));
      }
      torrentJSON.setPartners(partners);
      return torrentJSON;
    }

    public Pair<OverlayId, Torrent> fromJSON(OverlayIdFactory torrentIdFactory) {
      OverlayId tId = torrentIdFactory.id(new BasicBuilders.StringBuilder(baseId));
      TorrentState status = TorrentState.valueOf(torrentStatus);
      Torrent t = new Torrent(projectId, datasetId, torrentName, status);
      
      HDFSEndpoint hdfsEndpoint = HDFSEndpoint.getBasic(endpoint.getUrl(), endpoint.getUser());
      HDFSResource manifestResource = new HDFSResource(dirPath, MyTorrent.MANIFEST_NAME);
      MyStream manifestStream = new MyStream(hdfsEndpoint, manifestResource);
      t.setManifestStream(manifestStream);
      
      List<KAddress> p = new LinkedList<>();
      for (AddressJSON e : partners) {
        p.add(e.fromJSON());
      }
      t.setPartners(p);
      return Pair.with(tId, t);
    }
  }
}
