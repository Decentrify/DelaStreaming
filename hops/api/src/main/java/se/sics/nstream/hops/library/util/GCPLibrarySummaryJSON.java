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
package se.sics.nstream.hops.library.util;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import org.javatuples.Pair;
import se.sics.ktoolbox.util.identifiable.BasicBuilders;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayId;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayIdFactory;
import se.sics.ktoolbox.util.network.KAddress;
import se.sics.nstream.hops.library.Torrent;
import se.sics.nstream.hops.storage.gcp.GCPConfig;
import se.sics.nstream.hops.storage.gcp.GCPEndpoint;
import se.sics.nstream.hops.storage.gcp.GCPResource;
import se.sics.nstream.library.util.TorrentState;
import se.sics.nstream.storage.durable.util.MyStream;
import se.sics.nstream.transfer.MyTorrent;
import se.sics.nstream.util.TorrentExtendedStatus;

/**
 *
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class GCPLibrarySummaryJSON {

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

  public static class GCPEndpointJSON implements EndpointJSON {
    private String project;

    public GCPEndpointJSON() {
    }
    
    public GCPEndpointJSON(String project) {
      this.project = project;
    }
    
    public String getProject() {
      return project;
    }

    public void setProject(String project) {
      this.project = project;
    }

    public GCPEndpoint fromJSON(GCPConfig gcpConfig) {
      return new GCPEndpoint(gcpConfig.credentials, project);
    }
    
    public static GCPEndpointJSON toJSON(GCPEndpoint endpoint) {
      return new GCPEndpointJSON(endpoint.projectName);
    }
  }

  public static class GCPResourceJSON implements ResourceJSON {

    private String libDir;
    private String relativePath;

    public GCPResourceJSON() {
    }

    public GCPResourceJSON(String libDir, String relativePath) {
      this.libDir = libDir;
      this.relativePath = relativePath;
    }

    public String getLibDir() {
      return libDir;
    }

    public void setLibDir(String libDir) {
      this.libDir = libDir;
    }

    public String getRelativePath() {
      return relativePath;
    }

    public void setRelativePath(String relativePath) {
      this.relativePath = relativePath;
    }

    public GCPResource fromJSON(String file) {
      return new GCPResource(libDir, relativePath, file);
    }

    public static GCPResourceJSON toJSON(GCPResource resource) {
      return new GCPResourceJSON(resource.libDir, resource.relativePath);
    }
  }
  
  public static class TorrentJSON {

    private Integer projectId;
    private Integer datasetId;
    private String torrentName;
    private String torrentStatus;
    private String baseId;
    private GCPEndpointJSON endpoint;
    private GCPResourceJSON resource;
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

    public GCPResourceJSON getResource() {
      return resource;
    }

    public void setResource(GCPResourceJSON resource) {
      this.resource = resource;
    }
    
    public String getBaseId() {
      return baseId;
    }

    public void setBaseId(String baseId) {
      this.baseId = baseId;
    }

    public GCPEndpointJSON getEndpoint() {
      return endpoint;
    }

    public void setEndpoint(GCPEndpointJSON endpoint) {
      this.endpoint = endpoint;
    }

    public List<AddressJSON> getPartners() {
      return partners;
    }

    public void setPartners(List<AddressJSON> partners) {
      this.partners = partners;
    }

    public static TorrentJSON toJSON(OverlayId tId, Torrent t) {
      GCPLibrarySummaryJSON.TorrentJSON torrentJSON = new GCPLibrarySummaryJSON.TorrentJSON(t.projectId, t.datasetId,
        t.torrentName, t.getTorrentStatus().toString(), tId.baseId.toString());
      //endpoint
      GCPEndpoint gcpEndpoint = (GCPEndpoint) t.getManifestStream().endpoint;
      GCPEndpointJSON endpoint = new GCPEndpointJSON(gcpEndpoint.projectName);
      torrentJSON.setEndpoint(endpoint);
      //resource
      GCPResource gcpResource = (GCPResource) t.getManifestStream().resource;
      torrentJSON.setResource(GCPResourceJSON.toJSON(gcpResource));
      List<AddressJSON> partners = new LinkedList<>();
      for (KAddress p : t.getPartners()) {
        partners.add(AddressJSON.toJSON(p));
      }
      torrentJSON.setPartners(partners);
      return torrentJSON;
    }

    public Pair<OverlayId, Torrent> fromJSON(OverlayIdFactory torrentIdFactory, GCPConfig config) {
      OverlayId tId = torrentIdFactory.id(new BasicBuilders.StringBuilder(baseId));
      TorrentState status = TorrentState.valueOf(torrentStatus);
      TorrentExtendedStatus extendedStatus = new TorrentExtendedStatus(tId, status, 0, 0);
      Torrent t = new Torrent(projectId, datasetId, torrentName, extendedStatus);
      
      GCPEndpoint gcpEndpoint = new GCPEndpoint(config.credentials, endpoint.project);
      GCPResource manifestResource = resource.fromJSON(MyTorrent.MANIFEST_NAME);
      MyStream manifestStream = new MyStream(gcpEndpoint, manifestResource);
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
