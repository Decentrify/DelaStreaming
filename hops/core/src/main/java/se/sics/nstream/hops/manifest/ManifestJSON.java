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

import java.util.List;

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

  @Override
  public String toString() {
    return "ManifestJSON{" + "datasetName=" + datasetName + ", datasetDescription=" + datasetDescription
      + ", creatorEmail=" + creatorEmail + ", creatorDate=" + creatorDate + ", kafkaSupport=" + kafkaSupport
      + ", fileInfos=" + fileInfos + ", metaDataJsons=" + metaDataJsons + '}';
  }
}
