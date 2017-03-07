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
package se.sics.cobweb;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.UnsupportedEncodingException;
import java.util.List;
import se.sics.ktoolbox.util.result.Result;
import se.sics.nstream.transfer.MyTorrent;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class MyTorrentHelper {

  public static MyTorrent.Manifest torrent(String manifestPath) {
    MyTorrent.Manifest manifest = getManifest(readManifest(manifestPath).getValue());
    return manifest;
  }

  private static MyTorrent.Manifest getManifest(ManifestJSON manifestJSON) {
    return MyTorrent.buildDefinition(getManifestByte(manifestJSON));
  }

  private static ManifestJSON getManifestJSON(byte[] jsonByte) {
    String jsonString;
    try {
      jsonString = new String(jsonByte, "UTF-8");
    } catch (UnsupportedEncodingException ex) {
      throw new RuntimeException(ex);
    }
    Gson gson = new GsonBuilder().create();
    ManifestJSON manifest = gson.fromJson(jsonString, ManifestJSON.class);
    return manifest;
  }

  private static byte[] getManifestByte(ManifestJSON manifest) {
    Gson gson = new GsonBuilder().create();
    String jsonString = gson.toJson(manifest);
    byte[] jsonByte;
    try {
      jsonByte = jsonString.getBytes("UTF-8");
    } catch (UnsupportedEncodingException ex) {
      throw new RuntimeException(ex);
    }
    return jsonByte;
  }

  private static Result<ManifestJSON> readManifest(String manifestPath) {
    File file = new File(manifestPath);
    try (RandomAccessFile raf = new RandomAccessFile(file, "rw")) {
      long manifestLength = raf.length();
      byte[] manifestByte = new byte[(int) manifestLength];
      raf.readFully(manifestByte);
      ManifestJSON manifest = getManifestJSON(manifestByte);
      return Result.success(manifest);
    } catch (FileNotFoundException ex) {
      return Result.externalSafeFailure(ex);
    } catch (IOException ex) {
      return Result.externalSafeFailure(ex);
    }
  }


  public static class ManifestJSON {

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
  }

  public static class FileInfoJSON {

    private String fileName;
    private long length;
    private String schema;

    public FileInfoJSON() {
    }

    public String getFileName() {
      return fileName;
    }

    public void setFileName(String fileName) {
      this.fileName = fileName;
    }

    public String getSchema() {
      return schema;
    }

    public void setSchema(String schema) {
      this.schema = schema;
    }

    public long getLength() {
      return length;
    }

    public void setLength(long length) {
      this.length = length;
    }
  }
}
