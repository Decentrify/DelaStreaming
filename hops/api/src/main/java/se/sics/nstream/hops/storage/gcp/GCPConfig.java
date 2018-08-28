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
package se.sics.nstream.hops.storage.gcp;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.common.base.Optional;
import java.io.FileInputStream;
import java.io.IOException;
import se.sics.kompics.config.Config;
import se.sics.ktoolbox.util.trysf.Try;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class GCPConfig {

  public static class Names {
    public static final String GCP_CREDENTIAL_FILE = "gcp.credentials.file";
    public static final String GCP_PROJECT = "gcp.project";
    public static final String GCP_BUCKET = "gcp.bucket";
  }
  
  public final GoogleCredentials credentials;
  public final String project;
  public final String bucket;

  private GCPConfig(GoogleCredentials credentials, String project, String bucket) {
    this.credentials = credentials;
    this.project = project;
    this.bucket = bucket;
  }

  public static Try<GCPConfig> read(Config config) {
    Optional<String> credentialsFile = config.readValue(Names.GCP_CREDENTIAL_FILE, String.class);
    if (!credentialsFile.isPresent()) {
      return new Try.Failure(new IllegalStateException("gcp credential file - undefined"));
    }
    GoogleCredentials credentials;
    try {
      credentials = GoogleCredentials.fromStream(new FileInputStream(credentialsFile.get()));
    } catch (IOException ex) {
      return new Try.Failure(ex);
    }
    String project = config.getValue(Names.GCP_PROJECT, String.class);
    String bucket = config.getValue(Names.GCP_BUCKET, String.class);
    return new Try.Success(new GCPConfig(credentials, project, bucket));
  }
}
