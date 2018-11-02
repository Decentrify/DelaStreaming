/*
 * Copyright (C) 2009 Swedish Institute of Computer Science (SICS) Copyright (C)
 * 2009 Royal Institute of Technology (KTH)
 *
 * KompicsToolbox is free software; you can redistribute it and/or
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
package se.sics.dela.storage.aws;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.google.common.base.Optional;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import se.sics.dela.storage.common.DelaStorageException;
import se.sics.dela.storage.common.StorageType;
import se.sics.kompics.config.Config;
import se.sics.ktoolbox.util.trysf.Try;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class AWSConfig {

  public static final String AWS_CREDENTIAL_FILE = "aws.credentials.file";
  public static final String AWS_BUCKET = "aws.bucket";
  public static final String AWS_REGION = "aws.region";

  public final AWSCredentials credentials;
  public final String bucket;
  public final Regions region;

  private AWSConfig(AWSCredentials credentials, String bucket, Regions region) {
    this.credentials = credentials;
    this.bucket = bucket;
    this.region = region;
  }

  public static Try<AWSConfig> read(Config config) {
    Optional<String> credentialsFile = config.readValue(AWS_CREDENTIAL_FILE, String.class);
    if (!credentialsFile.isPresent()) {
      return new Try.Failure(new DelaStorageException("aws credential file - undefined", StorageType.AWS));
    }
    String bucket = config.getValue(AWS_BUCKET, String.class);
    Try<AWSCredentials> credentials = credentialsFromCSV(credentialsFile.get());
    if (!credentialsFile.isPresent()) {
      return new Try.Failure(new DelaStorageException("aws credential file - read problem", StorageType.AWS));
    }
    Regions region = config.getValue(AWS_REGION, Regions.class);
    return new Try.Success(new AWSConfig(credentials.get(), bucket, region));
  }

  public static Try<AWSCredentials> credentialsFromCSV(String csvKeyFile) {
    BufferedReader br;
    try {
      br = new BufferedReader(new FileReader(csvKeyFile));
      String header = br.readLine();
      if (!header.equals("Access key ID,Secret access key")) {
        return new Try.Failure(new DelaStorageException("csv key file has weird format", StorageType.AWS));
      }
      String[] content = br.readLine().split(",");
      if (content.length != 2) {
        return new Try.Failure(new DelaStorageException("csv key file has weird format", StorageType.AWS));
      }
      if (br.readLine() != null) {
        return new Try.Failure(new DelaStorageException("csv key file has weird format", StorageType.AWS));
      }
      return new Try.Success(new BasicAWSCredentials(content[0], content[1]));
    } catch (IOException ex) {
      return new Try.Failure(ex);
    }
  }
}
