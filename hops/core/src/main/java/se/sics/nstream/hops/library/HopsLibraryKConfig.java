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
package se.sics.nstream.hops.library;

import com.google.common.base.Optional;
import se.sics.kompics.config.Config;
import se.sics.ktoolbox.util.trysf.Try;
import se.sics.nstream.hops.storage.gcp.GCPConfig;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class HopsLibraryKConfig {

  public static class Names {

    public static final String STORAGE_TYPE = "hops.storage.type";
    public static final String LIBRARY_TYPE = "hops.library.type";
  }

  public final Config configCore;
  public final Details.Types storageType;
  public final LibraryType libraryType;
  public final Optional<GCPConfig> gcpConfig;

  private HopsLibraryKConfig(Config config, Details.Types storageType, LibraryType libraryType, 
    Optional<GCPConfig> gcpConfig) {
    this.configCore = config;
    this.storageType = storageType;
    this.libraryType = libraryType;
    this.gcpConfig = gcpConfig;
  }

  public static Try<HopsLibraryKConfig> read(Config config) {
    Optional<GCPConfig> gcpConfig;
    Optional<String> storageTypeString = config.readValue(Names.STORAGE_TYPE, String.class);
    if (!storageTypeString.isPresent()) {
      return new Try.Failure(new IllegalStateException("storage type undefined"));
    }
    Details.Types storageType = Details.Types.valueOf(storageTypeString.get());
    if (Details.Types.GCP.equals(storageType)) {
      Try<GCPConfig> gcpC = GCPConfig.read(config);
      if (gcpC.isSuccess()) {
        gcpConfig = Optional.of(gcpC.get());
      } else {
        return (Try.Failure)gcpC;
      }
    } else {
      gcpConfig = Optional.absent();
    }
    Optional<String> libraryTypeString = config.readValue(Names.LIBRARY_TYPE, String.class);
    if (!libraryTypeString.isPresent()) {
      return new Try.Failure(new IllegalStateException("library type undefined"));
    }
    LibraryType libraryType = LibraryType.valueOf(libraryTypeString.get());
    return new Try.Success(new HopsLibraryKConfig(config, storageType, libraryType, gcpConfig));
  }
}
