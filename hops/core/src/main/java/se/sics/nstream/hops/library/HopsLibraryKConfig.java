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
import se.sics.gvod.hops.api.LibraryType;
import se.sics.kompics.config.Config;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class HopsLibraryKConfig {
  public static class Names {
    public static final String LIBRARY_TYPE = "hops.library.type";
    public static final String STORAGE_TYPE = "hops.storage.type";
  }

  public final Config configCore;
  public final Details.Types baseEndpointType;
  public final LibraryType libraryType;

  public HopsLibraryKConfig(Config config) {
    this.configCore = config;
    Optional<String> baseEndpointString = config.readValue(Names.STORAGE_TYPE, String.class);
    if(!baseEndpointString.isPresent()) {
      throw new RuntimeException("storage type undefined");
    }
    baseEndpointType = Details.Types.valueOf(baseEndpointString.get());
    libraryType = config.getValueOrDefault(Names.LIBRARY_TYPE, LibraryType.DISK);
  }
}
