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
package se.sics.nstream.torrent.transfer;

import com.google.common.base.Optional;
import se.sics.kompics.config.Config;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class DwnlConnConfig {
  public static class Names {
    public static String REPORT_DIR = "transfer.download.report.dir";
    public static String MIN_RTO = "transfer.download.minRTO";
  }
  
  public static final long DEFAULT_MIN_RTO = 1000; //1s
  public final Optional<String> reportDir;
  public final long minRTO;
  
  public DwnlConnConfig(Config config) {
    reportDir = Optional.fromNullable(config.getValue(Names.REPORT_DIR, String.class));
    minRTO = config.getValueOrDefault(Names.MIN_RTO, DEFAULT_MIN_RTO);
  }
}
