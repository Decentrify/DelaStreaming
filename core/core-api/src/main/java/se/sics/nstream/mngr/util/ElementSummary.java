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
package se.sics.nstream.mngr.util;

import se.sics.ktoolbox.util.identifiable.overlay.OverlayId;
import se.sics.nstream.library.util.TorrentState;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public abstract class ElementSummary {

  public final String fileName;
  public final OverlayId torrentId;
  public final TorrentState status;

  private ElementSummary(String name, OverlayId torrentId, TorrentState status) {
    this.fileName = name;
    this.torrentId = torrentId;
    this.status = status;
  }

  public static class Download extends ElementSummary {
    public final long speed;
    public final double dynamic;
    
    public Download(String name, OverlayId torrentId, TorrentState status, long speed, double dynamic) {
      super(name, torrentId, status);
      this.speed = speed;
      this.dynamic = dynamic;
    }
  }
  
  public static class Upload extends ElementSummary {

    public Upload(String name, OverlayId torrentId, TorrentState status) {
      super(name, torrentId, status);
    }
  }
}
