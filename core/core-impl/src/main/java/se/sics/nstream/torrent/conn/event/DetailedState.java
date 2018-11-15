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
package se.sics.nstream.torrent.conn.event;

import se.sics.kompics.KompicsEvent;
import se.sics.kompics.util.Identifiable;
import se.sics.kompics.util.Identifier;
import se.sics.ktoolbox.util.result.Result;
import se.sics.nstream.transfer.MyTorrent;

/**
 *
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class DetailedState {

  public static class Set implements KompicsEvent, Identifiable {

    public final Identifier eventId;
    public final MyTorrent.ManifestDef manifestDef;

    public Set(Identifier eventId, MyTorrent.ManifestDef manifestDef) {
      this.eventId = eventId;
      this.manifestDef = manifestDef;
    }

    @Override
    public Identifier getId() {
      return eventId;
    }
  }

  public static class Deliver implements KompicsEvent, Identifiable {

    public final Identifier eventId;
    public final Result<MyTorrent.ManifestDef> manifestDef;

    public Deliver(Identifier eventId, Result<MyTorrent.ManifestDef> manifestDef) {
      this.eventId = eventId;
      this.manifestDef = manifestDef;
    }

    @Override
    public Identifier getId() {
      return eventId;
    }
  }
}
