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
package se.sics.silk.event;

import se.sics.kompics.KompicsEvent;
import se.sics.kompics.util.Identifiable;
import se.sics.kompics.util.Identifier;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayId;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class SilkEvent {

  public static interface NodeEvent {

    public Identifier nodeId();
  }

  public static interface TorrentEvent {

    public OverlayId torrentId();
  }

  public static interface FileEvent {

    public Identifier fileId();
  }

  public static abstract class E1 implements TorrentEvent, NodeEvent, KompicsEvent, Identifiable {

    public final Identifier eventId;
    public final OverlayId torrentId;
    public final Identifier nodeId;

    public E1(Identifier eventId, OverlayId torrentId, Identifier nodeId) {
      this.eventId = eventId;
      this.torrentId = torrentId;
      this.nodeId = nodeId;
    }

    @Override
    public Identifier getId() {
      return eventId;
    }

    @Override
    public OverlayId torrentId() {
      return torrentId;
    }

    @Override
    public Identifier nodeId() {
      return nodeId;
    }
  }
  
  public static abstract class E2 implements FileEvent, TorrentEvent, NodeEvent, KompicsEvent, Identifiable {

    public final Identifier eventId;
    public final Identifier fileId;
    public final OverlayId torrentId;
    public final Identifier nodeId;

    public E2(Identifier eventId, OverlayId torrentId, Identifier fileId, Identifier nodeId) {
      this.eventId = eventId;
      this.torrentId = torrentId;
      this.fileId = fileId;
      this.nodeId = nodeId;
    }

    @Override
    public Identifier getId() {
      return eventId;
    }

    @Override
    public Identifier fileId() {
      return fileId;
    }

    @Override
    public OverlayId torrentId() {
      return torrentId;
    }

    @Override
    public Identifier nodeId() {
      return nodeId;
    }
  }
  
  public static class E3 implements KompicsEvent, Identifiable, TorrentEvent {
    public final Identifier eventId;
    public final OverlayId torrentId;
    
    public E3(Identifier eventId, OverlayId torrentId) {
      this.eventId = eventId;
      this.torrentId = torrentId;
    }
    @Override
    public Identifier getId() {
      return eventId;
    }

    @Override
    public OverlayId torrentId() {
      return torrentId;
    }
  }
  
  public static class E4 implements KompicsEvent, Identifiable, TorrentEvent, FileEvent {
    public final Identifier eventId;
    public final OverlayId torrentId;
    public final Identifier fileId;
    
    public E4(Identifier eventId, OverlayId torrentId, Identifier fileId) {
      this.eventId = eventId;
      this.torrentId = torrentId;
      this.fileId = fileId;
    }
    
    @Override
    public Identifier getId() {
      return eventId;
    }

    @Override
    public OverlayId torrentId() {
      return torrentId;
    }

    @Override
    public Identifier fileId() {
      return fileId;
    }
    
  }
  
  public static class E5 implements KompicsEvent, TorrentEvent, FileEvent {
    public final Identifier eventId;
    public final OverlayId torrentId;
    public final Identifier fileId;
    
    public E5(Identifier eventId, OverlayId torrentId, Identifier fileId) {
      this.eventId = eventId;
      this.torrentId = torrentId;
      this.fileId = fileId;
    }
    
    @Override
    public OverlayId torrentId() {
      return torrentId;
    }

    @Override
    public Identifier fileId() {
      return fileId;
    }
  }
}
