///*
// * Copyright (C) 2009 Swedish Institute of Computer Science (SICS) Copyright (C)
// * 2009 Royal Institute of Technology (KTH)
// *
// * GVoD is free software; you can redistribute it and/or
// * modify it under the terms of the GNU General Public License
// * as published by the Free Software Foundation; either version 2
// * of the License, or (at your option) any later version.
// *
// * This program is distributed in the hope that it will be useful,
// * but WITHOUT ANY WARRANTY; without even the implied warranty of
// * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// * GNU General Public License for more details.
// *
// * You should have received a copy of the GNU General Public License
// * along with this program; if not, write to the Free Software
// * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
// */
//package se.sics.nstream.torrent.channel.msg;
//
//import se.sics.ktoolbox.nutil.fsm.api.FSMEvent;
//import se.sics.ktoolbox.util.identifiable.BasicIdentifiers;
//import se.sics.ktoolbox.util.identifiable.Identifier;
//import se.sics.ktoolbox.util.identifiable.overlay.OverlayId;
//import se.sics.ktoolbox.util.overlays.OverlayEvent;
//
///**
// * @author Alex Ormenisan <aaor@kth.se>
// */
//public class TorrentMsg {
//  
//  public static abstract class Base implements OverlayEvent, FSMEvent {
//    public final Identifier msgId;
//    public final OverlayId torrentId;
//
//    public Base(Identifier msgId, OverlayId torrentId) {
//      this.msgId = msgId;
//      this.torrentId = torrentId;
//    }
//    
//    @Override
//    public OverlayId overlayId() {
//      return torrentId;
//    }
//
//    @Override
//    public Identifier getId() {
//      return msgId;
//    }
//
//    @Override
//    public Identifier getFSMBaseId() {
//      return torrentId.baseId;
//    }
//  }
//
//  public static class Connect extends Base {
//
//    protected Connect(Identifier msgId, OverlayId torrentId) {
//      super(msgId, torrentId);
//    }
//
//    public Connect(OverlayId torrentId) {
//      this(BasicIdentifiers.msgId(), torrentId);
//    }
//
//    @Override
//    public String toString() {
//      return "Connect{" + "msgId=" + msgId + ", torrentId=" + torrentId + '}';
//    }
//    
//    public Connected connected() {
//      return new Connected(this);
//    }
//    
//    public Choke choke() {
//      return new Choke(this);
//    }
//    
//    public Disconnect disconnect() {
//      return new Disconnect(this);
//    }
//  }
//
//  public static class Connected extends Base {
//
//    protected Connected(Identifier msgId, OverlayId torrentId) {
//      super(msgId, torrentId);
//    }
//    
//    protected Connected(Connect req) {
//      this(req.msgId, req.torrentId);
//    }
//    
//    @Override
//    public String toString() {
//      return "Connected{" + "msgId=" + msgId + ", torrentId=" + torrentId + '}';
//    }
//  }
//
//  public static class Choke extends Base {
//
//    protected Choke(Identifier msgId, OverlayId torrentId) {
//      super(msgId, torrentId);
//    }
//    
//    protected Choke(Connect req) {
//      this(req.msgId, req.torrentId);
//    }
//
//    @Override
//    public String toString() {
//      return "Choke{" + "msgId=" + msgId + ", torrentId=" + torrentId + '}';
//    }
//  }
//
//  public static class Unchoke extends Base {
//
//    protected Unchoke(Identifier msgId, OverlayId torrentId) {
//      super(msgId, torrentId);
//    }
//
//    @Override
//    public String toString() {
//      return "Unchoke{" + "msgId=" + msgId + ", torrentId=" + torrentId + '}';
//    }
//  }
//  
//  public static class Disconnect extends Base {
//    
//    protected Disconnect(Identifier msgId, OverlayId torrentId) {
//      super(msgId, torrentId);
//    }
//
//    public Disconnect(OverlayId torrentId) {
//      this(BasicIdentifiers.msgId(), torrentId);
//    }
//    
//    protected Disconnect(Connect req) {
//      this(req.msgId, req.torrentId);
//    }
//
//    @Override
//    public String toString() {
//      return "Disconnect{" + "msgId=" + msgId + ", torrentId=" + torrentId + '}';
//    }
//  }
//}
