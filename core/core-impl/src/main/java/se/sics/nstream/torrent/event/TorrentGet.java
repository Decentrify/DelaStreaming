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
//package se.sics.nstream.torrent.event;
//
//import se.sics.gvod.stream.StreamEvent;
//import se.sics.ktoolbox.util.identifiable.Identifier;
//import se.sics.ktoolbox.util.identifiable.basic.UUIDIdentifier;
//import se.sics.ktoolbox.util.managedStore.core.util.Torrent;
//
///**
// * @author Alex Ormenisan <aaor@kth.se>
// */
//public class TorrentGet {
//    public static class Request implements StreamEvent {
//        public final Identifier eventId;
//        
//        public Request(Identifier eventId) {
//            this.eventId = eventId;
//        }
//        
//        public Request() {
//            this(UUIDIdentifier.randomId());
//        }
//        
//        public Response success(Torrent torrent) {
//            return new Response(eventId, torrent);
//        }
//        
//        @Override
//        public Identifier getId() {
//            return eventId;
//        }
//        
//        @Override
//        public String toString() {
//            return "TorrentGet.Request<" + getId() + ">";
//        }
//    }
//    
//    public static class Response implements StreamEvent {
//        public final Identifier eventId;
//        public final Torrent torrent;
//        
//        Response(Identifier eventId, Torrent torrent) {
//            this.eventId = eventId;
//            this.torrent = torrent;
//        }
//        
//        @Override
//        public Identifier getId() {
//            return eventId;
//        }
//        
//        @Override
//        public String toString() {
//            return "TorrentGet.Response<" + getId() + ">";
//        }
//    }
//}
