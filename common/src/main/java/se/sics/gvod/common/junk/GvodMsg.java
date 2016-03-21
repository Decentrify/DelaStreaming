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
//package se.sics.gvod.common.event;
//
//import java.util.UUID;
//import se.sics.kompics.KompicsEvent;
//
///**
// * @author Alex Ormenisan <aaor@sics.se>
// */
//public class GvodMsg {
//    
//    public static abstract class Base implements KompicsEvent, Content {
//        public final UUID id;
//        
//        public Base(UUID id) {
//            this.id = id;
//        }
//    }
//
//    public static abstract class Request extends Base {
//
//        public Request(UUID id) {
//            super(id);
//        }
//        
//        public abstract <E extends Request> E copy();
//    }
//
//    public static abstract class Response extends Base {
//
//        public final ReqStatus status;
//
//        public Response(UUID id, ReqStatus status) {
//            super(id);
//            this.status = status;
//        }
//        
//        public abstract <E extends Response> E copy();
//    }
//
//    public static abstract class OneWay extends Base {
//
//        public OneWay(UUID id) {
//            super(id);
//        }
//        
//        public abstract <E extends OneWay> E copy();
//    }
//}
