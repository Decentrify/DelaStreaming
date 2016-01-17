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
//package se.sics.gvod.common.util;
//
//import java.util.HashMap;
//import se.sics.kompics.KompicsEvent;
//import se.sics.p2ptoolbox.util.network.impl.DecoratedAddress;
//
///**
// * @author Alex Ormenisan <aaor@sics.se>
// */
//public class MsgProcessor {
//
//    private final HashMap<Class<? extends KompicsEvent>, Handler<? extends KompicsEvent>> subscribedHandlers;
//
//    public MsgProcessor() {
//        this.subscribedHandlers = new HashMap<Class<? extends KompicsEvent>, Handler<? extends KompicsEvent>>();
//    }
//
//    public <E extends KompicsEvent> void subscribe(Handler<E> handler) {
//        if (handler.eventType == null) {
//            System.exit(1);
//            throw new RuntimeException("Handler did not initialize handlerType");
//        }
//        subscribedHandlers.put(handler.eventType, handler);
//    }
//
//    public <E extends KompicsEvent> void trigger(DecoratedAddress src, E event) {
//        Class<? extends KompicsEvent> eventType = event.getClass();
//        Handler<?> handler = null;
//
//        for (Class<? extends KompicsEvent> eType : subscribedHandlers.keySet()) {
//            if (eType.isAssignableFrom(eventType)) {
//                handler = subscribedHandlers.get(eType);
//            }
//        }
//
//        if (handler != null) {
//            ((Handler<KompicsEvent>) handler).handle(src, event);
//        } else {
//            //silently drop message
//        }
//    }
//
//    public static abstract class Handler<E extends KompicsEvent> {
//
//        public final Class<E> eventType;
//
//        public Handler(Class<E> eventType) {
//            this.eventType = eventType;
//        }
//
//        public abstract void handle(DecoratedAddress src, E event);
//    }
//}
