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
//import com.google.common.base.Optional;
//import io.netty.buffer.ByteBuf;
//import org.javatuples.Pair;
//import se.sics.kompics.network.netty.serialization.Serializer;
//import se.sics.kompics.network.netty.serialization.Serializers;
//import se.sics.ktoolbox.util.identifiable.BasicIdentifiers;
//import se.sics.ktoolbox.util.identifiable.Identifier;
//import se.sics.ktoolbox.util.identifiable.IdentifierRegistry;
//import se.sics.ktoolbox.util.identifiable.overlay.OverlayId;
//
///**
// * @author Alex Ormenisan <aaor@kth.se>
// */
//public class TorrentMsgSerializer {
//
//  private static abstract class Base implements Serializer {
//
//    private final int id;
//    private final Class msgIdType;
//
//    protected Base(int id) {
//      this.id = id;
//      this.msgIdType = IdentifierRegistry.lookup(BasicIdentifiers.Values.MSG.toString()).idType();
//    }
//
//    @Override
//    public int identifier() {
//      return id;
//    }
//    
//    @Override
//    public void toBinary(Object o, ByteBuf buf) {
//      TorrentMsg.Base obj = (TorrentMsg.Base) o;
//      Serializers.lookupSerializer(msgIdType).toBinary(obj.msgId, buf);
//      Serializers.lookupSerializer(OverlayId.class).toBinary(obj.torrentId, buf);
//    }
//
//    protected Pair<Identifier, OverlayId> auxFromBinary(ByteBuf buf, Optional<Object> hint) {
//      Identifier msgId = (Identifier) Serializers.lookupSerializer(msgIdType).fromBinary(buf, hint);
//      OverlayId overlayId = (OverlayId) Serializers.lookupSerializer(OverlayId.class).fromBinary(buf, hint);
//      return Pair.with(msgId, overlayId);
//    }
//  }
//
//  public static class Connect extends Base {
//
//    public Connect(int id) {
//      super(id);
//    }
//    
//    @Override
//    public Object fromBinary(ByteBuf buf, Optional<Object> hint) {
//      Pair<Identifier, OverlayId> aux = auxFromBinary(buf, hint);
//      return new TorrentMsg.Connect(aux.getValue0(), aux.getValue1());
//    }
//  }
//  
//  public static class Connected extends Base {
//
//    public Connected(int id) {
//      super(id);
//    }
//    
//    @Override
//    public Object fromBinary(ByteBuf buf, Optional<Object> hint) {
//      Pair<Identifier, OverlayId> aux = auxFromBinary(buf, hint);
//      return new TorrentMsg.Connected(aux.getValue0(), aux.getValue1());
//    }
//  }
//  
//  public static class Choke extends Base {
//
//    public Choke(int id) {
//      super(id);
//    }
//    
//    @Override
//    public Object fromBinary(ByteBuf buf, Optional<Object> hint) {
//      Pair<Identifier, OverlayId> aux = auxFromBinary(buf, hint);
//      return new TorrentMsg.Choke(aux.getValue0(), aux.getValue1());
//    }
//  }
//  
//  public static class Unchoke extends Base {
//
//    public Unchoke(int id) {
//      super(id);
//    }
//    
//    @Override
//    public Object fromBinary(ByteBuf buf, Optional<Object> hint) {
//      Pair<Identifier, OverlayId> aux = auxFromBinary(buf, hint);
//      return new TorrentMsg.Unchoke(aux.getValue0(), aux.getValue1());
//    }
//  }
//  
//  public static class Disconnect extends Base {
//
//    public Disconnect(int id) {
//      super(id);
//    }
//    
//    @Override
//    public Object fromBinary(ByteBuf buf, Optional<Object> hint) {
//      Pair<Identifier, OverlayId> aux = auxFromBinary(buf, hint);
//      return new TorrentMsg.Disconnect(aux.getValue0(), aux.getValue1());
//    }
//  }
//}
