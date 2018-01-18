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
package se.sics.silk.r2torrent.torrent.msg;

import com.google.common.base.Optional;
import io.netty.buffer.ByteBuf;
import org.javatuples.Triplet;
import se.sics.kompics.network.netty.serialization.Serializer;
import se.sics.kompics.network.netty.serialization.Serializers;
import se.sics.kompics.util.Identifiable;
import se.sics.kompics.util.Identifier;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayId;
import se.sics.silk.event.SilkEvent;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class R1MetadataMsgSerializers {

  public static abstract class Base implements Serializer {

    private final int id;

    Base(int id) {
      this.id = id;
    }

    @Override
    public int identifier() {
      return id;
    }

    public void toBin(Object o, ByteBuf buf) {
      Serializers.toBinary(((Identifiable) o).getId(), buf);
      Serializers.toBinary(((SilkEvent.TorrentEvent) o).torrentId(), buf);
      Serializers.toBinary(((SilkEvent.FileEvent) o).fileId(), buf);
    }

    public Triplet<Identifier, OverlayId, Identifier> fromBin(ByteBuf buf, Optional<Object> hint) {
      Identifier msgId = (Identifier) Serializers.fromBinary(buf, hint);
      OverlayId torrentId = (OverlayId) Serializers.fromBinary(buf, hint);
      Identifier fileId = (Identifier) Serializers.fromBinary(buf, hint);
      return Triplet.with(msgId, torrentId, fileId);
    }
  }
  
  public static class Get extends Base {

    public Get(int id) {
      super(id);
    }

    @Override
    public void toBinary(Object o, ByteBuf buf) {
      toBin(o, buf);
    }

    @Override
    public Object fromBinary(ByteBuf buf, Optional<Object> hint) {
      Triplet<Identifier, OverlayId, Identifier> ids = fromBin(buf, hint);
      return new R1MetadataMsgs.Get(ids.getValue0(), ids.getValue1(), ids.getValue2());
    }
  }
  
  public static class Serve extends Base {

    public Serve(int id) {
      super(id);
    }

    @Override
    public void toBinary(Object o, ByteBuf buf) {
      toBin(o, buf);
    }

    @Override
    public Object fromBinary(ByteBuf buf, Optional<Object> hint) {
      Triplet<Identifier, OverlayId, Identifier> ids = fromBin(buf, hint);
      return new R1MetadataMsgs.Serve(ids.getValue0(), ids.getValue1(), ids.getValue2());
    }
  }
}
