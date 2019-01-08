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
package se.sics.dela.network.util;

import com.google.common.base.Optional;
import io.netty.buffer.ByteBuf;
import se.sics.dela.network.ledbat.util.LedbatContainer;
import se.sics.kompics.network.netty.serialization.Serializer;
import se.sics.kompics.network.netty.serialization.Serializers;
import se.sics.kompics.util.Identifier;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class DatumIdSerializer implements Serializer {

  private final int id;

  public DatumIdSerializer(int id) {
    this.id = id;
  }

  @Override
  public int identifier() {
    return id;
  }

  @Override
  public void toBinary(Object o, ByteBuf buf) {
    DatumId obj = (DatumId) o;
    Serializers.toBinary(obj.dataId(), buf);
    Serializers.toBinary(obj.unitId(), buf);
  }

  @Override
  public DatumId fromBinary(ByteBuf buf, Optional<Object> hint) {
    Identifier dataId = (Identifier)Serializers.fromBinary(buf, hint);
    Identifier unitId = (Identifier)Serializers.fromBinary(buf, hint);
    return new DatumId(dataId, unitId);
  }
}