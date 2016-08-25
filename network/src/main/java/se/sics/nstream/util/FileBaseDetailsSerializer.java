/*
 * Copyright (C) 2009 Swedish Institute of Computer Science (SICS) Copyright (C)
 * 2009 Royal Institute of Technology (KTH)
 *
 * KompicsToolbox is free software; you can redistribute it and/or
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
package se.sics.nstream.util;

import com.google.common.base.Optional;
import io.netty.buffer.ByteBuf;
import se.sics.kompics.network.netty.serialization.Serializer;
import se.sics.kompics.network.netty.serialization.Serializers;
import se.sics.ktoolbox.util.managedStore.core.util.HashUtil;

/**
 *
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class FileBaseDetailsSerializer implements Serializer {

    private final int identifier;

    public FileBaseDetailsSerializer(int identifier) {
        this.identifier = identifier;
    }

    @Override
    public int identifier() {
        return identifier;
    }

    @Override
    public void toBinary(Object o, ByteBuf buf) {
        FileBaseDetails obj = (FileBaseDetails) o;
        BlockDetailsSerializer bdS = (BlockDetailsSerializer) Serializers.lookupSerializer(BlockDetails.class);
        bdS.toBinary(obj.defaultBlock, buf);
        bdS.toBinary(obj.lastBlock, buf);
        buf.writeLong(obj.length);
        buf.writeInt(obj.nrBlocks);
        buf.writeByte(HashUtil.getAlgId(obj.hashAlg));
    }

    @Override
    public Object fromBinary(ByteBuf buf, Optional<Object> hint) {
        BlockDetailsSerializer bdS = (BlockDetailsSerializer) Serializers.lookupSerializer(BlockDetails.class);
        BlockDetails defaultBlock = (BlockDetails)bdS.fromBinary(buf, hint);
        BlockDetails lastBlock = (BlockDetails)bdS.fromBinary(buf, hint);
        long length = buf.readLong();
        int nrBlocks = buf.readInt();
        String hashAlg = HashUtil.getAlgName(buf.readByte());
        return new FileBaseDetails(length, nrBlocks, defaultBlock, lastBlock, hashAlg);
    }
}
