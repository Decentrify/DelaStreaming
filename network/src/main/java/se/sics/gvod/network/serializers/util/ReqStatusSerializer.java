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
package se.sics.gvod.network.serializers.util;

import com.google.common.base.Optional;
import io.netty.buffer.ByteBuf;
import se.sics.gvod.common.event.ReqStatus;
import se.sics.kompics.network.netty.serialization.Serializer;

/**
 * @author Alex Ormenisan <aaor@sics.se>
 */
public class ReqStatusSerializer implements Serializer {

    private final int id;

    public ReqStatusSerializer(int id) {
        this.id = id;
    }

    @Override
    public int identifier() {
        return id;
    }

    @Override
    public void toBinary(Object o, ByteBuf buf) {
        ReqStatus obj = (ReqStatus)o;
        switch (obj) {
            case FAIL:
                buf.writeByte(0x00);
                break;
            case SUCCESS:
                buf.writeByte(0x01);
                break;
            case MISSING:
                buf.writeByte(0x02);
                break;
            case BUSY:
                buf.writeByte(0x03);
                break;
            case TIMEOUT:
                buf.writeByte(0x04);
                break;
            default:
                throw new RuntimeException("no code for encoding status " + obj);
        }
    }

    @Override
    public Object fromBinary(ByteBuf buf, Optional<Object> hint) {
        byte status = buf.readByte();
        switch (status) {
            case 0x00:
                return ReqStatus.FAIL;
            case 0x01:
                return ReqStatus.SUCCESS;
            case 0x02:
                return ReqStatus.MISSING;
            case 0x03:
                return ReqStatus.BUSY;
            case 0x04:
                return ReqStatus.TIMEOUT;
            default:
                throw new RuntimeException("no code for decoding status byte " + status);
        }
    }
}