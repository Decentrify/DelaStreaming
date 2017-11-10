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

package se.sics.nstream.transfer;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.util.Set;
import se.sics.kompics.util.Identifier;

/**
 * @author Alex Ormenisan <aaor@sics.se>
 */
public class RWByteBuffer {
    private final ByteBuf buf;
    private final int length;
    
    public RWByteBuffer(int bufLength) {
        this.buf = Unpooled.wrappedBuffer(new byte[bufLength]);
        this.length = bufLength;
    }
    
    public byte[] read(Identifier readerId, long readPos, int readLength, Set<Integer> bufferBlocks) {
        if(readPos > Integer.MAX_VALUE) {
            throw new RuntimeException("In memory buffer only allow integer sizes");
        }
        if(readPos > length) {
            return null;
        }
        if(readPos + readLength > length) {
            throw new RuntimeException("not tested yet");
        }
        return read((int)readPos, (int)readLength);
    }
    
    private byte[] read(int readPos, int readLength) {
        byte[] result = new byte[readLength];
        buf.readerIndex(readPos);
        buf.readBytes(result);
        return result;
    }

    public int write(long writePos, byte[] bytes) {
        if(writePos > Integer.MAX_VALUE) {
            throw new RuntimeException("In memory buffer only allow integer sizes");
        }
        if(writePos > length) {
            return 0;
        }
        return write((int)writePos, bytes);
    }
    
    private int write(int writePos, byte[] bytes) {
        int auxWriterIndex = buf.writerIndex();
        buf.writerIndex(writePos);
        int rest = length - writePos;
        int writeBytes = (bytes.length < rest ? bytes.length : rest);
        buf.writeBytes(bytes, 0, writeBytes);
        int auxWriterIndex2 = buf.writerIndex();
        if(auxWriterIndex2 < auxWriterIndex) {
            buf.writerIndex(auxWriterIndex);
        }
        return writeBytes;
    }

    public long length() {
        return length;
    }
}
