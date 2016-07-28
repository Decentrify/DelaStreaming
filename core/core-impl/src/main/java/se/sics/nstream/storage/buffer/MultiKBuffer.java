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
package se.sics.nstream.storage.buffer;

import se.sics.nstream.util.result.WriteCallback;
import java.util.List;
import se.sics.ktoolbox.util.reference.KReference;
import se.sics.nstream.util.range.KBlock;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class MultiKBuffer implements KBuffer {

    private final List<KBuffer> buffers;

    public MultiKBuffer(List<KBuffer> buffers) {
        this.buffers = buffers;
    }
    
    @Override 
    public void start() {
        for (KBuffer buffer : buffers) {
            buffer.start();
        }
    }
    
    @Override
    public boolean isIdle() {
        boolean isEmpty = true;
        for (KBuffer buffer : buffers) {
            isEmpty = isEmpty && buffer.isIdle();
        }
        return isEmpty;
    }

    @Override
    public void close() {
        for (KBuffer buffer : buffers) {
            buffer.close();
        }
    }

    @Override
    public void write(KBlock writeRange, KReference<byte[]> val, WriteCallback delayedResult) {
        for (KBuffer buffer : buffers) {
            buffer.write(writeRange, val, delayedResult);
        }
    }
}
