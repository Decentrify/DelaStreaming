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

import se.sics.ktoolbox.util.reference.KReference;
import se.sics.nstream.util.range.KBlock;
import se.sics.nstream.util.result.WriteCallback;

/**
 *
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class NoKBuffer implements KBuffer {

    public NoKBuffer() {}
    
    @Override
    public void start() {
    }

    @Override
    public boolean isIdle() {
        return true;
    }

    @Override
    public void close() {
    }

    @Override
    public void write(KBlock writeRange, KReference<byte[]> val, WriteCallback delayedWrite) {
        throw new UnsupportedOperationException("Not supported");
    }

    @Override
    public KBufferReport report() {
        return new SimpleKBufferReport(0, 0, 0);
    }
}
