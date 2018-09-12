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
package se.sics.dela.storage.op;

import se.sics.dela.storage.buffer.KBuffer;
import se.sics.dela.storage.cache.KCache;
import se.sics.dela.storage.cache.KHint;
import se.sics.dela.storage.cache.ReadCallback;
import se.sics.dela.storage.buffer.WriteCallback;
import se.sics.kompics.util.Identifier;
import se.sics.ktoolbox.util.reference.KReference;
import se.sics.nstream.util.range.KBlock;
import se.sics.nstream.util.range.KRange;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class AsyncIncompleteStorage implements AsyncStorage {

    private final KCache cache;
    private final KBuffer buffer;

    public AsyncIncompleteStorage(KCache cache, KBuffer buffer) {
        this.cache = cache;
        this.buffer = buffer;
    }

    @Override
    public void start() {
        cache.start();
        buffer.start();
    }

    @Override
    public boolean isIdle() {
        return cache.isIdle() && buffer.isIdle();
    }

    @Override
    public void close() {
        buffer.close();
        cache.close();
    }
    
    public AsyncCompleteStorage complete() {
        buffer.close();
        return new AsyncCompleteStorage(cache);
    }

    //**************************************************************************
    @Override
    public void clean(Identifier reader) {
        cache.clean(reader);
    }

    @Override
    public void setFutureReads(Identifier reader, KHint.Expanded hint) {
        cache.setFutureReads(reader, hint);
    }

    //**************************************************************************
    @Override
    public void read(KRange readRange, ReadCallback readResult) {
        cache.read(readRange, readResult);
    }

    @Override
    public void write(KBlock writeRange, KReference<byte[]> val, WriteCallback writeResult) {
        cache.buffered(writeRange, val);
        buffer.write(writeRange, val, writeResult);
    }
    
    public KStorageReport report() {
        KStorageReport report = new KStorageReport(buffer.report(), cache.report());
        return report;
    }
    
    public boolean pendingBlocks() {
        return buffer.isIdle();
    }
}
