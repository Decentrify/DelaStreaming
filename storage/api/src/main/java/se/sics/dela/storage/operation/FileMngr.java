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
package se.sics.dela.storage.operation;

import java.util.Set;
import java.util.function.Consumer;
import se.sics.dela.storage.cache.CacheHint;
import se.sics.ktoolbox.util.reference.KReference;
import se.sics.ktoolbox.util.trysf.Try;
import se.sics.nstream.util.range.KBlock;
import se.sics.nstream.util.range.KRange;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class FileMngr {
    public static interface Reader extends CacheHint.Read, AsyncReadOp<KRange> {
        public boolean hasBlock(int blockNr);
        public boolean hasHash(int blockNr);
        public Set<Integer> nextBlocksMissing(int fromBlock, int nrBlocks, Set<Integer> except);
        public Set<Integer> nextHashesMissing(int fromBlock, int nrBlocks, Set<Integer> except);
        public void readHash(KBlock readRange, Consumer<Try<KReference<byte[]>>> callback);
    }
    
    public static interface Writer {
        public void writeHash(KBlock writeRange, KReference<byte[]> val, Consumer<Try<Boolean>> delayedResult);
        public void writeBlock(KBlock writeRange, KReference<byte[]> val, HashedBlockWriteCallback blockWC);
        public boolean isComplete();
        public int filePos();
        public int hashPos();
        public boolean pendingBlocks();
    }
}
