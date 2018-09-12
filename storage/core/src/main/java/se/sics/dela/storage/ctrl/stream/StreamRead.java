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
package se.sics.dela.storage.ctrl.stream;

import java.util.Map;
import java.util.function.Consumer;
import se.sics.dela.storage.cache.KHint;
import se.sics.kompics.util.Identifier;
import se.sics.ktoolbox.util.reference.KReference;
import se.sics.ktoolbox.util.trysf.Try;
import se.sics.nstream.util.BlockDetails;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public interface StreamRead extends StreamCtrl {

    public void clean(Identifier reader);

    public void setCacheHint(Identifier reader, KHint.Summary hint);

    //**************************************************************************
    public boolean hasBlock(int blockNr);
    public boolean hasHash(int blockNr);
    public void readHash(int blockNr, Consumer<Try<KReference<byte[]>>> callback);
    public void readBlock(int blockNr, Consumer<Try<KReference<byte[]>>> callback);
    public Map<Integer, BlockDetails> getIrregularBlocks();
}