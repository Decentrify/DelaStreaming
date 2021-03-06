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
package se.sics.nstream.torrent.fileMngr;

import com.google.common.base.Optional;
import java.util.Map;
import java.util.Set;
import org.javatuples.Pair;
import se.sics.ktoolbox.util.reference.KReference;
import se.sics.nstream.util.BlockDetails;

/**
 *
 * @author Alex Ormenisan <aaor@kth.se>
 */
public interface TFileWrite extends TFileMngr {

    public boolean isComplete();

    public TFileComplete complete();

    public boolean hasHashes();

    public boolean hasBlocks();

    public Set<Integer> requestHashes();

    public void hashes(Map<Integer, byte[]> hashes, Set<Integer> missingHashes);

    public Pair<Integer, Optional<BlockDetails>> requestBlock();

    public void block(final int blockNr, final KReference<byte[]> block);
    
    public void resetBlock(int blockNr);
}
