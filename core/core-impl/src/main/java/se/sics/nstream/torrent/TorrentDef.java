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
package se.sics.nstream.torrent;

import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;
import org.javatuples.Pair;
import se.sics.nstream.transfer.BlockHelper;
import se.sics.nstream.util.BlockDetails;
import se.sics.nstream.util.FileBaseDetails;
import se.sics.nstream.util.FileExtendedDetails;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class TorrentDef {

    public final Pair<Integer, BlockDetails> details;
    public final Map<Integer, byte[]> blocks;
    public final Map<String, FileBaseDetails> base;
    public final Map<String, FileExtendedDetails> extended;

    private TorrentDef(Pair<Integer, BlockDetails> details, Map<Integer, byte[]> blocks, Map<String, FileBaseDetails> base, Map<String, FileExtendedDetails> extended) {
        this.details = details;
        this.blocks = blocks;
        this.base = base;
        this.extended = extended;
    }

    public static TorrentDef buildDefinition(Map<String, FileBaseDetails> base, Map<String, FileExtendedDetails> extended, byte[] torrentBytes, BlockDetails defaultBlockDetails) {
        Pair<Integer, BlockDetails> details = BlockHelper.getFileDetails(torrentBytes.length, defaultBlockDetails);
        int lastBlockNr = details.getValue0()-1; //nr blocks -1
        BlockDetails lastBlockDetails = details.getValue1();

        Map<Integer, byte[]> blocks = new TreeMap<>();
        for (int i = 0; i < lastBlockNr; i++) {
            byte[] blockBytes = new byte[defaultBlockDetails.blockSize];
            System.arraycopy(torrentBytes, i * defaultBlockDetails.blockSize, blockBytes, 0, defaultBlockDetails.blockSize);
            blocks.put(i, blockBytes);
        }
        byte[] lastBlockBytes = new byte[lastBlockDetails.blockSize];
        System.arraycopy(torrentBytes, lastBlockNr * defaultBlockDetails.blockSize, lastBlockBytes, 0, lastBlockDetails.blockSize);
        blocks.put(lastBlockNr, lastBlockBytes);

        return new TorrentDef(details, blocks, base, extended);
    }

    public static class Builder {

        //<nrOfBlocks, lastBlockDetails>
        public final Pair<Integer, BlockDetails> details;
        private final Map<Integer, byte[]> blocks = new TreeMap<>();
        private Map<String, FileBaseDetails> base;
        private Map<String, FileExtendedDetails> extended;

        public Builder(Pair<Integer, BlockDetails> details) {
            this.details = details;
        }

        public void addBlocks(Map<Integer, byte[]> newBlocks) {
            blocks.putAll(newBlocks);
        }

        public boolean blocksComplete() {
            return blocks.size() == details.getValue0();
        }

        public byte[] assembleTorrent() {
            int size = 0;
            for (byte[] val : blocks.values()) {
                size += val.length;
            }
            byte[] torrentBytes = new byte[size];
            Iterator<Map.Entry<Integer, byte[]>> it = blocks.entrySet().iterator();
            int pos = 0;
            while (it.hasNext()) {
                byte[] next = it.next().getValue();
                System.arraycopy(next, 0, torrentBytes, pos, next.length);
                pos += next.length;
            }
            return torrentBytes;
        }

        public void setBase(Map<String, FileBaseDetails> base) {
            this.base = base;
        }

        public void setExtended(Map<String, FileExtendedDetails> extended) {
            this.extended = extended;
        }

        public TorrentDef build() {
            return new TorrentDef(details, blocks, base, extended);
        }
    }
}
