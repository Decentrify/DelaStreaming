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
 * GNU General Public License for more defLastBlock.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
 */
package se.sics.nstream.transfer;

import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;
import org.javatuples.Pair;
import se.sics.nstream.FileId;
import se.sics.nstream.storage.durable.util.FileExtendedDetails;
import se.sics.nstream.util.BlockDetails;
import se.sics.nstream.util.BlockHelper;
import se.sics.nstream.util.FileBaseDetails;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class MyTorrent {

    private static final int PIECE_SIZE = 1024;
    private static final int BLOCK_SIZE = 1024;
    public static final BlockDetails defaultDefBlock = new BlockDetails(BLOCK_SIZE * PIECE_SIZE, BLOCK_SIZE, PIECE_SIZE, PIECE_SIZE);
    public static final BlockDetails defaultDataBlock = new BlockDetails(BLOCK_SIZE * PIECE_SIZE, BLOCK_SIZE, PIECE_SIZE, PIECE_SIZE);

    
    public final Manifest manifest;
    public final Map<String, FileId> nameToId;
    public final Map<FileId, FileBaseDetails> base;
    public final Map<FileId, FileExtendedDetails> extended;

    public MyTorrent(Manifest manifest, Map<String, FileId> nameToId, Map<FileId, FileBaseDetails> base, Map<FileId, FileExtendedDetails> extended) {
        this.manifest = manifest;
        this.nameToId = nameToId;
        this.base = base;
        this.extended = extended;
    }
    
    public static Manifest buildDefinition(byte[] manifestBytes) {
        Pair<Integer, BlockDetails> details = BlockHelper.getFileDetails(manifestBytes.length, defaultDefBlock);
        int lastBlockNr = details.getValue0() - 1; //nr manifestBlocks -1
        BlockDetails lastBlockDetails = details.getValue1();

        Map<Integer, byte[]> manifestBlocks = new TreeMap<>();
        for (int i = 0; i < lastBlockNr; i++) {
            byte[] blockBytes = new byte[defaultDefBlock.blockSize];
            System.arraycopy(manifestBytes, i * defaultDefBlock.blockSize, blockBytes, 0, defaultDefBlock.blockSize);
            manifestBlocks.put(i, blockBytes);
        }
        byte[] lastBlockBytes = new byte[lastBlockDetails.blockSize];
        System.arraycopy(manifestBytes, lastBlockNr * defaultDefBlock.blockSize, lastBlockBytes, 0, lastBlockDetails.blockSize);
        manifestBlocks.put(lastBlockNr, lastBlockBytes);

        return new Manifest(details.getValue0(), defaultDefBlock, details.getValue1(), manifestBlocks, manifestBytes);
    }

    public static class ManifestDef {

        public final int nrBlocks;
        public final BlockDetails lastBlock;

        public ManifestDef(int nrBlocks, BlockDetails lastBlock) {
            this.nrBlocks = nrBlocks;
            this.lastBlock = lastBlock;
        }
    }

    public static class Manifest {

        public final int nrBlocks;
        public final BlockDetails defaultBlock;
        public final BlockDetails lastBlock;
        public final Map<Integer, byte[]> manifestBlocks;
        public final byte[] manifestByte;

        private Manifest(int nrBlocks, BlockDetails defaultBlock, BlockDetails lastBlock, Map<Integer, byte[]> manifestBlocks, byte[] manifestByte) {
            this.nrBlocks = nrBlocks;
            this.defaultBlock = defaultBlock;
            this.lastBlock = lastBlock;
            this.manifestBlocks = manifestBlocks;
            this.manifestByte = manifestByte;
        }
        
        public ManifestDef getDef() {
            return new ManifestDef(nrBlocks, lastBlock);
        }
    }

    public static class ManifestBuilder {

        public final int nrBlocks;
        public final BlockDetails defaultBlock;
        public final BlockDetails lastBlock;
        private final Map<Integer, byte[]> manifestBlocks = new TreeMap<>();

        public ManifestBuilder(int nrBlocks, BlockDetails lastBlock) {
            this.nrBlocks = nrBlocks;
            this.lastBlock = lastBlock;
            this.defaultBlock = MyTorrent.defaultDefBlock;
        }

        public void addBlocks(Map<Integer, byte[]> newBlocks) {
            manifestBlocks.putAll(newBlocks);
        }

        public boolean blocksComplete() {
            return manifestBlocks.size() == nrBlocks;
        }

        public byte[] assembleManifest() {
            int size = 0;
            for (byte[] val : manifestBlocks.values()) {
                size += val.length;
            }
            byte[] torrentBytes = new byte[size];
            Iterator<Map.Entry<Integer, byte[]>> it = manifestBlocks.entrySet().iterator();
            int pos = 0;
            while (it.hasNext()) {
                byte[] next = it.next().getValue();
                System.arraycopy(next, 0, torrentBytes, pos, next.length);
                pos += next.length;
            }
            return torrentBytes;
        }
        
        public Manifest build() {
            return new Manifest(nrBlocks, defaultBlock, lastBlock, manifestBlocks, assembleManifest());
        }
    }

    public static class Builder {

        public final ManifestBuilder manifestBuilder;
        private Map<String, FileId> nameToId;
        private Map<FileId, FileBaseDetails> base;
        private Map<FileId, FileExtendedDetails> extended;

        public Builder(int nrBlocks, BlockDetails lastBlock) {
            this.manifestBuilder = new ManifestBuilder(nrBlocks, lastBlock);
        }
        
        public Builder(ManifestDef manifestDef) {
            this(manifestDef.nrBlocks, manifestDef.lastBlock);
        }

        public void setBase(Map<String, FileId> nameToId, Map<FileId, FileBaseDetails> base) {
            this.nameToId = nameToId;
            this.base = base;
        }
        
        public Map<String, FileId> getNameToId() {
            return nameToId;
        }

        public void setExtended(Map<FileId, FileExtendedDetails> extended) {
            this.extended = extended;
        }

        public MyTorrent build() {
            return new MyTorrent(manifestBuilder.build(), nameToId, base, extended);
        }
    }
}
