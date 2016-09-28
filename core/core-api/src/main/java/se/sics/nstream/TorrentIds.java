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
package se.sics.nstream;

import com.google.common.primitives.Ints;
import java.util.Random;
import se.sics.ktoolbox.util.identifiable.BasicIdentifiers;
import se.sics.ktoolbox.util.identifiable.Identifier;
import se.sics.ktoolbox.util.identifiable.IdentifierRegistry;
import se.sics.ktoolbox.util.identifiable.basic.IntIdFactory;
import se.sics.ktoolbox.util.identifiable.basic.StringByteIdFactory;
import se.sics.ktoolbox.util.identifiable.basic.UUIDIdFactory;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayId;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class TorrentIds {

    public static enum Types implements OverlayId.Type {

        TORRENT
    }

    public static class TypeFactory implements OverlayId.TypeFactory {

        private final byte torrent;

        public TypeFactory(byte startWith) {
            assert startWith < 255;
            this.torrent = (byte) (startWith + 1);
        }

        @Override
        public OverlayId.Type fromByte(byte byteType) throws OverlayId.UnknownTypeException {
            if (byteType == torrent) {
                return Types.TORRENT;
            } else {
                throw new OverlayId.UnknownTypeException(byteType);
            }
        }

        @Override
        public byte toByte(OverlayId.Type type) throws OverlayId.UnknownTypeException {
            if (type instanceof Types) {
                Types torrentType = (Types) type;
                switch (torrentType) {
                    case TORRENT:
                        return torrent;
                    default:
                        throw new OverlayId.UnknownTypeException(type);
                }
            } else {
                throw new OverlayId.UnknownTypeException(type);
            }
        }

        @Override
        public byte lastUsed() {
            return torrent;
        }
    }

    public static class TypeComparator implements OverlayId.TypeComparator<Types> {

        @Override
        public int compare(Types o1, Types o2) {
            int result = Ints.compare(o1.ordinal(), o2.ordinal());
            return result;
        }
    }

    public static void registerDefaults(long seed) {
        Random rand = new Random(seed);
        IdentifierRegistry.register(BasicIdentifiers.Values.EVENT.toString(), new UUIDIdFactory());
        IdentifierRegistry.register(BasicIdentifiers.Values.MSG.toString(), new UUIDIdFactory());
        IdentifierRegistry.register(BasicIdentifiers.Values.OVERLAY.toString(), new StringByteIdFactory(rand, 64));
        IdentifierRegistry.register(BasicIdentifiers.Values.NODE.toString(), new IntIdFactory(rand));
    }

    public static boolean checkBasicIdentifiers() {
        if (IdentifierRegistry.lookup(BasicIdentifiers.Values.EVENT.toString()) == null) {
            return false;
        }
        if (IdentifierRegistry.lookup(BasicIdentifiers.Values.MSG.toString()) == null) {
            return false;
        }
        if (IdentifierRegistry.lookup(BasicIdentifiers.Values.OVERLAY.toString()) == null) {
            return false;
        }
        if (IdentifierRegistry.lookup(BasicIdentifiers.Values.NODE.toString()) == null) {
            return false;
        }
        return true;
    }

    public static FileId fileId(OverlayId torrentId, int fileNr) {
        return new FileId(torrentId, fileNr);
    }

    public static ConnId connId(FileId fileId, Identifier peerId, boolean leecher) {
        return new ConnId(fileId, peerId, leecher);
    }

    public static StreamId streamId(Identifier endpointId, FileId fileId) {
        return new StreamId(endpointId, fileId);
    }
}
