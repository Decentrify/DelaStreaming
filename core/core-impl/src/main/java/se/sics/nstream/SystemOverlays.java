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

import se.sics.ktoolbox.util.identifiable.overlay.OverlayId;
import se.sics.nstream.TorrentIds;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class SystemOverlays {

    public static class TypeFactory implements OverlayId.TypeFactory {

        private final OverlayId.BasicTypeFactory basicTypesFactory;
        private final TorrentIds.TypeFactory torrentTypesFactory;

        public TypeFactory() {
            this.basicTypesFactory = new OverlayId.BasicTypeFactory((byte)0);
            this.torrentTypesFactory = new TorrentIds.TypeFactory(basicTypesFactory.lastUsed());
        }

        @Override
        public OverlayId.Type fromByte(byte byteType) throws OverlayId.UnknownTypeException {
            try {
                return basicTypesFactory.fromByte(byteType);
            } catch (OverlayId.UnknownTypeException ex) {
                //maybe it is a different type
            } 
            try {
                return torrentTypesFactory.fromByte(byteType);
            } catch (OverlayId.UnknownTypeException ex) {
                //maybe it is a different type
            }
            throw new OverlayId.UnknownTypeException(byteType);
        }

        @Override
        public byte toByte(OverlayId.Type type) throws OverlayId.UnknownTypeException {
            try {
                return basicTypesFactory.toByte(type);
            } catch (OverlayId.UnknownTypeException ex) {
                //maybe it is a different type
            }
            try {
                return torrentTypesFactory.toByte(type);
            } catch (OverlayId.UnknownTypeException ex) {
                //maybe it is a different type
            }
            throw new OverlayId.UnknownTypeException(type);
        }

        @Override
        public byte lastUsed() {
            return torrentTypesFactory.lastUsed();
        }
    }
    
    public static class Comparator implements OverlayId.TypeComparator {
        private final OverlayId.BasicTypeComparator basicTypeComp = new OverlayId.BasicTypeComparator();
        private final TorrentIds.TypeComparator torrentTypeComp = new TorrentIds.TypeComparator();
        
        @Override
        public int compare(Object o1, Object o2) {
            if(o1 instanceof OverlayId.BasicTypes) {
                if(o2 instanceof OverlayId.BasicTypes) {
                    return basicTypeComp.compare((OverlayId.BasicTypes)o1, (OverlayId.BasicTypes)o2);
                }
                return -1;
            } else {
//            if(o1 instanceof TorrentIds.Types) {
                if(o2 instanceof OverlayId.BasicTypes) {
                    return 1;
                } else {
//                if(o2 instanceof TorrentIds.Types) {
                    return torrentTypeComp.compare((TorrentIds.Types)o1, (TorrentIds.Types)o2);
                }
            }
        }
    }
}
