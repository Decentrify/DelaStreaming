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
package se.sics.gvod.common.event.vod;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import se.sics.gvod.common.event.GVoDEvent;
import se.sics.gvod.common.event.ReqStatus;
import se.sics.ktoolbox.util.identifiable.Identifier;
import se.sics.ktoolbox.util.identifiable.basic.UUIDIdentifier;

/**
 * @author Alex Ormenisan <aaor@sics.se>
 */
public class Download {

    public static class DataRequest implements GVoDEvent {

        public final Identifier id;
        public final Identifier overlayId;
        public final int pieceId;

        public DataRequest(Identifier id, Identifier overlayId, int pieceId) {
            this.id = id;
            this.overlayId = overlayId;
            this.pieceId = pieceId;
        }
        
        public DataRequest(Identifier overlayId, int pieceId) {
            this(UUIDIdentifier.randomId(), overlayId, pieceId);
        }

        @Override
        public Identifier getId() {
            return id;
        }
        
        @Override
        public String toString() {
            return "Download.DataRequest<" + id + ">";
        }

        public DataResponse success(ByteBuffer piece) {
            return new DataResponse(this, ReqStatus.SUCCESS, piece);
        }

        public DataResponse missingPiece() {
            return new DataResponse(this, ReqStatus.MISSING, null);
        }

        public DataResponse timeout() {
            return new DataResponse(this, ReqStatus.TIMEOUT, null);
        }

        public DataResponse busy() {
            return new DataResponse(this, ReqStatus.BUSY, null);
        }

        @Override
        public int hashCode() {
            int hash = 5;
            hash = 37 * hash + Objects.hashCode(this.id);
            hash = 37 * hash + Objects.hashCode(this.overlayId);
            hash = 37 * hash + this.pieceId;
            return hash;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            final DataRequest other = (DataRequest) obj;
            if (!Objects.equals(this.id, other.id)) {
                return false;
            }
            if (!Objects.equals(this.overlayId, other.overlayId)) {
                return false;
            }
            if (this.pieceId != other.pieceId) {
                return false;
            }
            return true;
        }
    }

    public static class DataResponse implements GVoDEvent {

        public final Identifier id;
        public final Identifier overlayId;
        public final ReqStatus status;
        public final int pieceId;
        public final ByteBuffer piece;

        public DataResponse(Identifier id, Identifier overlayId, ReqStatus status, int pieceId, ByteBuffer piece) {
            this.id = id;
            this.overlayId = overlayId;
            this.status = status;
            this.pieceId = pieceId;
            this.piece = piece;
        }
        
        public DataResponse(DataRequest req, ReqStatus status, ByteBuffer piece) {
            this(req.id, req.overlayId, status, req.pieceId, piece);
        }
        
        @Override
        public Identifier getId() {
            return id;
        }

        @Override
        public String toString() {
            return "Download.DataResponse<" + id + ">";
        }

        @Override
        public int hashCode() {
            int hash = 5;
            hash = 53 * hash + Objects.hashCode(this.id);
            hash = 53 * hash + Objects.hashCode(this.overlayId);
            hash = 53 * hash + Objects.hashCode(this.status);
            hash = 53 * hash + this.pieceId;
            hash = 53 * hash + Objects.hashCode(this.piece);
            return hash;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            final DataResponse other = (DataResponse) obj;
            if (!Objects.equals(this.id, other.id)) {
                return false;
            }
            if (!Objects.equals(this.overlayId, other.overlayId)) {
                return false;
            }
            if (this.status != other.status) {
                return false;
            }
            if (this.pieceId != other.pieceId) {
                return false;
            }
            if (!Objects.equals(this.piece, other.piece)) {
                return false;
            }
            return true;
        }
    }

    public static class HashRequest implements GVoDEvent {

        public final Identifier id;
        public final Identifier overlayId;
        public final int targetPos;
        public final Set<Integer> hashes;

        public HashRequest(Identifier id, Identifier overlayId, int targetPos, Set<Integer> hashes) {
            this.id = id;
            this.targetPos = targetPos;
            this.hashes = hashes;
            this.overlayId = overlayId;
        }
        public HashRequest(Identifier overlayId, int targetPos, Set<Integer> hashes) {
            this(UUIDIdentifier.randomId(), overlayId, targetPos, hashes);
            
        }
        
         @Override
        public Identifier getId() {
            return id;
        }

        @Override
        public String toString() {
            return "Download.HashRequest<" + id + ">";
        }

        public HashResponse success(Map<Integer, ByteBuffer> hashes, Set<Integer> missingHashes) {
            return new HashResponse(this, ReqStatus.SUCCESS, hashes, missingHashes);
        }

        public HashResponse timeout() {
            return new HashResponse(this, ReqStatus.TIMEOUT, new HashMap<Integer, ByteBuffer>(), hashes);
        }

        public HashResponse busy() {
            return new HashResponse(this, ReqStatus.BUSY, new HashMap<Integer, ByteBuffer>(), hashes);
        }

        @Override
        public int hashCode() {
            int hash = 3;
            hash = 53 * hash + Objects.hashCode(this.id);
            hash = 53 * hash + Objects.hashCode(this.overlayId);
            hash = 53 * hash + this.targetPos;
            hash = 53 * hash + Objects.hashCode(this.hashes);
            return hash;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            final HashRequest other = (HashRequest) obj;
            if (!Objects.equals(this.id, other.id)) {
                return false;
            }
            if (!Objects.equals(this.overlayId, other.overlayId)) {
                return false;
            }
            if (this.targetPos != other.targetPos) {
                return false;
            }
            if (!Objects.equals(this.hashes, other.hashes)) {
                return false;
            }
            return true;
        }
    }

    public static class HashResponse implements GVoDEvent {
        
        public final Identifier id;
        public final Identifier overlayId;
        public final ReqStatus status;
        public final int targetPos;
        public final Map<Integer, ByteBuffer> hashes;
        public final Set<Integer> missingHashes;

        public HashResponse(Identifier id, Identifier overlayId, ReqStatus status, int targetPos, Map<Integer, ByteBuffer> hashes, Set<Integer> missingHashes) {
            this.id = id;
            this.overlayId = overlayId;
            this.targetPos = targetPos;
            this.status = status;
            this.hashes = hashes;
            this.missingHashes = missingHashes;
        }
        public HashResponse(HashRequest req, ReqStatus status, Map<Integer, ByteBuffer> hashes, Set<Integer> missingHashes) {
           this(req.id, req.overlayId, status, req.targetPos, hashes, missingHashes);
        }

        @Override
        public Identifier getId() {
            return id;
        }
        
        @Override
        public String toString() {
            return "Download.HashResponse<" + id + ">";
        }

        @Override
        public int hashCode() {
            int hash = 7;
            hash = 97 * hash + Objects.hashCode(this.id);
            hash = 97 * hash + Objects.hashCode(this.overlayId);
            hash = 97 * hash + Objects.hashCode(this.status);
            hash = 97 * hash + this.targetPos;
            hash = 97 * hash + Objects.hashCode(this.hashes);
            hash = 97 * hash + Objects.hashCode(this.missingHashes);
            return hash;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            final HashResponse other = (HashResponse) obj;
            if (!Objects.equals(this.id, other.id)) {
                return false;
            }
            if (!Objects.equals(this.overlayId, other.overlayId)) {
                return false;
            }
            if (this.status != other.status) {
                return false;
            }
            if (this.targetPos != other.targetPos) {
                return false;
            }
            if (!Objects.equals(this.hashes, other.hashes)) {
                return false;
            }
            if (!Objects.equals(this.missingHashes, other.missingHashes)) {
                return false;
            }
            return true;
        }
    }
}
