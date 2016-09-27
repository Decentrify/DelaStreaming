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
package se.sics.nstream.torrent.transfer.msg;

import java.util.Map;
import java.util.Set;
import se.sics.ktoolbox.util.identifiable.BasicIdentifiers;
import se.sics.ktoolbox.util.identifiable.Identifier;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayId;
import se.sics.nstream.torrent.FileIdentifier;
import se.sics.nstream.torrent.util.TorrentConnId;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class DownloadHash {
    public static class Request implements ConnectionMsg {
        public final Identifier msgId;
        public final FileIdentifier fileId;
        public final Set<Integer> hashes;
        
        protected Request(Identifier msgId, FileIdentifier fileId, Set<Integer> hashes) {
            this.msgId = msgId;
            this.fileId = fileId;
            this.hashes = hashes;
        }

        public Request(FileIdentifier fileId, Set<Integer> hashes) {
            this(BasicIdentifiers.eventId(), fileId, hashes);
        }
        
        @Override
        public Identifier getId() {
            return msgId;
        }

        @Override
        public OverlayId overlayId() {
            return fileId.overlayId;
        }
        
        @Override
        public TorrentConnId getConnectionId(Identifier target) {
            return new TorrentConnId(target, fileId, false);
        }
        
        public Response success(Map<Integer, byte[]> hashValues) {
            return new Response(this, hashValues);
        }
    }
    
    public static class Response implements ConnectionMsg {
        public final Identifier msgId;
        public final FileIdentifier fileId;
        public final Map<Integer, byte[]> hashValues;
        
        protected Response(Identifier msgId, FileIdentifier fileId, Map<Integer, byte[]> hashValues) {
            this.msgId = msgId;
            this.fileId = fileId;
            this.hashValues = hashValues;
        }
        
        private Response(Request req, Map<Integer, byte[]> hashValues) {
            this(req.msgId, req.fileId, hashValues);
        }
        
        @Override
        public Identifier getId() {
            return msgId;
        }
        
        @Override
        public OverlayId overlayId() {
            return fileId.overlayId;
        }

        @Override
        public TorrentConnId getConnectionId(Identifier target) {
            return new TorrentConnId(target, fileId, true);
        }
    }
}
