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

import se.sics.ktoolbox.util.identifiable.BasicIdentifiers;
import se.sics.ktoolbox.util.identifiable.Identifier;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayId;
import se.sics.nstream.storage.cache.KHint;
import se.sics.nstream.torrent.FileIdentifier;
import se.sics.nstream.torrent.util.TorrentConnId;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class CacheHint {

    public static class Request implements ConnectionMsg {

        public final Identifier msgId;
        public final FileIdentifier fileId;
        public final KHint.Summary requestCache;

        protected Request(Identifier msgId, FileIdentifier fileId, KHint.Summary requestCache) {
            this.msgId = msgId;
            this.fileId = fileId;
            this.requestCache = requestCache;
        }

        public Request(FileIdentifier fileId, KHint.Summary requestCache) {
            this(BasicIdentifiers.msgId(), fileId, requestCache);
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

        public Response success() {
            return new Response(this);
        }

        @Override
        public String toString() {
            return "CHReq<" + fileId.toString() + ",ts:" + requestCache.lStamp + "," + msgId.toString() + ">";
        }
    }

    public static class Response implements ConnectionMsg {

        public final Identifier msgId;
        public final FileIdentifier fileId;

        protected Response(Identifier msgId, FileIdentifier fileId) {
            this.msgId = msgId;
            this.fileId = fileId;
        }

        private Response(Request req) {
            this(req.msgId, req.fileId);
        }

        @Override
        public OverlayId overlayId() {
            return fileId.overlayId;
        }

        @Override
        public Identifier getId() {
            return msgId;
        }

        @Override
        public TorrentConnId getConnectionId(Identifier target) {
            return new TorrentConnId(target, fileId, true);
        }
        
         @Override
        public String toString() {
            return "CHResp<" + fileId.toString() + "," + msgId.toString() + ">";
        }
    }
}
