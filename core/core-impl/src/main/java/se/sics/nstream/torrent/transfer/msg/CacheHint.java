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

import se.sics.kompics.util.Identifier;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayId;
import se.sics.nstream.ConnId;
import se.sics.nstream.FileId;
import se.sics.nstream.TorrentIds;
import se.sics.nstream.storage.cache.KHint;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class CacheHint {

  public static class Request implements ConnectionMsg {

    public final Identifier msgId;
    public final FileId fileId;
    public final KHint.Summary requestCache;

    public Request(Identifier msgId, FileId fileId, KHint.Summary requestCache) {
      this.msgId = msgId;
      this.fileId = fileId;
      this.requestCache = requestCache;
    }

    @Override
    public Identifier getId() {
      return msgId;
    }

    @Override
    public OverlayId overlayId() {
      return fileId.torrentId;
    }

    @Override
    public ConnId getConnectionId(Identifier peer) {
      return TorrentIds.connId(fileId, peer, false);
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
    public final FileId fileId;

    protected Response(Identifier msgId, FileId fileId) {
      this.msgId = msgId;
      this.fileId = fileId;
    }

    private Response(Request req) {
      this(req.msgId, req.fileId);
    }

    @Override
    public OverlayId overlayId() {
      return fileId.torrentId;
    }

    @Override
    public Identifier getId() {
      return msgId;
    }

    @Override
    public ConnId getConnectionId(Identifier peer) {
      return TorrentIds.connId(fileId, peer, true);
    }

    @Override
    public String toString() {
      return "CHResp<" + fileId.toString() + "," + msgId.toString() + ">";
    }
  }
}
