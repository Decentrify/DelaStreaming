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
import se.sics.kompics.util.Identifier;
import se.sics.ktoolbox.util.identifiable.BasicIdentifiers;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayId;
import se.sics.nstream.ConnId;
import se.sics.nstream.FileId;
import se.sics.nstream.TorrentIds;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class DownloadHash {

  public static class Request implements ConnectionMsg {

    public final Identifier msgId;
    public final FileId fileId;
    public final Set<Integer> hashes;

    protected Request(Identifier msgId, FileId fileId, Set<Integer> hashes) {
      this.msgId = msgId;
      this.fileId = fileId;
      this.hashes = hashes;
    }

    public Request(FileId fileId, Set<Integer> hashes) {
      this(BasicIdentifiers.eventId(), fileId, hashes);
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

    public Success success(Map<Integer, byte[]> hashValues) {
      return new Success(this, hashValues);
    }

    public BadRequest badRequest() {
      return new BadRequest(this);
    }
  }

  public static class Success implements ConnectionMsg {

    public final Identifier msgId;
    public final FileId fileId;
    public final Map<Integer, byte[]> hashValues;

    protected Success(Identifier msgId, FileId fileId, Map<Integer, byte[]> hashValues) {
      this.msgId = msgId;
      this.fileId = fileId;
      this.hashValues = hashValues;
    }

    private Success(Request req, Map<Integer, byte[]> hashValues) {
      this(req.msgId, req.fileId, hashValues);
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
      return TorrentIds.connId(fileId, peer, true);
    }
  }

  public static class BadRequest implements ConnectionMsg {

    public final Identifier msgId;
    public final FileId fileId;
    public final Set<Integer> hashes;

    protected BadRequest(Identifier msgId, FileId fileId, Set<Integer> hashes) {
      this.msgId = msgId;
      this.fileId = fileId;
      this.hashes = hashes;
    }
    
    private BadRequest(Request req) {
      this(req.msgId, req.fileId, req.hashes);
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
      return TorrentIds.connId(fileId, peer, true);
    }
  }
}
