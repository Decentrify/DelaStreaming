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

import org.javatuples.Pair;
import se.sics.kompics.util.Identifier;
import se.sics.ktoolbox.util.Either;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayId;
import se.sics.ktoolbox.util.reference.KReference;
import se.sics.nstream.ConnId;
import se.sics.nstream.FileId;
import se.sics.nstream.TorrentIds;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class DownloadPiece {

  public static class Request implements ConnectionMsg {

    public final Identifier msgId;
    public final FileId fileId;
    public final Pair<Integer, Integer> piece;

    public Request(Identifier msgId, FileId fileId, Pair<Integer, Integer> piece) {
      this.msgId = msgId;
      this.fileId = fileId;
      this.piece = piece;
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

    public Success success(KReference<byte[]> val) {
      return new Success(this, val);
    }

    public BadRequest badRequest() {
      return new BadRequest(this);
    }

    @Override
    public String toString() {
      return "DwnlPieceReq<" + fileId.toString() + ",b:" + piece.getValue0() + ",p:" + piece.getValue1() + "," + msgId.
        toString() + ">";
    }
  }

  public static class Success implements ConnectionMsg {

    public final Identifier msgId;
    public final FileId fileId;
    public final Pair<Integer, Integer> piece;
    public final Either<KReference<byte[]>, byte[]> val;

    private Success(Identifier msgId, FileId fileId, Pair<Integer, Integer> piece, Either val) {
      this.msgId = msgId;
      this.fileId = fileId;
      this.piece = piece;
      this.val = val;
    }

    private Success(Request req, KReference<byte[]> val) {
      this(req.msgId, req.fileId, req.piece, Either.left(val));
    }

    protected Success(Identifier eventId, FileId fileId, Pair<Integer, Integer> piece, byte[] val) {
      this(eventId, fileId, piece, Either.right(val));
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

    @Override
    public String toString() {
      return "DwnlPieceSuccess<" + fileId.toString() + ",b:" + piece.getValue0() + ",p:" + piece.getValue1() + ","
        + msgId.
          toString() + ">";
    }
  }

  public static class BadRequest implements ConnectionMsg {

    public final Identifier msgId;
    public final FileId fileId;
    public final Pair<Integer, Integer> piece;

    protected BadRequest(Identifier msgId, FileId fileId, Pair<Integer, Integer> piece) {
      this.msgId = msgId;
      this.fileId = fileId;
      this.piece = piece;
    }

    private BadRequest(Request req) {
      this(req.msgId, req.fileId, req.piece);
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

    @Override
    public String toString() {
      return "DwnlPieceBadReq<" + fileId.toString() + ",b:" + piece.getValue0() + ",p:" + piece.getValue1() + ","
        + msgId.
          toString() + ">";
    }
  }
}
