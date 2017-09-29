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

import com.google.common.base.Optional;
import io.netty.buffer.ByteBuf;
import org.javatuples.Pair;
import se.sics.kompics.id.Identifier;
import se.sics.kompics.network.netty.serialization.Serializer;
import se.sics.kompics.network.netty.serialization.Serializers;
import se.sics.ktoolbox.util.identifiable.BasicIdentifiers;
import se.sics.ktoolbox.util.identifiable.IdentifierRegistry;
import se.sics.ktoolbox.util.reference.KReference;
import se.sics.ktoolbox.util.reference.KReferenceException;
import se.sics.nstream.FileId;

/**
 *
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class DownloadPieceSerializer {

  public static class Request implements Serializer {

    private final int id;
    private final Class msgIdType;

    public Request(int id) {
      this.id = id;
      this.msgIdType = IdentifierRegistry.lookup(BasicIdentifiers.Values.MSG.toString()).idType();
    }

    @Override
    public int identifier() {
      return id;
    }

    @Override
    public void toBinary(Object o, ByteBuf buf) {
      DownloadPiece.Request obj = (DownloadPiece.Request) o;
      Serializers.lookupSerializer(msgIdType).toBinary(obj.msgId, buf);
      Serializers.lookupSerializer(FileId.class).toBinary(obj.fileId, buf);
      buf.writeInt(obj.piece.getValue0());
      buf.writeInt(obj.piece.getValue1());
    }

    @Override
    public Object fromBinary(ByteBuf buf, Optional<Object> hint) {
      Identifier msgId = (Identifier) Serializers.lookupSerializer(msgIdType).fromBinary(buf, hint);
      FileId fileId = (FileId) Serializers.lookupSerializer(FileId.class).fromBinary(buf, hint);
      int blockNr = buf.readInt();
      int pieceNr = buf.readInt();
      return new DownloadPiece.Request(msgId, fileId, Pair.with(blockNr, pieceNr));
    }
  }

  public static class Success implements Serializer {

    private final int id;
    private final Class msgIdType;

    public Success(int id) {
      this.id = id;
      this.msgIdType = IdentifierRegistry.lookup(BasicIdentifiers.Values.MSG.toString()).idType();
    }

    @Override
    public int identifier() {
      return id;
    }

    @Override
    public void toBinary(Object o, ByteBuf buf) {
      DownloadPiece.Success obj = (DownloadPiece.Success) o;
      Serializers.lookupSerializer(msgIdType).toBinary(obj.msgId, buf);
      Serializers.lookupSerializer(FileId.class).toBinary(obj.fileId, buf);
      buf.writeInt(obj.piece.getValue0());
      buf.writeInt(obj.piece.getValue1());

      byte[] piece = getPiece(obj.val.getLeft());
      buf.writeInt(piece.length);
      buf.writeBytes(piece);
    }

    private byte[] getPiece(KReference<byte[]> pieceRef) {
      byte[] piece = pieceRef.getValue().get();
      try {
        pieceRef.release();
      } catch (KReferenceException ex) {
        throw new RuntimeException(ex);
      }
      return piece;
    }

    @Override
    public Object fromBinary(ByteBuf buf, Optional<Object> hint) {
      Identifier msgId = (Identifier) Serializers.lookupSerializer(msgIdType).fromBinary(buf, hint);
      FileId fileId = (FileId) Serializers.lookupSerializer(FileId.class).fromBinary(buf, hint);
      int blockNr = buf.readInt();
      int pieceNr = buf.readInt();

      int pieceSize = buf.readInt();
      byte[] piece = new byte[pieceSize];
      buf.readBytes(piece);
      return new DownloadPiece.Success(msgId, fileId, Pair.with(blockNr, pieceNr), piece);
    }
  }

  public static class BadRequest implements Serializer {

    private final int id;
    private final Class msgIdType;

    public BadRequest(int id) {
      this.id = id;
      this.msgIdType = IdentifierRegistry.lookup(BasicIdentifiers.Values.MSG.toString()).idType();
    }

    @Override
    public int identifier() {
      return id;
    }

    @Override
    public void toBinary(Object o, ByteBuf buf) {
      DownloadPiece.BadRequest obj = (DownloadPiece.BadRequest) o;
      Serializers.lookupSerializer(msgIdType).toBinary(obj.msgId, buf);
      Serializers.lookupSerializer(FileId.class).toBinary(obj.fileId, buf);
      buf.writeInt(obj.piece.getValue0());
      buf.writeInt(obj.piece.getValue1());
    }

    @Override
    public Object fromBinary(ByteBuf buf, Optional<Object> hint) {
      Identifier msgId = (Identifier) Serializers.lookupSerializer(msgIdType).fromBinary(buf, hint);
      FileId fileId = (FileId) Serializers.lookupSerializer(FileId.class).fromBinary(buf, hint);
      int blockNr = buf.readInt();
      int pieceNr = buf.readInt();
      return new DownloadPiece.BadRequest(msgId, fileId, Pair.with(blockNr, pieceNr));
    }
  }
}
