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
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import se.sics.kompics.util.Identifier;
import se.sics.kompics.network.netty.serialization.Serializer;
import se.sics.kompics.network.netty.serialization.Serializers;
import se.sics.ktoolbox.util.identifiable.BasicIdentifiers;
import se.sics.ktoolbox.util.identifiable.IdentifierRegistry;
import se.sics.nstream.FileId;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class DownloadHashSerializer {

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
      DownloadHash.Request obj = (DownloadHash.Request) o;
      Serializers.lookupSerializer(msgIdType).toBinary(obj.msgId, buf);
      Serializers.lookupSerializer(FileId.class).toBinary(obj.fileId, buf);
      buf.writeInt(obj.hashes.size());
      for (Integer h : obj.hashes) {
        buf.writeInt(h);
      }
    }

    @Override
    public Object fromBinary(ByteBuf buf, Optional<Object> hint) {
      Identifier eventId = (Identifier) Serializers.lookupSerializer(msgIdType).fromBinary(buf, hint);
      FileId fileId = (FileId) Serializers.lookupSerializer(FileId.class).fromBinary(buf, hint);
      Set<Integer> hashes = new TreeSet();
      int hashSize = buf.readInt();
      while (hashSize > 0) {
        hashSize--;
        hashes.add(buf.readInt());
      }
      return new DownloadHash.Request(eventId, fileId, hashes);
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
      DownloadHash.Success obj = (DownloadHash.Success) o;
      Serializers.lookupSerializer(msgIdType).toBinary(obj.msgId, buf);
      Serializers.lookupSerializer(FileId.class).toBinary(obj.fileId, buf);
      buf.writeInt(obj.hashValues.size());
      for (Map.Entry<Integer, byte[]> hv : obj.hashValues.entrySet()) {
        buf.writeInt(hv.getKey());
        buf.writeInt(hv.getValue().length);
        buf.writeBytes(hv.getValue());
      }
    }

    @Override
    public Object fromBinary(ByteBuf buf, Optional<Object> hint) {
      Identifier msgId = (Identifier) Serializers.lookupSerializer(msgIdType).fromBinary(buf, hint);
      FileId fileId = (FileId) Serializers.lookupSerializer(FileId.class).fromBinary(buf, hint);
      int hashSize = buf.readInt();
      Map<Integer, byte[]> hashValues = new TreeMap<>();
      while (hashSize > 0) {
        hashSize--;
        int hashNr = buf.readInt();
        int hashValueSize = buf.readInt();
        byte[] hashValue = new byte[hashValueSize];
        buf.readBytes(hashValue);
        hashValues.put(hashNr, hashValue);
      }
      return new DownloadHash.Success(msgId, fileId, hashValues);
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
      DownloadHash.BadRequest obj = (DownloadHash.BadRequest) o;
      Serializers.lookupSerializer(msgIdType).toBinary(obj.msgId, buf);
      Serializers.lookupSerializer(FileId.class).toBinary(obj.fileId, buf);
      buf.writeInt(obj.hashes.size());
      for (Integer h : obj.hashes) {
        buf.writeInt(h);
      }
    }

    @Override
    public Object fromBinary(ByteBuf buf, Optional<Object> hint) {
      Identifier eventId = (Identifier) Serializers.lookupSerializer(msgIdType).fromBinary(buf, hint);
      FileId fileId = (FileId) Serializers.lookupSerializer(FileId.class).fromBinary(buf, hint);
      Set<Integer> hashes = new TreeSet();
      int hashSize = buf.readInt();
      while (hashSize > 0) {
        hashSize--;
        hashes.add(buf.readInt());
      }
      return new DownloadHash.BadRequest(eventId, fileId, hashes);
    }
  }
}
