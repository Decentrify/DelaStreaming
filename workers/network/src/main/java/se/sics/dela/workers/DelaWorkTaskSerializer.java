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
package se.sics.dela.workers;

import com.google.common.base.Optional;
import io.netty.buffer.ByteBuf;
import se.sics.dela.workers.task.DelaWorkTask;
import se.sics.kompics.network.netty.serialization.Serializer;
import se.sics.kompics.network.netty.serialization.Serializers;
import se.sics.kompics.util.Identifier;
import se.sics.ktoolbox.util.network.KAddress;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class DelaWorkTaskSerializer {

  public static class LReceiver implements Serializer {

    private final int id;

    public LReceiver(int id) {
      this.id = id;
    }

    @Override
    public int identifier() {
      return id;
    }

    @Override
    public void toBinary(Object o, ByteBuf buf) {
      DelaWorkTask.LReceiver obj = (DelaWorkTask.LReceiver) o;
      Serializers.toBinary(obj.taskId, buf);
    }

    @Override
    public DelaWorkTask.LReceiver fromBinary(ByteBuf buf, Optional<Object> hint) {
      Identifier taskId = (Identifier) Serializers.fromBinary(buf, hint);
      return new DelaWorkTask.LReceiver(taskId);
    }
  }
  
  public static class LSender implements Serializer {

    private final int id;

    public LSender(int id) {
      this.id = id;
    }

    @Override
    public int identifier() {
      return id;
    }

    @Override
    public void toBinary(Object o, ByteBuf buf) {
      DelaWorkTask.LSender obj = (DelaWorkTask.LSender) o;
      Serializers.toBinary(obj.taskId, buf);
      Serializers.toBinary(obj.receiver, buf);
      buf.writeInt(obj.nrBlocks);
    }

    @Override
    public DelaWorkTask.LSender fromBinary(ByteBuf buf, Optional<Object> hint) {
      Identifier taskId = (Identifier) Serializers.fromBinary(buf, hint);
      KAddress receiver = (KAddress) Serializers.fromBinary(buf, hint);
      int nrBlocks = buf.readInt();
      return new DelaWorkTask.LSender(taskId, receiver, nrBlocks);
    }
  }

  public static class Status implements Serializer {

    private final int id;

    public Status(int id) {
      this.id = id;
    }

    @Override
    public int identifier() {
      return id;
    }

    @Override
    public void toBinary(Object o, ByteBuf buf) {
      DelaWorkTask.Status obj = (DelaWorkTask.Status) o;
      Serializers.toBinary(obj.taskId, buf);
    }

    @Override
    public DelaWorkTask.Status fromBinary(ByteBuf buf, Optional<Object> hint) {
      Identifier taskId = (Identifier) Serializers.fromBinary(buf, hint);
      return new DelaWorkTask.Status(taskId);
    }
  }

  public static class Fail implements Serializer {

    private final int id;

    public Fail(int id) {
      this.id = id;
    }

    @Override
    public int identifier() {
      return id;
    }

    @Override
    public void toBinary(Object o, ByteBuf buf) {
      DelaWorkTask.Fail obj = (DelaWorkTask.Fail) o;
      Serializers.toBinary(obj.taskId, buf);
    }

    @Override
    public DelaWorkTask.Fail fromBinary(ByteBuf buf, Optional<Object> hint) {
      Identifier taskId = (Identifier) Serializers.fromBinary(buf, hint);
      return new DelaWorkTask.Fail(taskId);
    }
  }
  
  public static class Success implements Serializer {

    private final int id;

    public Success(int id) {
      this.id = id;
    }

    @Override
    public int identifier() {
      return id;
    }

    @Override
    public void toBinary(Object o, ByteBuf buf) {
      DelaWorkTask.Success obj = (DelaWorkTask.Success) o;
      Serializers.toBinary(obj.taskId, buf);
    }

    @Override
    public DelaWorkTask.Success fromBinary(ByteBuf buf, Optional<Object> hint) {
      Identifier taskId = (Identifier) Serializers.fromBinary(buf, hint);
      return new DelaWorkTask.Success(taskId);
    }
  }
}
