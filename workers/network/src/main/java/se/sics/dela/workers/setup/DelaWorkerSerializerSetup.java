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
package se.sics.dela.workers.setup;

import se.sics.dela.workers.DelaWorkTaskSerializer;
import se.sics.dela.workers.task.DelaWorkTask;
import se.sics.kompics.network.netty.serialization.Serializers;

/**
 * @author Alex Ormenisan <aaor@sics.se>
 */
public class DelaWorkerSerializerSetup {

  //You may add up to max serializers without the need to recompile all the projects that use the serializer space after gvod
  public static final int MAX_SERIALIZERS = 10;
  public static final int SERIALIZERS = 4;

  public static enum BasicSerializers {
    DelaWorkTaskLSender(DelaWorkTask.LSender.class, "delaWorkTaskLSender"),
    DelaWorkTaskLReceiver(DelaWorkTask.LReceiver.class, "delaWorkTaskLReceiver"),
    DelaWorkTaskStatus(DelaWorkTask.Status.class, "delaWorkTaskStatus"),
    DelaWorkTaskSuccess(DelaWorkTask.Success.class, "delaWorkTaskSuccess"),
    DelaWorkTaskFail(DelaWorkTask.Fail.class, "delaWorkTaskFail");

    public final Class serializedClass;
    public final String serializerName;

    BasicSerializers(Class serializedClass, String serializerName) {
      this.serializedClass = serializedClass;
      this.serializerName = serializerName;
    }
  }

  public static boolean checkSetup() {
    for (BasicSerializers bs : BasicSerializers.values()) {
      if (Serializers.lookupSerializer(bs.serializedClass) == null) {
        return false;
      }
    }
    return true;
  }

  public static int registerBasicSerializers(int startingId) {
    if (startingId < 128) {
      throw new RuntimeException("start your serializer ids at 128");
    }
    int currentId = startingId;

    Serializers.register(new DelaWorkTaskSerializer.LSender(currentId++),
      BasicSerializers.DelaWorkTaskLSender.serializerName);
    Serializers.register(BasicSerializers.DelaWorkTaskLSender.serializedClass,
      BasicSerializers.DelaWorkTaskLSender.serializerName);
    
    Serializers.register(new DelaWorkTaskSerializer.LReceiver(currentId++),
      BasicSerializers.DelaWorkTaskLReceiver.serializerName);
    Serializers.register(BasicSerializers.DelaWorkTaskLReceiver.serializedClass,
      BasicSerializers.DelaWorkTaskLReceiver.serializerName);

    Serializers.register(new DelaWorkTaskSerializer.Status(currentId++),
      BasicSerializers.DelaWorkTaskStatus.serializerName);
    Serializers.register(BasicSerializers.DelaWorkTaskStatus.serializedClass,
      BasicSerializers.DelaWorkTaskStatus.serializerName);

    Serializers.register(new DelaWorkTaskSerializer.Success(currentId++),
      BasicSerializers.DelaWorkTaskSuccess.serializerName);
    Serializers.register(BasicSerializers.DelaWorkTaskSuccess.serializedClass,
      BasicSerializers.DelaWorkTaskSuccess.serializerName);
    
    Serializers.register(new DelaWorkTaskSerializer.Fail(currentId++),
      BasicSerializers.DelaWorkTaskFail.serializerName);
    Serializers.register(BasicSerializers.DelaWorkTaskFail.serializedClass,
      BasicSerializers.DelaWorkTaskFail.serializerName);

    assert startingId + SERIALIZERS == currentId;
    assert SERIALIZERS <= MAX_SERIALIZERS;
    return startingId + MAX_SERIALIZERS;
  }
}
