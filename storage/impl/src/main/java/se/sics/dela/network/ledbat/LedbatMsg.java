/*
 * Copyright (C) 2009 Swedish Institute of Computer Science (SICS) Copyright (C)
 * 2009 Royal Institute of Technology (KTH)
 *
 * KompicsToolbox is free software; you can redistribute it and/or
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
 * along with this program; if not, loss to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
 */
package se.sics.dela.network.ledbat;

import se.sics.dela.network.ledbat.util.OneWayDelay;
import se.sics.kompics.KompicsEvent;
import se.sics.kompics.util.Identifiable;
import se.sics.kompics.util.Identifier;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class LedbatMsg {
  
  public static class Data<D extends Identifiable> implements Identifiable, KompicsEvent {
    public final Identifier msgId;
    public final D data;
    public final OneWayDelay dataDelay;
    
    public Data(Identifier msgId, D data) {
      this(msgId, data, new OneWayDelay());
    }
    
    public Data(Identifier msgId, D data, OneWayDelay dataDelay) {
      this.msgId = msgId;
      this.data = data;
      this.dataDelay = dataDelay;
    }
    
    public Ack answer() {
      return new Ack(msgId, data.getId(), dataDelay);
    }

    @Override
    public Identifier getId() {
      return msgId;
    }
    
    @Override
    public String toString() {
      return "LedbatMsgData{" + "data=" + data.getId() + '}';
    }
  }
  
  public static class Ack implements Identifiable {

    public final Identifier msgId;
    public final Identifier dataId;
    public final OneWayDelay dataDelay;
    public final OneWayDelay ackDelay;

    public Ack(Identifier msgId, Identifier dataId, OneWayDelay dataDelay) {
      this(msgId, dataId, dataDelay, new OneWayDelay());
    }
    
    public Ack(Identifier msgId, Identifier dataId, OneWayDelay dataDelay, OneWayDelay ackDelay) {
      this.msgId = msgId;
      this.dataId = dataId;
      this.dataDelay = dataDelay;
      this.ackDelay = ackDelay;
    }

    @Override
    public Identifier getId() {
      return msgId;
    }
    
    @Override
    public String toString() {
      return "LedbatMsgAck{" + "data=" + dataId + '}';
    }
  }
}
