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
 * GNU General Public License for more defLastBlock.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
 */
package se.sics.dela.storage.mngr.stream.util.save;

import java.util.function.Consumer;
import se.sics.dela.storage.StreamStorage;
import se.sics.ktoolbox.util.trysf.Try;
import se.sics.nstream.StreamId;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class StreamStorageHandler {

  public final StreamId streamStorageId;
  public final StreamStorage streamStorage;
  private State state;
  private Consumer<Try<Boolean>> onReadyCallback;

  public StreamStorageHandler(StreamId streamStorageId, StreamStorage streamStorage) {
    this.streamStorageId = streamStorageId;
    this.streamStorage = streamStorage;
    this.state = State.DISCONNECTED;
  }
  
  public void connect(Consumer<Try<Boolean>> onReadyCallback) {
    state = State.PENDING_CONNECT;
    onReadyCallback = onReadyCallback;
  }
  
  public void connected(Try<Boolean> result) {
     state = State.CONNECTED;
     onReadyCallback.accept(result);
  } 
  
  public void disconect() {
    state = State.PENDING_DISCONNECT;
    onReadyCallback = null;
  }
  
  public void disconnected() {
    state = State.DISCONNECTED;
  }
  
  public static enum State {
    DISCONNECTED, PENDING_CONNECT, CONNECTED, PENDING_DISCONNECT
  }
}
