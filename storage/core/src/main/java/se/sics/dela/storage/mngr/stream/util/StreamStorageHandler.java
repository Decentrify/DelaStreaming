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
package se.sics.dela.storage.mngr.stream.util;

import se.sics.dela.storage.StreamStorage;
import se.sics.dela.storage.mngr.stream.util.StreamHandlerImpl.ConnectCollector;
import se.sics.dela.storage.mngr.stream.util.StreamHandlerImpl.DisconnectCollector;
import se.sics.ktoolbox.util.TupleHelper;
import se.sics.nstream.StreamId;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class StreamStorageHandler implements StreamStorageConnected, StreamStorageDisconnected {

  private final StreamId streamId;
  private final StreamStorage storage;
  private final TupleHelper.TripletConsumer<StreamId, StreamStorage, StreamStorageConnected> connect;
  private final TupleHelper.PairConsumer<StreamId, StreamStorageDisconnected> disconnect;
  private StreamStorageState state;
  
  private ConnectCollector connectC;
  private DisconnectCollector disconnectC;

  public StreamStorageHandler(StreamId streamId, StreamStorage streamStorage,
    TupleHelper.TripletConsumer<StreamId, StreamStorage, StreamStorageConnected> connect,
    TupleHelper.PairConsumer<StreamId, StreamStorageDisconnected> disconnect) {
    this.streamId = streamId;
    this.storage = streamStorage;
    this.connect = connect;
    this.disconnect = disconnect;
    this.state = StreamStorageState.DISCONNECTED;
  }

  @Override
  public String name() {
    return storage.getName();
  }

  public void connect(ConnectCollector collector) {
    this.connectC = collector;
    state = StreamStorageState.PENDING_CONNECT;
    connect.accept(streamId, storage, this);
  }

  @Override
  public void connected(long streamPos) {
    state = StreamStorageState.CONNECTED;
    connectC.collect(storage.getName(), streamPos);
  }

  public void disconnect(DisconnectCollector collector) {
    if (StreamStorageState.CONNECTED.equals(state)) {
      disconnectC = collector;
      state = StreamStorageState.PENDING_DISCONNECT;
      disconnect.accept(streamId, this);
    } else {
      collector.collect(storage.getName());
    }
  }

  @Override
  public void disconnected() {
    state = StreamStorageState.DISCONNECTED;
    disconnectC.collect(storage.getName());
  }
}
