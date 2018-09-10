package se.sics.dela.storage.mngr.stream.util.save;

///*
// * Copyright (C) 2009 Swedish Institute of Computer Science (SICS) Copyright (C)
// * 2009 Royal Institute of Technology (KTH)
// *
// * GVoD is free software; you can redistribute it and/or
// * modify it under the terms of the GNU General Public License
// * as published by the Free Software Foundation; either version 2
// * of the License, or (at your option) any later version.
// *
// * This program is distributed in the hope that it will be useful,
// * but WITHOUT ANY WARRANTY; without even the implied warranty of
// * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// * GNU General Public License for more defLastBlock.
// *
// * You should have received a copy of the GNU General Public License
// * along with this program; if not, write to the Free Software
// * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
// */
//package se.sics.dela.storage.mngr.stream.util;
//
//import java.util.ArrayList;
//import java.util.LinkedList;
//import java.util.List;
//import java.util.Optional;
//import java.util.function.Consumer;
//import org.javatuples.Pair;
//import se.sics.dela.storage.StreamStorage;
//import se.sics.nstream.StreamId;
//import se.sics.nstream.storage.durable.util.MyStream;
//
///**
// * @author Alex Ormenisan <aaor@kth.se>
// */
//public class StreamTransferHandler implements StreamHandler {
//
//  public final String name;
//  private State state;
//  private StreamGroupHandler parent;
//  private Optional<Consumer<String>> onCompletion;
//  public final StreamStorageHandler mainStorageStream;
//  public final List<StreamStorageHandler> secondaryStorageStreams;
//
//  private StreamTransferHandler(String name, StreamStorageHandler mainStorageStream, 
//    List<StreamStorageHandler> secondaryStorageStreams) {
//    this.name = name;
//    this.state = State.PENDING;
//    this.onCompletion = Optional.empty();
//    this.mainStorageStream = mainStorageStream;
//    this.secondaryStorageStreams = secondaryStorageStreams;
//  }
//
//  @Override
//  public int pending() {
//    if (State.PENDING.equals(state)) {
//      return 1;
//    }
//    return 0;
//  }
//
//  @Override
//  public int downloading() {
//    if (State.DOWNLOADING.equals(state)) {
//      return 1;
//    }
//    return 0;
//  }
//
//  @Override
//  public int uploading() {
//    if (State.UPLOADING.equals(state)) {
//      return 1;
//    }
//    return 0;
//  }
//  
//  @Override
//  public int idle() {
//    if (State.IDLE.equals(state)) {
//      return 1;
//    }
//    return 0;
//  }
//
//  @Override
//  public boolean isComplete() {
//    return State.IDLE.equals(state) || State.UPLOADING.equals(state);
//  }
//  
//  private void storageReady() {
//    if(State.PENDING.equals(state)) {
//      state = State.DOWNLOADING;
//      parent.
//    }
//  }
//
//  private void complete() {
//    state = State.COMPLETED;
//    if (onCompletion.isPresent()) {
//      onCompletion.get().accept(name);
//    }
//  }
//
//  @Override
//  public void onCompletion(Consumer<String> callback) {
//    onCompletion = Optional.of(callback);
//  }
//
//}
