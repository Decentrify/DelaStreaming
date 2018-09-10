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
//import java.util.HashMap;
//import java.util.Iterator;
//import java.util.Map;
//import java.util.Optional;
//import java.util.function.Consumer;
//import se.sics.ktoolbox.util.Either;
//
///**
// * @author Alex Ormenisan <aaor@kth.se>
// */
//public class StreamGroupHandler implements StreamHandler {
//
//  public final String name;
//  public final Optional<StreamGroupHandler> parent;
//  public final Map<String, StreamHandler>[] children = new Map[4];
//  private final int directChildren;
//
//  public StreamGroupHandler(String name, StreamGroupHandler parent, int directChildren) {
//    this.name = name;
//    this.parent = Optional.of(parent);
//    this.directChildren = directChildren;
//  }
//  
//  public StreamGroupHandler(String name, int directChildren) {
//    this.name = name;
//    this.parent = Optional.empty();
//    this.directChildren = directChildren;
//  }
//
//  void move(String child, StreamTransferState from, StreamTransferState to) {
//    StreamHandler sh = children[from.ordinal()].remove(child);
//    children[to.ordinal()].put(name, sh);
//    checkState(from, to);
//  }
//
//  private void checkState(StreamTransferState from, StreamTransferState to) {
//    if(StreamTransferState.PENDING.equals(from) && StreamTransferState.DOWNLOADING.equals(to)) {
//      //child moving from pending to download
//      if(children[StreamTransferState.PENDING.ordinal()].isEmpty() && parent.isPresent()) {
//        //as a group, moving to download only when out of pending
//        parent.get().move(name, StreamTransferState.PENDING, StreamTransferState.DOWNLOADING);
//      }
//    } else if(StreamTransferState.DOWNLOADING.equals(from) && StreamTransferState.UPLOADING.equals(to)) {
//      //child moving from downloading to uploading
//    }
//  }
//
//  @Override
//  public int pending() {
//    int size = 0;
//    size += pending.values().stream().map((child) -> child.pending()).reduce(0, Integer::sum);
//    size += downloading.values().stream().map((child) -> child.pending()).reduce(0, Integer::sum);
//    return size;
//  }
//
//  @Override
//  public int downloading() {
//    int size = 0;
//    size += downloading.values().stream().map((child) -> child.active()).reduce(0, Integer::sum);
//    return size;
//  }
//
//  @Override
//  public int completed() {
//    int size = 0;
//    size += downloading.values().stream().map((child) -> child.completed()).reduce(0, Integer::sum);
//    size += uploading.values().stream().map((child) -> child.completed()).reduce(0, Integer::sum);
//    return size;
//  }
//
//  @Override
//  public Optional<StreamTransferHandler.Transfer> next() {
//    for (StreamHandler active : downloading.values()) {
//      Optional<StreamTransferHandler.Transfer> next = active.next();
//      if (next.isPresent()) {
//        return next;
//      }
//    }
//    Iterator<Map.Entry<String, StreamHandler>> it = pending.entrySet().iterator();
//    while (it.hasNext()) {
//      Map.Entry<String, StreamHandler> next = it.next();
//      it.remove();
//      downloading.put(next.getKey(), next.getValue());
//      return next.getValue().next();
//    }
//    return Optional.empty();
//  }
//
//  @Override
//  public boolean isComplete() {
//    return pending.isEmpty() && downloading.isEmpty();
//  }
//
//  @Override
//  public void onCompletion(Consumer<String> callback) {
//    onCompletion = Optional.of(callback);
//  }
//
//  private Consumer onCompletionCallback() {
//    return new Consumer<String>() {
//      @Override
//      public void accept(String child) {
//        StreamHandler complete = downloading.remove(child);
//        uploading.put(child, complete);
//        if (isComplete() && onCompletion.isPresent()) {
//          onCompletion.get().accept(name);
//        }
//      }
//    };
//  }
//}
