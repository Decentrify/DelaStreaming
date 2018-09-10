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
//package se.sics.dela.storage.mngr.stream.util.save;
//
//import se.sics.dela.storage.mngr.stream.util.save.StreamHandler;
//import java.util.Optional;
//import org.junit.Assert;
//import org.junit.Test;
//
///**
// * @author Alex Ormenisan <aaor@kth.se>
// */
//public class StreamHandlerTest {
//
//  @Test
//  public void test() {
//    StreamHandler s = StreamHandlerHelper.streamHandler1();
//    streamState(s, 3, 0, 0);
//    Optional<StreamTransferHandler.Transfer> su1 = s.next();
//    Assert.assertTrue(su1.isPresent());
//    streamState(s, 2, 1, 0);
//    Optional<StreamTransferHandler.Transfer> su2 = s.next();
//    Assert.assertTrue(su2.isPresent());
//    streamState(s, 1, 2, 0);
//    storageSetup(su1.get());
//    streamState(s, 1, 1, 1);
//    Optional<StreamTransferHandler.Transfer> su3 = s.next();
//    Assert.assertTrue(su3.isPresent());
//    streamState(s, 0, 2, 1);
//    storageSetup(su2.get());
//    streamState(s, 0, 1, 2);
//    storageSetup(su3.get());
//    streamState(s, 0, 0, 3);
//    Assert.assertTrue(s.isComplete());
//  }
//  
//  private void storageSetup(StreamTransferHandler.Transfer transfer) {
//    transfer.mainStorageStream.completed();
//    transfer.secondaryStorageStreams.forEach((stream) -> stream.completed());
//  }
//  
//  private void streamState(StreamHandler s, int pending, int active, int completed) {
//    Assert.assertEquals(pending, s.pending());
//    Assert.assertEquals(active, s.active());
//    Assert.assertEquals(completed, s.completed());
//  }
//}
