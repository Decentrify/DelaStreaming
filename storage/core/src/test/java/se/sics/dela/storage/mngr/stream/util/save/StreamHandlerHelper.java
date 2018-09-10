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
//import se.sics.dela.storage.mngr.stream.util.save.StreamHandler;
//import java.util.LinkedList;
//import java.util.List;
//
//
///**
// * @author Alex Ormenisan <aaor@kth.se>
// */
//public class StreamHandlerHelper {
//  public static StreamHandler streamHandler1() {
//    //dir1
//    StreamTransferHandler.Storage s1 = new StreamTransferHandler.Storage(null, null);
//    StreamTransferHandler.Storage s2 = new StreamTransferHandler.Storage(null, null);
//    List<StreamTransferHandler.Storage> ss1 = new LinkedList<>();
//    ss1.add(s2);
//    StreamTransferHandler.Transfer t1 = new StreamTransferHandler.Transfer("file1", s1, ss1);
//    StreamTransferHandler.Storage s3 = new StreamTransferHandler.Storage(null, null);
//    StreamTransferHandler.Transfer t2 = new StreamTransferHandler.Transfer("file2", s3);
//    StreamGroupHandler g1 = new StreamGroupHandler("dir1");
//    g1.pending(t1.name, t1);
//    g1.pending(t2.name, t2);
//    //dir2
//    StreamTransferHandler.Storage s4 = new StreamTransferHandler.Storage(null, null);
//    StreamTransferHandler.Transfer t3 = new StreamTransferHandler.Transfer("file3", s4);
//    StreamGroupHandler g2 = new StreamGroupHandler("dir2");
//    g2.pending(t3.name, t3);
//    //dir3
//    StreamGroupHandler g3 = new StreamGroupHandler("dir3");
//    g3.pending(g1.name, g1);
//    g3.pending(g2.name, g2);
//    return g3;
//  }
//}
