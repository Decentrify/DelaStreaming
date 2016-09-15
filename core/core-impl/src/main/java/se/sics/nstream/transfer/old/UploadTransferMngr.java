///*
// * Copyright (C) 2009 Swedish Institute of Computer Science (SICS) Copyright (C)
// * 2009 Royal Institute of Technology (KTH)
// *
// * KompicsToolbox is free software; you can redistribute it and/or
// * modify it under the terms of the GNU General Public License
// * as published by the Free Software Foundation; either version 2
// * of the License, or (at your option) any later version.
// *
// * This program is distributed in the hope that it will be useful,
// * but WITHOUT ANY WARRANTY; without even the implied warranty of
// * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// * GNU General Public License for more details.
// *
// * You should have received a copy of the GNU General Public License
// * along with this program; if not, write to the Free Software
// * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
// */
//package se.sics.nstream.transfer.old;
//
//import org.javatuples.Pair;
//import se.sics.ktoolbox.util.identifiable.Identifier;
//import se.sics.nstream.storage.cache.KHint;
//import se.sics.nstream.storage.managed.CompleteFileMngr;
//import se.sics.nstream.transfer.BlockHelper;
//import se.sics.nstream.transfer.TransferMngr;
//import se.sics.nstream.transfer.UploadTMReport;
//import se.sics.nstream.util.FileBaseDetails;
//import se.sics.nstream.util.StreamControl;
//import se.sics.nstream.util.range.KBlock;
//import se.sics.nstream.util.range.KPiece;
//import se.sics.nstream.util.result.HashReadCallback;
//import se.sics.nstream.util.result.PieceReadCallback;
//import se.sics.nstream.util.result.ReadCallback;
//
///**
// * @author Alex Ormenisan <aaor@kth.se>
// */
//public class UploadTransferMngr implements StreamControl, TransferMngr.Reader {
//
//    private final FileBaseDetails fileDetails;
//    private final CompleteFileMngr file;
//
//    public UploadTransferMngr(FileBaseDetails fileDetails, CompleteFileMngr file) {
//        this.fileDetails = fileDetails;
//        this.file = file;
//    }
//
//    //********************************CONTROL***********************************
//    @Override
//    public void start() {
//        file.start();
//    }
//
//    @Override
//    public boolean isIdle() {
//        return file.isIdle();
//    }
//
//    @Override
//    public void close() {
//        file.close();
//    }
//    
//    public UploadTMReport report() {
//        return new UploadTMReport(file.report());
//    }
//
//    //*****************************CACHE_HINT_READ******************************
//    @Override
//    public void clean(Identifier reader) {
//        file.clean(reader);
//    }
//
//    @Override
//    public void setFutureReads(Identifier reader, KHint.Expanded hint) {
//        file.setFutureReads(reader, hint);
//    }
//
//    //***********************************READER*********************************
//    @Override
//    public boolean hasBlock(int blockNr) {
//        return true;
//    }
//
//    @Override
//    public boolean hasHash(int blockNr) {
//        return true;
//    }
//
//    @Override
//    public void readHash(int blockNr, HashReadCallback delayedResult) {
//        KBlock hashRange = BlockHelper.getHashRange(blockNr, fileDetails);
//        file.readHash(hashRange, delayedResult);
//    }
//
//    @Override
//    public void readBlock(int blockNr, ReadCallback delayedResult) {
//        KBlock blockRange = BlockHelper.getBlockRange(blockNr, fileDetails);
//        file.read(blockRange, delayedResult);
//    }
//
//    @Override
//    public void readPiece(Pair<Integer, Integer> pieceNr, PieceReadCallback pieceRC) {
//        KPiece pieceRange = BlockHelper.getPieceRange(pieceNr, fileDetails);
//        file.read(pieceRange, pieceRC);
//    }
//    //**************************************************************************
//    public long fileLength() {
//        return fileDetails.length;
//    }
//}
