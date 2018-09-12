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
package se.sics.dela.storage.ctrl.stream;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;
import org.javatuples.Pair;
import se.sics.dela.storage.cache.KHint;
import se.sics.dela.storage.op.CompleteFileMngr;
import se.sics.kompics.util.Identifier;
import se.sics.ktoolbox.util.reference.KReference;
import se.sics.ktoolbox.util.trysf.Try;
import se.sics.nstream.util.BlockDetails;
import se.sics.nstream.util.BlockHelper;
import se.sics.nstream.util.FileBaseDetails;
import se.sics.nstream.util.range.KBlock;

/**
 *
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class StreamComplete implements StreamRead {

  private final FileBaseDetails fileDetails;
  private final CompleteFileMngr file;

  public StreamComplete(CompleteFileMngr file, FileBaseDetails fileDetails) {
    this.fileDetails = fileDetails;
    this.file = file;
  }

  //********************************TFileMngr*********************************
  @Override
  public void start() {
    file.start();
  }

  @Override
  public boolean isIdle() {
    return file.isIdle();
  }

  @Override
  public void close() {
    file.close();
  }

  //*******************************CACHE_HINT*********************************
  @Override
  public void clean(Identifier reader) {
    file.clean(reader);
  }

  @Override
  public void setCacheHint(Identifier reader, KHint.Summary hint) {
    file.setFutureReads(reader, hint.expand(fileDetails));
  }

  //********************************READ_DATA*********************************
  @Override
  public boolean hasBlock(int blockNr) {
    return true;
  }

  @Override
  public boolean hasHash(int blockNr) {
    return true;
  }

  @Override
  public void readHash(int blockNr, Consumer<Try<KReference<byte[]>>> callback) {
    KBlock hashRange = BlockHelper.getHashRange(blockNr, fileDetails);
    file.readHash(hashRange, callback);
  }

  @Override
  public void readBlock(int blockNr, Consumer<Try<KReference<byte[]>>> callback) {
    KBlock blockRange = BlockHelper.getBlockRange(blockNr, fileDetails);
    file.read(blockRange, callback);
  }

  @Override
  public Map<Integer, BlockDetails> getIrregularBlocks() {
    Map<Integer, BlockDetails> irregularBlocks = new HashMap<>();
    irregularBlocks.put(fileDetails.nrBlocks - 1, fileDetails.lastBlock);
    return irregularBlocks;
  }

  // <totalSize, currentSize>
  public Pair<Long, Long> report() {
    return Pair.with(fileDetails.length, fileDetails.length);
  }
}
