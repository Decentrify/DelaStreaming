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
package se.sics.silk.r2torrent.storage.buffer;

import java.util.HashMap;
import java.util.Map;
import se.sics.ktoolbox.util.managedStore.core.util.HashUtil;
import se.sics.ktoolbox.util.reference.KReference;
import se.sics.ktoolbox.util.reference.KReferenceException;
import se.sics.silk.util.Counter;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class R1AsyncCheckedBuffer {

  private final String hashAlg;
  private final R1AsyncBuffer buffer;
  private final Map<Long, KReference<byte[]>> pendingBlocks = new HashMap<>();
  private final Map<Long, KReference<byte[]>> pendingHashes = new HashMap<>();
  private final Counter counter = new Counter(0);
  private final R1AsyncBufferCallback bufferCallback = new R1AsyncBufferCallback() {
    @Override
    public void completed() {
      counter.dec();
    }
  };

  public R1AsyncCheckedBuffer(R1AsyncBuffer buffer, String hashAlg) {
    this.buffer = buffer;
    this.hashAlg = hashAlg;
  }

  public void writeBlock(Long pos, KReference<byte[]> block) {
    KReference<byte[]> hash = pendingHashes.remove(pos);
    if (hash == null) {
      pendingBlocks.put(pos, block);
      block.retain();
      return;
    }
    if (!HashUtil.checkHash(hashAlg, block.getValue().get(), hash.getValue().get())) {
      throw new RuntimeException("not yet implemented");
    }
    buffer.write(pos, block, bufferCallback);
    try {
      hash.release();
    } catch (KReferenceException ex) {
      throw new RuntimeException(ex);
    }
  }

  public void writeHash(Long pos, KReference<byte[]> hash) {
    KReference<byte[]> block = pendingBlocks.remove(pos);
    if (block == null) {
      pendingHashes.put(pos, hash);
      hash.retain();
      return;
    }
    if (!HashUtil.checkHash(hashAlg, hash.getValue().get(), block.getValue().get())) {
      throw new RuntimeException("not yet implemented");
    }
    buffer.write(pos, block, bufferCallback);
    try {
      block.release();
    } catch (KReferenceException ex) {
      throw new RuntimeException(ex);
    }
  }

  public int size() {
    return counter.value() + pendingBlocks.size();
  }
}
