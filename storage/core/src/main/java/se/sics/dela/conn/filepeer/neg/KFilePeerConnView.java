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
package se.sics.dela.conn.filepeer.neg;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import se.sics.kompics.util.Identifier;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class KFilePeerConnView implements FilePeerConnView {

  private final Identifier fileId;
  private final Identifier peerId;
  private final Set<Integer> blockSlots = new HashSet<>();
  private int inUseSlots;
  private int provisionedSlots;
  private Optional<Consumer<Boolean>> provisionCallback = Optional.empty();

  public KFilePeerConnView(Identifier fileId, Identifier peerId, int provisionedSlots) {
    this.fileId = fileId;
    this.peerId = peerId;
    this.inUseSlots = 0;
    this.provisionedSlots = provisionedSlots;
  }

  @Override
  public boolean availableSlot() {
    return inUseSlots < provisionedSlots;
  }

  @Override
  public void useSlot(int blockNr) {
    inUseSlots++;
    blockSlots.add(blockNr);
  }

  @Override
  public void releaseSlot(int blockNr) {
    inUseSlots--;
    if (provisionCallback.isPresent() && inUseSlots <= provisionedSlots) {
      provisionCallback.get().accept(true);
      provisionCallback = Optional.empty();
    }
    blockSlots.remove(blockNr);
  }

  @Override
  public boolean isActive() {
    return blockSlots.size() > 0;
  }

  @Override
  public void close() {
    if (isActive()) {
      throw new RuntimeException("still active - or bad slot management");
    }
  }

  public void updateProvisionedSlots(int slots, Consumer<Boolean> callback) {
    if (provisionedSlots <= slots
      || inUseSlots <= slots && slots < provisionedSlots) {
      callback.accept(true);
    } else {
      provisionCallback = Optional.of(callback);
    }
    provisionedSlots = slots;
  }
}
