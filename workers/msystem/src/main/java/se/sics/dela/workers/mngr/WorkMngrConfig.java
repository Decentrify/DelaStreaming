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
package se.sics.dela.workers.mngr;

import com.google.common.base.Optional;
import se.sics.kompics.config.Config;
import se.sics.kompics.util.Identifier;
import se.sics.ktoolbox.util.identifiable.basic.IntIdFactory;
import se.sics.ktoolbox.util.trysf.Try;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class WorkMngrConfig {
  public static final String WORKER_OVERLAY_ID = "overlays.dela.worker";
  public static final String WORKER_MNGR_BATCH_ID = "dela.worker.mngr.id.batch";
  public static final String WORKER_MNGR_BASE_ID = "dela.worker.mngr.id.base";
  public static final String WORKER_UPDATE_PERIOD = "dela.worker.updatePeriod";
  
  public final Identifier overlayId;
  public final Identifier batchId;
  public final Identifier baseId;
  public final long updatePeriod;
  
  public WorkMngrConfig(Identifier overlayId, Identifier batchId, Identifier baseId, long updatePeriod) {
    this.overlayId = overlayId;
    this.batchId = batchId;
    this.baseId = baseId;
    this.updatePeriod = updatePeriod;
  }
  
  public static Try<WorkMngrConfig> instance(Config config) {
    IntIdFactory intIdFactory = new IntIdFactory(java.util.Optional.empty());
    Optional<Integer> overlayIdOpt = config.readValue(WORKER_OVERLAY_ID, Integer.class);
    Identifier overlayId;
    if(overlayIdOpt.isPresent()) {
      overlayId = intIdFactory.rawId(overlayIdOpt.get());
    } else {
      return new Try.Failure(new IllegalStateException("missing:" + WORKER_OVERLAY_ID));
    }
    Optional<Integer> batchIdOpt = config.readValue(WORKER_MNGR_BATCH_ID, Integer.class);
    Identifier batchId;
    if(batchIdOpt.isPresent()) {
      batchId = intIdFactory.rawId(batchIdOpt.get());
    } else {
      batchId = intIdFactory.rawId(0);
    }
    Optional<Integer> baseIdOpt = config.readValue(WORKER_MNGR_BASE_ID, Integer.class);
    Identifier baseId;
    if(baseIdOpt.isPresent()) {
      baseId = intIdFactory.rawId(baseIdOpt.get());
    } else {
      baseId = intIdFactory.rawId(0);
    }
    Optional<Long> updatePeriodOpt = config.readValue(WORKER_UPDATE_PERIOD, Long.class);
    long updatePeriod;
    if(updatePeriodOpt.isPresent()) {
      updatePeriod = updatePeriodOpt.get();
    } else {
      updatePeriod = 10000;
    }
    return new Try.Success(new WorkMngrConfig(overlayId, batchId, baseId, updatePeriod));
   }

  @Override
  public String toString() {
    return "WorkMngrConfig{" + "oId=" + overlayId + ", bId=" + batchId + ", iId=" + baseId +
      ", up=" + updatePeriod + '}';
  }
  
  
}
