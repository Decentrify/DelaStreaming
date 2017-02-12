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
package se.sics.nstream.hops.libmngr;

import java.util.List;
import se.sics.kompics.Direct;
import se.sics.ktoolbox.nutil.fsm.FSMEvent;
import se.sics.ktoolbox.util.identifiable.Identifier;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayId;
import se.sics.ktoolbox.util.network.KAddress;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class TorrentRestart {

  public static class DiskUpldReq extends Direct.Request<DiskUpldIndication> implements FSMEvent {
    public final OverlayId torrentId;
    public final List<KAddress> partners;
    
    public DiskUpldReq(OverlayId torrentId, List<KAddress> partners) {
      super();
      this.torrentId = torrentId;
      this.partners = partners;
    }
    
    public DiskUpldSuccess success() {
      return new DiskUpldSuccess(this);
    }
    
    public DiskUpldFail failed() {
      return new DiskUpldFail(this);
    }

    @Override
    public Identifier getBaseId() {
      return torrentId.baseId;
    }
  }

  public static abstract class DiskUpldIndication implements Direct.Response {
    public final DiskUpldReq req;
    
    public DiskUpldIndication(DiskUpldReq req) {
      this.req = req;
    }
  }
  
  public static class DiskUpldSuccess extends DiskUpldIndication {

    public DiskUpldSuccess(DiskUpldReq req) {
      super(req);
    }
  }

  public static class DiskUpldFail extends DiskUpldIndication {

    public DiskUpldFail(DiskUpldReq req) {
      super(req);
    }
  }
  
  public static class DiskDwldReq extends Direct.Request<DiskDwldIndication> implements FSMEvent {
    public final OverlayId torrentId;
    public final List<KAddress> partners;
    
    public DiskDwldReq(OverlayId torrentId, List<KAddress> partners) {
      super();
      this.torrentId = torrentId;
      this.partners = partners;
    }
    
    public DiskDwldSuccess success() {
      return new DiskDwldSuccess(this);
    }
    
    public DiskDwldFail failed() {
      return new DiskDwldFail(this);
    }

    @Override
    public Identifier getBaseId() {
      return torrentId.baseId;
    }
  }
  
  public static abstract class DiskDwldIndication implements Direct.Response {
    public final DiskDwldReq req;
    
    public DiskDwldIndication(DiskDwldReq req) {
      this.req = req;
    }
  }

  public static class DiskDwldSuccess extends DiskDwldIndication {

    public DiskDwldSuccess(DiskDwldReq req) {
      super(req);
    }
  }

  public static class DiskDwldFail extends DiskDwldIndication {
    public DiskDwldFail(DiskDwldReq req) {
      super(req);
    }
  }
}
