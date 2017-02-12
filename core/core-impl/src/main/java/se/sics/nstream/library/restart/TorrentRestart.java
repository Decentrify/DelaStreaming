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
package se.sics.nstream.library.restart;

import java.util.List;
import se.sics.kompics.Direct;
import se.sics.ktoolbox.nutil.fsm.FSMEvent;
import se.sics.ktoolbox.util.identifiable.Identifier;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayId;
import se.sics.ktoolbox.util.network.KAddress;
import se.sics.nstream.storage.durable.util.MyStream;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class TorrentRestart {

  public static class UpldReq extends Direct.Request<UpldIndication> implements FSMEvent {

    public final OverlayId torrentId;
    public final String torrentName;
    public final String projectId;
    public final List<KAddress> partners;
    public final MyStream manifestStream;

    public UpldReq(OverlayId torrentId, String torrentName, String projectId, List<KAddress> partners,
      MyStream manifestStream) {
      super();
      this.torrentId = torrentId;
      this.torrentName = torrentName;
      this.projectId = projectId;
      this.partners = partners;
      this.manifestStream = manifestStream;
    }

    public UpldSuccess success() {
      return new UpldSuccess(this);
    }

    public UpldFail failed() {
      return new UpldFail(this);
    }

    @Override
    public Identifier getBaseId() {
      return torrentId.baseId;
    }
  }

  public static abstract class UpldIndication implements Direct.Response {

    public final UpldReq req;

    public UpldIndication(UpldReq req) {
      this.req = req;
    }
  }

  public static class UpldSuccess extends UpldIndication {

    public UpldSuccess(UpldReq req) {
      super(req);
    }
  }

  public static class UpldFail extends UpldIndication {

    public UpldFail(UpldReq req) {
      super(req);
    }
  }

  public static class DwldReq extends Direct.Request<DwldIndication> implements FSMEvent {

    public final OverlayId torrentId;
    public final String torrentName;
    public final String projectId;
    public final List<KAddress> partners;
    public final MyStream manifestStream;

    public DwldReq(OverlayId torrentId, String torrentName, String projectId, List<KAddress> partners,
      MyStream manifestStream) {
      super();
      this.torrentId = torrentId;
      this.torrentName = torrentName;
      this.projectId = projectId;
      this.partners = partners;
      this.manifestStream = manifestStream;
    }

    public DwldSuccess success() {
      return new DwldSuccess(this);
    }

    public DwldFail failed() {
      return new DwldFail(this);
    }

    @Override
    public Identifier getBaseId() {
      return torrentId.baseId;
    }
  }

  public static abstract class DwldIndication implements Direct.Response {

    public final DwldReq req;

    public DwldIndication(DwldReq req) {
      this.req = req;
    }
  }

  public static class DwldSuccess extends DwldIndication {

    public DwldSuccess(DwldReq req) {
      super(req);
    }
  }

  public static class DwldFail extends DwldIndication {

    public DwldFail(DwldReq req) {
      super(req);
    }
  }
}
