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
package se.sics.silk.r2mngr;

import se.sics.kompics.PortType;
import se.sics.kompics.fsm.event.FSMWrongState;
import se.sics.silk.r2mngr.event.R2TorrentEvents;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class R2TorrentPort extends PortType {
  {
    request(R2TorrentEvents.GetMeta.class);
    request(R2TorrentEvents.ServeMeta.class);
    request(R2TorrentEvents.Hashing.class);
    request(R2TorrentEvents.Download.class);
    request(R2TorrentEvents.DownloadSlotReq.class);
    indication(R2TorrentEvents.DownloadSlotResp.class);
    request(R2TorrentEvents.Upload.class);
    request(R2TorrentEvents.UploadSlotReq.class);
    indication(R2TorrentEvents.UploadSlotResp.class);
    request(R2TorrentEvents.Stop.class);
    indication(FSMWrongState.class);
  }
}
