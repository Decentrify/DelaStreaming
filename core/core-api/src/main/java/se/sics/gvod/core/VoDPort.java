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

package se.sics.gvod.core;

import se.sics.gvod.core.event.DownloadVideo;
import se.sics.gvod.core.event.GetLibrary;
import se.sics.gvod.core.event.PlayReady;
import se.sics.gvod.core.event.UploadVideo;
import se.sics.kompics.PortType;

/**
 *
 * @author Alex Ormenisan <aaor@sics.se>
 */
public class VoDPort extends PortType {
    {
        request(GetLibrary.Request.class);
        indication(GetLibrary.Indication.class);
        request(UploadVideo.Request.class);
        request(DownloadVideo.Request.class);
        indication(PlayReady.class);
    }
}
