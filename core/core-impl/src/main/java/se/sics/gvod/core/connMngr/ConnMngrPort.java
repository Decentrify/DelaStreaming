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
package se.sics.gvod.core.connMngr;

import se.sics.nstream.torrent.event.Download;
import se.sics.gvod.core.connMngr.event.Ready;
import se.sics.kompics.PortType;

/**
 * @author Alex Ormenisan <aaor@sics.se>
 */
public class ConnMngrPort extends PortType {

    {
        indication(Ready.class);

        request(Download.HashRequest.class);
        request(Download.HashResponse.class);
        request(Download.DataRequest.class);
        request(Download.DataResponse.class);

        indication(Download.HashRequest.class);
        indication(Download.HashResponse.class);
        indication(Download.DataRequest.class);
        indication(Download.DataResponse.class);
    }
}
