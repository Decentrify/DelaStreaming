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
package se.sics.nstream.torrent.conn;

import se.sics.kompics.PortType;
import se.sics.nstream.torrent.conn.event.CloseTransfer;
import se.sics.nstream.torrent.conn.event.DetailedState;
import se.sics.nstream.torrent.conn.event.OpenTransfer;
import se.sics.nstream.torrent.conn.event.Seeder;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class ConnectionPort extends PortType {
    {
        request(Seeder.Connect.class);
        indication(Seeder.Success.class);
        indication(Seeder.Timeout.class);
        indication(Seeder.Suspect.class);
        
        request(DetailedState.Request.class);
        indication(DetailedState.Success.class);
        indication(DetailedState.Timeout.class);
        indication(DetailedState.None.class);
        
        request(OpenTransfer.LeecherRequest.class);
        indication(OpenTransfer.LeecherResponse.class);
        indication(OpenTransfer.LeecherTimeout.class);
        indication(OpenTransfer.SeederRequest.class);
        request(OpenTransfer.SeederResponse.class);
        
        request(CloseTransfer.Request.class);
        indication(CloseTransfer.Indication.class);
    }
}
