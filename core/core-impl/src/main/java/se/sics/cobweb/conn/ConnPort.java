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
package se.sics.cobweb.conn;

import se.sics.cobweb.conn.event.ConnE;
import se.sics.kompics.PortType;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class ConnPort extends PortType {
  {
    indication(ConnE.SeederSample.class);
    request(ConnE.Connect1Request.class);
    indication(ConnE.Connect1Accept.class);
    indication(ConnE.Connect2Request.class);
    request(ConnE.Connect2Accept.class);
  }
}
