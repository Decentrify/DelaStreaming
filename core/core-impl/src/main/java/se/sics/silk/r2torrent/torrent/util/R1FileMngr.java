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
package se.sics.silk.r2torrent.torrent.util;

import java.util.HashMap;
import java.util.Map;
import se.sics.kompics.util.Identifier;
import se.sics.nstream.StreamId;
import se.sics.nstream.storage.durable.util.MyStream;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class R1FileMngr {
  public final Map<Identifier, R1File> files = new HashMap<>();
  
  public void completed(Identifier fileId, StreamId streamId, MyStream stream) {
    files.put(fileId, new R1File(streamId, stream));
  }
  
  public boolean isComplete(Identifier fileId) {
    return files.containsKey(fileId);
  }
  
  public StreamId streamId(Identifier fileId) {
    return files.get(fileId).streamId;
  }
  
  public MyStream stream(Identifier fileId) {
    return files.get(fileId).stream;
  }
}
