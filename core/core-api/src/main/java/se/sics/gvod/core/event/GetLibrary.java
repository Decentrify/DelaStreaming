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
package se.sics.gvod.core.event;

import java.util.Map;
import org.javatuples.Pair;
import se.sics.gvod.common.event.GVoDEvent;
import se.sics.gvod.core.util.FileStatus;
import se.sics.gvod.core.util.ResponseStatus;
import se.sics.kompics.util.Identifier;

/**
 * @author Alex Ormenisan <aaor@sics.se>
 */
public class GetLibrary {

  public static class Request implements GVoDEvent {

    public final Identifier eventId;

    public Request(Identifier eventId) {
      this.eventId = eventId;
    }

    @Override
    public Identifier getId() {
      return eventId;
    }

    public Indication answer(ResponseStatus respStatus, Map<String, Pair<FileStatus, Identifier>> fileStatusMap) {
      return new Indication(eventId, respStatus, fileStatusMap);
    }
  }

  public static class Indication implements GVoDEvent {

    public final Identifier eventId;
    public final ResponseStatus respStatus;
    public final Map<String, Pair<FileStatus, Identifier>> fileStatusMap;

    public Indication(Identifier eventId, ResponseStatus respStatus, Map<String, Pair<FileStatus, Identifier>> fileStatusMap) {
      this.eventId = eventId;
      this.respStatus = respStatus;
      this.fileStatusMap = fileStatusMap;
    }

    @Override
    public Identifier getId() {
      return eventId;
    }
  }
}
