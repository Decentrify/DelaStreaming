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
package se.sics.dela.network.ledbat;

import se.sics.kompics.util.Identifiable;
import se.sics.kompics.util.Identifier;
import se.sics.ktoolbox.nutil.network.portsv2.SelectableEventV2;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public interface LedbatEvent extends Identifiable, SelectableEventV2 {

  public static final String EVENT_TYPE = "LEDBAT_EVENT";

  public Identifier rivuletId();
  
  public Identifier dataId();

  public static abstract class Basic implements LedbatEvent {

    public final Identifier id;
    public final Identifier rivuletId;
    public final Identifier dataId;

    public Basic(Identifier id, Identifier rivuletId, Identifier dataId) {
      this.id = id;
      this.rivuletId = rivuletId;
      this.dataId = dataId;
    }

    @Override
    public Identifier getId() {
      return id;
    }

    @Override
    public String eventType() {
      return EVENT_TYPE;
    }

    @Override
    public Identifier rivuletId() {
      return rivuletId;
    }
    
    @Override
    public Identifier dataId() {
      return dataId;
    }
  }
}
