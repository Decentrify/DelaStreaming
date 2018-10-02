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
package se.sics.dela.channel.resource;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class ChannelResources {

  public static class Fixed implements ChannelResource {

    private final int maxSlots;
    private int usedSlots;

    public Fixed(int maxSlots) {
      this.maxSlots = maxSlots;
      this.usedSlots = 0;

    }

    @Override
    public State getState() {
      if(maxSlots == usedSlots) {
        return State.SATURATED;
      } else if(maxSlots < usedSlots) {
        return State.LOW;
      } else {
        return State.HIGH;
      }
    }

    @Override
    public void reserveSlot() {
      usedSlots++;
    }

    @Override
    public void releaseSlot() {
      usedSlots--;
    }

    @Override
    public void releaseAllSlots() {
      usedSlots = 0;
    }
  }
}
