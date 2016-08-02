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
package se.sics.nstream.torrent;

import java.util.LinkedList;
import java.util.List;
import se.sics.ktoolbox.util.network.KAddress;

/**
 *
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class RoundRobinConnMngr implements Router {

    private int slots = 100;
    private final LinkedList<KAddress> partners = new LinkedList<>();

    public RoundRobinConnMngr(List<KAddress> partners) {
        this.partners.addAll(partners);
    }

    @Override
    public KAddress randomPartner() {
        KAddress first = partners.removeFirst();
        partners.add(first);
        return first;
    }
    
    @Override
    public boolean hasSlot() {
        return slots > 0;
    }

    @Override
    public void retainSlot() {
        slots--;
    }
    
    @Override
    public void retainSlots(int n) {
        slots = slots - n;
    }

    @Override
    public void releaseSlot() {
        slots++;
    }

    @Override
    public void timeoutSlot(KAddress target) {
        slots--;
    }
}
