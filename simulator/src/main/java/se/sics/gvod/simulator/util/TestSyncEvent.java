/*
 * Copyright (C) 2016 Swedish Institute of Computer Science (SICS) Copyright (C)
 * 2016 Royal Institute of Technology (KTH)
 *
 * Dozy is free software; you can redistribute it and/or
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
package se.sics.gvod.simulator.util;

import com.google.common.util.concurrent.SettableFuture;
import se.sics.kompics.KompicsEvent;
import se.sics.ktoolbox.util.identifiable.Identifiable;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class TestSyncEvent<E extends KompicsEvent & Identifiable> implements KompicsEvent {
    public final E req;
    public final SettableFuture result;
    public final long timeout;
    
    public TestSyncEvent(E req, SettableFuture result, long timeout) {
        this.req = req;
        this.result = result;
        this.timeout = timeout;
    }
}
