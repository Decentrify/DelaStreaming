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
package se.sics.nstream.util.actuator;

import java.util.LinkedList;
import java.util.List;
import se.sics.kompics.KompicsEvent;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class DownloadGear {

    private static abstract class Cog {
        private int ongoing;
        private List<Req> requests;
        
        private Cog() {
            this.ongoing = 0;
            this.requests = new LinkedList<>();
        }
        
        private void completeReq() {
            ongoing--;
        }
        
        public void scheduleReq(KompicsEvent event);
    }
    
    private static class OngoingCog extends Cog {
        @Override
        public void scheduleReq(KompicsEvent event) {
            requests.add(new Req(this, event));
            ongoing++;
        }
    }
    
    private static class Req {
        private State state;
        private final Cog cog;
        private final KompicsEvent payload;
        
        public Req(Cog cog, State state, KompicsEvent payload) {
            this.state = state;
            this.cog = cog;
            this.payload = payload;
        }
        
        public void success() {
            this.state = State.SUCCESS;
            cog.completeReq();
        }
        
        public void suspect() {
            this.state = State.SUSPECTED;
        }
    }
    
    private static enum State {
        ONGOING, SUCCESS, SUSPECTED
    }
}
