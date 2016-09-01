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

    private static class OngoingGear implements ReqMngr {
        private final int gearSize;
        private int ongoing;
        private OngoingCog current;
        private final List<OngoingCog> cogs;
        
        public OngoingGear(int gearSize) {
            this.gearSize = gearSize;
            this.cogs = new LinkedList<>();
        }

        public int ongoing() {
            return ongoing;
        }
        //*******************************ReqMngr********************************
        @Override
        public void success(Req req) {
            ongoing--;
        }
    }

    private static interface ReqMngr {

        public void success(Req req);
    }

    private static class OngoingCog implements ReqMngr {
        private final ReqMngr reqMngr;
        private int ongoing;
        private final List<Req> requests;

        private OngoingCog(ReqMngr reqMngr) {
            this.reqMngr = reqMngr;
            this.ongoing = 0;
            this.requests = new LinkedList<>();
        }

        public void scheduleReq(KompicsEvent event) {
            requests.add(new Req(this, State.ONGOING, event));
            ongoing++;
        }

        public List<Req> tick() {
            List<Req> suspected = new LinkedList<>();

            for (Req req : requests) {
                if (!State.SUCCESS.equals(req.state)) {
                    req.suspect();
                    suspected.add(req);
                }
            }
            requests.clear();
            return suspected;
        }

        //*******************************ReqMngr********************************
        @Override
        public void success(Req req) {
            ongoing--;
            //we don't clean the req now, we clean successfull requests on tick
            reqMngr.success(req);
        }
    }

    private static class Req {

        private State state;
        private final ReqMngr cog;
        private final KompicsEvent payload;

        public Req(ReqMngr cog, State state, KompicsEvent payload) {
            this.state = state;
            this.cog = cog;
            this.payload = payload;
        }

        public void success() {
            this.state = State.SUCCESS;
            cog.success(this);
        }

        public void suspect() {
            this.state = State.SUSPECTED;
        }
    }

    private static enum State {

        ONGOING, SUCCESS, SUSPECTED
    }
}
