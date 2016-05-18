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
package se.sics.gvod.stream.connection;

import java.util.List;
import java.util.Map;
import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.gvod.stream.connection.event.Connection;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Handler;
import se.sics.kompics.Negative;
import se.sics.kompics.Start;
import se.sics.ktoolbox.util.network.KAddress;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class ConnMngrComp extends ComponentDefinition {
    private static final Logger LOG = LoggerFactory.getLogger(ConnMngrComp.class);
    private String logPrefix = "";
    
    //******************************CONNECTIONS*********************************
    //*************************EXTERNAL_CONNECT_TO******************************
    Negative<ConnMngrPort> connectionPort = provides(ConnMngrPort.class);
    
    private final ConnMngr connMngr;
    
    public ConnMngrComp(Init init) {
        LOG.info("{}initiating...", logPrefix);
        
        connMngr = new ConnMngr();
        connMngr.addConn(init.partners);
        
        subscribe(handleStart, control);
        subscribe(handleConnRequest, connectionPort);
    }
    
    Handler handleStart = new Handler<Start>() {
        @Override
        public void handle(Start event) {
            LOG.info("{}starting...", logPrefix);
        }
    };
    
    Handler handleConnRequest = new Handler<Connection.Request>() {
        @Override
        public void handle(Connection.Request req) {
            LOG.trace("{}received:{}", logPrefix, req);
            Pair<Map, Map> publish = connMngr.publish();
            answer(req, req.answer(publish.getValue0(), publish.getValue1()));
        }
    };
    
    public static class Init extends se.sics.kompics.Init<ConnMngrComp> {
        public final List<KAddress> partners;
        
        public Init(List<KAddress> partners) {
            this.partners = partners;
        }
    }
}
