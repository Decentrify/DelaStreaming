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

import com.google.common.base.Optional;
import java.util.Set;
import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.cobweb.conn.event.ConnE;
import se.sics.cobweb.conn.msg.CHandleMsg;
import se.sics.ktoolbox.nutil.fsm.FSMBuilder;
import se.sics.ktoolbox.nutil.fsm.FSMachineDef;
import se.sics.ktoolbox.nutil.fsm.MultiFSM;
import se.sics.ktoolbox.nutil.fsm.api.FSMBasicStateNames;
import se.sics.ktoolbox.nutil.fsm.api.FSMException;
import se.sics.ktoolbox.nutil.fsm.api.FSMIdExtractor;
import se.sics.ktoolbox.nutil.fsm.genericsetup.OnFSMExceptionAction;
import se.sics.ktoolbox.nutil.fsm.ids.FSMDefId;
import se.sics.ktoolbox.nutil.fsm.ids.FSMId;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class CLeecherHandleFSM {

  private static final Logger LOG = LoggerFactory.getLogger(CLeecherHandleFSM.class);
  public static final String NAME = "torrent-leecher-conn-fsm";

  public static FSMachineDef fsm() throws FSMException {
    Pair<FSMBuilder.Machine, FSMBuilder.Handlers> fp = prepare();
    FSMachineDef m = FSMBuilder.fsmDef(NAME, fp.getValue0(), fp.getValue1());
    return m;
  }

  public static MultiFSM multifsm(CLeecherHandleExternal es, CLeecherHandleInternal.Builder isb, OnFSMExceptionAction oexa) throws FSMException {
    Pair<FSMBuilder.Machine, FSMBuilder.Handlers> fp = prepare();

    FSMIdExtractor<CLeecherHandleFSMEvent> fsmIdExtractor = new FSMIdExtractor<CLeecherHandleFSMEvent>() {
      private Set<Class> events;
      private Set<Class> positiveNetworkMsgs;
      private Set<Class> negativeNetworkMsgs;

      @Override
      public void set(Set<Class> events, Set<Class> positiveNetworkMsgs, Set<Class> negativeNetworkMsgs) {
        this.events = events;
        this.positiveNetworkMsgs = positiveNetworkMsgs;
        this.negativeNetworkMsgs = negativeNetworkMsgs;
      }

      @Override
      public Optional<FSMId> fromEvent(FSMDefId fsmdId, CLeecherHandleFSMEvent event) throws FSMException {
        if (events.contains(event.getClass()) || positiveNetworkMsgs.contains(event.getClass())
          || negativeNetworkMsgs.contains(event.getClass())) {
          return Optional.of(fsmdId.getFSMId(event.clhFSMId()));
        }

        return Optional.absent();
      }
    };
    MultiFSM fsm = FSMBuilder.multiFSM(NAME, fp.getValue0(), fp.getValue1(), es, isb, oexa, fsmIdExtractor);

    return fsm;
  }

  private static Pair<FSMBuilder.Machine, FSMBuilder.Handlers> prepare() throws FSMException {

    FSMBuilder.Machine machine = FSMBuilder.machine()
      .onState(FSMBasicStateNames.START)
        .nextStates(CLeecherHandleStates.WAITING_SEEDER)
        .buildTransition()
      .onState(CLeecherHandleStates.WAITING_SEEDER)
        .nextStates(CLeecherHandleStates.CONNECTED)
        .buildTransition()
      .onState(CLeecherHandleStates.CONNECTED)
        .toFinal()
        .buildTransition();

    FSMBuilder.Handlers handlers = FSMBuilder.events()
      .defaultFallback(CLeecherHandleHandlers.handleEventFallback, CLeecherHandleHandlers.handleMsgFallback)
      .negativePort(ConnPort.class)
        .onEvent(ConnE.Connect1Request.class)
          .subscribe(CLeecherHandleHandlers.handleConnectToSeeder, FSMBasicStateNames.START)
        .buildEvents()
      .positiveNetwork()
        .onMsg(CHandleMsg.Connected.class)
          .subscribe(CLeecherHandleHandlers.handleConnected, CLeecherHandleStates.WAITING_SEEDER)
        .buildMsgs()
      ;
         
    return Pair.with(machine, handlers);
  }
}
