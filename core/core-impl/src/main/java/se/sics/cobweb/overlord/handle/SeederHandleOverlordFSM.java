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
package se.sics.cobweb.overlord.handle;

import com.google.common.base.Optional;
import java.util.Set;
import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.cobweb.conn.ConnPort;
import se.sics.cobweb.conn.event.ConnE;
import se.sics.cobweb.transfer.handle.SeederHandleCtrlPort;
import se.sics.cobweb.transfer.handle.event.SeederHandleCtrlE;
import se.sics.cobweb.transfer.handlemngr.HandleMngrPort;
import se.sics.cobweb.transfer.handlemngr.event.HandleMngrE;
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
public class SeederHandleOverlordFSM {

  private static final Logger LOG = LoggerFactory.getLogger(SeederHandleOverlordFSM.class);
  public static final String NAME = "torrent-overlord-seeder-handle-fsm";

  public static FSMachineDef fsm() throws FSMException {
    Pair<FSMBuilder.Machine, FSMBuilder.Handlers> fp = prepare();
    FSMachineDef m = FSMBuilder.fsmDef(NAME, fp.getValue0(), fp.getValue1());
    return m;
  }

  public static MultiFSM multifsm(SeederHandleOverlordExternal es, SeederHandleOverlordInternal.Builder isb, OnFSMExceptionAction oexa)
    throws FSMException {
    Pair<FSMBuilder.Machine, FSMBuilder.Handlers> fp = prepare();

    FSMIdExtractor<SeederHandleOverlordFSMEvent> fsmIdExtractor = new FSMIdExtractor<SeederHandleOverlordFSMEvent>() {
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
      public Optional<FSMId> fromEvent(FSMDefId fsmdId, SeederHandleOverlordFSMEvent event) throws FSMException {
        if (events.contains(event.getClass()) || positiveNetworkMsgs.contains(event.getClass())
          || negativeNetworkMsgs.contains(event.getClass())) {
          return Optional.of(fsmdId.getFSMId(event.getSHOId()));
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
        .nextStates(SeederHandleOverlordStates.SETUP_HANDLE)
        .buildTransition()
      .onState(SeederHandleOverlordStates.SETUP_HANDLE)
        .nextStates(SeederHandleOverlordStates.SETUP_HANDLE, SeederHandleOverlordStates.ACTIVE)
        .buildTransition()
      .onState(SeederHandleOverlordStates.ACTIVE)
        .toFinal()
        .buildTransition();

    FSMBuilder.Handlers handlers = FSMBuilder.events()
      .defaultFallback(SeederHandleOverlordHandlers.handleEventFallback, SeederHandleOverlordHandlers.handleMsgFallback)
      .positivePort(ConnPort.class)
        .onEvent(ConnE.Connect2Request.class)
          .subscribe(SeederHandleOverlordHandlers.handleLeecherConnectTry, FSMBasicStateNames.START)
        .buildEvents()
      .positivePort(HandleMngrPort.class)
        .onEvent(HandleMngrE.SeederConnected.class)
          .subscribe(SeederHandleOverlordHandlers.handleSetupSeeder, SeederHandleOverlordStates.SETUP_HANDLE)
        .buildEvents()
      .positivePort(SeederHandleCtrlPort.class)
        .onEvent(SeederHandleCtrlE.Ready.class)
          .subscribe(SeederHandleOverlordHandlers.handleReadySeeder, SeederHandleOverlordStates.SETUP_HANDLE)
        .buildEvents();
    return Pair.with(machine, handlers);
  }
}
