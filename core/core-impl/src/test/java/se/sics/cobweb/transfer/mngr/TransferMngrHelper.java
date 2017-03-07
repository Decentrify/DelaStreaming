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
package se.sics.cobweb.transfer.mngr;

import com.google.common.base.Predicate;
import se.sics.cobweb.transfer.TransferComp;
import se.sics.cobweb.transfer.handle.LeecherHandlePort;
import se.sics.cobweb.transfer.handle.SeederHandlePort;
import se.sics.kompics.Component;
import se.sics.kompics.ComponentProxy;
import se.sics.kompics.Negative;
import se.sics.kompics.Positive;
import se.sics.kompics.testkit.TestContext;
import se.sics.kompics.testkit.Testkit;
import se.sics.ktoolbox.nutil.fsm.MultiFSM;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayId;
import se.sics.ktoolbox.util.network.KAddress;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class TransferMngrHelper {

  public static TestContext<TransferMngrComp> getContext(KAddress selfAdr) {
    TransferComp.Creator transferCreator = TransferComp.DEFAULT_CREATOR;
    TransferMngrComp.Init init = new TransferMngrComp.Init(selfAdr, transferCreator);
    TestContext<TransferMngrComp> context = Testkit.newTestContext(TransferMngrComp.class, init);
    return context;
  }

  public static Predicate<TransferMngrComp> emptyState() {
    return new Predicate<TransferMngrComp>() {
      @Override
      public boolean apply(TransferMngrComp t) {
        return t.isEmpty();
      }
    };
  }

  public static Predicate<TransferMngrComp> inspectState(final OverlayId torrentId,
    final TransferMngrStates expectedState) {
    return new Predicate<TransferMngrComp>() {
      @Override
      public boolean apply(TransferMngrComp comp) {
        MultiFSM mfsm = comp.getTransferFSM();
        return expectedState.equals(mfsm.getFSMState(TransferMngrFSM.NAME, torrentId.baseId));
      }
    };
  }

  public static TransferMngrComp.Creator testingCreator(final TestContext tc) {
    return new TransferMngrComp.Creator() {

      @Override
      public Component create(ComponentProxy proxy, KAddress selfAdr) {
        TransferMngrComp.Init init = new TransferMngrComp.Init(selfAdr, TransferComp.DEFAULT_CREATOR);
        Component comp = tc.create(TransferMngrComp.class, init);
        return comp;
      }

      @Override
      public void connect(ComponentProxy proxy, Component transferMngrComp,
        Positive<LeecherHandlePort> leecherHandlePort, Positive<SeederHandlePort> seederHandlePort,
        Negative<TransferMngrPort> transferMngrPort) {
        tc.connect(transferMngrComp.getNegative(LeecherHandlePort.class), leecherHandlePort);
        tc.connect(transferMngrComp.getNegative(SeederHandlePort.class), seederHandlePort);
        tc.connect(transferMngrPort, transferMngrComp.getPositive(TransferMngrPort.class));
      }

      @Override
      public void start(ComponentProxy proxy, Component transferMngrComp) {
      }
    };
  }
}
