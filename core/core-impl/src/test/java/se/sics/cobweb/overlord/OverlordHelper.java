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
package se.sics.cobweb.overlord;

import com.google.common.base.Predicate;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import org.javatuples.Pair;
import org.javatuples.Triplet;
import se.sics.cobweb.MockScenarioComp;
import se.sics.cobweb.ReflectPort;
import se.sics.cobweb.ReflectPortFunc;
import se.sics.cobweb.conn.ConnPort;
import se.sics.cobweb.overlord.conn.api.ConnectionDecider;
import se.sics.cobweb.overlord.conn.impl.ConnectionDeciderImpl;
import se.sics.cobweb.overlord.handle.LeecherHandleOverlordFSM;
import se.sics.cobweb.overlord.handle.LeecherHandleOverlordStates;
import se.sics.cobweb.overlord.handle.SeederHandleOverlordFSM;
import se.sics.cobweb.overlord.handle.SeederHandleOverlordStates;
import se.sics.cobweb.ports.ConnPortHelper;
import se.sics.cobweb.ports.HandleMngrPortHelper;
import se.sics.cobweb.ports.TransferPortHelper;
import se.sics.cobweb.transfer.handlemngr.HandleMngrPort;
import se.sics.cobweb.transfer.instance.TransferPort;
import se.sics.cobweb.util.HandleId;
import se.sics.kompics.Component;
import se.sics.kompics.ComponentProxy;
import se.sics.kompics.Positive;
import se.sics.kompics.testkit.TestContext;
import se.sics.kompics.testkit.Testkit;
import se.sics.ktoolbox.nutil.fsm.MultiFSM;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayId;
import se.sics.ktoolbox.util.network.KAddress;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class OverlordHelper {

  public static TestContext<OverlordComp> getContext(OverlayId torrentId, KAddress selfAdr) {
    ConnectionDeciderImpl.SeederSide seederSide = new ConnectionDeciderImpl.SeederSide();
    ConnectionDeciderImpl.LeecherSide leecherSide = new ConnectionDeciderImpl.LeecherSide();
    OverlordComp.Init init = new OverlordComp.Init(selfAdr, seederSide, leecherSide);
    TestContext<OverlordComp> context = Testkit.newTestContext(OverlordComp.class, init);
    return context;
  }
  
  public static Component connectOverlord(TestContext tc, OverlayId torrentId, KAddress selfAdr) {
    ConnectionDeciderImpl.SeederSide seederSide = new ConnectionDeciderImpl.SeederSide();
    ConnectionDeciderImpl.LeecherSide leecherSide = new ConnectionDeciderImpl.LeecherSide();
    OverlordComp.Init init = new OverlordComp.Init(selfAdr, seederSide, leecherSide);
    Component overlord = tc.create(OverlordComp.class, init);
    return overlord;
  }

  public static Component connectDriver(TestContext tc, Component overlord, ReflectPortFunc reflectPort, Triplet onStart,
    LinkedList scenario) {
    final List<Pair<Class, List<Class>>> positivePorts = new LinkedList<>();
    final List<Pair<Class, List<Class>>> negativePorts = new LinkedList<>();

    negativePorts.add((Pair) Pair.with(ConnPort.class, ConnPortHelper.request()));
    negativePorts.add((Pair) Pair.with(HandleMngrPort.class, HandleMngrPortHelper.request()));
    negativePorts.add((Pair) Pair.with(TransferPort.class, TransferPortHelper.request()));

    HashMap markers = new HashMap();
    Set<Class> reflectEvents = new HashSet<>();
    Component driver = tc.create(MockScenarioComp.class, new MockScenarioComp.Init(positivePorts, negativePorts,
      onStart, scenario, markers, reflectEvents));
    tc.connect(overlord.getNegative(ConnPort.class), driver.getPositive(ConnPort.class));
    tc.connect(overlord.getNegative(HandleMngrPort.class), driver.getPositive(HandleMngrPort.class));
    tc.connect(overlord.getNegative(TransferPort.class), driver.getPositive(TransferPort.class));

    reflectPort.setPort(driver.getNegative(ReflectPort.class));
    return driver;
  }

  public static Predicate<OverlordComp> inspectLeecherHandleState(final HandleId seederHandleId,
    final LeecherHandleOverlordStates expectedState) {
    return new Predicate<OverlordComp>() {
      @Override
      public boolean apply(OverlordComp comp) {
        MultiFSM mfsm = comp.getLeecherHandleFSMs();
        return expectedState.equals(mfsm.getFSMState(LeecherHandleOverlordFSM.NAME, seederHandleId));
      }
    };
  }

  public static Predicate<OverlordComp> inspectSeederHandleState(final HandleId leecherHandleId,
    final SeederHandleOverlordStates expectedState) {
    return new Predicate<OverlordComp>() {
      @Override
      public boolean apply(OverlordComp comp) {
        MultiFSM mfsm = comp.getSeederHandleFSMs();
        return expectedState.equals(mfsm.getFSMState(SeederHandleOverlordFSM.NAME, leecherHandleId));
      }
    };
  }

  public static Predicate<OverlordComp> inspectLeecherHandleSize(final int expectedSize) {
    return new Predicate<OverlordComp>() {
      @Override
      public boolean apply(OverlordComp comp) {
        MultiFSM mfsm = comp.getLeecherHandleFSMs();
        return expectedSize == mfsm.size();
      }
    };
  }

  public static Predicate<OverlordComp> inspectSeederHandleSize(final int expectedSize) {
    return new Predicate<OverlordComp>() {
      @Override
      public boolean apply(OverlordComp comp) {
        MultiFSM mfsm = comp.getSeederHandleFSMs();
        return expectedSize == mfsm.size();
      }
    };
  }

  public static OverlordCreator getCUTCreator(final TestContext tc) {
    return new OverlordCreator() {

      @Override
      public Component create(ComponentProxy proxy, KAddress selfAdr, ConnectionDecider.SeederSide seederSideDecider,
        ConnectionDecider.LeecherSide leecherSideDecider) {
        return tc.getComponentUnderTest();
      }

      @Override
      public void start(ComponentProxy proxy, Component component) {
        //nothing - tc deals with this
      }

      @Override
      public void connect(ComponentProxy proxy, Component overlord, Positive<ConnPort> connPort,
        Positive<HandleMngrPort> handleMngrPort, Positive<TransferPort> transferPort) {
        tc.connect(overlord.getNegative(ConnPort.class), connPort);
        tc.connect(overlord.getNegative(HandleMngrPort.class), handleMngrPort);
        tc.connect(overlord.getNegative(TransferPort.class), transferPort);
      }
    };
  }
}
