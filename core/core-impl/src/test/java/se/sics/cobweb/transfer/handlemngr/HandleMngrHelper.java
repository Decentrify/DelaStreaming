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
package se.sics.cobweb.transfer.handlemngr;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import org.javatuples.Pair;
import org.javatuples.Triplet;
import se.sics.cobweb.MockNetwork;
import se.sics.cobweb.MockScenarioComp;
import se.sics.cobweb.ReflectPort;
import se.sics.cobweb.ReflectPortFunc;
import se.sics.cobweb.ports.HandleMngrPortHelper;
import se.sics.cobweb.ports.LeecherHandleCtrlPortHelper;
import se.sics.cobweb.ports.LeecherHandlePortHelper;
import se.sics.cobweb.ports.SeederHandleCtrlPortHelper;
import se.sics.cobweb.ports.SeederHandlePortHelper;
import se.sics.cobweb.transfer.handle.LeecherHandleCtrlPort;
import se.sics.cobweb.transfer.handle.LeecherHandlePort;
import se.sics.cobweb.transfer.handle.SeederHandleCtrlPort;
import se.sics.cobweb.transfer.handle.SeederHandlePort;
import se.sics.cobweb.util.HandleId;
import se.sics.kompics.Channel;
import se.sics.kompics.Component;
import se.sics.kompics.ComponentProxy;
import se.sics.kompics.Init;
import se.sics.kompics.KompicsEvent;
import se.sics.kompics.Negative;
import se.sics.kompics.network.Msg;
import se.sics.kompics.network.Network;
import se.sics.kompics.testkit.TestContext;
import se.sics.kompics.testkit.Testkit;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayId;
import se.sics.ktoolbox.util.network.KAddress;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class HandleMngrHelper {

  public static TestContext<HandleMngrComp> getContext(KAddress selfAdr,
    LeecherHandleCreator leecher, SeederHandleCreator seeder) {
    HandleMngrComp.Init init = new HandleMngrComp.Init(selfAdr, leecher, seeder);
    TestContext<HandleMngrComp> context = Testkit.newTestContext(HandleMngrComp.class, init);
    return context;
  }

  public static Component connectDriver(TestContext tc, Component handleMngr, ReflectPortFunc reflectPort,
    Triplet onStart,
    LinkedList scenario) {
    final List<Pair<Class, List<Class>>> positivePorts = new LinkedList<>();
    final List<Pair<Class, List<Class>>> negativePorts = new LinkedList<>();

    positivePorts.add((Pair) Pair.with(HandleMngrPort.class, HandleMngrPortHelper.indication()));
    positivePorts.add((Pair) Pair.with(LeecherHandlePort.class, LeecherHandlePortHelper.indication()));
    positivePorts.add((Pair) Pair.with(LeecherHandleCtrlPort.class, LeecherHandleCtrlPortHelper.indication()));
    positivePorts.add((Pair) Pair.with(Network.class, new LinkedList<>()));

    HashMap markers = new HashMap();
    Set<Class> reflectEvents = new HashSet<>();
    Component driver = tc.create(MockScenarioComp.class, new MockScenarioComp.Init(positivePorts, negativePorts,
      onStart, scenario, markers, reflectEvents));
    tc.connect(handleMngr.getPositive(HandleMngrPort.class), driver.getNegative(HandleMngrPort.class));
    tc.connect(handleMngr.getPositive(LeecherHandlePort.class), driver.getNegative(LeecherHandlePort.class));
    tc.connect(handleMngr.getPositive(LeecherHandleCtrlPort.class), driver.getNegative(LeecherHandleCtrlPort.class));

    reflectPort.setPort(driver.getNegative(ReflectPort.class));
    return driver;
  }

  public static void setNetwork(TestContext tc, Component handleMngr, Component driver) {
    //cross connecting networks - shortcircuit
    Component net1 = tc.create(MockNetwork.class, Init.NONE);
    Component net2 = tc.create(MockNetwork.class, Init.NONE);
    ((MockNetwork) net1.getComponent()).setPair((MockNetwork) net2.getComponent());
    ((MockNetwork) net2.getComponent()).setPair((MockNetwork) net1.getComponent());
    tc.connect(handleMngr.getNegative(Network.class), net1.getPositive(Network.class));
    tc.connect(driver.getNegative(Network.class), net2.getPositive(Network.class));
  }

  public static LeecherHandleCreator getMockLeecherHandle(final Function<Boolean, Negative<ReflectPort>> reflectPort,
    final Set<Class> reflectEvents) {
    final List<Pair<Class, List<Class>>> positivePorts = new LinkedList<>();
    final List<Pair<Class, List<Class>>> negativePorts = new LinkedList<>();

    negativePorts.add((Pair) Pair.with(LeecherHandlePort.class, LeecherHandlePortHelper.request()));
    negativePorts.add((Pair) Pair.with(LeecherHandleCtrlPort.class, LeecherHandleCtrlPortHelper.request()));

    List<Class> networkEvents = new LinkedList<>();
    networkEvents.add(Msg.class);
    positivePorts.add((Pair) Pair.with(Network.class, networkEvents));

    return new LeecherHandleCreator() {

      @Override
      public Component connect(ComponentProxy proxy, OverlayId torrentId, HandleId handleId, KAddress selfAdr,
        KAddress seederAdr, int startingActiveBlocks) {
        Triplet onStart = null;
        LinkedList nextAction = new LinkedList();
        HashMap<Class, Predicate<KompicsEvent>> markers = new HashMap<>();

        MockScenarioComp.Init init = new MockScenarioComp.Init(positivePorts, negativePorts, onStart, nextAction,
          markers, reflectEvents);
        Component comp = proxy.create(MockScenarioComp.class, init);
        proxy.connect(comp.getPositive(ReflectPort.class), reflectPort.apply(true), Channel.TWO_WAY);
        return comp;
      }
    };
  }

  public static SeederHandleCreator getMockSeederHandle(final Function<Boolean, Negative<ReflectPort>> reflectPort,
    final Set<Class> reflectEvents) {
    final List<Pair<Class, List<Class>>> positivePorts = new LinkedList<>();
    final List<Pair<Class, List<Class>>> negativePorts = new LinkedList<>();

    negativePorts.add((Pair) Pair.with(SeederHandlePort.class, SeederHandlePortHelper.request()));
    negativePorts.add((Pair) Pair.with(SeederHandleCtrlPort.class, SeederHandleCtrlPortHelper.request()));

    List<Class> networkEvents = new LinkedList<>();
    networkEvents.add(Msg.class);
    positivePorts.add((Pair) Pair.with(Network.class, networkEvents));

    return new SeederHandleCreator() {

      @Override
      public Component connect(ComponentProxy proxy, OverlayId torrentId, HandleId handleId, KAddress selfAdr, KAddress leecherAdr) {
        Triplet onStart = null;
        LinkedList nextAction = new LinkedList();
        HashMap<Class, Predicate<KompicsEvent>> markers = new HashMap<>();

        MockScenarioComp.Init init = new MockScenarioComp.Init(positivePorts, negativePorts, onStart, nextAction,
          markers, reflectEvents);
        Component comp = proxy.create(MockScenarioComp.class, init);
        proxy.connect(comp.getPositive(ReflectPort.class), reflectPort.apply(true), Channel.TWO_WAY);
        return comp;
      }
    };
  }

  public static Predicate<HandleMngrComp> inspectState(final int expectedSize) {
    return new Predicate<HandleMngrComp>() {
      @Override
      public boolean apply(HandleMngrComp comp) {
        return expectedSize == comp.size();
      }
    };
  }
}
