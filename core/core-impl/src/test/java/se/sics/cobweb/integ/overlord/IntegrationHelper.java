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
package se.sics.cobweb.integ.overlord;

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
import se.sics.cobweb.conn.ConnComp;
import se.sics.cobweb.conn.MockConnNetwork;
import se.sics.cobweb.mngr.SpiderComp;
import se.sics.cobweb.mngr.SpiderCtrlPort;
import se.sics.cobweb.overlord.OverlordComp;
import se.sics.cobweb.overlord.OverlordCreator;
import se.sics.cobweb.overlord.OverlordHelper;
import se.sics.cobweb.overlord.conn.api.ConnectionDecider;
import se.sics.cobweb.overlord.conn.impl.ConnectionDeciderImpl;
import se.sics.cobweb.ports.CroupierPortHelper;
import se.sics.cobweb.ports.SpiderCtrlPortHelper;
import se.sics.cobweb.ports.TransferMngrPortHelper;
import se.sics.cobweb.transfer.handlemngr.HandleMngrComp;
import se.sics.cobweb.transfer.mngr.TransferMngrComp;
import se.sics.cobweb.transfer.mngr.TransferMngrHelper;
import se.sics.cobweb.transfer.mngr.TransferMngrPort;
import se.sics.kompics.Component;
import se.sics.kompics.Init;
import se.sics.kompics.network.Network;
import se.sics.kompics.testkit.TestContext;
import se.sics.ktoolbox.croupier.CroupierPort;
import se.sics.ktoolbox.util.network.KAddress;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class IntegrationHelper {

  public static Component connectParentSpider(TestContext tc, KAddress selfAdr) {
    ConnComp.Creator connComp = ConnComp.DEFAULT_CREATOR;
    HandleMngrComp.Creator handleMngrComp = HandleMngrComp.DEFAULT_CREATOR;
    TransferMngrComp.Creator transferMngrComp = TransferMngrHelper.testingCreator(tc);
    OverlordCreator overlordComp = OverlordHelper.getCUTCreator(tc);
    ConnectionDecider.SeederSide connSeederSide = new ConnectionDeciderImpl.SeederSide();
    ConnectionDecider.LeecherSide connLeecherSide = new ConnectionDeciderImpl.LeecherSide();
    SpiderComp.Init init = new SpiderComp.Init(selfAdr, connSeederSide, connLeecherSide, connComp, handleMngrComp, transferMngrComp, overlordComp);
    Component comp = tc.create(SpiderComp.class, init);

    return comp;
  }
  
  public static Component connectSpider(TestContext tc, KAddress selfAdr) {
    ConnComp.Creator connComp = ConnComp.DEFAULT_CREATOR;
    HandleMngrComp.Creator handleMngrComp = HandleMngrComp.DEFAULT_CREATOR;
    TransferMngrComp.Creator transferMngrComp = TransferMngrComp.DEFAULT_CREATOR;
    OverlordCreator overlordComp = OverlordComp.DEFAULT_CREATOR;
    ConnectionDecider.SeederSide connSeederSide = new ConnectionDeciderImpl.SeederSide();
    ConnectionDecider.LeecherSide connLeecherSide = new ConnectionDeciderImpl.LeecherSide();
    SpiderComp.Init init = new SpiderComp.Init(selfAdr, connSeederSide, connLeecherSide, connComp, handleMngrComp, transferMngrComp, overlordComp);
    Component comp = tc.create(SpiderComp.class, init);
    return comp;
  }

  public static Component connectDriver(TestContext tc, Component spider, ReflectPortFunc reflectPort, Triplet onStart,
    LinkedList scenario) {
    final List<Pair<Class, List<Class>>> positivePorts = new LinkedList<>();
    final List<Pair<Class, List<Class>>> negativePorts = new LinkedList<>();

    negativePorts.add((Pair) Pair.with(CroupierPort.class, CroupierPortHelper.request()));
    positivePorts.add((Pair) Pair.with(TransferMngrPort.class, TransferMngrPortHelper.indication()));
    positivePorts.add((Pair) Pair.with(SpiderCtrlPort.class, SpiderCtrlPortHelper.indication()));

    HashMap markers = new HashMap();
    Set<Class> reflectEvents = new HashSet<>();
    Component driver = tc.create(MockScenarioComp.class, new MockScenarioComp.Init(positivePorts, negativePorts,
      onStart, scenario, markers, reflectEvents));
    tc.connect(spider.getPositive(SpiderCtrlPort.class), driver.getNegative(SpiderCtrlPort.class));
    tc.connect(spider.getPositive(TransferMngrPort.class), driver.getNegative(TransferMngrPort.class));
    tc.connect(spider.getNegative(CroupierPort.class), driver.getPositive(CroupierPort.class));

    reflectPort.setPort(driver.getNegative(ReflectPort.class));
    return driver;
  }

  public static void connectNetwork(TestContext tc, Component comp1, Component comp2) {
    //cross connecting networks - shortcircuit
    Component net1 = tc.create(MockConnNetwork.class, Init.NONE);
    Component net2 = tc.create(MockConnNetwork.class, Init.NONE);
    ((MockConnNetwork) net1.getComponent()).setPair((MockConnNetwork) net2.getComponent());
    ((MockConnNetwork) net2.getComponent()).setPair((MockConnNetwork) net1.getComponent());
    tc.connect(comp1.getNegative(Network.class), net1.getPositive(Network.class));
    tc.connect(comp2.getNegative(Network.class), net2.getPositive(Network.class));
  }
}
