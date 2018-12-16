/*
 * Copyright (C) 2009 Swedish Institute of Computer Science (SICS) Copyright (C)
 * 2009 Royal Institute of Technology (KTH)
 *
 * KompicsToolbox is free software; you can redistribute it and/or
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
package se.sics.dela.workers.test;

import java.io.File;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.UnknownHostException;
import java.util.Optional;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import se.sics.kompics.Init;
import se.sics.kompics.Kompics;
import se.sics.kompics.util.Identifier;
import se.sics.ktoolbox.util.identifiable.BasicBuilders;
import se.sics.ktoolbox.util.identifiable.IdentifierRegistryV2;
import se.sics.ktoolbox.util.identifiable.basic.IntIdFactory;
import se.sics.ktoolbox.util.network.KAddress;
import se.sics.ktoolbox.util.network.basic.BasicAddress;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class DelaWorkersTest {

  static String workMngrConfigFile;
  static String workCtrlConfigFile1;
  static String workCtrlConfigFile2;

  @BeforeClass
  public static void setup() throws MalformedURLException {
    String testDir = System.getProperty("user.dir")
      + File.separator + "src"
      + File.separator + "test"
      + File.separator + "resources"
      + File.separator + "se"
      + File.separator + "sics"
      + File.separator + "dela"
      + File.separator + "workers"
      + File.separator + "simple";
    String workMngrConfigFileAux = testDir
      + File.separator + "workMngr.conf";
    workMngrConfigFile = new File(workMngrConfigFileAux).getAbsolutePath();
    String workCtrlConfigFileAux1 = testDir
      + File.separator + "workCtrl1.conf";
    workCtrlConfigFile1 = new File(workCtrlConfigFileAux1).getAbsolutePath();
    String workCtrlConfigFileAux2 = testDir
      + File.separator + "workCtrl2.conf";
    workCtrlConfigFile2 = new File(workCtrlConfigFileAux2).getAbsolutePath();
    IdentifierRegistryV2.registerBaseDefaults1(64, 1234l);
  }

//  @Ignore //run manually 
  @Test
  public void testSimpleProxy() throws UnknownHostException {
    IntIdFactory ids = new IntIdFactory(Optional.empty());

    Identifier id1 = ids.id(new BasicBuilders.IntBuilder(1));
    Identifier id2 = ids.id(new BasicBuilders.IntBuilder(2));
    Identifier id3 = ids.id(new BasicBuilders.IntBuilder(3));
    KAddress workerMngrAdr = new BasicAddress(InetAddress.getLocalHost(), 20000, id1);
    KAddress workerCtrlAdr1 = new BasicAddress(InetAddress.getLocalHost(), 30001, id2);
    KAddress workerCtrlAdr2 = new BasicAddress(InetAddress.getLocalHost(), 30002, id3);

    Init init = new se.sics.dela.workers.simple.HostComp.Init(workMngrConfigFile, workCtrlConfigFile1, workCtrlConfigFile2, 
      workerMngrAdr, workerCtrlAdr1, workerCtrlAdr2);
    if (Kompics.isOn()) {
      Kompics.shutdown();
    }
    // Yes 20 is totally arbitrary
    Kompics.createAndStart(se.sics.dela.workers.simple.HostComp.class, init,
      Runtime.getRuntime().availableProcessors(), 20);
    try {
      Kompics.waitForTermination();
    } catch (InterruptedException ex) {
      System.exit(1);
    }
  }
}
