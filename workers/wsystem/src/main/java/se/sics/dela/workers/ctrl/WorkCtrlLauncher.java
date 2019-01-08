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
package se.sics.dela.workers.ctrl;

import java.io.IOException;
import se.sics.dela.network.DelaStorageSerializerSetup;
import se.sics.dela.workers.setup.DelaWorkerSerializerSetup;
import se.sics.kompics.Init;
import se.sics.kompics.Kompics;
import se.sics.ktoolbox.croupier.CroupierSerializerSetup;
import se.sics.ktoolbox.gradient.GradientSerializerSetup;
import se.sics.ktoolbox.netmngr.NetworkMngrSerializerSetup;
import se.sics.ktoolbox.omngr.OMngrSerializerSetup;
import se.sics.ktoolbox.util.identifiable.IdentifierRegistryV2;
import se.sics.ktoolbox.util.identifiable.overlay.BaseOverlayIdentifiers;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayRegistryV2;
import se.sics.ktoolbox.util.setup.BasicSerializerSetup;
import se.sics.nat.stun.StunSerializerSetup;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class WorkCtrlLauncher {

  private static void setupSerializers() {
    int serializerId = 128;
    serializerId = BasicSerializerSetup.registerBasicSerializers(serializerId);
    serializerId = CroupierSerializerSetup.registerSerializers(serializerId);
    serializerId = GradientSerializerSetup.registerSerializers(serializerId);
    serializerId = OMngrSerializerSetup.registerSerializers(serializerId);
    serializerId = NetworkMngrSerializerSetup.registerSerializers(serializerId);
    serializerId = StunSerializerSetup.registerSerializers(serializerId);
    serializerId = DelaStorageSerializerSetup.registerSerializers(serializerId);
    serializerId = DelaWorkerSerializerSetup.registerBasicSerializers(serializerId);
  }

  private static void setupOverlays() {
    OverlayRegistryV2.initiate(new BaseOverlayIdentifiers.TypeFactory(), new BaseOverlayIdentifiers.TypeComparator());
  }

  private static void setupSystem() {
    IdentifierRegistryV2.registerBaseDefaults1(64);
    setupOverlays();
    setupSerializers();
  }

  public static void main(String[] args) throws IOException {
    System.setProperty("java.net.preferIPv4Stack", "true");
    setupSystem();

    if (Kompics.isOn()) {
      Kompics.shutdown();
    }
    // Yes 20 is totally arbitrary
    Kompics.createAndStart(WorkCtrlHostComp.class, Init.NONE, Runtime.getRuntime().availableProcessors(), 20);
    try {
      Kompics.waitForTermination();
    } catch (InterruptedException ex) {
      System.exit(1);
    }
  }
}
