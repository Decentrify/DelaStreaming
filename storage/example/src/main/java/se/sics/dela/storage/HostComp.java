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
package se.sics.dela.storage;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import se.sics.dela.storage.disk.DiskComp;
import se.sics.dela.storage.mngr.StorageProvider;
import se.sics.dela.storage.mngr.endpoint.StorageMngrComp;
import se.sics.dela.storage.mngr.endpoint.EndpointMngrPort;
import se.sics.dela.storage.mngr.stream.StreamMngrPort;
import se.sics.dela.storage.operation.StreamOpPort;
import se.sics.kompics.Channel;
import se.sics.kompics.Component;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Handler;
import se.sics.kompics.Kompics;
import se.sics.kompics.Start;
import se.sics.kompics.fsm.FSMException;
import se.sics.kompics.timer.Timer;
import se.sics.kompics.timer.java.JavaTimer;
import se.sics.kompics.util.Identifier;
import se.sics.ktoolbox.util.identifiable.BasicBuilders;
import se.sics.ktoolbox.util.identifiable.BasicIdentifiers;
import se.sics.ktoolbox.util.identifiable.IdentifierFactory;
import se.sics.ktoolbox.util.identifiable.IdentifierRegistry;
import se.sics.ktoolbox.util.identifiable.basic.IntIdFactory;
import se.sics.ktoolbox.util.identifiable.basic.StringByteIdFactory;
import se.sics.ktoolbox.util.identifiable.basic.UUIDIdFactory;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class HostComp extends ComponentDefinition {

  private Component timer;
  private Component storageMngr;
  private Component driver;
  private final String testPath = "./src/main/resources/example/storage";

  public HostComp(Init init) {
    setup(init.seed, init.selfId);
    subscribe(handleStart, control);
  }

  private void setup(long seed, Identifier selfId) {
    timer = create(JavaTimer.class, Init.NONE);
    storageMngr = create(StorageMngrComp.class, new StorageMngrComp.Init(selfId));
    connect(timer.getPositive(Timer.class), storageMngr.getNegative(Timer.class), Channel.TWO_WAY);
    driver = create(DriverComp.class, driverInit(seed, selfId));
    connect(storageMngr.getPositive(EndpointMngrPort.class), driver.getNegative(EndpointMngrPort.class),
      Channel.TWO_WAY);
    connect(storageMngr.getPositive(StreamMngrPort.class), driver.getNegative(StreamMngrPort.class), Channel.TWO_WAY);
    connect(storageMngr.getPositive(StreamOpPort.class), driver.getNegative(StreamOpPort.class), Channel.TWO_WAY);
  }
  
  private DriverComp.Init driverInit(long seed, Identifier selfId) {
    IdentifierFactory endpointIdFactory = new IntIdFactory(new Random(seed+2));
    List<StorageProvider> storageProviders = new ArrayList<>();
    storageProviders.add(new DiskComp.StorageProvider(selfId));
    return new DriverComp.Init(selfId, endpointIdFactory, storageProviders, testPath);
  }

  Handler<Start> handleStart = new Handler<Start>() {
    @Override
    public void handle(Start event) {
      logger.info("start");
    }
  };

  public static class Init extends se.sics.kompics.Init<HostComp> {

    public final long seed;
    public final Identifier selfId;

    public Init(long seed, Identifier selfId) {
      this.seed = seed;
      this.selfId = selfId;
    }
  }

  public static void registerDefaults(long seed) {
    Random rand = new Random(seed+1);
    IdentifierRegistry.register(BasicIdentifiers.Values.EVENT.toString(), new UUIDIdFactory());
    IdentifierRegistry.register(BasicIdentifiers.Values.MSG.toString(), new UUIDIdFactory());
    IdentifierRegistry.register(BasicIdentifiers.Values.OVERLAY.toString(), new StringByteIdFactory(rand, 64));
    IdentifierRegistry.register(BasicIdentifiers.Values.NODE.toString(), new IntIdFactory(rand));
  }

  public static void main(String[] args) throws IOException, FSMException, URISyntaxException {
    long seed = 1234;
    registerDefaults(seed);
    Init init = new Init(seed, BasicIdentifiers.nodeId());

    if (Kompics.isOn()) {
      Kompics.shutdown();
    }
    // Yes 20 is totally arbitrary
    Kompics.createAndStart(HostComp.class, init, Runtime.getRuntime().availableProcessors(), 20);
    try {
      Kompics.waitForTermination();
    } catch (InterruptedException ex) {
      System.exit(1);
    }
  }
}
