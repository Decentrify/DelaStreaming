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
package se.sics.dela.network.ledbat.receiver;

import com.google.common.base.Optional;
import java.util.Random;
import java.util.UUID;
import se.sics.dela.network.DelaSerializerSetup;
import se.sics.dela.network.ledbat.LedbatReceiverComp;
import se.sics.dela.network.ledbat.LedbatReceiverPort;
import se.sics.dela.network.ledbat.LedbatSerializerSetup;
import se.sics.kompics.Channel;
import se.sics.kompics.ClassMatchedHandler;
import se.sics.kompics.Component;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Handler;
import se.sics.kompics.Init;
import se.sics.kompics.Kompics;
import se.sics.kompics.Positive;
import se.sics.kompics.Start;
import se.sics.kompics.config.Config;
import se.sics.kompics.network.Network;
import se.sics.kompics.timer.Timer;
import se.sics.kompics.timer.java.JavaTimer;
import se.sics.ktoolbox.croupier.CroupierSerializerSetup;
import se.sics.ktoolbox.gradient.GradientSerializerSetup;
import se.sics.ktoolbox.netmngr.NetworkMngrSerializerSetup;
import se.sics.ktoolbox.netmngr.event.NetMngrReady;
import se.sics.ktoolbox.omngr.OMngrSerializerSetup;
import se.sics.ktoolbox.util.config.options.BasicAddressOption;
import se.sics.ktoolbox.util.identifiable.BasicIdentifiers;
import se.sics.ktoolbox.util.identifiable.IdentifierFactory;
import se.sics.ktoolbox.util.identifiable.IdentifierRegistry;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayIdFactory;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayRegistry;
import se.sics.ktoolbox.util.network.KAddress;
import se.sics.ktoolbox.util.setup.BasicSerializerSetup;
import se.sics.ktoolbox.util.status.Status;
import se.sics.ktoolbox.util.status.StatusPort;
import se.sics.nat.mngr.SimpleNatMngrComp;
import se.sics.nat.stun.StunSerializerSetup;
import se.sics.nstream.TorrentIds;
import se.sics.nstream.hops.SystemOverlays;

/**
 *
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class HostComp extends ComponentDefinition {

  private Positive<StatusPort> otherStatusPort = requires(StatusPort.class);

  private Component timerComp;
  private Component networkMngrComp;
  private Component ledbatReceiverComp;
  private Component driverComp;

  private KAddress selfAdr;
  private KAddress srcAdr;

  public HostComp() {
    HostConfig hostConfig = new HostConfig(config());
    srcAdr = hostConfig.srcAdr;
    subscribe(handleStart, control);
    subscribe(handleNetReady, otherStatusPort);

  }

  Handler handleStart = new Handler<Start>() {
    @Override
    public void handle(Start event) {
      timerComp = create(JavaTimer.class, Init.NONE);
      setNetworkMngr();

      trigger(Start.event, timerComp.control());
      trigger(Start.event, networkMngrComp.control());
    }
  };

  private void setNetworkMngr() {
    logger.info("setting up network mngr");
    SimpleNatMngrComp.ExtPort netExtPorts = new SimpleNatMngrComp.ExtPort(timerComp.getPositive(Timer.class));
    networkMngrComp = create(SimpleNatMngrComp.class, new SimpleNatMngrComp.Init(netExtPorts));
    connect(networkMngrComp.getPositive(StatusPort.class), otherStatusPort.getPair(), Channel.TWO_WAY);
  }

  ClassMatchedHandler handleNetReady
    = new ClassMatchedHandler<NetMngrReady, Status.Internal<NetMngrReady>>() {
    @Override
    public void handle(NetMngrReady content, Status.Internal<NetMngrReady> container) {
      logger.info("network mngr ready");
      selfAdr = content.systemAdr;
      setLedbatReceiver();
      setDriver();
      trigger(Start.event, ledbatReceiverComp.control());
      trigger(Start.event, driverComp.control());
      logger.info("starting complete...");
    }
  };

  private void setLedbatReceiver() {
    logger.info("setting up ledbat receiver");
    ledbatReceiverComp = create(LedbatReceiverComp.class, new LedbatReceiverComp.Init(selfAdr, srcAdr));
    connect(ledbatReceiverComp.getNegative(Network.class), networkMngrComp.getPositive(Network.class), Channel.TWO_WAY);
  }

  private void setDriver() {
    logger.info("setting up driver");
    driverComp = create(DriverComp.class, Init.NONE);
    connect(driverComp.getNegative(LedbatReceiverPort.class), ledbatReceiverComp.getPositive(LedbatReceiverPort.class),
      Channel.TWO_WAY);
  }

  public static class HostConfig {

    public static BasicAddressOption srcAdrOpt = new BasicAddressOption("transfer.src");

    public KAddress srcAdr;

    public HostConfig(Config config) {
      srcAdr = srcAdrOpt.readValue(config).get();
    }
  }

  private static void setupSerializers() {
    int serializerId = 128;
    serializerId = BasicSerializerSetup.registerBasicSerializers(serializerId);
    serializerId = CroupierSerializerSetup.registerSerializers(serializerId);
    serializerId = GradientSerializerSetup.registerSerializers(serializerId);
    serializerId = OMngrSerializerSetup.registerSerializers(serializerId);
    serializerId = NetworkMngrSerializerSetup.registerSerializers(serializerId);
    serializerId = StunSerializerSetup.registerSerializers(serializerId);
    serializerId = DelaSerializerSetup.registerSerializers(serializerId);
  }
  
  private static OverlayIdFactory setupOverlayIdFactory() {
    OverlayRegistry.initiate(new SystemOverlays.TypeFactory(), new SystemOverlays.Comparator());

    byte torrentOwnerId = 1;
    OverlayRegistry.registerPrefix(TorrentIds.TORRENT_OVERLAYS, torrentOwnerId);

    IdentifierFactory torrentBaseIdFactory = IdentifierRegistry.lookup(BasicIdentifiers.Values.OVERLAY.toString());
    return new OverlayIdFactory(torrentBaseIdFactory, TorrentIds.Types.TORRENT, torrentOwnerId);
  }

  private static void setupBasic(Config.Builder builder) {
    Random rand = new Random();
    Long seed = builder.getValue("system.seed", Long.class);
    if (seed == null) {
      builder.setValue("system.seed", rand.nextLong());
    }
  }
  
  private static OverlayIdFactory setupSystem() {
    Config.Impl config = (Config.Impl) Kompics.getConfig();
    Config.Builder builder = Kompics.getConfig().modify(UUID.randomUUID());
    setupBasic(builder);
    TorrentIds.registerDefaults(builder.getValue("system.seed", Long.class));
    OverlayIdFactory torrentIdFactory = setupOverlayIdFactory();
    setupSerializers();
    config.apply(builder.finalise(), (Optional) Optional.absent());
    Kompics.setConfig(config);
    return torrentIdFactory;
  }
  public static void main(String[] args)  {
    OverlayIdFactory torrentIdFactory = setupSystem();

    if (Kompics.isOn()) {
      Kompics.shutdown();
    }
    // Yes 20 is totally arbitrary
    Kompics.createAndStart(HostComp.class, Init.NONE, Runtime.getRuntime().availableProcessors(), 20);
    try {
      Kompics.waitForTermination();
    } catch (InterruptedException ex) {
      System.exit(1);
    }
  }
}
