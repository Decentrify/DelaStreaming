///*
// * Copyright (C) 2009 Swedish Institute of Computer Science (SICS) Copyright (C)
// * 2009 Royal Institute of Technology (KTH)
// *
// * GVoD is free software; you can redistribute it and/or
// * modify it under the terms of the GNU General Public License
// * as published by the Free Software Foundation; either version 2
// * of the License, or (at your option) any later version.
// *
// * This program is distributed in the hope that it will be useful,
// * but WITHOUT ANY WARRANTY; without even the implied warranty of
// * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// * GNU General Public License for more details.
// *
// * You should have received a copy of the GNU General Public License
// * along with this program; if not, write to the Free Software
// * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
// */
//package se.sics.cobweb.conn2.mngr;
//
//import se.sics.cobweb.conn2.mngr.ConnCtrlPort;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//import se.sics.cobweb.OverlayComp;
//import se.sics.cobweb.conn2.ConnPort;
//import se.sics.cobweb.conn2.mngr.event.ConnCtrlE;
//import se.sics.kompics.ComponentDefinition;
//import se.sics.kompics.Handler;
//import se.sics.kompics.Negative;
//import se.sics.kompics.Positive;
//import se.sics.kompics.Start;
//import se.sics.kompics.network.Network;
//import se.sics.kompics.timer.Timer;
//import se.sics.ktoolbox.croupier.CroupierPort;
//import se.sics.ktoolbox.util.identifiable.overlay.OverlayId;
//import se.sics.ktoolbox.util.network.KAddress;
//
///**
// * @author Alex Ormenisan <aaor@kth.se>
// */
//public class MockConnComp extends ComponentDefinition implements OverlayComp {
//  private static final Logger LOG = LoggerFactory.getLogger(MockConnComp.class);
//  private final String logPrefix;
//
//  private final Negative<ConnPort> conn = provides(ConnPort.class);
//  private final Negative<ConnCtrlPort> connCtrl = provides(ConnCtrlPort.class);
//  private final Positive<Network> network = requires(Network.class);
//  private final Positive<Timer> timer = requires(Timer.class);
//  private final Positive<CroupierPort> croupier = requires(CroupierPort.class);
//  //********************************************************************************************************************
//  private final KAddress selfAdr;
//  private final OverlayId torrentId;
//  //********************************************************************************************************************
//  
//  public MockConnComp(Init init) {
//    this.selfAdr = init.selfAdr;
//    this.torrentId = init.torrentId;
//    logPrefix = "<nid:" + selfAdr.getId() + "," + torrentId + ">";
//    LOG.info("{}initiating...", logPrefix);
//    
//    subscribe(handleStart, control);
//    subscribe(handleClean, connCtrl);
//  }
//  
//  //********************************************************************************************************************
//  Handler handleStart = new Handler<Start>() {
//    @Override
//    public void handle(Start event) {
//      LOG.info("{}starting", logPrefix);
//    }
//  };
//  
//  @Override
//  public void tearDown() {
//    LOG.info("{}tear down", logPrefix);
//  }
//  //********************************************************************************************************************
//  Handler handleClean = new Handler<ConnCtrlE.CleanReq>() {
//    @Override
//    public void handle(ConnCtrlE.CleanReq req) {
//      LOG.info("{}cleaning", logPrefix);
//      answer(req, req.getResponse());
//    }
//  };
//
//  @Override
//  public OverlayId overlayId() {
//    return torrentId;
//  }
//  
//  public static class Init extends se.sics.kompics.Init<MockConnComp> {
//
//    public final KAddress selfAdr;
//    public final OverlayId torrentId;
//
//    public Init(KAddress selfAdr, OverlayId torrentId) {
//      this.selfAdr = selfAdr;
//      this.torrentId = torrentId;
//    }
//  }
//}
