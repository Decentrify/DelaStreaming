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
//package se.sics.cobweb.conn2.instance;
//
//import java.util.HashMap;
//import java.util.List;
//import java.util.Map;
//import se.sics.kompics.ComponentDefinition;
//import se.sics.kompics.Handler;
//import se.sics.kompics.Negative;
//import se.sics.kompics.Start;
//import se.sics.ktoolbox.croupier.CroupierPort;
//import se.sics.ktoolbox.croupier.event.CroupierSample;
//import se.sics.ktoolbox.util.identifiable.Identifier;
//import se.sics.ktoolbox.util.identifiable.overlay.OverlayId;
//import se.sics.ktoolbox.util.network.KAddress;
//import se.sics.ktoolbox.util.other.AgingAdrContainer;
//import se.sics.ktoolbox.util.other.Container;
//import se.sics.ktoolbox.util.update.View;
//
///**
// * @author Alex Ormenisan <aaor@kth.se>
// */
//public class MockCroupierComp extends ComponentDefinition {
//  private final Negative<CroupierPort> croupierPort = provides(CroupierPort.class);
//  
//  private final CroupierSample sample;
//  
//  public MockCroupierComp(Init init) {
//    Map<Identifier, AgingAdrContainer<KAddress, View>> publicSample = new HashMap<>();
//    for(KAddress peer : init.peers) {
//      publicSample.put(peer.getId(), new MockContainer(peer));
//    }
//    Map<Identifier, AgingAdrContainer<KAddress, View>> privateSample = new HashMap<>();
//    sample = new CroupierSample(init.overlayId, publicSample, privateSample);
//    subscribe(handleStart, control);
//  }
//  
//  Handler handleStart = new Handler<Start>() {
//    @Override
//    public void handle(Start event) {
//      trigger(sample, croupierPort);
//    }
//  };
//  
//  public static class Init extends se.sics.kompics.Init<MockCroupierComp> {
//    public final OverlayId overlayId;
//    public final List<KAddress> peers;
//    
//    public Init(OverlayId overlayId, List<KAddress> peers) {
//      this.overlayId = overlayId;
//      this.peers = peers;
//    }
//  }
//  
//  public static class MockContainer implements AgingAdrContainer<KAddress, View> {
//    public final KAddress peer;
//    
//    public MockContainer(KAddress peer) {
//      this.peer = peer;
//    }
//    
//    @Override
//    public void incrementAge() {
//      throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
//    }
//
//    @Override
//    public int getAge() {
//      throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
//    }
//
//    @Override
//    public KAddress getSource() {
//      return peer;
//    }
//
//    @Override
//    public View getContent() {
//      throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
//    }
//
//    @Override
//    public Container<KAddress, View> copy() {
//      throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
//    }
//  }
//}
