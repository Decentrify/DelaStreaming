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
//package se.sics.nstream.torrent.channel;
//
//import se.sics.kompics.ComponentProxy;
//import se.sics.kompics.Positive;
//import se.sics.kompics.network.Network;
//import se.sics.ktoolbox.nutil.fsm.api.FSMExternalState;
//import se.sics.nstream.torrent.channel.state.SeederSideViewImpl;
//import se.sics.nstream.torrent.channel.state.api.ConnectionDecider;
//
///**
// * @author Alex Ormenisan <aaor@kth.se>
// */
//public class SeederTorrentExternal  implements FSMExternalState {
//  private ComponentProxy proxy;
//  public final Positive<Network> network;
//  public final ConnectionDecider.SeederSideTorrent torrentPolicy;
//  public final SeederSideViewImpl overview = new SeederSideViewImpl();
//  
//  public SeederTorrentExternal(Positive<Network> network, ConnectionDecider.SeederSideTorrent seederSidePolicy) {
//    this.network = network;
//    this.torrentPolicy = seederSidePolicy;
//  }
//
//  @Override
//  public void setProxy(ComponentProxy proxy) {
//    this.proxy = proxy;
//  }
//
//  @Override
//  public ComponentProxy getProxy() {
//    return proxy;
//  }
//}
