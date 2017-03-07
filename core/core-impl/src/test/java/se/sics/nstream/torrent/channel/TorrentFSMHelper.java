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
//import com.google.common.base.Predicate;
//import se.sics.kompics.testkit.TestContext;
//import se.sics.kompics.testkit.Testkit;
//import se.sics.ktoolbox.nutil.fsm.MultiFSM;
//import se.sics.ktoolbox.util.identifiable.overlay.OverlayId;
//import se.sics.ktoolbox.util.network.KAddress;
//
///**
// *
// * @author Alex Ormenisan <aaor@kth.se>
// */
//public class TorrentFSMHelper {
//
//  public static TestContext<ChannelComp> getContext(KAddress selfAdr) {
//    ChannelComp.Init init = new ChannelComp.Init(selfAdr);
//    TestContext<ChannelComp> context = Testkit.newTestContext(ChannelComp.class, init);
//    return context;
//  }
//
//  public static Predicate<ChannelComp> inspectSeederState(final OverlayId torrentId, final SeederTorrentStates expectedState) {
//    return new Predicate<ChannelComp>() {
//      @Override
//      public boolean apply(ChannelComp lm) {
//        MultiFSM mfsm = ChannelCompHelper.getSeederFSM(lm);
//        return expectedState.equals(mfsm.getFSMState(SeederTorrentFSM.NAME, torrentId.baseId));
//      }
//    };
//  }
//  
//  public static Predicate<ChannelComp> inspectLeecherState(final OverlayId torrentId, final LeecherTorrentStates expectedState) {
//    return new Predicate<ChannelComp>() {
//      @Override
//      public boolean apply(ChannelComp lm) {
//        MultiFSM mfsm = ChannelCompHelper.getLeecherFSM(lm);
//        return expectedState.equals(mfsm.getFSMState(LeecherTorrentFSM.NAME, torrentId.baseId));
//      }
//    };
//  }
//  
//}
