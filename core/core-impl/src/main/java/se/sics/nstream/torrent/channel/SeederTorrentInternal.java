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
//import com.google.common.base.Optional;
//import se.sics.kompics.Promise;
//import se.sics.ktoolbox.nutil.fsm.api.FSMException;
//import se.sics.ktoolbox.nutil.fsm.api.FSMInternalState;
//import se.sics.ktoolbox.nutil.fsm.api.FSMInternalStateBuilder;
//import se.sics.ktoolbox.nutil.fsm.ids.FSMId;
//import se.sics.ktoolbox.util.network.KAddress;
//import se.sics.nstream.torrent.channel.msg.TorrentMsg;
//
///**
// * @author Alex Ormenisan <aaor@kth.se>
// */
//public class SeederTorrentInternal implements FSMInternalState {
//  public final FSMId fsmId;
//  public final ActiveRequest activeRequest = new ActiveRequest();
//  public KAddress target;
//  public LocalConnectionState connState;
//  
//  private SeederTorrentInternal(FSMId fsmId) {
//    this.fsmId = fsmId;
//  }
//
//  @Override
//  public FSMId getFSMId() {
//    return fsmId;
//  }
//  
//  public void connectLeecher(TorrentMsg.Connect req, KAddress target) {
//    this.target = target;
//    this.connState = new LocalConnectionState(req);
//  }
//  
//  public static class ActiveRequest {
//
//    //either one
//    private Optional<Promise> activeRequest = Optional.absent();
//
//    private ActiveRequest() {
//    }
//
//    private void setActive(Promise req) throws FSMException {
//      if (activeRequest.isPresent()) {
//        throw new FSMException("concurrent pending requests");
//      }
//      this.activeRequest = Optional.of(req);
//    }
//
//    public void reset() {
//      activeRequest = Optional.absent();
//    }
//
//    public Optional<Promise> active() {
//      return activeRequest;
//    }
//  }
//  
//  public static class LocalConnectionState {
//    public final TorrentMsg.Connect req;
//    
//    public LocalConnectionState(TorrentMsg.Connect req) {
//      this.req = req;
//    }
//  }
//  
//  public static class Builder implements FSMInternalStateBuilder {
//
//    @Override
//    public FSMInternalState newState(FSMId fsmId) {
//      return new SeederTorrentInternal(fsmId);
//    }
//  }
//}
