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
package se.sics.cobweb.transfer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.cobweb.transfer.event.TransferLocalE;
import se.sics.cobweb.transfer.mngr.event.TransferE;
import se.sics.cobweb.util.FileId;
import se.sics.ktoolbox.nutil.fsm.api.FSMException;
import se.sics.ktoolbox.nutil.fsm.api.FSMStateName;
import se.sics.ktoolbox.nutil.fsm.handler.FSMEventHandler;
import se.sics.ktoolbox.nutil.fsm.handler.FSMMsgHandler;
import se.sics.ktoolbox.util.network.KAddress;
import se.sics.ktoolbox.util.network.KContentMsg;
import se.sics.ktoolbox.util.network.KHeader;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class TransferLeecherHandlers {
  
  private static final Logger LOG = LoggerFactory.getLogger(TransferLeecherHandlers.class);

  static FSMEventHandler handleEventFallback
    = new FSMEventHandler<TransferLeecherExternal, TransferLeecherInternal, TransferLeecherFSMEvent>() {
      @Override
      public FSMStateName handle(FSMStateName state, TransferLeecherExternal es, TransferLeecherInternal is,
        TransferLeecherFSMEvent event) throws
      FSMException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
      }
    };

  static FSMMsgHandler handleMsgFallback
    = new FSMMsgHandler<TransferLeecherExternal, TransferLeecherInternal, TransferLeecherFSMEvent>() {
      @Override
      public FSMStateName handle(FSMStateName state, TransferLeecherExternal es, TransferLeecherInternal is,
        TransferLeecherFSMEvent payload, KContentMsg<KAddress, KHeader<KAddress>, TransferLeecherFSMEvent> msg) throws
      FSMException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
      }
    };
  
    static FSMEventHandler handleMetadataGet
    = new FSMEventHandler<TransferLeecherExternal, TransferLeecherInternal, TransferLocalE.LeecherStart>() {
      @Override
      public FSMStateName handle(FSMStateName state, TransferLeecherExternal es, TransferLeecherInternal is,
        TransferLocalE.LeecherStart event) throws
      FSMException {
        LOG.trace("{}", event);
        TransferE.LeecherStarted req = new TransferE.LeecherStarted(es.torrentId, new FileId(es.torrentId.baseId, 0));
        es.getProxy().trigger(req, es.transferPort);
        return TransferLeecherStates.GETTING_METADATA;
      }
    };
}
