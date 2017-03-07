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
package se.sics.cobweb.transfer.mngr;

import com.google.common.base.Optional;
import se.sics.cobweb.transfer.TransferComp;
import se.sics.cobweb.transfer.handle.LeecherHandlePort;
import se.sics.cobweb.transfer.handle.SeederHandlePort;
import se.sics.cobweb.transfer.instance.TransferPort;
import se.sics.kompics.Component;
import se.sics.kompics.ComponentProxy;
import se.sics.kompics.Negative;
import se.sics.kompics.Positive;
import se.sics.ktoolbox.nutil.fsm.api.FSMExternalState;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayId;
import se.sics.ktoolbox.util.idextractor.EventOverlayIdExtractor;
import se.sics.ktoolbox.util.network.KAddress;
import se.sics.ktoolbox.util.network.ports.One2NChannel;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class TransferMngrExternal implements FSMExternalState {

  public final KAddress selfAdr;
  private ComponentProxy proxy;
  public final TransferComp.Creator transferCreator;

  public final Positive<TransferCtrlPort> transferCtrl;
  public final Negative<TransferPort> transfer;
  public final Positive<LeecherHandlePort> leecherHandlePort;
  public final Positive<SeederHandlePort> seederHandlePort;

  private final One2NChannel transferCtrlChannel;
  private final One2NChannel transferChannel;
  private final One2NChannel leecherHandleChannel;
  private final One2NChannel seederHandleChannel;

  public TransferMngrExternal(TransferMngrComp.Init init, Positive<TransferCtrlPort> transferCtrl,
    Negative<TransferPort> transfer, Positive<LeecherHandlePort> leecherHandlePort,
    Positive<SeederHandlePort> seederHandlePort) {
    this.selfAdr = init.selfAdr;
    this.transferCreator = init.transferCreator;

    this.transferCtrl = transferCtrl;
    this.transfer = transfer;
    this.leecherHandlePort = leecherHandlePort;
    this.seederHandlePort = seederHandlePort;

    transferCtrlChannel = One2NChannel.getChannel("<nid:" + selfAdr.getId() + ">transfer-ctrl", transferCtrl,
      new EventOverlayIdExtractor());
    transferChannel = One2NChannel.getChannel("<nid:" + selfAdr.getId() + ">transfer", transfer,
      new EventOverlayIdExtractor());
    leecherHandleChannel = One2NChannel.getChannel("<nid:" + selfAdr.getId() + ">leecher handle", leecherHandlePort,
      new EventOverlayIdExtractor());
    seederHandleChannel = One2NChannel.getChannel("<nid:" + selfAdr.getId() + ">seeder handle", seederHandlePort,
      new EventOverlayIdExtractor());
  }

  @Override
  public void setProxy(ComponentProxy proxy) {
    this.proxy = proxy;
  }

  @Override
  public ComponentProxy getProxy() {
    return proxy;
  }

  public void setChannels(OverlayId torrentId, Component comp) {
    transferCtrlChannel.addChannel(torrentId, (Negative<TransferCtrlPort>) comp.getPositive(TransferCtrlPort.class).
      getPair());
    transferChannel.addChannel(torrentId, comp.getPositive(TransferPort.class));
    leecherHandleChannel.addChannel(torrentId, comp.getNegative(LeecherHandlePort.class));
    seederHandleChannel.addChannel(torrentId, comp.getNegative(SeederHandlePort.class));
  }

  public void cleanChannels(OverlayId torrentId, Component comp) {
    transferCtrlChannel.removeChannel(torrentId, comp.getPositive(TransferCtrlPort.class));
    transferChannel.addChannel(torrentId, comp.getPositive(TransferPort.class));
    leecherHandleChannel.removeChannel(torrentId, comp.getNegative(LeecherHandlePort.class));
    seederHandleChannel.removeChannel(torrentId, comp.getNegative(SeederHandlePort.class));
  }

  public Component createConnComp(OverlayId torrentId, Optional torrent) {
    return transferCreator.create(proxy, selfAdr, torrentId, torrent);
  }
}
