/*
 * This file is part of the Kompics component model runtime.
 *
 * Copyright (C) 2009 Swedish Institute of Computer Science (SICS)
 * Copyright (C) 2009 Royal Institute of Technology (KTH)
 *
 * This program is free software; you can redistribute it and/or
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
package se.sics.dela.network.ledbat2;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.commons.lang.NotImplementedException;
import org.slf4j.MDC;
import se.sics.kompics.network.Address;
import se.sics.kompics.network.MessageNotify;
import se.sics.kompics.network.Msg;
import se.sics.kompics.network.netty.NotifyAck;

/**
 *
 * @author lkroll
 */
class MessageQueueManager {

  private final LedbatNetwork component;

  private final HashMap<InetSocketAddress, Queue<MessageWrapper>> tcpDelays = new HashMap<>();
  private final HashMap<InetSocketAddress, Queue<MessageWrapper>> udtDelays = new HashMap<>();
  private final ConcurrentHashMap<UUID, MessageNotify.Req> awaitingDelivery = new ConcurrentHashMap<>();

  MessageQueueManager(LedbatNetwork component) {
    this.component = component;
  }

  void send(Msg msg) {
    send(new MessageWrapper(msg));
  }

  void send(MessageNotify.Req notify) {
    send(new MessageWrapper(notify));
  }

  private void send(MessageWrapper msg) {
    switch (msg.msg.getProtocol()) {
      case TCP:
        throw new NotImplementedException("ups");
      case UDT:
        throw new NotImplementedException("ups");
      case UDP: {
        ChannelFuture cf = component.sendUdpMessage(msg);
        if (msg.notify.isPresent()) {
          if (cf != null) {
            cf.addListener(new NotifyListener(msg.notify.get()));
          } else {
            msg.notify.get().prepareResponse(System.currentTimeMillis(), false, System.nanoTime());
            component.notify(msg.notify.get());
          }
        }
      }
      break;
      default:
        throw new Error("Unknown Transport type");
    }
  }

  void ack(NotifyAck ack) {
    MessageNotify.Req req = awaitingDelivery.remove(ack.id);
    if (req != null) {
      component.notify(req, req.deliveryResponse(System.currentTimeMillis(), true, System.nanoTime()));
    } else {
      component.extLog.warn("Could not find MessageNotify.Req with id: {}!", ack.id);
    }
  }

  void clear() {
    component.extLog.info("Cleaning message queues.");
    this.tcpDelays.clear();
    this.udtDelays.clear();
    this.awaitingDelivery.clear();
  }

  class NotifyListener implements ChannelFutureListener {

    public final MessageNotify.Req notify;

    NotifyListener(MessageNotify.Req notify) {
      this.notify = notify;
    }

    @Override
    public void operationComplete(ChannelFuture future) throws Exception {
      component.setCustomMDC();
      try {
        if (future.isSuccess()) {
          notify.prepareResponse(System.currentTimeMillis(), true, System.nanoTime());
          if (notify.notifyOfDelivery) {
            awaitingDelivery.put(notify.getMsgId(), notify);
          }
        } else {
          component.extLog.warn("Sending of message {} did not succeed :( : {}", notify.msg, future.cause());
          notify.prepareResponse(System.currentTimeMillis(), false, System.nanoTime());
        }
        component.notify(notify);
      } finally {
        MDC.clear();
      }

    }
  }
}
