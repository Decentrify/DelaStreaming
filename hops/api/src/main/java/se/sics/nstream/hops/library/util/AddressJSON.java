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
package se.sics.nstream.hops.library.util;

import java.net.InetAddress;
import java.net.UnknownHostException;
import se.sics.kompics.util.Identifier;
import se.sics.ktoolbox.util.identifiable.BasicBuilders;
import se.sics.ktoolbox.util.identifiable.BasicIdentifiers;
import se.sics.ktoolbox.util.identifiable.IdentifierFactory;
import se.sics.ktoolbox.util.identifiable.IdentifierRegistry;
import se.sics.ktoolbox.util.identifiable.basic.IntId;
import se.sics.ktoolbox.util.network.KAddress;
import se.sics.ktoolbox.util.network.basic.BasicAddress;
import se.sics.ktoolbox.util.network.nat.NatAwareAddressImpl;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class AddressJSON {

  private String ip;
  private int port;
  private int id;

  public AddressJSON(String ip, int port, int id) {
    this.ip = ip;
    this.port = port;
    this.id = id;
  }

  public AddressJSON() {
  }

  public String getIp() {
    return ip;
  }

  public void setIp(String ip) {
    this.ip = ip;
  }

  public int getPort() {
    return port;
  }

  public void setPort(int port) {
    this.port = port;
  }

  public int getId() {
    return id;
  }

  public void setId(int id) {
    this.id = id;
  }

  public KAddress fromJSON() {
    try {
      IdentifierFactory nodeIdFactory = IdentifierRegistry.lookup(BasicIdentifiers.Values.NODE.toString());
      Identifier nodeId = nodeIdFactory.id(new BasicBuilders.IntBuilder(id));
      return NatAwareAddressImpl.open(new BasicAddress(InetAddress.getByName(ip), port, nodeId));
    } catch (UnknownHostException ex) {
      throw new RuntimeException(ex);
    }
  }

  public static AddressJSON toJSON(KAddress adr) {
    return new AddressJSON(adr.getIp().getHostAddress(), adr.getPort(), ((IntId) adr.getId()).id);
  }
}
