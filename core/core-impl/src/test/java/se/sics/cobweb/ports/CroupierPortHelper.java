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
package se.sics.cobweb.ports;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import org.javatuples.Triplet;
import se.sics.cobweb.conn.ConnHandleView;
import se.sics.ktoolbox.croupier.CroupierPort;
import se.sics.ktoolbox.croupier.event.CroupierSample;
import se.sics.ktoolbox.util.identifiable.Identifier;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayId;
import se.sics.ktoolbox.util.network.KAddress;
import se.sics.ktoolbox.util.other.AgingAdrContainer;
import se.sics.ktoolbox.util.other.Container;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class CroupierPortHelper {

  public static List<Class> indication() {
    List<Class> events = new LinkedList<>();
    events.add(CroupierSample.class);
    return events;
  }

  public static List<Class> request() {
    List<Class> events = new LinkedList<>();
    return events;
  }

  public static Triplet seederSample(OverlayId overlayId, OverlayId torrentId, KAddress seeder) {
    ConnHandleView view = new ConnHandleView(torrentId);
    MockContainer container = new MockContainer(seeder, view);
    Map<Identifier, MockContainer> publicSample = new HashMap<>();
    publicSample.put(seeder.getId(), container);
    CroupierSample msg = new CroupierSample(overlayId, publicSample, new HashMap());
    Triplet result = Triplet.with(msg, false, CroupierPort.class);
    return result;
  }

  public static class MockContainer implements AgingAdrContainer<KAddress, ConnHandleView> {

    public final KAddress source;
    public final ConnHandleView content;

    public MockContainer(KAddress source, ConnHandleView content) {
      this.source = source;
      this.content = content;
    }

    @Override
    public void incrementAge() {
      throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public int getAge() {
      throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public KAddress getSource() {
      return source;
    }

    @Override
    public ConnHandleView getContent() {
      return content;
    }

    @Override
    public Container<KAddress, ConnHandleView> copy() {
      throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

  }
}
