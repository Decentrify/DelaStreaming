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
package se.sics.cobweb;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.javatuples.Pair;
import org.javatuples.Triplet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.cobweb.DriverHelper.Switch;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Direct;
import se.sics.kompics.Handler;
import se.sics.kompics.KompicsEvent;
import se.sics.kompics.Negative;
import se.sics.kompics.Port;
import se.sics.kompics.Positive;
import se.sics.kompics.Start;
import se.sics.ktoolbox.nutil.fsm.genericsetup.GenericSetup;
import se.sics.ktoolbox.nutil.fsm.genericsetup.OnEventAction;
import se.sics.ktoolbox.nutil.fsm.genericsetup.OnMsgAction;
import se.sics.ktoolbox.util.Either;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class MockScenarioComp extends ComponentDefinition {

  private static final Logger LOG = LoggerFactory.getLogger(MockScenarioComp.class);

  private final Negative<ReflectPort> reflectProvide = provides(ReflectPort.class);
  private final Positive<ReflectPort> reflectRequire = requires(ReflectPort.class);

  private final LinkedList<Either<Function<Direct.Request, Direct.Response>, Triplet<KompicsEvent, Boolean, Class>>> nextEvents;

  public MockScenarioComp(final Init init) {
    nextEvents = init.nextEvents;
    OnEventAction oea = new OnEventAction() {
      @Override
      public void handle(KompicsEvent event) {
        LOG.info("{}", event);
        if (init.reflectEvents.contains(event.getClass())) {
          trigger(new ReflectEvent(event), reflectProvide);
        }
        Predicate<KompicsEvent> marker = init.markers.get(event.getClass());
        if (marker != null) {
          marker.apply(event);
        }
        if (!nextEvents.isEmpty()) {
          nextWithEvent(event);
        }
      }
    };

    List<Pair<Class, List<Pair<OnEventAction, Class>>>> posPorts = new LinkedList<>();
    for (Pair<Class, List<Class>> p : init.positivePorts) {
      List<Pair<OnEventAction, Class>> posEvents = new LinkedList<>();
      posPorts.add(Pair.with(p.getValue0(), posEvents));
      for (Class e : p.getValue1()) {
        posEvents.add(Pair.with(oea, e));
      }
    }

    List<Pair<Class, List<Pair<OnEventAction, Class>>>> negPorts = new LinkedList<>();
    for (Pair<Class, List<Class>> p : init.negativePorts) {
      List<Pair<OnEventAction, Class>> negEvents = new LinkedList<>();
      negPorts.add(Pair.with(p.getValue0(), negEvents));
      for (Class e : p.getValue1()) {
        negEvents.add(Pair.with(oea, e));
      }
    }
    GenericSetup.portsAndHandledEvents(proxy, posPorts, negPorts,
      new LinkedList<Pair<OnMsgAction, Class>>(), new LinkedList<Pair<OnMsgAction, Class>>());

    if (init.onStart != null) {
      subscribe(new Handler<Start>() {
        @Override
        public void handle(Start event) {
          Port port;
          if (init.onStart.getValue1()) {
            port = proxy.getNegative(init.onStart.getValue2()).getPair();
          } else {
            port = proxy.getPositive(init.onStart.getValue2()).getPair();
          }
          trigger(init.onStart.getValue0(), port);
        }
      }, control);
    }

    subscribe(new Handler<ReflectEvent>() {

      @Override
      public void handle(ReflectEvent event) {
        if (!init.nextEvents.isEmpty()) {
          Either first = init.nextEvents.removeFirst();
          if (first.isLeft()) {
            throw new RuntimeException("no");
          } else {
            Triplet<KompicsEvent, Boolean, Class> next = (Triplet) first.getRight();
            Port port;
            if (next.getValue1()) {
              port = proxy.getNegative(next.getValue2()).getPair();
            } else {
              port = proxy.getPositive(next.getValue2()).getPair();
            }
            trigger(next.getValue0(), port);
          }
        }
      }
    }, reflectRequire);
  }

  private void nextWithEvent(KompicsEvent event) {
    Either first = nextEvents.removeFirst();
    if (first.isLeft()) {
      Function<Direct.Request, Direct.Response> mapper = (Function) first.getLeft();
      Direct.Request req = (Direct.Request) event;
      answer(req, mapper.apply(req));
    } else {
      next(first);
    }
  }

  public void next() {
    if (nextEvents.isEmpty()) {
      return;
    }
    Either first = nextEvents.removeFirst();
    if (first.isRight()) {
      next(first);
    } else {
      return;
    }
  }

  private void next(Either first) {
    Triplet<KompicsEvent, Boolean, Class> next = (Triplet) first.getRight();
    if (next.getValue2() == null) {
      Switch s = (Switch) next.getValue0();
      s.driver.next();
      return;
    } else {
      Port port;
      if (next.getValue1()) {
        port = proxy.getNegative(next.getValue2()).getPair();
      } else {
        port = proxy.getPositive(next.getValue2()).getPair();
      }
      trigger(next.getValue0(), port);
    }
  }

  public static class Init extends se.sics.kompics.Init<MockScenarioComp> {

    public final List<Pair<Class, List<Class>>> positivePorts;
    public final List<Pair<Class, List<Class>>> negativePorts;
    public final LinkedList<Either<Function<Direct.Request, Direct.Response>, Triplet<KompicsEvent, Boolean, Class>>> nextEvents;
    public final Triplet<KompicsEvent, Boolean, Class> onStart;
    public final Map<Class, Predicate<KompicsEvent>> markers;
    public final Set<Class> reflectEvents;

    public Init(List positivePorts, List negativePorts, Triplet onStart, LinkedList nextEvents, Map markers,
      Set reflectEvents) {
      this.positivePorts = positivePorts;
      this.negativePorts = negativePorts;
      this.nextEvents = nextEvents;
      this.onStart = onStart;
      this.markers = markers;
      this.reflectEvents = reflectEvents;
    }
  }
}
