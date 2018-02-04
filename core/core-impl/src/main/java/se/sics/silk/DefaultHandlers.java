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
package se.sics.silk;

import se.sics.kompics.KompicsEvent;
import se.sics.kompics.PatternExtractor;
import se.sics.kompics.fsm.FSMBasicStateNames;
import se.sics.kompics.fsm.FSMExternalState;
import se.sics.kompics.fsm.FSMInternalState;
import se.sics.kompics.fsm.FSMStateName;
import se.sics.kompics.fsm.handler.FSMBasicEventHandler;
import se.sics.kompics.fsm.handler.FSMPatternEventHandler;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class DefaultHandlers {

  public static FSMBasicEventHandler basicDefault() {
    return new FSMBasicEventHandler<FSMExternalState, FSMInternalState, KompicsEvent>() {
      @Override
      public FSMStateName handle(FSMStateName state, FSMExternalState es, FSMInternalState is, KompicsEvent req) {
        if (FSMBasicStateNames.START.equals(state)) {
          return FSMBasicStateNames.FINAL;
        } else {
          throw new IllegalStateException("unexpected message");
        }
      }
    };
  }

  public static FSMPatternEventHandler patternDefault() {
    return new FSMPatternEventHandler<FSMExternalState, FSMInternalState, KompicsEvent, PatternExtractor>() {
      @Override
      public FSMStateName handle(FSMStateName state, FSMExternalState es, FSMInternalState is, KompicsEvent req,
        PatternExtractor container) {
        if (FSMBasicStateNames.START.equals(state)) {
          return FSMBasicStateNames.FINAL;
        } else {
          throw new IllegalStateException("unexpected message");
        }
      }
    };
  }
}
