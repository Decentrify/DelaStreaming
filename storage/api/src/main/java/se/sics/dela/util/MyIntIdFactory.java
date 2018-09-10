/*
 * This file is part of the Kompics Simulator.
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
package se.sics.dela.util;

import se.sics.kompics.util.Identifier;
import se.sics.ktoolbox.util.identifiable.BasicBuilders;
import se.sics.ktoolbox.util.identifiable.IdentifierBuilder;
import se.sics.ktoolbox.util.identifiable.basic.IntId;
import se.sics.ktoolbox.util.identifiable.basic.IntIdFactory;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class MyIntIdFactory implements MyIdentifierFactory<IntId> {
  private final IntIdFactory base;
  private int startingId;

  public MyIntIdFactory(IntIdFactory base, int startingId) {
    this.base = base;
    this.startingId = startingId;
  }
  
  @Override
  public Identifier next() {
    return base.id(new BasicBuilders.IntBuilder(startingId++));
  }

  @Override
  public IntId randomId() {
    return base.randomId();
  }

  @Override
  public IntId id(IdentifierBuilder builder) {
    return base.id(builder);
  }

  @Override
  public Class idType() {
    return base.idType();
  }

  @Override
  public void setRegisteredName(String name) {
    base.setRegisteredName(name);
  }

  @Override
  public String getRegisteredName() {
    return base.getRegisteredName();
  }
}
