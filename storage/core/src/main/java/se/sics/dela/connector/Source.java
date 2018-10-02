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
package se.sics.dela.connector;

import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import se.sics.ktoolbox.util.TupleHelper;
import se.sics.ktoolbox.util.reference.KReference;
import se.sics.ktoolbox.util.trysf.Try;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public interface Source extends ConnectorCtrl {
  public String getName();
  public void readHash(Set<Integer> hashNrs, 
    TupleHelper.PairConsumer<Try<Map<Integer, KReference<byte[]>>>, Set<Integer>> callback);
  public void cancelHash(int hash);
  public void read(int blockNr, Consumer<Try<KReference<byte[]>>> callback);
  public void cancelBlock(int block);
}
