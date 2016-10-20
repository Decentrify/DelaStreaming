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
package se.sics.nstream.tracker;

import java.util.Set;
import java.util.TreeSet;

/**
 * @author Alex Ormenisan <aaor@sics.se>
 */
public class CompleteTracker implements ComponentTracker {

    private final int nrComponents;

    public CompleteTracker(int nrComponents) {
        this.nrComponents = nrComponents;
    }

    @Override
    public boolean isComplete() {
        return true;
    }

    @Override
    public int nextComponentMissing(int fromComponentNr) {
        return -1;
    }

    @Override
    public Integer nextComponentMissing(int fromComponentNr, Set<Integer> except) {
        return -1;
    }

    @Override
    public Set<Integer> nextComponentMissing(int fromComponentNr, int n, Set<Integer> except) {
        return new TreeSet<Integer>();
    }

    @Override
    public boolean hasComponent(int componentNr) {
        if (componentNr < nrComponents) {
            return true;
        }
        return false;
    }

    @Override
    public void addComponent(int componentNr) {
        throw new RuntimeException("Should not call write related methods on a CompleteTracker");
    }

    @Override
    public int completedComponents() {
        return nrComponents;
    }

    @Override
    public int nrComponents() {
        return nrComponents;
    }
}
