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
package se.sics.nstream.test;

import se.sics.ktoolbox.util.test.EqualComparator;
import se.sics.nstream.storage.cache.KHint;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class KHintSummaryEC implements EqualComparator<KHint.Summary>{
    @Override
    public boolean isEqual(KHint.Summary o1, KHint.Summary o2) {
        if(o1.lStamp != o2.lStamp) {
            return false;
        }
        if(!o1.blocks.equals(o2.blocks)) {
            return false;
        }
        return true;
    }
}
