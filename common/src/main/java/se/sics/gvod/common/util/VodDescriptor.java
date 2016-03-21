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

package se.sics.gvod.common.util;

import se.sics.ktoolbox.util.update.View;

/**
 * @author Alex Ormenisan <aaor@sics.se>
 */
public class VodDescriptor implements View {
    public final int downloadPos;
    
    public VodDescriptor(int downloadPosition) {
        this.downloadPos = downloadPosition;
    }
    
    public VodDescriptor deepCopy() {
        return new VodDescriptor(downloadPos);
    }
    
    @Override
    public String toString() {
        return "VodDescriptor:<pos:" + downloadPos + ">";
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 59 * hash + this.downloadPos;
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final VodDescriptor other = (VodDescriptor) obj;
        if (this.downloadPos != other.downloadPos) {
            return false;
        }
        return true;
    }
    
    
}
