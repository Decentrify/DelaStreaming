/*
 * Copyright (C) 2009 Swedish Institute of Computer Science (SICS) Copyright (C)
 * 2009 Royal Institute of Technology (KTH)
 *
 * KompicsToolbox is free software; you can redistribute it and/or
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
package se.sics.nstream.util.range;

import com.google.common.base.Optional;
import com.google.common.collect.Range;
import java.util.Arrays;
import se.sics.ktoolbox.util.reference.KReference;
import se.sics.ktoolbox.util.reference.KReferenceException;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class RangeKReference implements KReference<byte[]> {
    
    private final KReference<byte[]> base;
    private final Range<Long> range;

    private RangeKReference(KReference<byte[]> base, Range<Long> range) {
        this.base = base;
        this.range = range;
    }

    @Override
    public boolean isValid() {
        return base.isValid();
    }

    @Override
    public boolean retain() {
        return base.retain();
    }

    @Override
    public void release() throws KReferenceException {
        base.release();
    }

    @Override
    public Optional<byte[]> getValue() {
        try {
            byte[] baseVal = base.getValue().get();
            //i know they are int
            byte[] val = Arrays.copyOfRange(baseVal, (int)(long)range.lowerEndpoint(), (int)(long)range.upperEndpoint() + 1);
            return Optional.of(val);
        } catch (IllegalStateException ex) {
            return Optional.absent();
        }
    }

    public static RangeKReference createInstance(KReference<byte[]> base, long blockPos, KPiece range) {
        base.retain();
        return new RangeKReference(base, range.translate(-1 * blockPos));
    }
}
