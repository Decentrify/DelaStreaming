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
package se.sics.dela.storage.cache;

import com.google.common.base.Optional;
import se.sics.ktoolbox.util.reference.KReference;
import se.sics.ktoolbox.util.reference.KReferenceException;
import se.sics.ktoolbox.util.reference.KReferenceFactory;

/**
 * Special type of reference. Means of communication between cache(1) and
 * buffer(N). While buffers did not receive an answer that the remote resources
 * have saved this particular part of the resource, the cache should retain it
 *
 * @author Alex Ormenisan <aaor@kth.se>
 */
class CacheKReference implements KReference<KReference<byte[]>> {
    private final KReference<Boolean> ref;
    private final KReference<byte[]> value;

    CacheKReference(KReference<byte[]> value) {
        this.ref = KReferenceFactory.getReference(true);
        this.value = value;
    }

    @Override
    public boolean isValid() {
        return ref.isValid();
    }

    @Override
    public synchronized boolean retain() {
        return ref.retain();
    }

    @Override
    public synchronized void release() throws KReferenceException {
        ref.release();
        if (!ref.isValid()) {
            value.release();
        }
    }

    @Override
    public Optional<KReference<byte[]>> getValue() {
        return Optional.of(value);
    }
    
    /**
     * we know that CacheKReference always has value
     * @return 
     */
    public KReference<byte[]> value() {
        return value;
    }

    public Optional<byte[]> getBaseValue() {
        Optional<byte[]> val = value.getValue();
        return val;
    }

    public static CacheKReference createInstance(KReference<byte[]> value) {
        value.retain();
        CacheKReference ref = new CacheKReference(value);
        return ref;
    }
}
