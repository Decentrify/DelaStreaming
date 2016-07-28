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
package se.sics.nstream.hops;

import com.google.common.base.Optional;
import java.util.ArrayList;
import java.util.List;
import org.javatuples.Pair;
import se.sics.nstream.hops.hdfs.HDFSEndpoint;
import se.sics.nstream.hops.hdfs.HDFSResource;
import se.sics.nstream.hops.kafka.KafkaEndpoint;
import se.sics.nstream.hops.kafka.KafkaResource;
import se.sics.nstream.util.FileExtendedDetails;
import se.sics.nstream.util.StreamEndpoint;
import se.sics.nstream.util.StreamResource;

/**
 *
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class HopsFED implements FileExtendedDetails {
    public final Pair<HDFSEndpoint, HDFSResource> mainResource;
    public final Optional<Pair<KafkaEndpoint, KafkaResource>> secondaryResource;
    
    public HopsFED(Pair<HDFSEndpoint, HDFSResource> mainResource, Optional<Pair<KafkaEndpoint, KafkaResource>> secondaryResource) {
        this.mainResource = mainResource;
        this.secondaryResource = secondaryResource;
    }
    
    @Override
    public Pair<StreamEndpoint, StreamResource> getMainResource() {
        return (Pair)mainResource;
    }

    @Override
    public List<Pair<StreamEndpoint, StreamResource>> getSecondaryResource() {
        List<Pair<StreamEndpoint, StreamResource>> sr = new ArrayList<>();
        if(secondaryResource.isPresent()) {
            sr.add((Pair)secondaryResource.get());
        }
        return sr;
    }
    
}
