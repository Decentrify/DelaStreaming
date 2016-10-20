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
package se.sics.nstream.storage.durable.disk;

import java.util.ArrayList;
import java.util.List;
import org.javatuples.Pair;
import se.sics.nstream.StreamId;
import se.sics.nstream.storage.durable.util.FileExtendedDetails;
import se.sics.nstream.storage.durable.util.MyStream;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class DiskFED implements FileExtendedDetails {
    public final Pair<StreamId, MyStream> diskStream;
    
    public DiskFED(Pair<StreamId, MyStream> diskStream) {
        this.diskStream = diskStream;
    }
    
    public DiskFED(StreamId streamId, MyStream stream) {
        this(Pair.with(streamId, stream));
    }
    
    @Override
    public Pair<StreamId, MyStream> getMainStream() {
        return diskStream;
    }

    @Override
    public List<Pair<StreamId, MyStream>> getSecondaryStreams() {
        return new ArrayList<Pair<StreamId, MyStream>>();
    }
}
