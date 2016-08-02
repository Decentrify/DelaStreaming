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
package se.sics.nstream.hops.library.event.core;

import com.google.common.base.Optional;
import java.util.List;
import java.util.Map;
import org.javatuples.Pair;
import org.javatuples.Triplet;
import se.sics.kompics.Direct;
import se.sics.ktoolbox.util.identifiable.Identifier;
import se.sics.ktoolbox.util.identifiable.basic.UUIDIdentifier;
import se.sics.ktoolbox.util.network.KAddress;
import se.sics.ktoolbox.util.result.Result;
import se.sics.nstream.StreamEvent;
import se.sics.nstream.hops.hdfs.HDFSEndpoint;
import se.sics.nstream.hops.hdfs.HDFSResource;
import se.sics.nstream.hops.kafka.KafkaEndpoint;
import se.sics.nstream.util.FileExtendedDetails;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class HopsTorrentDownloadEvent {

    public static class StartRequest extends Direct.Request<StartResponse> implements StreamEvent {

        public final Identifier eventId;
        public final Identifier torrentId;
        public final String torrentName;
        public final HDFSEndpoint hdfsEndpoint;
        public final HDFSResource manifest;
        public final List<KAddress> partners;

        public StartRequest(Identifier eventId, Identifier torrentId, String torrentName, HDFSEndpoint hdfsEndpoint, HDFSResource manifest, List<KAddress> partners) {
            this.eventId = eventId;
            this.torrentId = torrentId;
            this.torrentName = torrentName;
            this.hdfsEndpoint = hdfsEndpoint;
            this.manifest = manifest;
            this.partners = partners;
        }

        public StartRequest(Identifier torrentId, String torrentName, HDFSEndpoint hdfsEndpoint, HDFSResource manifest, List<KAddress> partners) {
            this(UUIDIdentifier.randomId(), torrentId, torrentName, hdfsEndpoint, manifest, partners);
        }

        @Override
        public Identifier getId() {
            return eventId;
        }

        public StartResponse alreadyExists(Result<Pair<Triplet<String, HDFSEndpoint, HDFSResource>, Map<String, FileExtendedDetails>>> result) {
            return new AlreadyExists(this, result);
        }

        public StartResponse starting(Result<Boolean> result) {
            return new Starting(this, result);
        }
    }

    public static interface StartResponse extends Direct.Response, StreamEvent {
    }

    public static class Starting implements StartResponse {

        public final StartRequest req;
        public final Result<Boolean> result;

        public Starting(StartRequest req, Result<Boolean> result) {
            this.req = req;
            this.result = result;
        }

        @Override
        public Identifier getId() {
            return req.eventId;
        }
    }

    public static class AlreadyExists implements StartResponse {

        public final StartRequest req;
        public final Result<Pair<Triplet<String, HDFSEndpoint, HDFSResource>, Map<String, FileExtendedDetails>>> result;

        public AlreadyExists(StartRequest req, Result<Pair<Triplet<String, HDFSEndpoint, HDFSResource>, Map<String, FileExtendedDetails>>> result) {
            this.req = req;
            this.result = result;
        }

        @Override
        public Identifier getId() {
            return req.eventId;
        }
    }

    public static class AdvanceRequest extends Direct.Request<AdvanceResponse> implements StreamEvent {

        public final Identifier eventId;
        public final Identifier torrentId;
        public final HDFSEndpoint hdfsEndpoint;
        public final Optional<KafkaEndpoint> kafkaEndpoint;
        public final Result<Map<String, FileExtendedDetails>> extendedDetails;

        public AdvanceRequest(Identifier eventId, Identifier torrentId, HDFSEndpoint hdfsEndpoint, Optional<KafkaEndpoint> kafkaEndpoint,
                Result<Map<String, FileExtendedDetails>> extendedDetails) {
            this.eventId = eventId;
            this.torrentId = torrentId;
            this.hdfsEndpoint = hdfsEndpoint;
            this.kafkaEndpoint = kafkaEndpoint;
            this.extendedDetails = extendedDetails;
        }

        public AdvanceRequest(Identifier torrentId, HDFSEndpoint hdfsEndpoint, Optional<KafkaEndpoint> kafkaEndpoint, 
                Result<Map<String, FileExtendedDetails>> extendedDetails) {
            this(UUIDIdentifier.randomId(), torrentId, hdfsEndpoint, kafkaEndpoint, extendedDetails);
        }

        @Override
        public Identifier getId() {
            return eventId;
        }

        public AdvanceResponse answer(Result<Boolean> result) {
            return new AdvanceResponse(this, result);
        }
    }

    public static class AdvanceResponse implements Direct.Response, StreamEvent {

        public final AdvanceRequest req;
        public final Result<Boolean> result;

        public AdvanceResponse(AdvanceRequest req, Result<Boolean> result) {
            this.req = req;
            this.result = result;
        }

        @Override
        public Identifier getId() {
            return req.eventId;
        }
    }
}
