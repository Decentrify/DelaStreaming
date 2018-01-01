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
package se.sics.nstream.torrent.transfer;

import java.util.HashSet;
import java.util.Set;
import se.sics.kompics.KompicsEvent;
import se.sics.kompics.PatternExtractor;
import se.sics.kompics.util.PatternExtractorHelper;
import se.sics.ktoolbox.util.network.KContentMsg;
import se.sics.ktoolbox.util.network.ports.ChannelFilter;
import se.sics.nstream.torrent.conn.msg.NetCloseTransfer;
import se.sics.nstream.torrent.conn.msg.NetConnect;
import se.sics.nstream.torrent.conn.msg.NetDetailedState;
import se.sics.nstream.torrent.conn.msg.NetOpenTransfer;
import se.sics.nstream.torrent.transfer.msg.CacheHint;
import se.sics.nstream.torrent.transfer.msg.DownloadHash;
import se.sics.nstream.torrent.transfer.msg.DownloadPiece;

/**
 *
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class TransferCompFilters {

    public static final ChannelFilter connInclusionFilter = new ChannelFilter() {
        private final Set<Class> allowedClasses = new HashSet<>();

        {
            allowedClasses.add(NetConnect.Request.class);
            allowedClasses.add(NetConnect.Response.class);
            allowedClasses.add(NetDetailedState.Request.class);
            allowedClasses.add(NetDetailedState.Response.class);
            allowedClasses.add(NetOpenTransfer.Request.class);
            allowedClasses.add(NetOpenTransfer.Response.class);
            allowedClasses.add(NetCloseTransfer.class);
        }

        @Override
        public boolean filter(KompicsEvent event) {
            if (!(event instanceof KContentMsg)) {
                return false;
            }
            Object baseContent = ((KContentMsg)event).getContent();
            if (baseContent instanceof PatternExtractor) {
                baseContent = PatternExtractorHelper.peelAllLayers((PatternExtractor)baseContent);
            }
            return !allowedClasses.contains(baseContent.getClass());
        }
    };

    public static final ChannelFilter transferInclusionFilter = new ChannelFilter() {
        private final Set<Class> allowedClasses = new HashSet<>();

        {
            allowedClasses.add(CacheHint.Request.class);
            allowedClasses.add(CacheHint.Response.class);
            allowedClasses.add(DownloadPiece.Request.class);
            allowedClasses.add(DownloadPiece.Success.class);
            allowedClasses.add(DownloadHash.Request.class);
            allowedClasses.add(DownloadHash.Success.class);
        }

        @Override
        public boolean filter(KompicsEvent event) {
            if (!(event instanceof KContentMsg)) {
                return false;
            }
            Object baseContent = ((KContentMsg)event).getContent();
            if (baseContent instanceof PatternExtractor) {
                baseContent = PatternExtractorHelper.peelAllLayers((PatternExtractor)baseContent);
            }
            return !allowedClasses.contains(baseContent.getClass());
        }
    };
}
