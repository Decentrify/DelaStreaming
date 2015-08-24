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
package se.sics.gvod.core.downloadMngr;

import com.typesafe.config.Config;
import se.sics.gvod.common.util.GVoDConfigException;
import se.sics.p2ptoolbox.util.network.impl.DecoratedAddress;

/**
 *
 * @author Alex Ormenisan <aaor@sics.se>
 */
public class DownloadMngrConfig {

    private final Config config;
    private final DecoratedAddress selfAddress;
    public final int overlayId;
    public final int startPieces;
    public final long descriptorUpdate;
    
    public final int pieceSize;
    public final int piecesPerBlock;
    public final int hashesPerMsg = 10;
    public final int minHashAhead = 20; //if piece = 5 and hash = 20, you download hash, if hash is 25, you download piece
    public final String hashAlg = "SHA";
    public final long speedupPeriod = 2000;

    public DownloadMngrConfig(Config config, DecoratedAddress selfAddress, int overlayId, int startPieces, long descriptorUpdate, int pieceSize, int piecesPerBlock) {
        this.config = config;
        this.selfAddress = selfAddress;
        this.overlayId = overlayId;
        this.startPieces = startPieces;
        this.descriptorUpdate = descriptorUpdate;
        this.pieceSize = pieceSize;
        this.piecesPerBlock = piecesPerBlock;
    }

    public DecoratedAddress getSelf() {
        return selfAddress;
    }

    public static class Builder {

        private final Config config;
        private final DecoratedAddress selfAddress;
        private final int overlayId;

        public Builder(Config config, DecoratedAddress selfAddress, int overlayId) {
            this.config = config;
            this.selfAddress = selfAddress;
            this.overlayId = overlayId;
        }

        public DownloadMngrConfig finalise() throws GVoDConfigException.Missing {
            int startPieces = config.getInt("vod.video.startPieces");
            long descriptorUpdate = config.getLong("vod.video.descriptorUpdate");
            int pieceSize = config.getInt("vod.video.pieceSize");
            int piecesPerBlock = config.getInt("vod.video.piecesPerBlock");
            return new DownloadMngrConfig(config, selfAddress, overlayId, startPieces, descriptorUpdate, pieceSize, piecesPerBlock);
        }
    }
}
