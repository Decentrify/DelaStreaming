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

import se.sics.kompics.config.Config;
import se.sics.ktoolbox.util.aggregation.AggregationLevel;
import se.sics.ktoolbox.util.config.KConfigHelper;

/**
 * @author Alex Ormenisan <aaor@sics.se>
 */
public class DownloadMngrKCWrapper {

    private final Config configCore;
    public final int startPieces;
    public final long descriptorUpdate;
    
    public final int pieceSize;
    public final int piecesPerBlock;
    public final int hashesPerMsg = 10;
    public final int minHashAhead = 20; //if piece = 5 and hash = 20, you download hash, if hash is 25, you download piece
    public final String hashAlg = "SHA";
    public final long speedupPeriod = 2000;
    public final long internalStateCheckPeriod = 30000;
    
    public final AggregationLevel downloadMngrAggLevel;
    public final long downloadMngrAggPeriod;

    public DownloadMngrKCWrapper(Config configCore) {
        this.configCore = configCore;
        startPieces = KConfigHelper.read(configCore, DownloadMngrKConfig.startPieces);
        descriptorUpdate = KConfigHelper.read(configCore, DownloadMngrKConfig.descriptorUpdate);
        pieceSize = KConfigHelper.read(configCore, DownloadMngrKConfig.pieceSize);
        piecesPerBlock = KConfigHelper.read(configCore, DownloadMngrKConfig.piecesPerBlock);
        downloadMngrAggLevel = KConfigHelper.read(configCore, DownloadMngrKConfig.aggLevel);
        downloadMngrAggPeriod = KConfigHelper.read(configCore, DownloadMngrKConfig.aggPeriod);
    }
    
    public DownloadMngrKCWrapper(Config configCore, int startPieces, long descriptorUpdate, int pieceSize, 
            int piecesPerBlock, AggregationLevel aggLvl, long aggPeriod) {
        this.configCore = configCore;
        this.startPieces = startPieces;
        this.descriptorUpdate = descriptorUpdate;
        this.pieceSize = pieceSize;
        this.piecesPerBlock = piecesPerBlock;
        this.downloadMngrAggLevel = aggLvl;
        this.downloadMngrAggPeriod = aggPeriod;
    }
}
