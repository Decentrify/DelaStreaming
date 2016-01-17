package se.sics.gvod.common.junk;

///*
// * Copyright (C) 2009 Swedish Institute of Computer Science (SICS) Copyright (C)
// * 2009 Royal Institute of Technology (KTH)
// *
// * GVoD is free software; you can redistribute it and/or
// * modify it under the terms of the GNU General Public License
// * as published by the Free Software Foundation; either version 2
// * of the License, or (at your option) any later version.
// *
// * This program is distributed in the hope that it will be useful,
// * but WITHOUT ANY WARRANTY; without even the implied warranty of
// * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// * GNU General Public License for more details.
// *
// * You should have received a copy of the GNU General Public License
// * along with this program; if not, write to the Free Software
// * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
// */
//
//package se.sics.gvod.common.utility;
//
//import com.typesafe.config.Config;
//import com.typesafe.config.ConfigException;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
///**
// * @author Alex Ormenisan <aaor@sics.se>
// */
//public class GVoDReferenceConfig {
//    private static final Logger LOG = LoggerFactory.getLogger("GVoDConfig");
//    private static final String logPrefix = "reference configuration";
//
//    private final Config config;
//    
//    private int pieceSize;
//    private int piecesPerBlock;
//    private String hashAlg;
//    
//    public GVoDReferenceConfig(Config config) {
//        this.config = config;
//        try {
//            loadVoDConfig();
//        } catch (ConfigException.Missing ex) {
//            LOG.error("{} error:{}", logPrefix, ex.getMessage());
//            throw new RuntimeException("reference configuration error", ex);
//        }
//    }
//    
//    private void loadVoDConfig() {
//        pieceSize = config.getInt("vod.video.pieceSize");
//        piecesPerBlock = config.getInt("vod.video.piecesPerBlock");
//        hashAlg = config.getString("vod.hashAlg");
//    }
//
//    /**
//     * @return
//     * @deprecated Should not need any extra config parameters, they should already be parsed in the GVoDConfig and GVoDReferenceConfig
//     */
//    @Deprecated
//    public Config getConfig() {
//        return config;
//    }
//
//    public int getPieceSize() {
//        return pieceSize;
//    }
//
//    public int getPiecesPerBlock() {
//        return piecesPerBlock;
//    }
//
//    public String getHashAlg() {
//        return hashAlg;
//    }
//}
