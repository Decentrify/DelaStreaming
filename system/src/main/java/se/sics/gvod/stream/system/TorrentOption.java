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
package se.sics.gvod.stream.system;

import com.google.common.base.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.kompics.config.Config;
import se.sics.ktoolbox.util.config.KConfigOption.Base;
import se.sics.ktoolbox.util.config.KConfigOption.Basic;
import se.sics.ktoolbox.util.identifiable.Identifier;
import se.sics.ktoolbox.util.identifiable.basic.IntIdentifier;
import se.sics.ktoolbox.util.managedStore.core.util.FileInfo;
import se.sics.ktoolbox.util.managedStore.core.util.Torrent;
import se.sics.ktoolbox.util.managedStore.core.util.TorrentInfo;

/**
 *
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class TorrentOption extends Base<Torrent> {

    private static final Logger LOG = LoggerFactory.getLogger("KConfig");

    public TorrentOption(String name) {
        super(name, Torrent.class);
    }

    @Override
    public Optional<Torrent> readValue(Config config) {
        Basic<Integer> oIdOpt = new Basic(name + ".overlayId", Integer.class);
        Optional<Integer> oId = oIdOpt.readValue(config);
        if (!oId.isPresent()) {
            LOG.debug("missing:{}", oIdOpt.name);
            return Optional.absent();
        }
        Identifier overlayId = new IntIdentifier(oId.get());
        
        Basic<String> nameOpt = new Basic(name + ".name", String.class);
        Optional<String> name = nameOpt.readValue(config);
        if (!name.isPresent()) {
            LOG.debug("missing:{}", nameOpt.name);
            return Optional.absent();
        }
        Basic<Integer> sizeOpt = new Basic(name + ".size", Integer.class);
        Optional<Integer> size = sizeOpt.readValue(config);
        if (!size.isPresent()) {
            LOG.debug("missing:{}", sizeOpt.name);
            return Optional.absent();
        }
        FileInfo fileInfo = FileInfo.newFile(name.get(), size.get());
        
        Basic<Integer> pieceSizeOpt = new Basic(name + ".pieceSize", Integer.class);
        Optional<Integer> pieceSize = pieceSizeOpt.readValue(config);
        if (!pieceSize.isPresent()) {
            LOG.debug("missing:{}", pieceSizeOpt.name);
            return Optional.absent();
        }
        Basic<Integer> piecesPerBlockOpt = new Basic(name + ".piecesPerBlock", Integer.class);
        Optional<Integer> piecesPerBlock = piecesPerBlockOpt.readValue(config);
        if (!piecesPerBlock.isPresent()) {
            LOG.debug("missing:{}", piecesPerBlockOpt.name);
            return Optional.absent();
        }
        Basic<String> hashAlgOpt = new Basic(name + ".hashAlg", String.class);
        Optional<String> hashAlg = hashAlgOpt.readValue(config);
        if (!hashAlg.isPresent()) {
            LOG.debug("missing:{}", hashAlgOpt.name);
            return Optional.absent();
        }
        Basic<Long> hashFileSizeOpt = new Basic(name + ".hashFileSize", Long.class);
        Optional<Long> hashFileSize = hashFileSizeOpt.readValue(config);
        if (!hashFileSize.isPresent()) {
            LOG.debug("missing:{}", hashFileSizeOpt.name);
            return Optional.absent();
        }
        TorrentInfo torrentInfo = new TorrentInfo(pieceSize.get(), piecesPerBlock.get(), hashAlg.get(), hashFileSize.get());
        Torrent torrent = new Torrent(overlayId, fileInfo, torrentInfo);
        
        return Optional.of(torrent);
    }

}
