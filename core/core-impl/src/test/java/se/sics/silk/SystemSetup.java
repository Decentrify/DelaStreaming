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
package se.sics.silk;

import com.google.common.base.Optional;
import java.util.Properties;
import java.util.UUID;
import se.sics.kompics.Kompics;
import se.sics.kompics.config.Config;
import se.sics.kompics.fsm.FSMException;
import se.sics.kompics.fsm.id.FSMIdentifierFactory;
import se.sics.ktoolbox.util.identifiable.BasicIdentifiers;
import se.sics.ktoolbox.util.identifiable.IdentifierFactory;
import se.sics.ktoolbox.util.identifiable.IdentifierRegistry;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayIdFactory;
import se.sics.ktoolbox.util.identifiable.overlay.OverlayRegistry;
import se.sics.nstream.TorrentIds;
import se.sics.silk.r2torrent.R1Hash;
import se.sics.silk.r2torrent.R1MetadataGet;
import se.sics.silk.r2torrent.R2NodeLeecher;
import se.sics.silk.r2torrent.R2Torrent;
import se.sics.silk.r2torrent.conn.R1TorrentSeeder;
import se.sics.silk.r2torrent.conn.R2NodeSeeder;
import se.sics.silkold.torrentmngr.TorrentMngrFSM;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class SystemSetup {
  public static OverlayIdFactory systemSetup(String applicationConfPath) throws FSMException {
    Properties props = System.getProperties();
    props.setProperty("config.file", applicationConfPath);

    TorrentIds.registerDefaults(1234);
    OverlayIdFactory oidFactory = overlaysSetup();
    fsmSetup();
    return oidFactory;
  }
  
  private static OverlayIdFactory overlaysSetup() {
    OverlayRegistry.reset();
    OverlayRegistry.initiate(new SystemOverlays.TypeFactory(), new SystemOverlays.Comparator());

    byte torrentOwnerId = 1;
    OverlayRegistry.registerPrefix(TorrentIds.TORRENT_OVERLAYS, torrentOwnerId);

    IdentifierFactory torrentBaseIdFactory = IdentifierRegistry.lookup(BasicIdentifiers.Values.OVERLAY.toString());
    return new OverlayIdFactory(torrentBaseIdFactory, TorrentIds.Types.TORRENT, torrentOwnerId);
  }

  private static void fsmSetup() throws FSMException {
    FSMIdentifierFactory fsmIdFactory = FSMIdentifierFactory.DEFAULT;
    fsmIdFactory.reset();
    fsmIdFactory.registerFSMDefId(TorrentMngrFSM.NAME);
    fsmIdFactory.registerFSMDefId(R2NodeSeeder.NAME);
    fsmIdFactory.registerFSMDefId(R2NodeLeecher.NAME);
    fsmIdFactory.registerFSMDefId(R2Torrent.NAME);
    fsmIdFactory.registerFSMDefId(R1MetadataGet.NAME);
    fsmIdFactory.registerFSMDefId(R1Hash.NAME);
    fsmIdFactory.registerFSMDefId(R1TorrentSeeder.NAME);
    
    Config.Impl config = (Config.Impl) Kompics.getConfig();
    Config.Builder builder = Kompics.getConfig().modify(UUID.randomUUID());
    builder.setValue(FSMIdentifierFactory.CONFIG_KEY, fsmIdFactory);
    config.apply(builder.finalise(), (Optional) Optional.absent());
    Kompics.setConfig(config);
  }
}