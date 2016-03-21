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
package se.sics.gvod.core.aggregation;

import org.javatuples.Pair;
import org.javatuples.Triplet;
import se.sics.ktoolbox.util.aggregation.StatePacket;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class ConnMngrStatePacket implements StatePacket {

    //<uppConn, downConn>
    public final Pair<Integer, Integer> connections;
    //<downloaders, downloadingHashReq, downloadingDataReq>
    public final Triplet<Integer, Integer, Integer> uploadInfo;
    //<uploaders, uploadingHashReq, uploadingDataReq>
    public final Triplet<Integer, Integer, Integer> downloadInfo;

    public ConnMngrStatePacket(Pair<Integer, Integer> connections,
            Triplet<Integer, Integer, Integer> uploadInfo,
            Triplet<Integer, Integer, Integer> downloadInfo) {
        this.connections = connections;
        this.uploadInfo = uploadInfo;
        this.downloadInfo = downloadInfo;
    }

    @Override
    public String shortPrint() {
        return "ConnMngr state:"
                + " c<u:" + connections.getValue0() + ", d:" + connections.getValue1() + ">"
                + " u<nr:" + uploadInfo.getValue0() + ", h:" + uploadInfo.getValue1() + ", d:" + uploadInfo.getValue2() + ">"
                + " d<nr:" + downloadInfo.getValue0() + ", h:" + downloadInfo.getValue1() + ", d:" + downloadInfo.getValue2() + ">"
                + ">";
    }
}
