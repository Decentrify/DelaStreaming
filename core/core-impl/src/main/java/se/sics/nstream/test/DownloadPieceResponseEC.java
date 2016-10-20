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
package se.sics.nstream.test;

import java.util.Arrays;
import java.util.Objects;
import se.sics.ktoolbox.util.test.EqualComparator;
import se.sics.nstream.torrent.transfer.msg.DownloadPiece;

/**
 *
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class DownloadPieceResponseEC implements EqualComparator<DownloadPiece.Response>{
    @Override
    public boolean isEqual(DownloadPiece.Response o1, DownloadPiece.Response o2) {
        if(o1 == null && o2 == null) {
            return true;
        }
        if(o1 == null || o2 == null) {
            return false;
        }
        if(!o1.msgId.equals(o2.msgId)) {
            return false;
        }
        if(!o1.fileId.equals(o2.fileId)) {
            return false;
        }
        if(!Objects.equals(o1.piece, o2.piece)) {
            return false;
        }
        
        byte[] v1 = o1.val.isLeft() ? o1.val.getLeft().getValue().get() : o1.val.getRight();
        byte[] v2 = o2.val.isLeft() ? o2.val.getLeft().getValue().get() : o2.val.getRight();
        if(Arrays.equals(v1, v2)) {
            return true;
        }
        return false;
    }
}