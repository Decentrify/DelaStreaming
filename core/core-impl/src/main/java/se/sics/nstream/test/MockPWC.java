/*
 * Copyright (C) 2009 Swedish Institute of Computer Science (SICS) Copyright (C)
 * 2009 Royal Institute of Technology (KTH)
 *
 * KompicsToolbox is free software; you can redistribute it and/or
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

import se.sics.ktoolbox.util.result.Result;
import se.sics.nstream.storage.buffer.WriteResult;
import se.sics.nstream.util.result.BlockWriteCallback;
import se.sics.nstream.util.result.PieceWriteCallback;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class MockPWC implements PieceWriteCallback {
    public MockBWC blockCallback = new MockBWC();
    public boolean waitingOnBlock = false;
    public WriteResult pieceResult;
    public boolean done = false;
    
    @Override
    public BlockWriteCallback getBlockCallback() {
        waitingOnBlock = true;
        return blockCallback;
    }

    @Override
    public boolean fail(Result<WriteResult> result) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public boolean success(Result<WriteResult> result) {
        pieceResult = result.getValue();
        done = true;
        return true;
    }
    
}
