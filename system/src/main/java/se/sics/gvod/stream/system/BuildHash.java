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

import java.io.File;
import java.io.IOException;
import se.sics.ktoolbox.util.managedStore.core.util.HashUtil;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class BuildHash {

    public static void main(String[] args) throws IOException, HashUtil.HashBuilderException {
        File dataFile = new File("./src/main/resources/experiment1/uploader/test.txt");
        if(!dataFile.exists()) {
            throw new RuntimeException("missing file");
        }
        File hashFile = new File("./src/main/resources/experiment1/uploader/test.hash");
        hashFile.createNewFile();
        String hashAlg = HashUtil.getAlgName(HashUtil.SHA);
        int pieceSize = 1024;
        int piecesPerBlock = 1024;
        int blockSize = pieceSize * piecesPerBlock;
        HashUtil.makeHashes(dataFile.getAbsolutePath(), hashFile.getAbsolutePath(), hashAlg, blockSize);
    }
}
