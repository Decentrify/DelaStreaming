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
//package se.sics.gvod.stream.system;
//
//import java.io.File;
//
///**
// * @author Alex Ormenisan <aaor@kth.se>
// */
//public class BuildFile {
//
//    public static void main(String[] args) {
//        File uploadFile = new File("./src/main/resources/experiment1/uploader/test.txt");
//        uploadFile.getParentFile().mkdirs();
//        uploadFile.createNewFile();
//        generateFile(uploadFile, rand);
//        HashUtil.makeHashes(uploadFilePath, uploadHashPath, hashAlg, blockSize);
//    }
//
//    private void generateFile(File file, Random rand) throws IOException {
//        FileOutputStream out = new FileOutputStream(file);
//        for (int i = 0; i < fileBlockSize - 1; i++) {
//            byte[] data = new byte[blockSize];
//            rand.nextBytes(data);
//            out.write(data);
//        }
//        byte[] data = new byte[blockSize - 1];
//        rand.nextBytes(data);
//        out.write(data);
//        LOG.info("generatedSize:{}", fileBlockSize);
//        out.flush();
//        out.close();
//    }
//}
//}
