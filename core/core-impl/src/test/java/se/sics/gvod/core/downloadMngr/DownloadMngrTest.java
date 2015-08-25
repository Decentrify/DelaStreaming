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

import com.google.common.base.Optional;
import com.google.common.io.Files;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.security.SecureRandom;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import org.javatuples.Pair;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.p2ptoolbox.util.managedStore.FileMngr;
import se.sics.p2ptoolbox.util.managedStore.HashMngr;
import se.sics.p2ptoolbox.util.managedStore.HashUtil;
import se.sics.p2ptoolbox.util.managedStore.StorageMngrFactory;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class DownloadMngrTest {

    private static final Logger LOG = LoggerFactory.getLogger(DownloadMngrTest.class);
    String testDirPath = "./src/test/resources/dmTest/";
    String hashFilePath = testDirPath + "uploadFile.hash";
    String uploadFilePath = testDirPath + "uploadFile.mp4";
    String download1FilePath = testDirPath + "downloadFile1.mp4";
    String download1HashPath = testDirPath + "downloadFile1.hash";
    String download2FilePath = testDirPath + "downloadFile2.mp4";
    String download2HashPath = testDirPath + "downloadFile2.hash";

    String hashAlg = HashUtil.getAlgName(HashUtil.SHA);
    int generateSize = 100;
    int pieceSize = 1024;
    int piecesPerBlock = 1024;
    int randomRuns = 10;
    int nrBlocks;
    double lossRate = 0.1;
    double jumpRate = 0.3;
    double corruptionRate = 0.0001; //piece wise

    public void setup(Random rand) throws IOException, HashUtil.HashBuilderException {
        cleanup();
        File testDir = new File(testDirPath);
        testDir.mkdirs();
        File uploadFile = new File(uploadFilePath);
        uploadFile.createNewFile();
        generateFile(uploadFile, rand);
        nrBlocks = StorageMngrFactory.nrPieces(uploadFile.length(), piecesPerBlock * pieceSize);
        HashUtil.makeHashes(uploadFilePath, hashFilePath, hashAlg, piecesPerBlock * pieceSize);
    }

    private void generateFile(File file, Random rand) throws IOException {
        FileOutputStream out = new FileOutputStream(file);
        for (int i = 0; i < generateSize; i++) {
            byte[] data = new byte[piecesPerBlock * pieceSize];
            rand.nextBytes(data);
            out.write(data);
//            LOG.info("{} block:{}", i, BaseEncoding.base16().encode(data));
        }
        byte[] data = new byte[piecesPerBlock * pieceSize - 1];
        rand.nextBytes(data);
        out.write(data);
//        LOG.info("{} block:{}", generateSize, BaseEncoding.base16().encode(data));
        out.flush();
        out.close();
    }

    public void cleanup() {
        File testDir = new File(testDirPath);
        if (testDir.exists()) {
            File uploadFile = new File(uploadFilePath);
            uploadFile.delete();
            File hashFile = new File(hashFilePath);
            hashFile.delete();
            File download1File = new File(download1FilePath);
            download1File.delete();
            File download1Hash = new File(download1HashPath);
            download1Hash.delete();
            File download2File = new File(download2FilePath);
            download2File.delete();
            File download2Hash = new File(download2HashPath);
            download2Hash.delete();
        }
    }

    @Test
    public void testRandom() throws IOException, HashUtil.HashBuilderException {
        Random rand;
        Random seedGen = new SecureRandom();
        for (int i = 0; i < randomRuns; i++) {
            int seed = seedGen.nextInt();
            LOG.info("random test seed:{}", seed);
            rand = new Random(seed);
            setup(rand);
            run(rand, new RandomJumpDownloader(rand, nrBlocks, jumpRate, piecesPerBlock * pieceSize, corruptionRate, lossRate));
            Assert.assertTrue(Files.equal(new File(uploadFilePath), new File(download1FilePath)));
            Assert.assertTrue(Files.equal(new File(uploadFilePath), new File(download2FilePath)));
            cleanup();
        }
    }

    public void run(Random rand, Downloader downloader) throws IOException {
        File uploadFile = new File(uploadFilePath);
        long fileSize = uploadFile.length();
        File hashFile = new File(hashFilePath);
        long hashFileSize = hashFile.length();

        DownloadMngrConfig config = new DownloadMngrConfig(null, null, -1, -1, -1, pieceSize, piecesPerBlock);
        int blockSize = piecesPerBlock * pieceSize;
        HashMngr uploadHashMngr = StorageMngrFactory.getCompleteHashMngr(hashFilePath, hashAlg, hashFileSize, HashUtil.getHashSize(hashAlg));
        FileMngr uploadFileMngr = StorageMngrFactory.getCompleteFileMngr(uploadFilePath, fileSize, blockSize, pieceSize);
        DownloadMngr uploader = new DownloadMngr(config, uploadHashMngr, uploadFileMngr);

        FileMngr download1FileMngr = StorageMngrFactory.getIncompleteFileMngr(download1FilePath, fileSize, blockSize, pieceSize);
        HashMngr download1HashMngr = StorageMngrFactory.getIncompleteHashMngr(download1HashPath, hashAlg, hashFileSize, HashUtil.getHashSize(hashAlg));
        Pair<DownloadMngr, Integer> downloader1 = Pair.with(new DownloadMngr(config, download1HashMngr, download1FileMngr), 0);

        FileMngr download2FileMngr = StorageMngrFactory.getIncompleteFileMngr(download2FilePath, fileSize, blockSize, pieceSize);
        HashMngr download2HashMngr = StorageMngrFactory.getIncompleteHashMngr(download2HashPath, hashAlg, hashFileSize, HashUtil.getHashSize(hashAlg));
        Pair<DownloadMngr, Integer> downloader2 = Pair.with(new DownloadMngr(config, download2HashMngr, download2FileMngr), 0);

//      LOG.info("status file1:{} file2:{} pPieces1:{} pPieces2:{}",
//                new Object[]{download1FileMngr.contiguous(0), download2FileMngr.contiguous(0), download1.getValue2().size(), download2.getValue2().size()});
        while (!(downloader1.getValue0().isComplete() && downloader2.getValue0().isComplete())) {
            Pair<Set<Integer>, Map<Integer, byte[]>> blockInfo1 = downloader1.getValue0().checkCompleteBlocks();
            if (blockInfo1.getValue1().size() > 0) {
//                LOG.info("downloader1 reset:{}", blockInfo1.getValue1().keySet());
            }
            if (blockInfo1.getValue0().size() > 0) {
//                LOG.info("downloader1 completed:{}", blockInfo1.getValue0());
            }
//            LOG.info("downloader1 status:{}", downloader1.getValue0());
            if (!downloader1.getValue0().isComplete()) {
//                LOG.info("file1 block:{}", download1.getValue1().getValue0());
                DownloadMngr currentUploader = rand.nextInt(2) == 0 ? uploader : downloader2.getValue0();
                downloader.download(currentUploader, downloader1);
            }
            Pair<Set<Integer>, Map<Integer, byte[]>> blockInfo2 = downloader2.getValue0().checkCompleteBlocks();
            if (blockInfo2.getValue1().size() > 0) {
//                LOG.info("downloader2 reset:{}", blockInfo2.getValue1().keySet());
            }
            if (blockInfo2.getValue0().size() > 0) {
//                LOG.info("downloader2 completed:{}", blockInfo2.getValue0());
            }
//            LOG.info("downloader2 status:{}", downloader2.getValue0());
            if (!download2FileMngr.isComplete(0)) {
//                LOG.info("file2 block:{}", download2.getValue1().getValue0());
                DownloadMngr currentUploader = rand.nextInt(2) == 0 ? uploader : downloader1.getValue0();
                downloader.download(currentUploader, downloader2);
            }
//            LOG.info("status file1:{} file2:{}", download1FileMngr.contiguous(0), download2FileMngr.contiguous(0));
        }
//        LOG.info("done1:{} done2:{}", download1FileMngr.isComplete(0), download2FileMngr.isComplete(0));
    }

    public static interface Downloader {

        public Pair<DownloadMngr, Integer> download(DownloadMngr uploader, Pair<DownloadMngr, Integer> downloader);
    }

    public class RandomJumpDownloader implements Downloader {

        private Random rand;
        private double lossRate;
        private double jumpRate;
        private double corruptionRate;
        private int nrBlocks;
        private int maxWindowSize;

        public RandomJumpDownloader(Random rand, int nrBlocks, double jumpRate, int maxWindowSize, double corruptionRate, double lossRate) {
            this.rand = rand;
            this.nrBlocks = nrBlocks;
            this.jumpRate = jumpRate;
            this.maxWindowSize = maxWindowSize;
            this.corruptionRate = corruptionRate;
            this.lossRate = lossRate;
        }

        @Override
        public Pair<DownloadMngr, Integer> download(DownloadMngr uploader, Pair<DownloadMngr, Integer> downloader) {
            int downloadPos = downloader.getValue1();
            if (rand.nextDouble() < jumpRate) {
                downloadPos = rand.nextInt(nrBlocks);
                downloader.getValue0().download(downloadPos);
            }
            int windowSize = rand.nextInt(maxWindowSize);
            while (windowSize > 0) {
                windowSize--;
                Optional<Set<Integer>> reqHash = downloader.getValue0().downloadHash();
                if (reqHash.isPresent()) {
                    Pair<Map<Integer, byte[]>, Set<Integer>> respHash = uploader.hashRequest(reqHash.get());
                    downloader.getValue0().hashResponse(respHash.getValue0(), respHash.getValue1());
                    if (respHash.getValue0().isEmpty()) {
                        break;
                    }
                }
                Optional<Integer> pieceId = downloader.getValue0().downloadData();
                if (pieceId.isPresent()) {
                    Optional<byte[]> piece = uploader.dataRequest(pieceId.get());
                    if (piece.isPresent()) {
                        if (rand.nextDouble() < lossRate) {
                            piece = Optional.absent();
                        } else {
                            if (rand.nextDouble() < corruptionRate) {
                                byte[] pieceB = piece.get();
                                pieceB[0] = pieceB[0] == 0 ? (byte) 1 : (byte) 0;
                                piece = Optional.of(pieceB);
                            }
                        }
                    }
                    downloader.getValue0().dataResponse(pieceId.get(), piece);
                    if (!piece.isPresent()) {
                        break;
                    }
                }
                if (!downloader.getValue0().download(downloadPos)) {
                    if (!downloader.getValue0().download(0)) {
                        break;
                    }
                }
            }
            return Pair.with(downloader.getValue0(), downloadPos);
        }
    }
}
