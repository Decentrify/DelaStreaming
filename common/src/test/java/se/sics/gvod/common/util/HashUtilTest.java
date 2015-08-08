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
package se.sics.gvod.common.util;

import se.sics.p2ptoolbox.util.managedStore.HashUtil;
import com.google.common.base.Objects;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import org.junit.Assert;
import org.junit.Test;
import se.sics.p2ptoolbox.util.managedStore.HashUtil.HashBuilderException;

/**
 * @author Alex Ormenisan <aaor@sics.se>
 */
public class HashUtilTest {

    private String filePath;
    private String hashPath;
    private int fileLength;

    @Test
    public void testMakeHash() throws HashUtil.HashBuilderException, IOException {
        prepareFiles();
        System.out.println(filePath);
        System.out.println(hashPath);

        HashUtil.makeHashes(filePath, hashPath, "SHA", 1024);
        testHashes("SHA", 1024);
    }

    private void prepareFiles() throws IOException {
        File file = File.createTempFile("memMapTest", "file");
        filePath = file.getPath();
        File hashFile = File.createTempFile("memMapTest", "hash");
        hashPath = hashFile.getPath();
        hashFile.delete();

        Writer writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file.getPath())));
        for (int i = 0; i < 10000; i++) {
            writer.write("abc" + i + "\n");
        }
        writer.flush();
        writer.close();
        fileLength = (int) file.length();
    }

    private void testHashes(String hashAlg, int pieceSize) throws HashBuilderException {
        InputStream hashReader = null;
        InputStream fileReader = null;
        try {
            hashReader = new FileInputStream(hashPath);
            fileReader = new FileInputStream(filePath);
            MessageDigest md = MessageDigest.getInstance(hashAlg);
            while (fileReader.available() >= pieceSize) {
                byte[] piece = new byte[pieceSize];
                fileReader.read(piece);
                byte[] hash = new byte[HashUtil.getHashSize(hashAlg)];
                hashReader.read(hash);
                byte[] expectedHash = md.digest(piece);
                Assert.assertTrue(Arrays.equals(expectedHash, hash));
            }
            int rest = fileReader.available();
            if (rest != 0) {
                byte[] piece = new byte[rest];
                fileReader.read(piece);
                byte[] hash = new byte[HashUtil.getHashSize(hashAlg)];
                hashReader.read(hash);
                byte[] expectedHash = md.digest(piece);
                Assert.assertTrue(Arrays.equals(expectedHash, hash));
            }
        } catch (FileNotFoundException ex) {
            throw new HashBuilderException(ex);
        } catch (NoSuchAlgorithmException ex) {
            throw new HashBuilderException(ex);
        } catch (IOException ex) {
            throw new HashBuilderException(ex);
        } catch (GVoDConfigException.Missing ex) {
            throw new HashBuilderException(ex);
        } finally {
            try {
                if (hashReader != null) {
                    hashReader.close();
                }
                if (fileReader != null) {
                    fileReader.close();
                }
            } catch (IOException ex) {
                throw new HashBuilderException(ex);
            }
        }
    }
}
