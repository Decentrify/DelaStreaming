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
package se.sics.nstream.hops.manifest;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.ktoolbox.util.result.Result;
import se.sics.nstream.storage.durable.disk.DiskResource;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class DiskHelper {

    private final static Logger LOG = LoggerFactory.getLogger(DiskHelper.class);
    private static String logPrefix = "";

    public static Result<ManifestJSON> readManifest(DiskResource resource) {
        final String filePath = resource.dirPath + File.separator + resource.fileName;
        LOG.debug("{}reading manifest:{}", new Object[]{logPrefix, filePath});
        File file = new File(filePath);
        try (RandomAccessFile raf = new RandomAccessFile(file, "rw")) {
            long manifestLength = raf.length();
            byte[] manifestByte = new byte[(int) manifestLength];
            raf.readFully(manifestByte);
            ManifestJSON manifest = ManifestHelper.getManifestJSON(manifestByte);
            return Result.success(manifest);
        } catch (FileNotFoundException ex) {
            LOG.warn("{}could not find manifest file:{} ex:{}", new Object[]{logPrefix, filePath, ex.getMessage()});
            return Result.externalSafeFailure(ex);
        } catch (IOException ex) {
            LOG.warn("{}could not read from manifest file:{} ex:{}", new Object[]{logPrefix, filePath, ex.getMessage()});
            return Result.externalSafeFailure(ex);
        }
    }

    public static Result<Boolean> writeManifest(DiskResource resource, final ManifestJSON manifest) {
        final String filePath = resource.dirPath + File.separator + resource.fileName;
        LOG.debug("{}writing manifest:{}", new Object[]{logPrefix, filePath});
        File dir = new File(resource.dirPath);
        if (!dir.isDirectory()) {
            if (!dir.mkdirs()) {
                return Result.internalFailure(new IOException("could not create dirs:" + resource.dirPath));
            }
        }
        File file = new File(filePath);
        if (file.isFile()) {
            file.delete();
            //TODO Alex - maybe later - fix and check for reuse
//            return Result.internalFailure(new IOException("file:" + filePath + " exists - no overwrite"));
        }
        try (RandomAccessFile raf = new RandomAccessFile(file, "rw")) {
            byte[] manifestByte = ManifestHelper.getManifestByte(manifest);
            raf.write(manifestByte);
            return Result.success(true);
        } catch (IOException ex) {
            LOG.warn("{}could not create file:{}", logPrefix, ex.getMessage());
            return Result.externalUnsafeFailure(ex);
        }
    }
}
