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
package se.sics.nstream.torrent.transfer.tracking;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import se.sics.ktoolbox.util.identifiable.Identifier;
import se.sics.ledbat.ncore.msg.LedbatMsg;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class DwnlConnTracker {

  private final BufferedWriter timeoutsFile;
  private final BufferedWriter lateFile;
  private final long start;

  public DwnlConnTracker(BufferedWriter timeoutsFile, BufferedWriter lateFile) {
    this.timeoutsFile = timeoutsFile;
    this.lateFile = lateFile;
    this.start = System.currentTimeMillis();
  }

  public void reportTimeout(long now, Identifier eventId, long rto) {
    try {
      long expTime = now - start;
      timeoutsFile.write(expTime + "," + eventId + "," + rto + "\n");
      timeoutsFile.flush();
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }

  public void reportLate(long now, LedbatMsg.Response late) {
    try {
      long expTime = now - start;
      long t1 = late.leecherAppReqSendT - start;
      long t2 = late.seederNetRespSendT - start;
      long t3 = late.leecherNetRespT - start;
      lateFile.write(expTime + "," + late.getId() + "," + t1 + "," + t2 + "," + t3 + "\n");
      lateFile.flush();
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }

  public void close() {
    try {
      timeoutsFile.close();
      lateFile.close();
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }

  public static DwnlConnTracker onDisk(String dirPath, Identifier id) {
    try {
      DateFormat sdf = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss");
      Date date = new Date();
      String timeoutfName = "timeout_dwnl_" + id + "_" + sdf.format(date) + ".csv";
      File timeoutf = new File(dirPath + File.separator + timeoutfName);
      if (timeoutf.exists()) {
        timeoutf.delete();
      }
      timeoutf.createNewFile();

      String latefName = "late_dwnl_" + id + "_" + sdf.format(date) + ".csv";
      File latef = new File(dirPath + File.separator + latefName);
      if (latef.exists()) {
        latef.delete();
      }
      latef.createNewFile();

      return new DwnlConnTracker(new BufferedWriter(new OutputStreamWriter(new FileOutputStream(timeoutf))),
        new BufferedWriter(new OutputStreamWriter(new FileOutputStream(latef))));
    } catch (FileNotFoundException ex) {
      throw new RuntimeException(ex);
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }
}
