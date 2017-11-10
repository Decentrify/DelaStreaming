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
import java.util.ArrayList;
import java.util.Date;
import se.sics.kompics.util.Identifier;
import se.sics.ledbat.ncore.msg.LedbatMsg;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class DwnlConnTracker {

  private final BufferedWriter timeoutsFile;
  private final BufferedWriter lateFile;
  private final long start;
  private final BufferedWriter portsf;
  private final ArrayList<Integer> portEvents;
  private final int nPorts;
  private long portsLastReported;

  public DwnlConnTracker(BufferedWriter timeoutsFile, BufferedWriter lateFile, BufferedWriter portsf, int nPorts) {
    this.timeoutsFile = timeoutsFile;
    this.lateFile = lateFile;
    this.portsf = portsf;
    start = System.currentTimeMillis();
    portsLastReported = start;
    this.nPorts = nPorts;
    portEvents = new ArrayList<>(nPorts);
    for (int i = 0; i < nPorts; i++) {
      portEvents.add(i, 0);
    }
  }

  private void resetPorts() {
    for (int i = 0; i < nPorts; i++) {
      portEvents.set(i, 0);
    }
  }

  public void reportPortEvent(long now, int nPort) {
    portEvents.set(nPort, portEvents.get(nPort) + 1);
    if (now > portsLastReported + 100) {
      portsLastReported = now;
      try {
        long expTime = now - start;
        portsf.write(expTime + "");
        for (int i = 0; i < nPorts; i++) {
          portsf.write("," + portEvents.get(i));
        }
        portsf.write("\n");
        portsf.flush();
      } catch (IOException ex) {
        throw new RuntimeException(ex);
      }
      resetPorts();
    }
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
      portsf.close();
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }

  public static DwnlConnTracker onDisk(String dirPath, Identifier id, int nPorts) {
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

      String portsfName = "ports_be_" + id + "_" + sdf.format(date) + ".csv";
      File portsf = new File(dirPath + File.separator + portsfName);
      if (portsf.exists()) {
        portsf.delete();
      }
      portsf.createNewFile();

      return new DwnlConnTracker(new BufferedWriter(new OutputStreamWriter(new FileOutputStream(timeoutf))),
        new BufferedWriter(new OutputStreamWriter(new FileOutputStream(latef))),
        new BufferedWriter(new OutputStreamWriter(new FileOutputStream(portsf))), nPorts);
    } catch (FileNotFoundException ex) {
      throw new RuntimeException(ex);
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }
}
