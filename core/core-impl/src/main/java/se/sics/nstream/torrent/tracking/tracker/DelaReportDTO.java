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
package se.sics.nstream.torrent.tracking.tracker;

import javax.xml.bind.annotation.XmlRootElement;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */

@XmlRootElement
public class DelaReportDTO {

  private String delaId;
  private String torrentId;
  private long reportId;
  private String reportVal;

  public DelaReportDTO() {
  }
  
  public DelaReportDTO(String delaId, String torrentId, long reportId, String reportVal) {
    this.delaId = delaId;
    this.torrentId = torrentId;
    this.reportId = reportId;
    this.reportVal = reportVal;
  }

  public String getDelaId() {
    return delaId;
  }

  public void setDelaId(String delaId) {
    this.delaId = delaId;
  }

  public String getTorrentId() {
    return torrentId;
  }

  public void setTorrentId(String torrentId) {
    this.torrentId = torrentId;
  }

  public long getReportId() {
    return reportId;
  }

  public void setReportId(long reportId) {
    this.reportId = reportId;
  }

  public String getReportVal() {
    return reportVal;
  }

  public void setReportVal(String data) {
    this.reportVal = data;
  }

  @Override
  public String toString() {
    return "DelaReportDTO{" + "delaId=" + delaId + ", torrentId=" + torrentId + ", reportId=" + reportId + ", reportVal=" +
      reportVal + '}';
  }
}

