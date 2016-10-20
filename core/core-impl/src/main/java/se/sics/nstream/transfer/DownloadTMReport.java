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
package se.sics.nstream.transfer;

import se.sics.nstream.storage.managed.AppendFMReport;

/**
 *
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class DownloadTMReport {

    public final AppendFMReport file;
    public final int workPos;
    public final int hashPos;
    public final int cachePos;
    public final int pendingSize;

    public DownloadTMReport(AppendFMReport file, int workPos, int hashPos, int cachePos, int pendingSize) {
        this.file = file;
        this.workPos = workPos;
        this.hashPos = hashPos;
        this.cachePos = cachePos;
        this.pendingSize = pendingSize;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("report download wp:").append(workPos).append(" hp:").append(hashPos).append(" cp:").append(cachePos).append(" pending:").append(pendingSize).append("\n");
        sb.append(file.toString());
        return sb.toString();
    }
}
