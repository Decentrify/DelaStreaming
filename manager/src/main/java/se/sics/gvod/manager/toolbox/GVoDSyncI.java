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

package se.sics.gvod.manager.toolbox;

import com.google.common.util.concurrent.SettableFuture;
import java.util.Map;
import se.sics.gvod.manager.util.FileStatus;
import se.sics.ktoolbox.util.identifiable.Identifier;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public interface GVoDSyncI {
    public boolean isReady();
    public void getFiles(SettableFuture<Result<Map<String, FileStatus>>> opFuture);
    public void pendingUpload(VideoInfo videoInfo, SettableFuture<Result<Identifier>> opFuture);
    public void upload(VideoInfo videoInfo, SettableFuture<Result<Boolean>> opFuture);
    public void download(VideoInfo videoInfo, SettableFuture<Result<Boolean>> opFuture);
    public void play(VideoInfo videoInfo, SettableFuture<Result<Integer>> opFuture);
    public void stop(VideoInfo videoInfo, SettableFuture<Result<Boolean>> opFuture);
}
