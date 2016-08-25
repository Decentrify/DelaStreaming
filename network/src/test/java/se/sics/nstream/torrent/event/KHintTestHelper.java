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
package se.sics.nstream.torrent.event;

import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import se.sics.nstream.storage.cache.KHint;
import se.sics.nstream.storage.cache.KHint;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class KHintTestHelper {
    public static Map<String, KHint.Summary> getCacheHints1() {
        Map<String, KHint.Summary> files = new TreeMap<>();
        Set<Integer> file1Blocks = new TreeSet<>();
        file1Blocks.add(1);
        file1Blocks.add(2);
        file1Blocks.add(3);
        files.put("file1", new KHint.Summary(1l, file1Blocks));
        return files;
    }
}
