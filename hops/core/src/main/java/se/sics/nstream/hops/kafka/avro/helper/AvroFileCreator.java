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
package se.sics.nstream.hops.kafka.avro.helper;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Random;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Parser;
import se.sics.nstream.hops.kafka.avro.AvroParser;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class AvroFileCreator {

  public static void main(String[] args) throws IOException {
    File clean = new File("src/main/resources/avro/test_record.file");
    if(clean.isFile()) {
      clean.delete();
    }
    File file = new File("src/main/resources/avro/test_record.avro");
    Schema schema = new Parser().parse(file);
    System.out.println(schema);
    try (FileOutputStream fos = new FileOutputStream("src/main/resources/avro/test_record.file")) {
      for (int i = 0; i < 100; i++) {
        int batchNrMsgs = 1000;
        fos.write(AvroParser.nAvroToBlob(schema, batchNrMsgs, new Random(1234)));
      }
    }
  }
}
