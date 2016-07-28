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
package se.sics.nstream.hops.kafka.avro;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.buffer.Unpooled;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class AvroParser {

    private static final Logger LOG = LoggerFactory.getLogger(AvroParser.class);

    public static GenericRecord blobToAvro(Schema schema, ByteBuf data) {
        int readPos = data.readerIndex();
        GenericDatumReader<GenericRecord> reader = new GenericDatumReader<>(schema);
        try (InputStream in = new ByteBufInputStream(data)) {
            BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(in, null);
            try {
                GenericRecord record = reader.read(null, decoder);
                readPos = data.readerIndex() - decoder.inputStream().available();
                data.readerIndex(readPos);
                return record;
            } catch (EOFException ex) {
                data.readerIndex(readPos);
                return null;
            }
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    public static List<GenericRecord> blobToAvroList(Schema schema, InputStream in) {
        List<GenericRecord> records = new ArrayList<>();
        GenericDatumReader<GenericRecord> reader = new GenericDatumReader<>(schema);
        BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(in, null);
        try {
            while (true) {
                GenericRecord record = reader.read(null, decoder);
                records.add(record);
            }
        } catch (EOFException ex) {

        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
        return records;
    }

    public static byte[] avroToBlob(Schema schema, GenericRecord record) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<>(schema);
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
        try {
            writer.write(record, encoder);
            encoder.flush();
        } catch (Exception ex) {
            throw new RuntimeException("hmmm", ex);
        }
        byte[] bData = out.toByteArray();
        return bData;
    }

    public static byte[] nAvroToBlob(Schema schema, int nrMsgs, Random rand) {
        ByteBuf buf = Unpooled.buffer();
        OutputStream out = new ByteBufOutputStream(buf);
        GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<>(schema);
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
        GenericRecordBuilder grb;

        for (int i = 0; i < nrMsgs; i++) {
            grb = new GenericRecordBuilder(schema);
            for (Schema.Field field : schema.getFields()) {
                //TODO Alex - I assume each field is a string
                grb.set(field, "val" + (1000 + rand.nextInt(1000)));
            }
            try {
                writer.write(grb.build(), encoder);
            } catch (IOException ex) {
                throw new RuntimeException(ex);
            }
        }
        try {
            encoder.flush();
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
        byte[] result = new byte[buf.writerIndex()];
        buf.readBytes(result);
        return result;
    }
}
