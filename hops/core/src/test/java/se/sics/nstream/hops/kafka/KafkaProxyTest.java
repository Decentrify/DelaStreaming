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
package se.sics.nstream.hops.kafka;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.ktoolbox.util.result.Result;
import se.sics.ktoolbox.util.test.EventContentValidator;
import se.sics.ktoolbox.util.test.MockComponentProxy;
import se.sics.ktoolbox.util.test.PortValidator;
import se.sics.ktoolbox.util.test.Validator;
import se.sics.nstream.hops.kafka.avro.AvroParser;
import se.sics.nstream.hops.kafka.test.TestKafkaEndpoint;
import se.sics.nstream.hops.kafka.test.TestKafkaResource;
import se.sics.nstream.storage.StorageWrite;
import se.sics.nstream.test.StreamWriteRespEC;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class KafkaProxyTest {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaProxyTest.class);

    private final Schema schema1;
    private final ByteBuf stream1;

    {
        schema1 = SchemaBuilder
                .record("schema")
                .namespace("org.apache.avro.ipc")
                .fields()
                .name("field1").type().nullable().stringType().noDefault()
                .name("field2").type().nullable().stringType().noDefault()
                .name("field3").type().nullable().stringType().noDefault()
                .endRecord();

        GenericRecordBuilder recordBuilder = new GenericRecordBuilder(schema1);
        byte[] bRecord, piece;
        GenericData.Record record;
        GenericRecord pipedRecord;

        int nrRecords = 7;
        int recordSize = 18;
        int bufferSize = nrRecords * recordSize;
        stream1 = Unpooled.buffer(nrRecords * recordSize);
        for (int i = 0; i < nrRecords; i++) {
            recordBuilder.set("field1", "val" + i);
            recordBuilder.set("field2", "val" + i);
            recordBuilder.set("field3", "val" + i);
            record = recordBuilder.build();

            bRecord = AvroParser.avroToBlob(schema1, record);
            stream1.writeBytes(bRecord);
        }
        Assert.assertEquals(nrRecords * recordSize, stream1.writerIndex());
    }

    @Test
    public void simpleTest() {
        LOG.info("***********************************************************");
        LOG.info("simple test");

        stream1.readerIndex(0);
        
        KafkaEndpoint kafkaEndpoint = new TestKafkaEndpoint();
        KafkaResource kafkaResource = new TestKafkaResource(schema1);
        MockComponentProxy proxy = new MockComponentProxy();
        Validator validator;

        byte[] v1 = new byte[18];
        stream1.readBytes(v1);
        StorageWrite.Request req1 = new StorageWrite.Request(kafkaResource, 0, v1);
        byte[] v2 = new byte[2 * 18];
        stream1.readBytes(v2);
        StorageWrite.Request req2 = new StorageWrite.Request(kafkaResource, 18, v2);
        byte[] v3 = new byte[10];
        stream1.readBytes(v3);
        StorageWrite.Request req3 = new StorageWrite.Request(kafkaResource, 3 * 18, v3);
        byte[] v4 = new byte[5];
        stream1.readBytes(v4);
        StorageWrite.Request req4 = new StorageWrite.Request(kafkaResource, 3 * 18 + 10, v4);
        byte[] v5 = new byte[3];
        stream1.readBytes(v5);
        StorageWrite.Request req5 = new StorageWrite.Request(kafkaResource, 3 * 18 + 15, v5);
        byte[] v6 = new byte[21];
        stream1.readBytes(v6);
        StorageWrite.Request req6 = new StorageWrite.Request(kafkaResource, 4 * 18, v6);
        byte[] v7 = new byte[18];
        stream1.readBytes(v7);
        StorageWrite.Request req7 = new StorageWrite.Request(kafkaResource, 5 * 18+3, v7);
        byte[] v8 = new byte[15];
        stream1.readBytes(v8);
        StorageWrite.Request req8 = new StorageWrite.Request(kafkaResource, 6 * 18+3, v8);

        KafkaProxy kafka = buildKafka(proxy, kafkaEndpoint);
        startKafka(proxy, kafka);
        //write one msg - full value, no leftover
        proxy.expect(new EventContentValidator(new StreamWriteRespEC(), req1.respond(Result.success(true))));
        kafka.handleWriteRequest.handle(req1);
        validator = proxy.validateNext();
        Assert.assertTrue(validator.toString(), validator.isValid());
        //write two msg - full value, no leftover
        proxy.expect(new EventContentValidator(new StreamWriteRespEC(), req2.respond(Result.success(true))));
        kafka.handleWriteRequest.handle(req2);
        validator = proxy.validateNext();
        Assert.assertTrue(validator.toString(), validator.isValid());
        //write no msg - full value is leftover
        kafka.handleWriteRequest.handle(req3);
        //write no msg - full value is leftover
        kafka.handleWriteRequest.handle(req4);
        //write msg - full value ends the leftover
        proxy.expect(new EventContentValidator(new StreamWriteRespEC(), req3.respond(Result.success(true))));
        proxy.expect(new EventContentValidator(new StreamWriteRespEC(), req4.respond(Result.success(true))));
        proxy.expect(new EventContentValidator(new StreamWriteRespEC(), req5.respond(Result.success(true))));
        kafka.handleWriteRequest.handle(req5);
        validator = proxy.validateNext();
        Assert.assertTrue(validator.toString(), validator.isValid());
        validator = proxy.validateNext();
        Assert.assertTrue(validator.toString(), validator.isValid());
        validator = proxy.validateNext();
        Assert.assertTrue(validator.toString(), validator.isValid());
        //write msg with leftover
        kafka.handleWriteRequest.handle(req6);
        validator = proxy.validateNext();
        Assert.assertFalse(validator.toString(), validator.isValid());
        //write msg with leftover, but finishing previous write
        proxy.expect(new EventContentValidator(new StreamWriteRespEC(), req6.respond(Result.success(true))));
        kafka.handleWriteRequest.handle(req7);
        validator = proxy.validateNext();
        Assert.assertTrue(validator.toString(), validator.isValid());
        //complete previous write and leave no leftovers
        proxy.expect(new EventContentValidator(new StreamWriteRespEC(), req7.respond(Result.success(true))));
        proxy.expect(new EventContentValidator(new StreamWriteRespEC(), req8.respond(Result.success(true))));
        kafka.handleWriteRequest.handle(req8);
        validator = proxy.validateNext();
        Assert.assertTrue(validator.toString(), validator.isValid());
        validator = proxy.validateNext();
        Assert.assertTrue(validator.toString(), validator.isValid());
        closeKafka(proxy, kafka);
    }

    private KafkaProxy buildKafka(MockComponentProxy proxy, KafkaEndpoint kafkaEndpoint) {
        proxy.expect(new PortValidator(KafkaPort.class, false));
        KafkaProxy kafka = new KafkaProxy(proxy, kafkaEndpoint);
        Validator v;
        v = proxy.validateNext();
        Assert.assertTrue(v.toString(), v.isValid());
        //nothing more
        v = proxy.validateNext();
        Assert.assertFalse(v.toString(), v.isValid());
        return kafka;
    }

    private void startKafka(MockComponentProxy proxy, KafkaProxy kafka) {
        kafka.start();
        Validator v;
        //nothing more
        v = proxy.validateNext();
        Assert.assertFalse(v.toString(), v.isValid());
    }

    private void closeKafka(MockComponentProxy proxy, KafkaProxy kafka) {
        kafka.close();
        Validator v;
        //nothing more
        v = proxy.validateNext();
        Assert.assertFalse(v.toString(), v.isValid());
    }
}
