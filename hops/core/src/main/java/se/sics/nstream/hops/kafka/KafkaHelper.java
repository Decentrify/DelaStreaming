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

import io.hops.kafkautil.HopsKafkaConsumer;
import io.hops.kafkautil.HopsKafkaProducer;
import io.hops.kafkautil.HopsKafkaUtil;
import io.hops.kafkautil.NHopsKafkaUtil;
import io.hops.kafkautil.SchemaNotFoundException;
import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class KafkaHelper {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaHelper.class);

    public static HopsKafkaProducer getKafkaProducer(KafkaEndpoint kafkaEndpoint, KafkaResource kafkaResource) {
        LOG.warn("do not start multiple kafka workers in parallel - risk of race condition (setup/getProducer/getConsumer");
        HopsKafkaUtil hopsKafkaUtil = HopsKafkaUtil.getInstance();
        int projectId = Integer.parseInt(kafkaEndpoint.projectId);
        LOG.info("getting producer session:{}, project:{} topic:{} domain:{} broker:{} rest:{} key:{} trust:{}",
                new Object[]{kafkaResource.sessionId, projectId, kafkaResource.topicName, kafkaEndpoint.domain, kafkaEndpoint.brokerEndpoint, kafkaEndpoint.restEndpoint,
                    kafkaEndpoint.keyStore, kafkaEndpoint.trustStore});
        hopsKafkaUtil.setup(kafkaResource.sessionId, projectId, kafkaResource.topicName, kafkaEndpoint.domain, kafkaEndpoint.brokerEndpoint, kafkaEndpoint.restEndpoint,
                kafkaEndpoint.keyStore, kafkaEndpoint.trustStore);
        HopsKafkaProducer kp;
        try {
            //TODO Alex - hardcoded linger delay
            kp = hopsKafkaUtil.getHopsKafkaProducer(kafkaResource.topicName, 5);
            return kp;
        } catch (SchemaNotFoundException ex) {
            throw new RuntimeException(ex);
        }
    }

    public static HopsKafkaConsumer getKafkaConsumer(KafkaEndpoint kafkaEndpoint, KafkaResource kafkaResource) {
        LOG.warn("do not start multiple kafka workers in parallel - risk of race condition (setup/getProducer/getConsumer");
        HopsKafkaUtil hopsKafkaUtil = HopsKafkaUtil.getInstance();
        int projectId = Integer.parseInt(kafkaEndpoint.projectId);
        LOG.info("getting consumer session:{}, project:{} topic:{} domain:{} broker:{} rest:{} key:{} trust:{}",
                new Object[]{kafkaResource.sessionId, projectId, kafkaResource.topicName, kafkaEndpoint.domain, kafkaEndpoint.brokerEndpoint, kafkaEndpoint.restEndpoint,
                    kafkaEndpoint.keyStore, kafkaEndpoint.trustStore});
        hopsKafkaUtil.setup(kafkaResource.sessionId, projectId, kafkaResource.topicName, kafkaEndpoint.domain, kafkaEndpoint.brokerEndpoint, kafkaEndpoint.restEndpoint,
                kafkaEndpoint.keyStore, kafkaEndpoint.trustStore);
        HopsKafkaConsumer kc;
        try {
            kc = hopsKafkaUtil.getHopsKafkaConsumer(kafkaResource.topicName);
            return kc;
        } catch (SchemaNotFoundException ex) {
            throw new RuntimeException(ex);
        }
    }

    public static Schema getKafkaSchemaByTopic(KafkaEndpoint kafkaEndpoint, KafkaResource kafkaResource) {
        HopsKafkaUtil hopsKafkaUtil = HopsKafkaUtil.getInstance();
        int projectId = Integer.parseInt(kafkaEndpoint.projectId);
        LOG.info("getting schema session:{}, project:{} topic:{} domain:{} broker:{} rest:{} key:{} trust:{}",
                new Object[]{kafkaResource.sessionId, projectId, kafkaResource.topicName, kafkaEndpoint.domain, kafkaEndpoint.brokerEndpoint, kafkaEndpoint.restEndpoint,
                    kafkaEndpoint.keyStore, kafkaEndpoint.trustStore});
        hopsKafkaUtil.setup(kafkaResource.sessionId, projectId, kafkaResource.topicName, kafkaEndpoint.domain, kafkaEndpoint.brokerEndpoint, kafkaEndpoint.restEndpoint,
                kafkaEndpoint.keyStore, kafkaEndpoint.trustStore);
        String stringSchema;
        try {
            stringSchema = NHopsKafkaUtil.getSchemaByTopic(hopsKafkaUtil, kafkaResource.topicName);
        } catch (SchemaNotFoundException ex) {
            throw new RuntimeException(ex);
        }
        LOG.info("schema:{}", stringSchema);
        Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse(stringSchema);
        return schema;
    }
}
