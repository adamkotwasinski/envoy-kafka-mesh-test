package envoy.mesh;

import java.util.Properties;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 3 Kafka clusters, each with 1 broker:
 * - 1 -> listening on 9092, with default partitions = 1,
 * - 2 -> listening on 9192, with default partitions = 1,
 * - 3 -> listening on 9292, with default partitions = 5
 *
 * 1 Envoy instance with mesh-filter listening on 19092, with forwarding rules:
 * - a.+ -> cluster 1,
 * - b.+ -> cluster 2,
 * - c.+ -> cluster 3
 */
public class MeshFilterTest {

    private static final Logger LOG = LoggerFactory.getLogger(MeshFilterTest.class);

    private static final int MESSAGE_COUNT = 5;

    @Test
    public void shouldSendRecordsToMesh()
            throws Exception {

        final Properties properties = new Properties();
        properties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "localhost:19092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        final KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(properties);

        // apples & apricots -> cluster 1
        // bananas           -> cluster 2
        // cherries          -> cluster 3
        for (final String topic : new String[] { "apples", "apricots", "bananas", "cherries" }) {
            for (int i = 0; i < MESSAGE_COUNT; ++i) {

                final byte[] key = null;
                final byte[] value = new byte[128];
                final ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(topic, key, value);
                final RecordMetadata metadata = producer.send(record).get();
                LOG.info("Record for topic [{}] was saved at partition {}, offset = {}",
                        topic, metadata.partition(), metadata.offset());
            }
        }
        producer.close();
    }

}
