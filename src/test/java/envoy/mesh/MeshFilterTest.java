package envoy.mesh;

import java.util.Arrays;
import java.util.List;
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
 * - 2 -> listening on 9192, with default partitions = 2,
 * - 3 -> listening on 9292, with default partitions = 5
 *
 * 1 Envoy instance with mesh-filter listening on 19092, with forwarding rules:
 * - starts with 'a' -> cluster 1,
 * - starts with 'b' -> cluster 2,
 * - starts with 'c' -> cluster 3
 */
public class MeshFilterTest {

    private static final Logger LOG = LoggerFactory.getLogger(MeshFilterTest.class);

    private static final String ENVOY = "localhost:19092";

    private static final int MESSAGE_COUNT = 10;

    // apples & apricots -> cluster 1
    // bananas           -> cluster 2
    // cherries          -> cluster 3
    private static final List<String> TOPICS = Arrays.asList("apples", "apricots", "bananas", "cherries");

    @Test
    public void shouldSendRecordsToMesh()
            throws Exception {

        final Properties properties = new Properties();
        properties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, ENVOY);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        final KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(properties);

        for (final String topic : TOPICS) {
            LOG.info("=== Sending for topic [{}] ===", topic);
            for (int i = 0; i < MESSAGE_COUNT; ++i) {

                /*
                 * Because we use default partitioner + do not provide key/partition,
                 * the target partition will be computed from metadata.
                 */
                final byte[] key = null;
                final byte[] value = new byte[128];
                final ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(topic, key, value);
                final RecordMetadata metadata = producer.send(record).get();
                LOG.info("=== Record for topic [{}] was saved at partition {}, offset = {} ===",
                        topic, metadata.partition(), metadata.offset());
            }
        }
        producer.close();
    }

}
