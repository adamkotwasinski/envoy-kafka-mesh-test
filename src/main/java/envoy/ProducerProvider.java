package envoy;

import java.util.Properties;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;

public class ProducerProvider {

    /**
     * Create a producer that points to Envoy (through the broker filter).
     * @return producer
     */
    public static Producer<byte[], byte[]> makeProducerToEnvoyBroker() {
        return makeProducer(Environment.ENVOY_BROKER);
    }

    /**
     * Create a producer that points to Envoy (through the mesh filter).
     * @return producer
     */
    public static Producer<byte[], byte[]> makeProducerToEnvoyMesh() {
        return makeProducer(Environment.ENVOY_MESH, ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "false");
    }

    /**
     * Create a producer that points to a particular upstream Kafka cluster (instead of Envoy with mesh listener).
     * <p>
     * This is intended only for testing / setup purposes.
     *
     * @param uc cluster
     * @return producer
     */
    public static Producer<byte[], byte[]> makeProducerToKafkaDirectly(final UpstreamCluster uc) {
        return makeProducer(uc.getBrokers());
    }

    private static Producer<byte[], byte[]> makeProducer(final String bootstrapServers, final String... extraArgs) {
        final Properties properties = new Properties();
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        properties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        for (int i = 0; i < extraArgs.length / 2; ++i) {
            properties.put(extraArgs[2 * i], extraArgs[2 * i + 1]);
        }
        return new KafkaProducer<>(properties);
    }

    private ProducerProvider() {
    }

}
