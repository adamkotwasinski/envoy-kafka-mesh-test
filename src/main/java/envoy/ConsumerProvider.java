package envoy;

import java.util.Properties;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

public class ConsumerProvider {

    /**
     * Create a consumer consuming from Envoy (through the broker filter).
     */
    public static Consumer<byte[], byte[]> makeConsumerFromEnvoyBroker(final String... extraArgs) {
        return makeConsumer(Environment.ENVOYS_BOOTSTRAP_ADDRESS, extraArgs);
    }

    /**
     * Create a consumer consuming from Envoy (through the mesh filter).
     */
    public static Consumer<byte[], byte[]> makeConsumerFromEnvoyMesh(final String... extraArgs) {
        return makeConsumer(Environment.ENVOY_MESH, extraArgs);
    }

    /**
     * Creates a consumer consuming straight from upstream Kafka cluster (instead of Envoy with mesh listener).
     * <p>
     * This is intended only for testing / setup purposes.
     */
    public static Consumer<byte[], byte[]> makeConsumerFromKafkaDirectly(final UpstreamCluster cluster,
                                                                         final String... extraArgs) {

        return makeConsumer(cluster.getBrokers(), extraArgs);
    }

    private static Consumer<byte[], byte[]> makeConsumer(final String bootstrapServers, final String... extraArgs) {
        final Properties properties = new Properties();
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        for (int i = 0; i < extraArgs.length / 2; ++i) {
            properties.put(extraArgs[2 * i], extraArgs[2 * i + 1]);
        }
        return new KafkaConsumer<>(properties);
    }

    private ConsumerProvider() {
    }

}
