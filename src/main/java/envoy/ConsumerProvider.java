package envoy;

import java.util.Properties;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

public class ConsumerProvider {

    /**
     * Creates a consumer consuming from Envoy.
     */
    public static Consumer<byte[], byte[]> makeBrokerConsumer(final String... extraArgs) {
        final Properties properties = new Properties();
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, Environment.ENVOY_BROKER);
        for (int i = 0; i < extraArgs.length / 2; ++i) {
            properties.put(extraArgs[2 * i], extraArgs[2 * i + 1]);
        }
        return new KafkaConsumer<>(properties);
    }

    /**
     * Creates a consumer consuming straight from upstream cluster.
     */
    public static Consumer<byte[], byte[]> makeClusterConsumer(final UpstreamCluster cluster,
                                                               final String... extraArgs) {

        final Properties properties = new Properties();
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, cluster.getBrokers());
        for (int i = 0; i < extraArgs.length / 2; ++i) {
            properties.put(extraArgs[2 * i], extraArgs[2 * i + 1]);
        }
        return new KafkaConsumer<>(properties);
    }

    private ConsumerProvider() {
    }

}
