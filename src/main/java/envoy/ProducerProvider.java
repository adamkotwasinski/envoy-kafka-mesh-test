package envoy;

import java.util.Properties;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;

public class ProducerProvider {

    /**
     * Producer that points to Envoy mesh.
     * @return producer
     */
    public static Producer<byte[], byte[]> makeMeshProducer() {
        final Properties properties = new Properties();
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "false");
        properties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, Environment.ENVOY_MESH);
        return new KafkaProducer<>(properties);
    }

    /**
     * Producer that points to Envoy (as a broker).
     * @return producer
     */
    public static Producer<byte[], byte[]> makeBrokerProducer() {
        final Properties properties = new Properties();
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        properties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, Environment.ENVOY_BROKER);
        return new KafkaProducer<>(properties);
    }

    private ProducerProvider() {
    }

}
