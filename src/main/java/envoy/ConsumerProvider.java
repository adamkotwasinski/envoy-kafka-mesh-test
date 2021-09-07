package envoy;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

public class ConsumerProvider {

    public static Consumer<byte[], byte[]> makeConsumer1() {
        final Properties properties = new Properties();
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, Environment.CLUSTER_1);
        final KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(properties);
        consumer.assign(Arrays.asList(new TopicPartition(Environment.CLUSTER_1_TOPIC, 0)));
        return consumer;
    }

    private ConsumerProvider() {
    }

}
