package envoy.broker;

import static envoy.Records.equalRecordContents;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.Set;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Iterables;

/**
 * 1 Kafka cluster with 3 brokers:
 * - 1 -> listening on 9092, advertised on 19092,
 * - 2 -> listening on 9093, advertised on 19093,
 * - 3 -> listening on 9094, advertised on 19094
 *
 * 1 Envoy instance proxying each of Kafka brokers (listening on 19092, 19093, 19094).
 */
public class BrokerFilterTest {

    private static final Logger LOG = LoggerFactory.getLogger(BrokerFilterTest.class);

    /**
     * == Envoy (not Kafka!)
     */
    private static final String BOOTSTRAP_SERVERS = "localhost:19092,localhost:19093,localhost:19094";

    @Test
    public void shouldProduceAndConsume()
            throws Exception {

        // given
        final TopicPartition tp = new TopicPartition("mytopic", 0);

        final Properties consumerProperties = new Properties();
        consumerProperties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, "mytestgroup");

        final KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(consumerProperties);

        consumer.assign(Collections.singleton(tp));
        final long currentPosition = consumer.position(tp);
        LOG.info("Current consumer position is {}", currentPosition);

        final Properties producerProperties = new Properties();
        producerProperties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        final KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(producerProperties);

        final byte[] key = new byte[128];
        final byte[] value = new byte[128];
        final ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(tp.topic(), tp.partition(), key, value);

        // when
        final RecordMetadata rm = producer.send(record).get();
        LOG.info("Saved at position {}", rm.offset());

        // then
        assertThat(rm.offset(), equalTo(currentPosition));

        // when - 2
        final ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofSeconds(5));

        // then - 2
        assertThat(records.count(), equalTo(1));

        final ConsumerRecord<byte[], byte[]> received = Iterables.getOnlyElement(records);
        assertThat(equalRecordContents(record, received), equalTo(true));

        // when, then - 3
        consumer.commitSync();

        // cleanup
        consumer.close();
        producer.close();
    }

    @Test
    public void shouldHandleAdminOperations()
            throws Exception {

        // given
        final Properties properties = new Properties();
        properties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        final Admin admin = AdminClient.create(properties);

        final NewTopic newTopic = new NewTopic("bbb", 13, (short) 1);

        // when
        final CreateTopicsResult ctr = admin.createTopics(Collections.singleton(newTopic));

        // then
        ctr.all().get();

        // when - 2
        final Set<String> names = admin.listTopics().names().get();

        // then - 2
        assertThat(names, hasItem(newTopic.name()));

        // when - 3
        final DeleteTopicsResult dtr = admin.deleteTopics(Collections.singleton(newTopic.name()));

        // then - 3
        dtr.all().get();

        // cleanup
        admin.close();
    }

}
