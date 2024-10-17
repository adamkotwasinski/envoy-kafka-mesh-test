package envoy.mesh;

import java.nio.charset.Charset;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import envoy.ConsumerProvider;
import envoy.Environment;
import envoy.ProducerProvider;
import envoy.UpstreamCluster;

/**
 * Here we are going to create some consumers, and expect to receive some messages.
 *
 * As the proxy is stateful, each invocation might need an Envoy restart
 * (at least until I implement some way to seek the Envoy's consumers - e.g. through Envoy management plugin).
 *
 * Only partition 0 is being used in this test.
 */
public class StatefulConsumerProxyTest {

    private static final Logger LOG = LoggerFactory.getLogger(StatefulConsumerProxyTest.class);

    private static final int MESSAGES_PER_TOPIC = 1000;

    private static final Random RANDOM = new Random();

    private static final List<UpstreamCluster> TEST_CLUSTERS = Arrays.asList(
            Environment.CLUSTER1, Environment.CLUSTER3);

    private static final String T1 = "apples"; // Cluster 1.
    private static final String T2 = "cherries"; // Cluster 3.

    private static final int PARALLEL_CONSUMERS = 4;

    @Before
    public void setUp()
            throws Exception {

        Preconditions.setupEmptyTopics(TEST_CLUSTERS);
        for (final UpstreamCluster cluster : TEST_CLUSTERS) {
            setupMessages(cluster);
        }
        LOG.info("Setup finished");
    }

    private void setupMessages(final UpstreamCluster cluster)
            throws Exception {

        try (Consumer<byte[], byte[]> consumer = ConsumerProvider.makeConsumerFromKafkaDirectly(cluster)) {
            final List<String> topics = cluster.getTopics();
            for (final String topic : topics) {
                final TopicPartition tp = new TopicPartition(topic, 0);
                consumer.assign(Arrays.asList(tp));

                final long start = consumer.beginningOffsets(Arrays.asList(tp)).get(tp);
                final long end = consumer.endOffsets(Arrays.asList(tp)).get(tp);
                if (start == end) {
                    LOG.info("Push some data to topic [{}] in {}", topic, cluster);
                    putSomeMessagesIntoTopic(cluster, topic);
                }
            }
        }
    }

    private void putSomeMessagesIntoTopic(final UpstreamCluster cluster, final String topic)
            throws Exception {

        try (Producer<byte[], byte[]> pr = ProducerProvider.makeProducerToKafkaDirectly(cluster)) {
            for (int i = 0; i < MESSAGES_PER_TOPIC; ++i) {
                final byte[] key = String.format("k-%s-%s", topic, i).getBytes(Charset.forName("UTF-8"));
                final byte[] value = String.format("v-%s-%s", topic, i).getBytes(Charset.forName("UTF-8"));

                final ProducerRecord<byte[], byte[]> record = RANDOM.nextInt(100) >= 5
                        ? new ProducerRecord<>(topic, 0, key, value)
                        : new ProducerRecord<>(topic, 0, null, null); // haha

                final RecordMetadata metadata = pr.send(record).get();
                LOG.info("Record saved at {}-{} offset={}", topic, metadata.partition(), metadata.offset());
            }
        }
    }

    @Test
    public void shouldReceiveRecordsFromUpstream()
            throws Exception {

        final List<TopicPartition> assignment = Arrays.asList(new TopicPartition(T1, 0), new TopicPartition(T2, 0));

        final ExecutorService executor = Executors.newFixedThreadPool(PARALLEL_CONSUMERS);

        final List<ConsumerRecord<byte[], byte[]>> records = Collections.synchronizedList(new ArrayList<>());

        final Instant start = Instant.now();
        for (int i = 0; i < PARALLEL_CONSUMERS; ++i) {
            final int id = i; // argh.
            final Consumer<byte[], byte[]> consumer = ConsumerProvider.makeConsumerFromEnvoyMesh(
                    ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, "3000");
            consumer.assign(assignment);
            final Runnable runnable = () -> consumerWorkerLoop(id, consumer, records);
            executor.submit(runnable);
        }

        executor.shutdown();
        executor.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
        LOG.info("Finished in {}", Duration.between(start, Instant.now()));
    }

    private void consumerWorkerLoop(final int id,
                                    final Consumer<byte[], byte[]> consumer,
                                    /* out, synchronized */ final List<ConsumerRecord<byte[], byte[]>> received) {

        try {
            while (true) {
                final ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofSeconds(5));
                LOG.info("Consumer {} received {} records", id, records.count());
                for (final ConsumerRecord<byte[], byte[]> record : records) {
                    LOG.info("Received {}-{}/{}: {}[{}] / {}[{}]",
                            record.topic(), record.partition(), record.offset(),
                            record.serializedKeySize(), null != record.key() ? new String(record.key()) : "-",
                            record.serializedValueSize(), null != record.value() ? new String(record.value()) : "-");
                    received.add(record);
                }

                synchronized (received) {
                    if (received.size() >= consumer.assignment().size() * MESSAGES_PER_TOPIC) {
                        LOG.info("Detected that all messages have been received, finishing consumer [{}]", id);
                        break;
                    }
                    else {
                        LOG.info("Received size = {}", received.size());
                    }
                }
            }
        }
        finally {
            consumer.close();
        }
    }

}
