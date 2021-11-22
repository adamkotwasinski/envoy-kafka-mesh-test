package envoy.mesh;

import static com.google.common.util.concurrent.Uninterruptibles.awaitUninterruptibly;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.collect.Iterables;

import envoy.ConsumerProvider;
import envoy.Delayer;
import envoy.Environment;
import envoy.ProducerProvider;
import envoy.Records;
import envoy.UpstreamCluster;

/**
 * Here we are going to create N producers, sending each record to a random topic to be handled by Envoy.
 * Later, we receive these these records from upstream clusters, and verify that data is correct.
 * We also track a "drift" metric that is introduced by Envoy re-batching records in between.
 */
public class MultipleTopicMeshTest {

    private static final Logger LOG = LoggerFactory.getLogger(MultipleTopicMeshTest.class);

    /**
     * Each message sent by producer will go to a random topic (they all go through Envoy).
     */
    private static int CONCURRENT_PRODUCERS = 50;

    /**
     * Each producer will send this many messages.
     */
    private static int MESSAGES_PER_PRODUCER = 2000;

    /**
     * After each message we will wait _up to_ this much time.
     * This will semi-randomly kick off producer's batching.
     * The lower this value gets, the higher the offset drift will be (because there's more chance for several requests
     * to be handled at the same time by the same Envoy worker).
     */
    private static final int MESSAGE_SEND_DELAY_MS_MAX = 50;

    private final Delayer delayer = new Delayer();

    private List<Producer<byte[], byte[]>> producers;
    private List<Consumer<byte[], byte[]>> consumers;

    private ExecutorService executor;

    @Before
    public void setUp() {
        this.producers = createProducers();
        this.consumers = createConsumers();
        this.executor = Executors.newFixedThreadPool(this.producers.size() + this.consumers.size());
    }

    private List<Producer<byte[], byte[]>> createProducers() {
        final List<Producer<byte[], byte[]>> result = new ArrayList<>();
        for (int i = 0; i < CONCURRENT_PRODUCERS; ++i) {
            final Producer<byte[], byte[]> producer = ProducerProvider.makeMeshProducer();
            result.add(producer);
        }
        return result;
    }

    private List<Consumer<byte[], byte[]>> createConsumers() {
        final List<Consumer<byte[], byte[]>> result = new ArrayList<>();
        for (final UpstreamCluster cluster : Environment.CLUSTERS) {
            final Consumer<byte[], byte[]> consumer = ConsumerProvider.makeClusterConsumer(cluster,
                    ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
            final List<TopicPartition> partitions = cluster.allConsumerPartitions();
            consumer.assign(partitions);
            partitions.forEach(partition -> {
                final long position = consumer.position(partition);
                LOG.info("Reading from [{}], start position = {}", partition, position);
            });
            result.add(consumer);
        }
        return result;
    }

    @After
    public void tearDown() {
        this.consumers.forEach(Consumer::close);
        this.producers.forEach(Producer::close);
        this.executor.shutdownNow();
    }

    @Test
    public void test()
            throws Exception {

        // given
        final List<Future<List<DeliveryInfo>>> producerFutures = new ArrayList<>();
        this.producers.forEach(producer -> {
            final Future<List<DeliveryInfo>> future = this.executor.submit(() -> sendRecords(producer));
            producerFutures.add(future);
        });

        final Map<String, List<DeliveryInfo>> topicToRecords = new TreeMap<>();
        Environment.ALL_TOPICS.forEach(topic -> topicToRecords.put(topic, new ArrayList<>()));
        for (final Future<List<DeliveryInfo>> future : producerFutures) {
            final List<DeliveryInfo> deliveries = future.get();
            for (final DeliveryInfo delivery : deliveries) {
                topicToRecords.get(delivery.record.topic()).add(delivery);
            }
        }
        LOG.info("Sent all records");

        topicToRecords.entrySet().forEach(e -> {
            final int count = e.getValue().size();
            LOG.info("Records sent to [{}]: {}", e.getKey(), count);
        });

        // when
        final List<ConsumerRecord<byte[], byte[]>> received = new ArrayList<>();
        for (final Consumer<byte[], byte[]> consumer : this.consumers) {
            final List<ConsumerRecord<byte[], byte[]>> records = receiveRecords(consumer);
            received.addAll(records);
        }

        // then
        assertThat(received, hasSize(CONCURRENT_PRODUCERS * MESSAGES_PER_PRODUCER));
        LOG.info("Received all records");

        final Map<String, Long> driftPerTopic = new TreeMap<>();
        Environment.ALL_TOPICS.forEach(topic -> driftPerTopic.put(topic, 0L));
        for (final ConsumerRecord<byte[], byte[]> receivedRecord : received) {
            final String topic = receivedRecord.topic();
            final List<DeliveryInfo> potentialMatches = topicToRecords.get(topic);
            final Optional<Long> matchDrift = findMatchingRecord(receivedRecord, potentialMatches);
            assertThat(matchDrift.isPresent(), is(true));
            driftPerTopic.put(topic, driftPerTopic.get(topic) + matchDrift.get());
        }

        LOG.info("Offset drift = {}", driftPerTopic);
        LOG.info("Summary drift = {}", driftPerTopic.values().stream().mapToLong(x -> x).sum());
    }

    private List<DeliveryInfo> sendRecords(final Producer<byte[], byte[]> producer) {
        final List<DeliveryInfo> recordsSent = Collections.synchronizedList(new ArrayList<>());
        final CountDownLatch cl = new CountDownLatch(MESSAGES_PER_PRODUCER);

        final int trip = Math.max(MESSAGES_PER_PRODUCER / 20, 1);
        for (int i = 1; i <= MESSAGES_PER_PRODUCER; ++i) {
            final ProducerRecord<byte[], byte[]> record = Records.makeRecord(Environment.randomTopic());
            producer.send(record, (metadata, exception) -> {

                if (null != metadata) {
                    LOG.debug("Sent {}-{}: {}", metadata.topic(), metadata.partition(), metadata.offset());
                    recordsSent.add(new DeliveryInfo(record, metadata.offset()));
                    cl.countDown();
                }
                else {
                    throw new IllegalStateException("delivery failure", exception);
                }

            });

            if (0 == i % trip) {
                LOG.info("Submitted {} / {} messages", i, MESSAGES_PER_PRODUCER);
            }

            this.delayer.delay(MESSAGE_SEND_DELAY_MS_MAX);
        }

        awaitUninterruptibly(cl);
        return recordsSent;
    }

    private List<ConsumerRecord<byte[], byte[]>> receiveRecords(final Consumer<byte[], byte[]> consumer) {
        final List<ConsumerRecord<byte[], byte[]>> result = new ArrayList<>();
        while (hasMoreMessages(consumer)) {
            final ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(Long.MAX_VALUE));
            Iterables.addAll(result, records);
        }
        LOG.info("Consumer {} received {} messages", consumer.assignment(), result.size());
        return result;
    }

    private static boolean hasMoreMessages(final Consumer<byte[], byte[]> consumer) {
        final Set<TopicPartition> assignment = consumer.assignment();
        final Map<TopicPartition, Long> endOffsets = consumer.endOffsets(assignment);
        for (final TopicPartition partition : assignment) {
            final long position = consumer.position(partition);
            if (position < endOffsets.get(partition)) {
                return true;
            }
        }
        return false;
    }

    private static Optional<Long> findMatchingRecord(final ConsumerRecord<byte[], byte[]> record,
                                                     final List<DeliveryInfo> potentialMatches) {

        final int valueHash = Arrays.hashCode(record.value());
        for (final DeliveryInfo candidate : potentialMatches) {
            if (valueHash != candidate.valueHash) {
                continue;
            }
            if (Records.equalRecordContents(candidate.record, record)) {
                // If multiple records got batched by KafkaProducer, the offsets might have drifter.
                return Optional.of(Math.abs(record.offset() - candidate.offset));
            }
        }
        // This should never happen, because we should receive all the messages that we have sent, and vice versa.
        return Optional.absent();
    }

    private static class DeliveryInfo {

        public final ProducerRecord<byte[], byte[]> record;
        public final int valueHash;
        public final long offset;

        public DeliveryInfo(final ProducerRecord<byte[], byte[]> record, final long offset) {
            this.record = record;
            this.valueHash = Arrays.hashCode(record.value());
            this.offset = offset;
        }

    }

}
