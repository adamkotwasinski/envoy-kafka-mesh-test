package envoy.mesh;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.TreeMap;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Iterables;

import envoy.ConsumerProvider;
import envoy.Environment;
import envoy.ProducerProvider;
import envoy.Records;

/**
 * @author adam.kotwasinski
 * @since test
 */
public class SimpleMeshTest {

    private static final Logger LOG = LoggerFactory.getLogger(SimpleMeshTest.class);

    private static final String TOPIC = Iterables.get(Environment.CLUSTER1.getTopics(), 0);
    private static final TopicPartition PARTITION = new TopicPartition(TOPIC, 0);

    private Producer<byte[], byte[]> producer;

    @Before
    public void setUp()
            throws Exception {

        Preconditions.setupEmptyTopics(Environment.CLUSTER1);
        this.producer = ProducerProvider.makeProducerToEnvoyMesh();
    }

    @After
    public void tearDown() {
        this.producer.close(Duration.ofMillis(Long.MAX_VALUE));
    }

    /**
     * Send messages one by one.
     * Receive messages - verify that payloads and offsets are the same as send()'s results.
     */
    @Test
    public void shouldSendRecordsToCluster()
            throws Exception {

        final int recordCount = 200;
        final List<ProducerRecord<byte[], byte[]>> sent = IntStream.range(0, recordCount)
                .mapToObj(x -> Records.makeRecord(PARTITION.topic()))
                .collect(Collectors.toList());

        final TreeMap<Long, ProducerRecord<byte[], byte[]>> offsetToRecord = new TreeMap<>();
        for (final ProducerRecord<byte[], byte[]> record : sent) {
            final Future<RecordMetadata> future = this.producer.send(record);
            final RecordMetadata metadata = future.get();
            offsetToRecord.put(metadata.offset(), record);
            LOG.trace("Record saved at offset {}", metadata.offset());
        }

        int received = 0;
        final Consumer<byte[], byte[]> consumer = ConsumerProvider.makeConsumerFromKafkaDirectly(Environment.CLUSTER1);
        consumer.assign(Collections.singleton(PARTITION));

        // We do not need to re-read all messages.
        consumer.seek(PARTITION, offsetToRecord.firstKey());

        // We should receive the records we have sent, and only these.
        while (received < recordCount) {
            final ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(Long.MAX_VALUE));
            for (final ConsumerRecord<byte[], byte[]> receivedRecord : records) {
                final long receivedOffset = receivedRecord.offset();
                LOG.trace("Received record at offset {}", receivedOffset);

                final ProducerRecord<byte[], byte[]> sentRecord = offsetToRecord.get(receivedOffset);
                if (null == sentRecord) {
                    throw new IllegalStateException("missing record at offset: " + receivedOffset);
                }

                if (!Records.equalRecordContents(sentRecord, receivedRecord)) {
                    throw new IllegalStateException("invalid data received: " + receivedOffset);
                }

                received++;
            }
        } // while
    }

}
