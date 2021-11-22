package envoy;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.kafka.common.TopicPartition;

/**
 * Represents upstream Kafka cluster.
 * Compare to what is provided to Envoy config.
 */
public class UpstreamCluster {

    private final String brokers;
    private final int partitionCount;
    private final List<String> topics;

    public UpstreamCluster(final String brokers, final int partitionCount, final String... topics) {
        this.brokers = brokers;
        this.partitionCount = partitionCount;
        this.topics = Collections.unmodifiableList(Arrays.asList(topics));
    }

    public String getBrokers() {
        return this.brokers;
    }

    public List<String> getTopics() {
        return this.topics;
    }

    public List<TopicPartition> allConsumerPartitions() {
        final List<TopicPartition> partitions = new ArrayList<>();
        for (final String topic : this.topics) {
            for (int i = 0; i < this.partitionCount; ++i) {
                partitions.add(new TopicPartition(topic, i));
            }
        }
        return partitions;
    }

}
