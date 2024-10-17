package envoy.mesh;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.DeletedRecords;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.RecordsToDelete;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TopicExistsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import envoy.AdminProvider;
import envoy.UpstreamCluster;

/**
 * @author adam.kotwasinski
 * @since test
 */
public class Preconditions {

    private static final Logger LOG = LoggerFactory.getLogger(Preconditions.class);

    public static void setupEmptyTopics(final UpstreamCluster... clusters)
            throws Exception {

        setupEmptyTopics(Arrays.asList(clusters));
    }

    public static void setupEmptyTopics(final Collection<UpstreamCluster> clusters)
            throws Exception {

        for (final UpstreamCluster cluster : clusters) {
            LOG.info("Setting up empty topics in {}", cluster);
            try (final Admin admin = AdminProvider.makeClusterAdmin(cluster)) {

                final int partitionCount = cluster.getPartitionCount();

                // Create topics if absent.
                final Set<NewTopic> topics = cluster.getTopics()
                        .stream()
                        .map(x -> new NewTopic(x, partitionCount, (short) 1))
                        .collect(Collectors.toSet());

                final var createFutures = admin.createTopics(topics).values();
                for (final var entry : createFutures.entrySet()) {
                    handleCreationFuture(entry.getKey(), entry.getValue());
                }

                // Get last offsets.
                final Map<TopicPartition, OffsetSpec> listOffsetsRequest = new HashMap<>();
                cluster.allPartitions().forEach(x -> listOffsetsRequest.put(x, OffsetSpec.latest()));
                final var offsetsFuture = admin.listOffsets(listOffsetsRequest).all().get();

                // Delete data, if any.
                final Map<TopicPartition, RecordsToDelete> deleteRequest = new HashMap<>();
                for (final var entry : offsetsFuture.entrySet()) {
                    final TopicPartition tp = entry.getKey();
                    final long offset = entry.getValue().offset();
                    deleteRequest.put(tp, RecordsToDelete.beforeOffset(offset));
                }
                final var deleteFutures = admin.deleteRecords(deleteRequest).lowWatermarks();
                for (final var entry : deleteFutures.entrySet()) {
                    handleDeleteFuture(entry.getKey(), entry.getValue());
                }
            }
        }
    }

    private static void handleCreationFuture(final String topic,
                                             final KafkaFuture<Void> future) {

        try {
            future.get();
            LOG.info("Topic [{}] created", topic);
        }
        catch (final ExecutionException e) {
            if (e.getCause() instanceof TopicExistsException) {
                LOG.info("Topic [{}] already exists, continuing", topic);
            }
        }
        catch (final Exception e) {
            LOG.info("Failed to create topic [{}]", topic, e);
        }
    }

    private static void handleDeleteFuture(final TopicPartition tp, final KafkaFuture<DeletedRecords> future) {
        try {
            future.get();
            LOG.info("Data in {} deleted", tp);
        }
        catch (final Exception e) {
            LOG.info("Failed to delete data in {}", tp, e);
        }
    }

    private Preconditions() {
    }

}
