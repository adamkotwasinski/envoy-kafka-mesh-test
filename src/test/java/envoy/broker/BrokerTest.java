package envoy.broker;

import static envoy.Environment.ENVOY_BROKER_HOST;
import static envoy.Environment.ENVOY_BROKER_PORTS;
import static envoy.Records.equalRecordContents;
import static java.util.stream.Collectors.toSet;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasKey;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.DescribeClusterOptions;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.DescribeConsumerGroupsOptions;
import org.apache.kafka.clients.admin.LogDirDescription;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.junit.After;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Iterables;

import envoy.AdminProvider;
import envoy.ConsumerProvider;
import envoy.EnvoyMetrics;
import envoy.ProducerProvider;
import envoy.Records;

/**
 * @author adam.kotwasinski
 * @since test
 */
public class BrokerTest {

    private static final Logger LOG = LoggerFactory.getLogger(BrokerTest.class);

    @After
    public void after()
            throws Exception {

        final Map<Integer, Integer> unknownRequestCounts = EnvoyMetrics.collectUnknownRequestCounts();
        assertThat(new HashSet<>(unknownRequestCounts.values()), contains(0));
    }

    @Test
    public void shouldProduceAndConsume()
            throws Exception {

        // given
        final TopicPartition tp = new TopicPartition("envoybrokerfiltertest", 0);

        final Consumer<byte[], byte[]> consumer = ConsumerProvider.makeConsumerFromEnvoyBroker(
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest",
                ConsumerConfig.GROUP_ID_CONFIG, "mytestgroup");
        consumer.assign(Collections.singleton(tp));
        final long currentPosition = consumer.position(tp);
        LOG.info("Current consumer position is {}", currentPosition);

        final Producer<byte[], byte[]> producer = ProducerProvider.makeProducerToEnvoyBroker();
        final ProducerRecord<byte[], byte[]> record = Records.makeRecord(tp.topic(), tp.partition());

        // when
        final RecordMetadata mt = producer.send(record).get();
        LOG.info("Saved at position {}", mt.offset());

        // then
        assertThat(mt.offset(), equalTo(currentPosition));

        // when - 2
        final ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofSeconds(5));

        // then - 2
        assertThat(records.count(), equalTo(1));

        final ConsumerRecord<byte[], byte[]> received = Iterables.getOnlyElement(records);
        assertThat(equalRecordContents(record, received), equalTo(true));

        // when, then - 3
        consumer.commitSync();
    }

    @Test
    public void shouldHandleAdminOperations()
            throws Exception {

        // given
        final Admin admin = AdminProvider.makeBrokerAdmin();
        final NewTopic newTopic = new NewTopic("bbb", 13, (short) 1);

        // when
        final CreateTopicsResult ctr = admin.createTopics(Collections.singleton(newTopic));

        // then
        ctr.all().get(); // No exceptions.

        // when - 2
        final Set<String> names = admin.listTopics().names().get();

        // then - 2
        assertThat(names, hasItem(newTopic.name()));

        // when - 3
        final DeleteTopicsResult dtr = admin.deleteTopics(Collections.singleton(newTopic.name()));

        // then - 3
        dtr.all().get();
    }

    @Test
    public void shouldHandleLogDirDescription()
            throws Exception {

        // given
        final Admin admin = AdminProvider.makeBrokerAdmin();
        final int brokerId = 1;

        // when
        final Map<String, LogDirDescription> result = admin.describeLogDirs(Collections.singleton(brokerId))
                .descriptions()
                .get(brokerId)
                .get();

        // then
        assertThat(result.size(), greaterThan(0));
        final LogDirDescription ldd = result.values().iterator().next();
        assertThat(ldd.totalBytes().isPresent(), equalTo(true));
        assertThat(ldd.usableBytes().isPresent(), equalTo(true));
    }

    @Test
    public void shouldHandleDescribeCluster()
            throws Exception {

        // given
        final Admin admin = AdminProvider.makeBrokerAdmin();

        final DescribeClusterOptions opt = new DescribeClusterOptions();

        // when
        final DescribeClusterResult result = admin.describeCluster(opt);

        // then
        final Collection<Node> nodes = result.nodes().get();
        assertThat(nodes.stream().map(Node::host).collect(toSet()), contains(ENVOY_BROKER_HOST));
        assertThat(nodes.stream().map(Node::port).collect(toSet()), contains(ENVOY_BROKER_PORTS.toArray()));
    }

    @Test
    public void shouldHandleDescribeConsumerGroup()
            throws Exception {

        // given - 1 - consumer
        final String groupName = "shouldHandleDescribeConsumerGroup";
        try (Consumer<byte[], byte[]> consumer = ConsumerProvider.makeConsumerFromEnvoyBroker(
                ConsumerConfig.GROUP_ID_CONFIG, groupName,
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")) {

            // This will make some topic / offsets work happen on the backend.
            consumer.assign(Collections.singleton(new TopicPartition(groupName, 0)));
            consumer.poll(Duration.ofSeconds(1));
            consumer.commitSync();
        }

        // given - 2
        final Admin admin = AdminProvider.makeBrokerAdmin();
        final DescribeConsumerGroupsOptions opt = new DescribeConsumerGroupsOptions();

        // when
        final var result = admin.describeConsumerGroups(Collections.singleton(groupName), opt);

        // then
        final Map<String, ConsumerGroupDescription> descriptions = result.all().get();
        assertThat(descriptions, hasKey(groupName));
        final ConsumerGroupDescription description = descriptions.get(groupName);
        assertThat(description.members(), empty());
    }

}
