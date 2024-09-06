package envoy.broker;

import static envoy.Environment.ENVOY_BROKER_HOST;
import static envoy.Environment.ENVOY_BROKER_PORTS;
import static org.apache.kafka.common.protocol.ApiKeys.ADD_PARTITIONS_TO_TXN;
import static org.apache.kafka.common.protocol.ApiKeys.ASSIGN_REPLICAS_TO_DIRS;
import static org.apache.kafka.common.protocol.ApiKeys.BEGIN_QUORUM_EPOCH;
import static org.apache.kafka.common.protocol.ApiKeys.BROKER_HEARTBEAT;
import static org.apache.kafka.common.protocol.ApiKeys.BROKER_REGISTRATION;
import static org.apache.kafka.common.protocol.ApiKeys.CONSUMER_GROUP_HEARTBEAT;
import static org.apache.kafka.common.protocol.ApiKeys.CONTROLLER_REGISTRATION;
import static org.apache.kafka.common.protocol.ApiKeys.DESCRIBE_ACLS;
import static org.apache.kafka.common.protocol.ApiKeys.DESCRIBE_QUORUM;
import static org.apache.kafka.common.protocol.ApiKeys.DESCRIBE_TOPIC_PARTITIONS;
import static org.apache.kafka.common.protocol.ApiKeys.END_QUORUM_EPOCH;
import static org.apache.kafka.common.protocol.ApiKeys.ENVELOPE;
import static org.apache.kafka.common.protocol.ApiKeys.FETCH_SNAPSHOT;
import static org.apache.kafka.common.protocol.ApiKeys.GET_TELEMETRY_SUBSCRIPTIONS;
import static org.apache.kafka.common.protocol.ApiKeys.LIST_CLIENT_METRICS_RESOURCES;
import static org.apache.kafka.common.protocol.ApiKeys.OFFSET_FETCH;
import static org.apache.kafka.common.protocol.ApiKeys.PRODUCE;
import static org.apache.kafka.common.protocol.ApiKeys.PUSH_TELEMETRY;
import static org.apache.kafka.common.protocol.ApiKeys.UNREGISTER_BROKER;
import static org.apache.kafka.common.protocol.ApiKeys.VOTE;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

import org.apache.kafka.common.message.ApiMessageType;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.MessageSizeAccumulator;
import org.apache.kafka.common.protocol.ObjectSerializationCache;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.requests.ResponseHeader;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableSet;

import envoy.EnvoyMetrics;
import envoy.ReadHelper;

/**
 * In this test suite, we just generate basic requests (fortunately there's a factory method for that) and send them upstream.
 * For most of the requests, the upstream sends a response, so we try to get it as well (usually it's an error code, as we send empty stuff).
 *
 * @author adam.kotwasinski
 * @since test
 */
public class BrokerRequestTest {

    private static final Logger LOG = LoggerFactory.getLogger(BrokerRequestTest.class);

    // Requets that get ignored/rejected by upstream if they have a dummy value.
    // Quirky but not surprising (what's the value of empty PRODUCE?).
    private static final List<ApiKeys> NO_RESPONSE = Arrays.asList(
            PRODUCE,
            OFFSET_FETCH,
            ADD_PARTITIONS_TO_TXN,
            DESCRIBE_ACLS,
            VOTE,
            BEGIN_QUORUM_EPOCH,
            END_QUORUM_EPOCH,
            DESCRIBE_QUORUM,
            ENVELOPE,
            FETCH_SNAPSHOT,
            BROKER_REGISTRATION,
            BROKER_HEARTBEAT,
            UNREGISTER_BROKER,
            CONSUMER_GROUP_HEARTBEAT,
            CONTROLLER_REGISTRATION,
            ASSIGN_REPLICAS_TO_DIRS,
            LIST_CLIENT_METRICS_RESOURCES,
            DESCRIBE_TOPIC_PARTITIONS);

    // Requests that are simply not supported by the filter.
    private static final Predicate<ApiKeys> NOT_SUPPORTED = ImmutableSet.of(
            GET_TELEMETRY_SUBSCRIPTIONS, PUSH_TELEMETRY)::contains;

    @Test
    public void shouldSendAllRequests()
            throws Exception {

        final Map<Integer, Integer> unknownsAtStart = EnvoyMetrics.collectUnknownRequestCounts();

        for (final ApiKeys apiKey : ApiKeys.values()) {

            if (NOT_SUPPORTED.test(apiKey)) {
                LOG.info("Ignoring {}", apiKey);
                continue;
            }

            final SocketAddress address = new InetSocketAddress(ENVOY_BROKER_HOST, ENVOY_BROKER_PORTS.get(0));
            try (SocketChannel channel = SocketChannel.open(address)) {

                final short version = apiKey.latestVersion();
                LOG.info("Sending {}/{}", apiKey, version);
                sendRequest(channel, apiKey, version, 0); // This should not fail.

                if (NO_RESPONSE.contains(apiKey)) {
                    LOG.info("Response will not be sent for dummy {} request", apiKey);
                }
                else {
                    LOG.info("Receiving {}", apiKey);
                    receiveResponse(channel, apiKey, version);
                }

                final Map<Integer, Integer> unknownsAtEnd = EnvoyMetrics.collectUnknownRequestCounts();
                assertThat("unknown request for " + apiKey, unknownsAtEnd, equalTo(unknownsAtStart));
            }
        }
    }

    private static void sendRequest(final SocketChannel channel,
                                    final ApiKeys apiKey,
                                    final short version,
                                    final int correlationId)
            throws Exception {

        final ApiMessageType apiMessageType = ApiMessageType.fromApiKey(apiKey.id);
        final ApiMessage data = apiMessageType.newRequest(); // Haha!
        final RequestHeader header = new RequestHeader(apiKey, version, "", correlationId);

        final ByteBuffer bytes = toBytes(header, data);
        channel.write(bytes);
    }

    private static ByteBuffer toBytes(final RequestHeader header, final ApiMessage data) {

        final short headerVersion = header.headerVersion();
        final short apiVersion = header.apiVersion();

        final ObjectSerializationCache serializationCache = new ObjectSerializationCache();

        final MessageSizeAccumulator ms = new MessageSizeAccumulator();
        header.data().addSize(ms, serializationCache, headerVersion);
        data.addSize(ms, serializationCache, apiVersion);

        final ByteBuffer bb = ByteBuffer.allocate(ms.sizeExcludingZeroCopy() + 4);
        final ByteBufferAccessor bba = new ByteBufferAccessor(bb);

        bba.writeInt(ms.totalSize());
        header.data().write(bba, serializationCache, headerVersion);
        data.write(bba, serializationCache, apiVersion);

        bb.flip();

        return bb;
    }

    private static void receiveResponse(final SocketChannel channel, final ApiKeys apiKey, final short version) {
        final ByteBuffer data = ReadHelper.receive(channel);
        final ResponseHeader header = ResponseHeader.parse(data, apiKey.responseHeaderVersion(version));
        final AbstractResponse response = AbstractResponse.parseResponse(apiKey, data, version);
        LOG.info("Response [{}]: {}", header.correlationId(), response);
    }

}
