package envoy.broker;

import static envoy.Environment.ENVOY_BROKER_HOST;
import static envoy.Environment.ENVOY_BROKER_PORTS;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.Arrays;
import java.util.List;

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

    // Requets that get ignored by upstream if they have a dummy value - quirky but not surprising (what's the value of empty PRODUCE?).
    private static final List<ApiKeys> NO_RESPONSE = Arrays.asList(
            ApiKeys.PRODUCE,
            ApiKeys.ADD_PARTITIONS_TO_TXN,
            ApiKeys.CONSUMER_GROUP_HEARTBEAT);

    // Requests that ruin the connection (upstream closes it / ignores future requests) if they have a dummy value.
    private static final List<ApiKeys> NOT_TESTED = Arrays.asList(
            ApiKeys.OFFSET_FETCH,
            ApiKeys.DESCRIBE_ACLS,
            ApiKeys.VOTE,
            ApiKeys.BEGIN_QUORUM_EPOCH,
            ApiKeys.END_QUORUM_EPOCH,
            ApiKeys.DESCRIBE_QUORUM,
            ApiKeys.ENVELOPE,
            ApiKeys.FETCH_SNAPSHOT,
            ApiKeys.BROKER_REGISTRATION,
            ApiKeys.BROKER_HEARTBEAT,
            ApiKeys.UNREGISTER_BROKER);

    // Manual verification: go to http://localhost:9901/stats and take a look at kafka.broker.request.* metrics.
    // XXX automate this :)
    @Test
    public void shouldSendAllRequests()
            throws Exception {

        for (final ApiKeys apiKey : ApiKeys.values()) {

            if (NOT_TESTED.contains(apiKey)) {
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
        LOG.info("Respose [{}]: {}", header.correlationId(), response);
    }

}
