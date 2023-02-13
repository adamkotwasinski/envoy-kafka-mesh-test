package envoy.broker;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

import org.apache.kafka.common.message.ApiMessageType;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.MessageSizeAccumulator;
import org.apache.kafka.common.protocol.ObjectSerializationCache;
import org.apache.kafka.common.requests.RequestHeader;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * In this test suite, we just generate basic requests (fortunately there's a factory method for that) and send them upstream.
 * Unfortunately we cannot wait for replies, as the upstream seems to hang on some malformed requests like PRODUCE.
 *
 * @author adam.kotwasinski
 */
public class AllRequestsBrokerTest {

    private static final Logger LOG = LoggerFactory.getLogger(AllRequestsBrokerTest.class);

    // Manual verification: go to http://localhost:9901/stats and take a look at kafka.broker.request.* metrics.
    // XXX automate this :)
    @Test
    public void shouldSendAllRequests()
            throws Exception {

        final SocketAddress address = new InetSocketAddress("localhost", 19092);
        try (SocketChannel channel = SocketChannel.open(address)) {
            int correlationId = 0;
            for (final ApiKeys apiKey : ApiKeys.values()) {
                LOG.info("Sending {}", apiKey);
                sendRequest(channel, apiKey, apiKey.latestVersion(), correlationId++); // This should not fail.
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

}
