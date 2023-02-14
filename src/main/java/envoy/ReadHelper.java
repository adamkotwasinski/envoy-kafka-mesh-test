package envoy;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reads a response (its 4-byte length header and N following bytes).
 *
 * @author adam.kotwasinski
 */
public class ReadHelper {

    private static final Logger LOG = LoggerFactory.getLogger(ReadHelper.class);

    private static final int HEADER_SIZE = 4;

    public final ByteBuffer hb;
    public ByteBuffer db;

    private ReadHelper() {
        this.hb = ByteBuffer.allocate(HEADER_SIZE);
        this.db = null;
    }

    public void read(final SocketChannel channel)
            throws IOException {

        while (this.hb.hasRemaining()) {
            channel.read(this.hb);
        }
        this.hb.flip();
        final int dataLength = this.hb.getInt();
        LOG.trace("Response length = {}", dataLength);

        if (dataLength >= 0) {
            this.db = ByteBuffer.allocate(dataLength);
            while (this.db.hasRemaining()) {
                channel.read(this.db);
            }
            this.db.flip();
        }
        else {
            throw new IllegalArgumentException("No data: " + dataLength);
        }
    }

    public static ByteBuffer receive(final SocketChannel channel) {
        final ReadHelper rh = new ReadHelper();
        try {
            rh.read(channel);
            return rh.db;
        }
        catch (final IOException e) {
            throw new IllegalArgumentException("IO failure", e);
        }
    }

}
