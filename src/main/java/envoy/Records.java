package envoy;

import java.util.Arrays;
import java.util.Random;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;

public class Records {

    private static final Random RANDOM = new Random();

    public static ProducerRecord<byte[], byte[]> makeRecord(final String topic) {
        final byte[] key = new byte[256];
        RANDOM.nextBytes(key);
        final byte[] value = new byte[2048];
        RANDOM.nextBytes(value);
        return new ProducerRecord<>(topic, key, value);
    }

    /**
     * This method needs to be aware of what Envoy does with records.
     * Right now no headers!
     */
    public static boolean equalRecordContents(final ProducerRecord<byte[], byte[]> sentRecord,
                                              final ConsumerRecord<byte[], byte[]> receivedRecord) {

        return Arrays.equals(sentRecord.key(), receivedRecord.key())
                && Arrays.equals(sentRecord.value(), receivedRecord.value());
    }

    private Records() {
    }

}
