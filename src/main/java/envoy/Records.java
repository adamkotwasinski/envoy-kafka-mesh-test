package envoy;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

public class Records {

    public static ProducerRecord<byte[], byte[]> makeRecord(final String topic) {
        final byte[] key = RandomHolder.bytes(256);
        final byte[] value = RandomHolder.bytes(2048);
        final ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(topic, key, value);
        for (int i = 0; i < RandomHolder.nextInt(32); ++i) {
            for (int j = 0; j < RandomHolder.nextInt(3); j++) {
                // Let's make some records with the same names.
                record.headers().add("hh_" + i, RandomHolder.bytes(128));
            }
        }
        return record;
    }

    /**
     * This method needs to be aware of what Envoy does with records.
     */
    public static boolean equalRecordContents(final ProducerRecord<byte[], byte[]> sentRecord,
                                              final ConsumerRecord<byte[], byte[]> receivedRecord) {

        final boolean equalData = Arrays.equals(sentRecord.key(), receivedRecord.key())
                && Arrays.equals(sentRecord.value(), receivedRecord.value());
        if (!equalData) {
            return false;
        }

        final boolean equalHeaderCounts = Iterables.size(sentRecord.headers()) == Iterables
                .size(receivedRecord.headers());
        if (!equalHeaderCounts) {
            return false;
        }

        final List<Header> sentHeaders = Lists.newArrayList(sentRecord.headers());
        final List<Header> receivedHeaders = Lists.newArrayList(receivedRecord.headers());
        // RecordHeader.equals appears reasonable
        return Objects.equals(sentHeaders, receivedHeaders);
    }

    private Records() {
    }

}
