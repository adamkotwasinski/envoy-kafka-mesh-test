package envoy;

import java.util.Random;

public class RandomHolder {

    public static Random RANDOM = new Random();

    public static byte[] bytes(final int sz) {
        final byte[] bytes = new byte[sz];
        RANDOM.nextBytes(bytes);
        return bytes;
    }

    public static int nextInt(final int bound) {
        return RANDOM.nextInt(bound);
    }

    private RandomHolder() {
    }

}
