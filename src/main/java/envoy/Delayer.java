package envoy;

public class Delayer {

    public void delay(final long millis) {
        if (millis <= 0) {
            return;
        }

        final long delay = RandomHolder.RANDOM.nextLong() % millis;
        if (delay > 0) {
            try {
                Thread.sleep(delay);
            }
            catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("interrupted", e);
            }
        }
    }

}
