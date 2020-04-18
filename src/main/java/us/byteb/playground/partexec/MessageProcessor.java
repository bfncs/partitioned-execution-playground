package us.byteb.playground.partexec;

import java.util.Random;

public class MessageProcessor {

    private static final int MAX_DELAY_MS = 10;
    private final Random random = new Random();
    private long totalExecutionTime = 0;

    public void process(final Message message) {
        // We emulate the non-equal distribution of delay between sources by
        // adding the source index to the delay.
        final int sourceDelayMs = message.getSource() % MAX_DELAY_MS / 2;
        final long delayMs = random.nextInt(MAX_DELAY_MS / 2) + sourceDelayMs;
        totalExecutionTime += delayMs;

        try {
            Thread.sleep(delayMs);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public long getTotalExecutionTime() {
        return totalExecutionTime;
    }
}
