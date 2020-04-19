package us.byteb.playground.partexec;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

public class MessageProcessor {

    private static final int MAX_DELAY_MS = 20;
    private final Random random = new Random();
    Map<Integer, Long> executionTimes = new ConcurrentHashMap<>();

    public void process(final Message message) {
        // We emulate the non-equal distribution of delay between sources by
        // adding the source index to the delay.
        final int sourceDelayMs = message.getSource() % MAX_DELAY_MS / 2;
        final long delayMs = random.nextInt(MAX_DELAY_MS / 2) + sourceDelayMs;
        executionTimes.compute(message.getSource(), (source, acc) -> (acc == null ? 0 : acc) + delayMs);

        try {
            Thread.sleep(delayMs);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public Map<Integer, Long> getExecutionTimes() {
        return executionTimes;
    }
}
