package us.byteb.playground.threadparty;

import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

public class MessageProcessor {

    private static final int MIN_DELAY_MS = 2;
    private static final int MAX_DELAY_MS = 25;
    private final Random random = new Random();
    Map<Integer, Long> executionTimes = new ConcurrentHashMap<>();

    public void process(final Message message) {
        final long delayMs = MIN_DELAY_MS + random.nextInt(MAX_DELAY_MS - MIN_DELAY_MS);
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
