package us.byteb.playground.partexec;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicReference;

public class MessageProducer {

    private final BlockingQueue<Message> queue = new LinkedBlockingQueue<>();
    private final Map<Integer, Integer> sourceStates = new HashMap<>();
    private final AtomicReference<Long> durationNs = new AtomicReference<>(null);

    public MessageProducer(final int numSources, final int numMessages) {
        final Random random = new Random();
        new Thread(() -> {
            final long startTime = System.nanoTime();
            for (int i = 0; i < numMessages; i++) {
                final int source = random.nextInt(numSources);
                final Integer value = sourceStates.get(source);

                final int nextValue = value == null ? 0 : value + 1;

                queue.add(new Message(source, nextValue));
                sourceStates.put(source, nextValue);
                busySleep(700_000);
            }
            durationNs.set(System.nanoTime() - startTime);
        }).start();
    }

    private static void busySleep(long nanos) {
        long elapsed;
        final long startTime = System.nanoTime();
        do {
            elapsed = System.nanoTime() - startTime;
        } while (elapsed < nanos);
    }

    public List<Message> nextBatch(final int batchSize) {
        final ArrayList<Message> messages = new ArrayList<>();

        int i = 0;
        Message msg;
        while (i < batchSize && (msg = queue.poll()) != null) {
            messages.add(msg);
        }

        return messages;
    }

    public Map<Integer, Integer> getSourceStates() {
        return sourceStates;
    }

    public Optional<Duration> getProductionDuration() {
        final Long duration = durationNs.get();
        if (duration == null) {
            return Optional.empty();
        }
        return Optional.of(Duration.ofNanos(duration));
    }
}
