package us.byteb.playground.threadparty;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static java.text.MessageFormat.format;

public class MessageConsumer {
    private final Map<Integer, Integer> sourceStates = new HashMap<>();
    private final List<Long> latencies = new ArrayList<>();

    public void consumeBatch(List<Message> messages, final Consumer<Message> processor) {
        messages.forEach(message -> consume(message, processor));
    }

    public void consume(final Message message, final Consumer<Message> processor) {
        final int source = message.getSource();
        final int newValue = message.getValue();

        // TODO: verify after completion
        final Integer oldValue = sourceStates.get(source);
        if (oldValue == null) {
            if (newValue != 0) {
                throw new IllegalStateException(
                        format("Received unexpected first for source {0}: {1}", source, newValue));
            }
        } else {
            final int expectedValue = oldValue + 1;
            if (newValue != expectedValue) {
                throw new IllegalStateException(
                        MessageFormat.format(
                                "Received unexpected value for source {0}: {1} (expected {2})",
                                source, newValue, expectedValue));
            }
        }

        processor.accept(message);
        latencies.add(System.nanoTime() - message.getCreated());
        sourceStates.put(source, newValue);
    }

    public Map<Integer, Integer> getSourceStates() {
        return sourceStates;
    }

    public List<Long> getLatencies() {
        return latencies;
    }
}
