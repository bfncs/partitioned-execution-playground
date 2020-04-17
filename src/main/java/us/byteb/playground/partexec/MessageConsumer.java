package us.byteb.playground.partexec;

import java.text.MessageFormat;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static java.text.MessageFormat.format;

public class MessageConsumer {
  private final Map<Integer, Integer> sourceStates = new HashMap<>();

  public void consumeBatch(final List<Message> messages) {
      consumeBatch(messages, msg -> {});
  }

  public void consumeBatch(List<Message> messages, final Consumer<Message> processor) {
    messages.forEach(
        message -> {
          final int source = message.getSource();
          final int newValue = message.getValue();

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

          sourceStates.put(source, newValue);
        });
  }

  public Map<Integer, Integer> getSourceStates() {
    return sourceStates;
  }
}
