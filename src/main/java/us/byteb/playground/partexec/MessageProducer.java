package us.byteb.playground.partexec;

import java.util.*;

import static java.util.stream.Collectors.toMap;

public class MessageProducer {

  private final int numSources;
  private final Map<Integer, Integer> sourceStates = new HashMap<>();
  private final Random random = new Random();

  public MessageProducer(final int numSources) {
    this.numSources = numSources;
  }

  public List<Message> nextBatch(final int batchSize) {
    final ArrayList<Message> messages = new ArrayList<>();
    for (int i = 0; i < batchSize; i++) {
      final int source = random.nextInt(numSources);
      final Integer value = sourceStates.get(source);

      final int nextValue = value == null ? 0 : value + 1;

      messages.add(new Message(source, nextValue));
      sourceStates.put(source, nextValue);
    }

    return messages;
  }

  public Map<Integer, Integer> getSourceStates() {
    return sourceStates;
  }
}
