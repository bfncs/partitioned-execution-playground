package us.byteb.playground.partexec;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

class MessageConsumerTest {

  @Test
  void consumeBatch() {
    assertDoesNotThrow(
        () -> {
          final List<Message> messages =
              buildMessageBatch(1, 0, 10);
          new MessageConsumer().consumeBatch(messages, msg -> {});
        });
  }

  private static List<Message> buildMessageBatch(final int source, final int start, final int end) {
    final List<Message> messages = new ArrayList<>();

    for (int i = start; i < end; i++) {
      messages.add(new Message(source, i));
    }

    return messages;
  }

}
