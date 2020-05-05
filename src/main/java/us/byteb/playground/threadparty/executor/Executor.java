package us.byteb.playground.threadparty.executor;

import us.byteb.playground.threadparty.MessageConsumer;
import us.byteb.playground.threadparty.MessageProcessor;
import us.byteb.playground.threadparty.MessageProducer;

public interface Executor extends AutoCloseable {
  void execute(
      MessageProducer producer,
      MessageProcessor processor,
      MessageConsumer consumer,
      int totalMessages,
      int batchSize);
}
