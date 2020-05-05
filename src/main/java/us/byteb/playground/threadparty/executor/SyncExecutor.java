package us.byteb.playground.threadparty.executor;

import us.byteb.playground.threadparty.Message;
import us.byteb.playground.threadparty.MessageConsumer;
import us.byteb.playground.threadparty.MessageProcessor;
import us.byteb.playground.threadparty.MessageProducer;

import java.util.List;

public class SyncExecutor implements Executor {
  @Override
  public void execute(
      final MessageProducer producer,
      final MessageProcessor processor,
      final MessageConsumer consumer,
      final int totalMessages,
      final int batchSize) {
    final int numBatches = totalMessages / batchSize;

    int messagesReceived = 0;
    while (messagesReceived < totalMessages) {
      final List<Message> messages = producer.nextBatch(batchSize);
      messagesReceived += messages.size();
      consumer.consumeBatch(messages, processor::process);
    }
  }

  @Override
  public void close() throws Exception {
    // nothing to clean up
  }
}
