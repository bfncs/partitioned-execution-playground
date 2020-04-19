package us.byteb.playground.partexec.executor;

import us.byteb.playground.partexec.Message;
import us.byteb.playground.partexec.MessageConsumer;
import us.byteb.playground.partexec.MessageProcessor;
import us.byteb.playground.partexec.MessageProducer;

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
