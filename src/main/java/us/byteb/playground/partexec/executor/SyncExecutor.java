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
          final int numBatches,
          final int batchSize) {
    for (int i = 0; i < numBatches; i++) {
      final List<Message> messages = producer.nextBatch(batchSize);
      consumer.consumeBatch(messages, processor::process);
    }
  }
}
