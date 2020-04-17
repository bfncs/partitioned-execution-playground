package us.byteb.playground.partexec.executor;

import us.byteb.playground.partexec.MessageConsumer;
import us.byteb.playground.partexec.MessageProcessor;
import us.byteb.playground.partexec.MessageProducer;

public interface Executor {
    void execute(
            MessageProducer producer,
            MessageProcessor processor,
            MessageConsumer consumer,
            int numBatches,
            int batchSize);
}
