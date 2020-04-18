package us.byteb.playground.partexec.executor;

import us.byteb.playground.partexec.Message;
import us.byteb.playground.partexec.MessageConsumer;
import us.byteb.playground.partexec.MessageProcessor;
import us.byteb.playground.partexec.MessageProducer;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class PartitionedQueueExecutor implements Executor {

    public static final int POLL_TIMEOUT_MS = 10;
    private static final int PARTITION_COUNT = 16;
    final AtomicBoolean stopExecution = new AtomicBoolean(false);
    final CountDownLatch finishedCountdown = new CountDownLatch(PARTITION_COUNT);
    final List<BlockingQueue<Runnable>> queues;

    public PartitionedQueueExecutor() {
        queues = IntStream.range(0, PARTITION_COUNT).boxed()
                .map(unused -> new LinkedBlockingQueue<Runnable>())
                .collect(Collectors.toList());

        queues.forEach(queue -> {
            final Thread worker = new Thread(() -> {
                while (true) {
                    final Runnable task = queue.poll();
                    if (task == null) {
                        if (stopExecution.get()) {
                            finishedCountdown.countDown();
                            return;
                        } else {
                            try {
                                Thread.sleep(POLL_TIMEOUT_MS);
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                            }
                        }
                    } else {
                        task.run();
                    }
                }
            });
            worker.start();
        });
    }

    private static int getPartition(final String partitionKey) {
        int h = partitionKey.hashCode();
        h = h ^ (h >>> 16);
        final int partition = h % PARTITION_COUNT;
        return partition;
    }

    @Override
    public void execute(
            final MessageProducer producer,
            final MessageProcessor processor,
            final MessageConsumer consumer,
            final int numBatches,
            final int batchSize) {
        for (int i = 0; i < numBatches; i++) {
            final List<Message> messages = producer.nextBatch(batchSize);
            messages.forEach(
                    message -> {
                        final int partition = getPartition(Integer.toString(message.getSource()));
                        queues.get(partition).add(() -> {
                            //System.out.println(message);
                            consumer.consume(message, processor::process);
                        });
                    });
        }

        stopExecutionGracefully();
    }

    private void stopExecutionGracefully() {
        stopExecution.set(true);
        try {
            finishedCountdown.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void close() throws Exception {
        stopExecutionGracefully();
    }
}
