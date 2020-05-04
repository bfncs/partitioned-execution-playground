package us.byteb.playground.partexec;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import us.byteb.playground.partexec.executor.Executor;
import us.byteb.playground.partexec.executor.GroupingMultithreadedExecutor;
import us.byteb.playground.partexec.executor.PartitionedQueueExecutor;
import us.byteb.playground.partexec.executor.SyncExecutor;

import java.util.Collection;

public class App {

    private static final int NUM_BATCHES = 10;
    private static final int BATCH_SIZE = 512;
    private static final int NUM_SOURCES = 128;

    public static void main(String[] args) {
        benchmark(PartitionedQueueExecutor.class);
        benchmark(GroupingMultithreadedExecutor.class);
        benchmark(SyncExecutor.class);

        // TODO: benchmark with https://github.com/LMAX-Exchange/disruptor
    }

    private static void benchmark(final Class<? extends Executor> executorClass) {
        final int numMessages = NUM_BATCHES * BATCH_SIZE;
        final MessageProducer producer = new MessageProducer(NUM_SOURCES, numMessages);
        final MessageProcessor processor = new MessageProcessor();
        final MessageConsumer consumer = new MessageConsumer();

        final long startTime;
        try (final Executor executor = executorClass.getConstructor().newInstance()) {
            startTime = System.nanoTime();
            executor.execute(producer, processor, consumer, numMessages, BATCH_SIZE);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }

        final float durationMs = (System.nanoTime() - startTime) / 1000000.0f;
        final DescriptiveStatistics statistics =
                new DescriptiveStatistics(nanosToMillis(consumer.getLatencies()));
        System.out.printf(
                "%s done in %.3fms (" +
                        "total producer execution %dms, " +
                        "total executor execution %dms, " +
                        "median %.1fms, p.75 %.1fms, p.99 %.1fms" +
                        ")%n",
                executorClass.getSimpleName(),
                durationMs,
                producer.getProductionDuration().get().toMillis(),
                processor.getExecutionTimes().values().stream().mapToLong(Long::longValue).sum(),
                statistics.getPercentile(50.0),
                statistics.getPercentile(75),
                statistics.getPercentile(99)
        );

        if (!producer.getSourceStates().equals(consumer.getSourceStates())) {
            System.out.println("Final producer state: " + producer.getSourceStates());
            System.out.println("Final consumer state: " + consumer.getSourceStates());
            throw new IllegalStateException("Final producer and consumer states are not equal!");
        }
    }

    private static double[] nanosToMillis(final Collection<Long> values) {
        return ArrayUtils.toPrimitive(values.stream().map(l -> l / 1000000.0d).toArray(Double[]::new));
    }
}
