package us.byteb.playground.partexec;

import us.byteb.playground.partexec.executor.Executor;
import us.byteb.playground.partexec.executor.GroupingMultithreadedExecutor;
import us.byteb.playground.partexec.executor.SyncExecutor;

public class App {

  private static final int NUM_BATCHES = 10;
  private static final int BATCH_SIZE = 100;
  private static final int NUM_SOURCES = 16;

  public static void main(String[] args) throws InterruptedException {
    benchmark(SyncExecutor.class);
    benchmark(GroupingMultithreadedExecutor.class);
  }

  private static void benchmark(final Class<? extends Executor> executorClass) {
    final MessageProducer producer = new MessageProducer(NUM_SOURCES);
    final MessageProcessor processor = new MessageProcessor();
    final MessageConsumer consumer = new MessageConsumer();

    try (final Executor executor = executorClass.newInstance()) {
      final long startTime = System.nanoTime();
      executor.execute(producer, processor, consumer, NUM_BATCHES, BATCH_SIZE);
      final long endTime = System.nanoTime();
      final float durationMs = (endTime - startTime) / 1000000.0f;
      System.out.printf("%s done in %.3fms%n", executorClass.getSimpleName(), durationMs);
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }

    if (!producer.getSourceStates().equals(consumer.getSourceStates())) {
      System.out.println("Final producer state: " + producer.getSourceStates());
      System.out.println("Final consumer state: " + consumer.getSourceStates());
      throw new IllegalStateException("Final producer and consumer states are not equal!");
    }
  }
}
