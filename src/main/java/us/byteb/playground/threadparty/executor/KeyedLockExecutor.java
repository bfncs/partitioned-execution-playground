package us.byteb.playground.threadparty.executor;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.function.Function;
import us.byteb.playground.threadparty.Message;
import us.byteb.playground.threadparty.MessageConsumer;
import us.byteb.playground.threadparty.MessageProcessor;
import us.byteb.playground.threadparty.MessageProducer;

public class KeyedLockExecutor implements Executor {

  public static final int NUM_WORKER_THREADS = 32;
  public static final int POLL_TIMEOUT_MS = 10;

  private final KeyedLockQueue<Message, Integer> queue = new KeyedLockQueue<>(Message::getSource);
  private final AtomicBoolean stopExecution = new AtomicBoolean(false);
  private final CountDownLatch finishedCountdown = new CountDownLatch(NUM_WORKER_THREADS);

  @Override
  public void execute(
      final MessageProducer producer,
      final MessageProcessor processor,
      final MessageConsumer consumer,
      final int totalMessages,
      final int batchSize) {

    for (int i = 0; i < NUM_WORKER_THREADS; i++) {
      final Thread worker = new Thread(() -> {
        while (true) {
          final boolean didProcess = queue
              .processNext(message -> consumer.consume(message, processor::process));
          if (!didProcess) {
            if (stopExecution.get()) {
              finishedCountdown.countDown();
              return;
            } else {
              try {
                // TODO: implement without busy waiting
                Thread.sleep(POLL_TIMEOUT_MS);
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
              }
            }
          }
        }
      }, String.format("%s-Worker-%02d", KeyedLockExecutor.class.getSimpleName(), i));
      worker.start();
    }

    int messagesReceived = 0;
    while (messagesReceived < totalMessages) {
      final List<Message> messages = producer.nextBatch(batchSize);
      messagesReceived += messages.size();
      queue.addAll(messages);
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
  public void close() {
    stopExecutionGracefully();
  }

  private static class KeyedLockQueue<T, K> {

    private final List<T> items = Collections.synchronizedList(new LinkedList<>());
    private final Set<K> lockedKeys = ConcurrentHashMap.newKeySet();
    private final ReentrantLock nextItemLock = new ReentrantLock();
    private final Function<T, K> keyAccessor;

    public KeyedLockQueue(final Function<T, K> keyAccessor) {
      this.keyAccessor = keyAccessor;
    }

    void addAll(Collection<T> items) {
      this.items.addAll(items);
    }

    public boolean processNext(final Consumer<T> processor) {
      final T message = getNextMessage();
      if (message == null) {
        return false;
      }

      try {
        processor.accept(message);
      } catch (Exception e) {
        System.out.printf("Error consuming message (%s): %s%n", message, e);
      } finally {
        lockedKeys.removeIf(s -> s == keyAccessor.apply(message));
      }
      return true;
    }

    private T getNextMessage() {
      nextItemLock.lock();
      final Set<K> lockedSourcesSnapshot = new HashSet<>(lockedKeys);

      T result = null;
      try {
        for (int i = 0; i < items.size(); i++) {
            final T item = items.get(i);
            final K key = keyAccessor.apply(item);
            if (!lockedSourcesSnapshot.contains(key)) {
              items.remove(i);
              lockedKeys.add(key);
              result = item;
              break;
            }
        }
      } catch (IndexOutOfBoundsException e) {
        // desired item was removed, do nothing
      }

      nextItemLock.unlock();
      return result;
    }
  }
}
