package us.byteb.playground.threadparty.executor;

import us.byteb.playground.threadparty.Message;
import us.byteb.playground.threadparty.MessageConsumer;
import us.byteb.playground.threadparty.MessageProcessor;
import us.byteb.playground.threadparty.MessageProducer;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

public class LockingPoolExecutor implements Executor {

    public static final int NUM_WORKER_THREADS = 32;
    public static final int POLL_TIMEOUT_MS = 10;
    final AtomicBoolean stopExecution = new AtomicBoolean(false);
    final CountDownLatch finishedCountdown = new CountDownLatch(NUM_WORKER_THREADS);
    final List<Message> queue = Collections.synchronizedList(new LinkedList<>());
    final Set<Integer> lockedSources = ConcurrentHashMap.newKeySet();
    private final ReentrantLock lock = new ReentrantLock();

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
                    final Message message = getNextMessage();
                    if (message != null) {
                        consumer.consume(message, processor::process);
                        lockedSources.removeIf(s -> s == message.getSource());
                    } else {
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
                    }
                }
            }, String.format("%s-Worker-%02d", LockingPoolExecutor.class.getSimpleName(), i));
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

    private Message getNextMessage() {
        lock.lock();
        Message message = null;
        for (int i = 0; i < queue.size(); i++) {
            final Message msg = queue.get(i);
            if (!lockedSources.contains(msg.getSource())) {
                queue.remove(i);
                lockedSources.add(msg.getSource());
                message = msg;
                break;
            }
        }

        lock.unlock();
        return message;
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
