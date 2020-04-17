package us.byteb.playground.partexec;

import java.util.Random;

public class MessageProcessor {

    private static final int MAX_DELAY_MS = 10;
    private final Random random = new Random();

    public void process(final Message message) {
        try {
            Thread.sleep(random.nextInt(MAX_DELAY_MS));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
