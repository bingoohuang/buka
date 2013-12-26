package kafka.network;

import kafka.common.KafkaException;

import java.io.IOException;
import java.nio.channels.Selector;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A base class with some helper variables and methods
 */
public abstract class AbstractServerThread implements Runnable {
    protected Selector selector;
    private CountDownLatch startupLatch = new CountDownLatch(1);
    private CountDownLatch shutdownLatch = new CountDownLatch(1);
    private AtomicBoolean alive = new AtomicBoolean(false);

    public AbstractServerThread() {
        try {
            selector = Selector.open();
        } catch (IOException e) {
            throw new KafkaException(e);
        }
    }


    /**
     * Initiates a graceful shutdown by signaling to stop and waiting for the shutdown to complete
     */
    public void shutdown() {
        alive.set(false);
        selector.wakeup();
        try {
            shutdownLatch.await();
        } catch (InterruptedException e) {
            throw new KafkaException(e);
        }
    }

    /**
     * Wait for the thread to completely start up
     */
    public void awaitStartup() {
        try {
            startupLatch.await();
        } catch (InterruptedException e) {
            throw new KafkaException(e);
        }
    }

    /**
     * Record that the thread startup is complete
     */
    protected void startupComplete() {
        alive.set(true);
        startupLatch.countDown();
    }

    /**
     * Record that the thread shutdown is complete
     */
    protected void shutdownComplete() {
        shutdownLatch.countDown();
    }

    /**
     * Is the server still running?
     */
    protected boolean isRunning() {
        return alive.get();
    }

    /**
     * Wakeup the thread for selection.
     */
    public Selector wakeup() {
        return selector.wakeup();
    }

}
