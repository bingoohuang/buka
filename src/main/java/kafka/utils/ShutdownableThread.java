package kafka.utils;

import kafka.common.KafkaException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

public abstract class ShutdownableThread extends Thread {
    public String name;
    public boolean isInterruptible;

    protected ShutdownableThread(String name) {
        this(name, true);
    }

    protected ShutdownableThread(String name, boolean isInterruptible) {
        super(name);
        this.name = name;
        this.isInterruptible = isInterruptible;

        this.setDaemon(false);
        logger = LoggerFactory.getLogger(ShutdownableThread.class + "[" + name + "], ");
    }

    Logger logger;
    public AtomicBoolean isRunning = new AtomicBoolean(true);
    private CountDownLatch shutdownLatch = new CountDownLatch(1);


    public void shutdown() {
        logger.info("Shutting down");
        isRunning.set(false);
        if (isInterruptible)
            interrupt();
        awaitShutdown();
        logger.info("Shutdown completed");
    }

    /**
     * After calling shutdown(), use this API to wait until the shutdown is complete
     */
    public void awaitShutdown() {
        try {
            shutdownLatch.await();
        } catch (InterruptedException e) {
            throw new KafkaException(e);
        }
    }

    public abstract void doWork();

    @Override
    public void run() {
        logger.info("Starting ");
        try {
            while (isRunning.get()) {
                doWork();
            }
        } catch (Throwable e) {
            if (isRunning.get())
                logger.error("Error due to ", e);
        }
        shutdownLatch.countDown();
        logger.info("Stopped ");
    }
}
