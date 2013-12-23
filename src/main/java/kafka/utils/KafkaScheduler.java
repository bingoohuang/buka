package kafka.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class KafkaScheduler extends Scheduler {
    public final int threads;
    public final String threadNamePrefix /*= "kafka-scheduler-"*/;
    public final boolean daemon/* = true*/;

    /**
     * A scheduler based on java.util.concurrent.ScheduledThreadPoolExecutor
     * <p/>
     * It has a pool of kafka-scheduler- threads that do the actual work.
     *
     * @param threads          The number of threads in the thread pool
     * @param threadNamePrefix The name to use for scheduler threads. This prefix will have a number appended to it.
     * @param daemon           If true the scheduler threads will be "daemon" threads and will not block jvm shutdown.
     */
    public KafkaScheduler(int threads, String threadNamePrefix, boolean daemon) {
        this.threads = threads;
        this.threadNamePrefix = threadNamePrefix;
        this.daemon = daemon;
    }

    volatile private ScheduledThreadPoolExecutor executor = null;
    private AtomicInteger schedulerThreadId = new AtomicInteger(0);

    Logger logger = LoggerFactory.getLogger(KafkaScheduler.class);

    @Override
    public void startup() {
        logger.debug("Initializing task scheduler.");
        synchronized (this) {
            if (executor != null)
                throw new IllegalStateException("This scheduler has already been started!");
            executor = new ScheduledThreadPoolExecutor(threads);
            executor.setContinueExistingPeriodicTasksAfterShutdownPolicy(false);
            executor.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
            executor.setThreadFactory(new ThreadFactory() {
                @Override
                public Thread newThread(Runnable r) {
                    return Utils.newThread(threadNamePrefix + schedulerThreadId.getAndIncrement(), r, daemon);
                }
            });
        }
    }

    @Override
    public void shutdown() {
        logger.debug("Shutting down task scheduler.");
        ensureStarted();
        executor.shutdown();
        try {
            executor.awaitTermination(1, TimeUnit.DAYS);
        } catch (InterruptedException e) {
            logger.warn("shutdown", e);
        }
        this.executor = null;
    }

    @Override
    public void schedule(final String name, final Runnable fun, long delay, long period, TimeUnit unit) {
        logger.debug("Scheduling task {} with initial delay {} ms and period {} ms.",
                name, TimeUnit.MILLISECONDS.convert(delay, unit), TimeUnit.MILLISECONDS.convert(period, unit));
        ensureStarted();
        Runnable runnable = new Runnable() {
            public void run() {
                try {
                    logger.trace("Begining execution of scheduled task '{}'.", name);
                    fun.run();
                } catch (Throwable e) {
                    logger.error("Uncaught exception in scheduled task '{}'", name, e);
                } finally {
                    logger.trace("Completed execution of scheduled task '{}'.", name);
                }
            }
        };

        if (period >= 0)
            executor.scheduleAtFixedRate(runnable, delay, period, unit);
        else
            executor.schedule(runnable, delay, unit);
    }

    private void ensureStarted() {
        if (executor == null)
            throw new IllegalStateException("Kafka scheduler has not been started");
    }
}
