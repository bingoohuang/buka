package kafka.log;


import kafka.common.OptimisticLockFailureException;
import kafka.utils.Throttler;
import kafka.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * The cleaner threads do the actual log cleaning. Each thread processes does its cleaning repeatedly by
 * choosing the dirtiest log, cleaning it, and then swapping in the cleaned segments.
 */
public class CleanerThread extends Thread {
    LogCleaner logCleaner;
    CleanerConfig config;
    AtomicInteger threadId;
    Throttler throttler;
    Time time;

    public CleanerThread(LogCleaner logCleaner, CleanerConfig config, AtomicInteger threadId, Throttler throttler, Time time) {
        this.logCleaner = logCleaner;
        this.config = config;
        this.threadId = threadId;
        this.throttler = throttler;
        this.time = time;

        init();
    }

    public Cleaner cleaner;
    Logger logger = LoggerFactory.getLogger(CleanerThread.class);

    private void init() {
        if (config.dedupeBufferSize / config.numThreads > Integer.MAX_VALUE)
            logger.warn("Cannot use more than 2G of cleaner buffer space per cleaner thread, ignoring excess buffer space...");
        cleaner = new Cleaner(threadId.getAndIncrement(),
                new SkimpyOffsetMap((int) Math.min(config.dedupeBufferSize / config.numThreads, Integer.MAX_VALUE), config.hashAlgorithm),
                /*ioBufferSize = */config.ioBufferSize / config.numThreads / 2,
                /*maxIoBufferSize = */config.maxMessageSize,
                /*dupBufferLoadFactor = */config.dedupeBufferLoadFactor,
                /*throttler = */throttler,
                /*time = */time);

        setName("kafka-log-cleaner-thread-" + cleaner.id);
        setDaemon(false);
    }

    /**
     * The main loop for the cleaner thread
     */
    @Override
    public void run() {
        logger.info("Starting cleaner thread {}...", cleaner.id);
        try {
            while (!isInterrupted()) {
                cleanOrSleep();
            }
        } catch (InterruptedException e) {
            // all done
        } catch (Exception e) {
            logger.error("Error in cleaner thread {}:", cleaner.id, e);
        }
        logger.info("Shutting down cleaner thread {}.", cleaner.id);
    }

    /**
     * Clean a log if there is a dirty log available, otherwise sleep for a bit
     */
    private void cleanOrSleep() throws InterruptedException {
        LogToClean cleanable = logCleaner.grabFilthiestLog();
        if (cleanable == null) {
            // there are no cleanable logs, sleep a while
            time.sleep(config.backOffMs);
        } else {
            // there's a log, clean it
            long endOffset = cleanable.firstDirtyOffset;
            try {
                endOffset = cleaner.clean(cleanable);
                logStats(cleaner.id, cleanable.log.name(), cleanable.firstDirtyOffset, endOffset, cleaner.stats);
            } catch (OptimisticLockFailureException e) {
                logger.info("Cleaning of log was aborted due to colliding truncate operation.");

            } finally {
                logCleaner.doneCleaning(cleanable.topicPartition, cleanable.log.dir.getParentFile(), endOffset);
            }
        }
    }

    public double mb(double bytes) {
        return bytes / (1024 * 1024);
    }

    /**
     * Log out statistics on a single run of the cleaner.
     */
    public void logStats(int id, String name, long from, long to, CleanerStats stats) {
        // def mb(bytes: Double) = bytes / (1024*1024);
        String message =
                String.format("%n\tLog cleaner %d cleaned log %s (dirty section = [%d, %d])%n", id, name, from, to) +
                        String.format("\t%,.1f MB of log processed in %,.1f seconds (%,.1f MB/sec).%n", mb(stats.bytesRead),
                                stats.elapsedSecs(),
                                mb(stats.bytesRead / stats.elapsedSecs())) +
                        String.format("\tIndexed %,.1f MB in %.1f seconds (%,.1f Mb/sec, %.1f%% of total time)%n", mb(stats.mapBytesRead),
                                stats.elapsedIndexSecs(),
                                mb(stats.mapBytesRead) / stats.elapsedIndexSecs(),
                                ((double) 100 * stats.elapsedIndexSecs()) / stats.elapsedSecs()) +
                        String.format("\tCleaned %,.1f MB in %.1f seconds (%,.1f Mb/sec, %.1f%% of total time)%n", mb(stats.bytesRead),
                                stats.elapsedSecs() - stats.elapsedIndexSecs(),
                                mb(stats.bytesRead) / (stats.elapsedSecs() - stats.elapsedIndexSecs()), 100 * ((double) (stats.elapsedSecs() - stats.elapsedIndexSecs()) / stats.elapsedSecs())) +
                        String.format("\tStart size: %,.1f MB (%,d messages)%n", mb(stats.bytesRead), stats.messagesRead) +
                        String.format("\tEnd size: %,.1f MB (%,d messages)%n", mb(stats.bytesWritten), stats.messagesWritten) +
                        String.format("\t%.1f%% size reduction (%.1f%% fewer messages)%n", 100.0 * (1.0 - ((double) stats.bytesWritten) / stats.bytesRead),
                                100.0 * (1.0 - ((double) stats.messagesWritten) / stats.messagesRead));
        logger.info(message);
    }
}
