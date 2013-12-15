package kafka.log;

import com.google.common.base.Predicate;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import kafka.common.TopicAndPartition;
import kafka.server.OffsetCheckpoint;
import kafka.utils.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class LogCleaner {
    /**
     * The cleaner is responsible for removing obsolete records from logs which have the dedupe retention strategy.
     * A message with key K and offset O is obsolete if there exists a message with key K and offset O' such that O < O'.
     * <p/>
     * Each log can be thought of being split into two sections of segments: a "clean" section which has previously been cleaned followed by a
     * "dirty" section that has not yet been cleaned. The active log segment is always excluded from cleaning.
     * <p/>
     * The cleaning is carried out by a pool of background threads. Each thread chooses the dirtiest log that has the "dedupe" retention policy
     * and cleans that. The dirtiness of the log is guessed by taking the ratio of bytes in the dirty section of the log to the total bytes in the log.
     * <p/>
     * To clean a log the cleaner first builds a mapping of key=>last_offset for the dirty section of the log. See kafka.log.OffsetMap for details of
     * the implementation of the mapping.
     * <p/>
     * Once the key=>offset map is built, the log is cleaned by recopying each log segment but omitting any key that appears in the offset map with a
     * higher offset than what is found in the segment (i.e. messages with a key that appears in the dirty section of the log).
     * <p/>
     * To avoid segments shrinking to very small sizes with repeated cleanings we implement a rule by which if we will merge successive segments when
     * doing a cleaning if their log and index size are less than the maximum log and index size prior to the clean beginning.
     * <p/>
     * Cleaned segments are swapped into the log as they become available.
     * <p/>
     * One nuance that the cleaner must handle is log truncation. If a log is truncated while it is being cleaned the cleaning of that log is aborted.
     * <p/>
     * Messages with null payload are treated as deletes for the purpose of log compaction. This means that they receive special treatment by the cleaner.
     * The cleaner will only retain delete records for a period of time to avoid accumulating space indefinitely. This period of time is configurable on a per-topic
     * basis and is measured from the time the segment enters the clean portion of the log (at which point any prior message with that key has been removed).
     * Delete markers in the clean section of the log that are older than this time will not be retained when log segments are being recopied as part of cleaning.
     *
     * @param config  Configuration parameters for the cleaner
     * @param logDirs The directories where offset checkpoints reside
     * @param logs    The pool of logs
     * @param time    A way to control the passage of time
     */
    public LogCleaner(final CleanerConfig config, List<File> logDirs, Pool<TopicAndPartition, Log> logs, final Time time) {
        this.config = config;
        this.logDirs = logDirs;
        this.logs = logs;
        this.time = time;

        checkpoints = Utils.map(logDirs, new Function1<File, Tuple2<File, OffsetCheckpoint>>() {
            @Override
            public Tuple2<File, OffsetCheckpoint> apply(File dir) {
                return Tuple2.make(dir, new OffsetCheckpoint(new File(dir, "cleaner-offset-checkpoint")));
            }
        });

        throttler = new Throttler(/*desiredRatePerSec =*/ config.maxIoBytesPerSecond,
                /*checkIntervalMs =*/ 300,
                /*throttleDown =*/ true,
                time);

        cleaners = Utils.flatList(0, config.numThreads, new Function1<Integer, CleanerThread>() {
            @Override
            public CleanerThread apply(Integer arg) {
                return new CleanerThread(LogCleaner.this, config, threadId, throttler, time);
            }
        });
    }

    public CleanerConfig config;
    public List<File> logDirs;
    public Pool<TopicAndPartition, Log> logs;
    public Time time;

    /* the offset checkpoints holding the last cleaned point for each log */
    private Map<File, OffsetCheckpoint> checkpoints;

    /* the set of logs currently being cleaned */
    private Set<TopicAndPartition> inProgress = Sets.newHashSet();

    /* a global lock used to control all access to the in-progress set and the offset checkpoints */
    private Object lock = new Object();

    /* a counter for creating unique thread names*/
    private AtomicInteger threadId = new AtomicInteger(0);

    /* a throttle used to limit the I/O of all the cleaner threads to a user-specified maximum rate */
    private Throttler throttler;
    /* the threads */
    private List<CleanerThread> cleaners;

    /* a hook for testing to synchronize on log cleaning completions */
    private Semaphore cleaned = new Semaphore(0);

    Logger logger = LoggerFactory.getLogger(LogCleaner.class);

    /**
     * Start the background cleaning
     */
    public void startup() {
        logger.info("Starting the log cleaner");
        Utils.foreach(cleaners, new Callable1<CleanerThread>() {
            @Override
            public void apply(CleanerThread _) {
                _.start();
            }
        });
    }


    /**
     * Stop the background cleaning
     */
    public void shutdown() {
        logger.info("Shutting down the log cleaner.");
        Utils.foreach(cleaners, new Callable1<CleanerThread>() {
            @Override
            public void apply(CleanerThread _) {
                _.interrupt();
            }
        });
        Utils.foreach(cleaners, new Callable1<CleanerThread>() {
            @Override
            public void apply(CleanerThread _) {
                try {
                    _.join();
                } catch (InterruptedException e) {
                    logger.warn(e.getMessage(), e);
                }
            }
        });
    }

    /**
     * For testing, a way to know when work has completed. This method blocks until the
     * cleaner has processed up to the given offset on the specified topic/partition
     */
    public void awaitCleaned(String topic, int part, long offset, long timeout /* = 30000L*/) throws InterruptedException {
        while (!allCleanerCheckpoints().containsKey(new TopicAndPartition(topic, part)))
            cleaned.tryAcquire(timeout, TimeUnit.MILLISECONDS);
    }

    /**
     * @return the position processed for all logs.
     */
    public Map<TopicAndPartition, Long> allCleanerCheckpoints() {
        return Utils.maps(checkpoints.values(), new Function1<OffsetCheckpoint, Map<TopicAndPartition, Long>>() {
            @Override
            public Map<TopicAndPartition, Long> apply(OffsetCheckpoint _) {
                return _.read();
            }
        });
    }

    /**
     * Choose the log to clean next and add it to the in-progress set. We recompute this
     * every time off the full set of logs to allow logs to be dynamically added to the pool of logs
     * the log manager maintains.
     */
    public LogToClean grabFilthiestLog() {
        synchronized (lock) {
            final Map<TopicAndPartition, Long> lastClean = allCleanerCheckpoints();
            List<LogToClean> cleanableLogs = Utils.mapList(
                    Utils.filter(logs, new Predicate<Map.Entry<TopicAndPartition, Log>>() {
                        @Override
                        public boolean apply(Map.Entry<TopicAndPartition, Log> l) {
                            return l.getValue().config.dedupe  // skip any logs marked for delete rather than dedupe
                                    && !inProgress.contains(l.getKey());   // skip any logs already in-progress
                        }
                    }), new Function1<Map.Entry<TopicAndPartition, Log>, LogToClean>() {
                @Override
                public LogToClean apply(Map.Entry<TopicAndPartition, Log> l) {
                    return new LogToClean(l.getKey(), l.getValue(), Utils.getOrElse(lastClean, l.getKey(), 0L));
                }
            });

            List<LogToClean> dirtyLogs = Utils.filter(cleanableLogs, new Predicate<LogToClean>() {
                @Override
                public boolean apply(LogToClean l) {
                    return l.totalBytes() > 0  // must have some bytes
                            && l.cleanableRatio > l.log.config.minCleanableRatio; // and must meet the minimum threshold for dirty byte ratio
                }
            });

            if (dirtyLogs.isEmpty()) {
                return null;
            } else {
                LogToClean filthiest = Utils.max(dirtyLogs);
                inProgress.add(filthiest.topicPartition);
                return filthiest;
            }
        }
    }

    /**
     * Save out the endOffset and remove the given log from the in-progress set.
     */
    public void doneCleaning(TopicAndPartition topicAndPartition, File dataDir, long endOffset) {
        synchronized (lock) {
            OffsetCheckpoint checkpoint = checkpoints.get(dataDir);
            Map<TopicAndPartition, Long> offsets = Maps.newHashMap();
            offsets.putAll(checkpoint.read());
            offsets.put(topicAndPartition, endOffset);
            checkpoint.write(offsets);
            inProgress.remove(topicAndPartition);
        }
        cleaned.release();
    }

}
