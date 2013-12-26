package kafka.log;

import com.google.common.base.Predicate;
import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multiset;
import com.google.common.collect.Table;
import kafka.common.KafkaException;
import kafka.common.TopicAndPartition;
import kafka.server.OffsetCheckpoint;
import kafka.utils.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

@ThreadSafe
public class LogManager {
    public List<File> logDirs;
    public Map<String, LogConfig> topicConfigs;
    public LogConfig defaultConfig;
    public CleanerConfig cleanerConfig;
    public Long flushCheckMs;
    public Long flushCheckpointMs;
    public Long retentionCheckMs;
    public Scheduler scheduler;
    private Time time;

    /**
     * The entry point to the kafka log management subsystem. The log manager is responsible for log creation, retrieval, and cleaning.
     * All read and write operations are delegated to the individual log instances.
     * <p/>
     * The log manager maintains logs in one or more directories. New logs are created in the data directory
     * with the fewest logs. No attempt is made to move partitions after the fact or balance based on
     * size or I/O rate.
     * <p/>
     * A background thread handles log retention by periodically truncating excess log segments.
     */
    public LogManager(List<File> logDirs,
                      Map<String, LogConfig> topicConfigs,
                      LogConfig defaultConfig,
                      CleanerConfig cleanerConfig,
                      Long flushCheckMs,
                      Long flushCheckpointMs,
                      Long retentionCheckMs,
                      Scheduler scheduler,
                      Time time) {
        this.logDirs = logDirs;
        this.topicConfigs = topicConfigs;
        this.defaultConfig = defaultConfig;
        this.cleanerConfig = cleanerConfig;
        this.flushCheckMs = flushCheckMs;
        this.flushCheckpointMs = flushCheckpointMs;
        this.retentionCheckMs = retentionCheckMs;
        this.scheduler = scheduler;
        this.time = time;

        createAndValidateLogDirs(logDirs);
        dirLocks = lockLogDirs(logDirs);

        recoveryPointCheckpoints = Utils.map(logDirs, new Function1<File, Tuple2<File, OffsetCheckpoint>>() {
            @Override
            public Tuple2<File, OffsetCheckpoint> apply(File dir) {
                return Tuple2.make(dir, new OffsetCheckpoint(new File(dir, RecoveryPointCheckpointFile)));
            }
        });

        loadLogs(logDirs);
        cleaner = cleanerConfig.enableCleaner ? new LogCleaner(cleanerConfig, logDirs, logs, time) : null;
    }

    public static final String RecoveryPointCheckpointFile = "recovery-point-offset-checkpoint";
    public static final String LockFile = ".lock";
    public int InitialTaskDelayMs = 30 * 1000;
    private Object logCreationLock = new Object();
    private Pool<TopicAndPartition, Log> logs = new Pool<TopicAndPartition, Log>();


    private List<FileLock> dirLocks;
    private Map<File, OffsetCheckpoint> recoveryPointCheckpoints;

    Logger logger = LoggerFactory.getLogger(LogManager.class);

    private LogCleaner cleaner;

    /**
     * Create and check validity of the given directories, specifically:
     * <ol>
     * <li> Ensure that there are no duplicates in the directory list
     * <li> Create each directory if it doesn't exist
     * <li> Check that each path is a readable directory
     * </ol>
     */
    private void createAndValidateLogDirs(List<File> dirs) {
        if (Utils.mapSet(dirs, new Function1<File, String>() {
            @Override
            public String apply(File _) {
                try {
                    return _.getCanonicalPath();
                } catch (IOException e) {
                    throw new KafkaException(e);
                }
            }
        }).size() < dirs.size())
            throw new KafkaException("Duplicate log directory found: " + logDirs);
        for (File dir : dirs) {
            if (!dir.exists()) {
                logger.info("Log directory '{}' not found, creating it.", dir.getAbsolutePath());
                boolean created = dir.mkdirs();
                if (!created)
                    throw new KafkaException("Failed to create data directory " + dir.getAbsolutePath());
            }
            if (!dir.isDirectory() || !dir.canRead())
                throw new KafkaException(dir.getAbsolutePath() + " is not a readable log directory.");
        }
    }

    /**
     * Lock all the given directories
     */
    private List<FileLock> lockLogDirs(List<File> dirs) {
        return Utils.mapList(dirs, new Function1<File, FileLock>() {
            @Override
            public FileLock apply(File dir) {
                FileLock lock = new FileLock(new File(dir, LockFile));
                if (!lock.tryLock())
                    throw new KafkaException("Failed to acquire lock on file .lock in " + lock.file.getParentFile().getAbsolutePath() +
                            ". A Kafka instance in another process or thread is using this directory.");
                return lock;
            }
        });
    }

    /**
     * Recover and load all logs in the given data directories
     */
    private void loadLogs(List<File> dirs) {
        for (File dir : dirs) {
            Map<TopicAndPartition, Long> recoveryPoints = this.recoveryPointCheckpoints.get(dir).read();
      /* load the logs */
            File[] subDirs = dir.listFiles();
            if (subDirs != null) {
                File cleanShutDownFile = new File(dir, Logs.CleanShutdownFile);
                if (cleanShutDownFile.exists())
                    logger.info("Found clean shutdown file. Skipping recovery for all logs in data directory '{}'", dir.getAbsolutePath());
                for (File subDir : subDirs) {
                    if (subDir.isDirectory()) {
                        logger.info("Loading log '" + subDir.getName() + "'");
                        TopicAndPartition topicPartition = parseTopicPartitionName(subDir.getName());
                        LogConfig config = Utils.getOrElse(topicConfigs, topicPartition.topic, defaultConfig);
                        Log log = new Log(subDir,
                                config,
                                Utils.getOrElse(recoveryPoints, topicPartition, 0L),
                                scheduler,
                                time);
                        Log previous = this.logs.put(topicPartition, log);
                        if (previous != null)
                            throw new IllegalArgumentException(String.format("Duplicate log directories found: %s, %s!", log.dir.getAbsolutePath(), previous.dir.getAbsolutePath()));
                    }
                }
                cleanShutDownFile.delete();
            }
        }
    }

    /**
     * Start the background threads to flush logs and do log cleanup
     */
    public void startup() {
    /* Schedule the cleanup task to delete old logs */
        if (scheduler != null) {
            final LogManager lm = this;
            logger.info("Starting log cleanup with a period of {} ms.", retentionCheckMs);
            scheduler.schedule("kafka-log-retention", new Runnable() {
                @Override
                public void run() {
                    lm.cleanupLogs();
                }
            },
                    InitialTaskDelayMs,
                    retentionCheckMs,
                    TimeUnit.MILLISECONDS);
            logger.info("Starting log flusher with a default period of {} ms.", flushCheckMs);
            scheduler.schedule("kafka-log-flusher", new Runnable() {
                @Override
                public void run() {
                    lm.flushDirtyLogs();
                }
            },
                    InitialTaskDelayMs,
                    flushCheckMs,
                    TimeUnit.MILLISECONDS);
            scheduler.schedule("kafka-recovery-point-checkpoint", new Runnable() {
                @Override
                public void run() {
                    lm.checkpointRecoveryPointOffsets();
                }
            },
                    InitialTaskDelayMs,
                    flushCheckpointMs,
                    TimeUnit.MILLISECONDS);
        }
        if (cleanerConfig.enableCleaner)
            cleaner.startup();
    }

    /**
     * Close all the logs
     */
    public void shutdown() {
        logger.debug("Shutting down.");
        try {
            // stop the cleaner first
            if (cleaner != null)
                Utils.swallow(new Runnable() {
                    @Override
                    public void run() {
                        cleaner.shutdown();
                    }
                });
            // flush the logs to ensure latest possible recovery point
            Utils.foreach(allLogs(), new Callable1<Log>() {
                @Override
                public void apply(Log _) {
                    _.flush();
                }
            });
            // close the logs
            Utils.foreach(allLogs(), new Callable1<Log>() {
                @Override
                public void apply(Log _) {
                    _.close();
                }
            });
            // update the last flush point
            checkpointRecoveryPointOffsets();
            // mark that the shutdown was clean by creating the clean shutdown marker file
            Utils.foreach(logDirs, new Callable1<File>() {
                @Override
                public void apply(final File dir) {
                    Utils.swallow(new Runnable() {
                        @Override
                        public void run() {
                            try {
                                new File(dir, Logs.CleanShutdownFile).createNewFile();
                            } catch (IOException e) {
                                throw new KafkaException(e);
                            }
                        }
                    });
                }
            });
        } finally {
            // regardless of whether the close succeeded, we need to unlock the data directories
            Utils.foreach(dirLocks, new Callable1<FileLock>() {
                @Override
                public void apply(FileLock _) {
                    _.destroy();
                }
            });
        }

        logger.debug("Shutdown complete.");
    }

    /**
     * Truncate the partition logs to the specified offsets and checkpoint the recovery point to this offset
     *
     * @param partitionAndOffsets Partition logs that need to be truncated
     */
    public void truncateTo(Map<TopicAndPartition, Long> partitionAndOffsets) {
        Utils.foreach(partitionAndOffsets, new Callable2<TopicAndPartition, Long>() {
            @Override
            public void apply(TopicAndPartition topicAndPartition, Long truncateOffset) {
                Log log = logs.get(topicAndPartition);
                // If the log does not exist, skip it
                if (log != null) {
                    log.truncateTo(truncateOffset);
                }
            }
        });

        checkpointRecoveryPointOffsets();
    }

    /**
     * Write out the current recovery point for all logs to a text file in the log directory
     * to avoid recovering the whole log on startup.
     */
    public void checkpointRecoveryPointOffsets() {
        Table<String, TopicAndPartition, Log> recoveryPointsByDir = Utils.groupBy(this.logsByTopicPartition(), new Function2<TopicAndPartition, Log, String>() {
            @Override
            public String apply(TopicAndPartition _1, Log _2) {
                return _2.dir.getParent();
            }
        });
        for (File dir : logDirs) {
            Map<TopicAndPartition, Log> recoveryPoints = recoveryPointsByDir.row(dir.toString());
            if (recoveryPoints != null)
                this.recoveryPointCheckpoints.get(dir).write(Utils.map(recoveryPoints, new Function2<TopicAndPartition, Log, Tuple2<TopicAndPartition, Long>>() {
                    @Override
                    public Tuple2<TopicAndPartition, Long> apply(TopicAndPartition arg1, Log log) {
                        return Tuple2.make(arg1, log.recoveryPoint);
                    }
                }));
        }
    }

    /**
     * Get the log if it exists, otherwise return None
     */
    public Log getLog(TopicAndPartition topicAndPartition) {
        return logs.get(topicAndPartition);
    }

    /**
     * Create a log for the given topic and the given partition
     * If the log already exists, just return a copy of the existing log
     */
    public Log createLog(TopicAndPartition topicAndPartition, LogConfig config) {
        synchronized (logCreationLock) {
            Log log = logs.get(topicAndPartition);

            // check if the log has already been created in another thread
            if (log != null) return log;

            // if not, create it
            File dataDir = nextLogDir();
            File dir = new File(dataDir, topicAndPartition.topic + "-" + topicAndPartition.partition);
            dir.mkdirs();
            log = new Log(dir,
                    config,
                    /*recoveryPoint = */0L,
                    scheduler,
                    time);
            logs.put(topicAndPartition, log);
            logger.info("Created log for partition [{},{}] in {} with properties {{}}.",
                    topicAndPartition.topic,
                    topicAndPartition.partition,
                    dataDir.getAbsolutePath(),
                    config.toProps());
            return log;
        }
    }

    /**
     * Choose the next directory in which to create a log. Currently this is done
     * by calculating the number of partitions in each directory and then choosing the
     * data directory with the fewest partitions.
     */
    private File nextLogDir() {
        if (logDirs.size() == 1) {
            return logDirs.get(0);
        } else {
            // count the number of logs in each parent directory (including 0 for empty directories
            final Multiset<String> logCount = HashMultiset.create();

            Utils.foreach(allLogs(), new Callable1<Log>() {
                @Override
                public void apply(Log log) {
                    logCount.add(log.dir.getParent(), (int) (log.size() + 1));
                }
            });

            Utils.foreach(logDirs, new Callable1<File>() {
                @Override
                public void apply(File dir) {
                    logCount.add(dir.getPath());
                }
            });

            // choose the directory with the least logs in it
            int minCount = Integer.MAX_VALUE;
            String min = "max";

            for (Multiset.Entry<String> entry : logCount.entrySet()) {
                if (entry.getCount() < minCount) {
                    minCount = entry.getCount();
                    min = entry.getElement();
                }
            }


            return new File(min);
        }
    }

    /**
     * Runs through the log removing segments older than a certain age
     */
    private int cleanupExpiredSegments(final Log log) {
        final long startMs = time.milliseconds();
        String topic = parseTopicPartitionName(log.name()).topic;
        return log.deleteOldSegments(new Predicate<LogSegment>() {
            @Override
            public boolean apply(LogSegment _) {
                return startMs - _.lastModified() > log.config.retentionMs;
            }
        });
    }

    /**
     * Runs through the log removing segments until the size of the log
     * is at least logRetentionSize bytes in size
     */
    private int cleanupSegmentsToMaintainSize(Log log) {
        if (log.config.retentionSize < 0 || log.size() < log.config.retentionSize) return 0;

        final AtomicLong diff = new AtomicLong(log.size() - log.config.retentionSize);

        return log.deleteOldSegments(new Predicate<LogSegment>() {
            @Override
            public boolean apply(LogSegment segment) {
                if (diff.get() - segment.size() >= 0) {
                    diff.addAndGet(-segment.size());
                    return true;
                } else {
                    return false;
                }
            }
        });
    }

    /**
     * Delete any eligible logs. Return the number of segments deleted.
     */
    public void cleanupLogs() {
        logger.debug("Beginning log cleanup...");
        int total = 0;
        long startMs = time.milliseconds();
        for (Log log : allLogs()) {
            if (log.config.dedupe) continue;

            logger.debug("Garbage collecting '{}'", log.name());
            total += cleanupExpiredSegments(log) + cleanupSegmentsToMaintainSize(log);
        }
        logger.debug("Log cleanup completed. " + total + " files deleted in " +
                (time.milliseconds() - startMs) / 1000 + " seconds");
    }

    /**
     * Get all the partition logs
     */
    public Collection<Log> allLogs() {
        return logs.values();
    }

    /**
     * Get a map of TopicAndPartition => Log
     */
    public Map<TopicAndPartition, Log> logsByTopicPartition() {
        return logs.toMap();
    }

    /**
     * Flush any log which has exceeded its flush interval and has unwritten messages.
     */
    private void flushDirtyLogs() {
        logger.debug("Checking for dirty logs to flush...");
        Utils.foreach(logs, new Callable1<Map.Entry<TopicAndPartition, Log>>() {
            @Override
            public void apply(Map.Entry<TopicAndPartition, Log> _) {
                TopicAndPartition topicAndPartition = _.getKey();
                Log log = _.getValue();

                try {
                    long timeSinceLastFlush = time.milliseconds() - log.lastFlushTime();
                    logger.debug("Checking if flush is needed on " + topicAndPartition.topic + " flush interval  " + log.config.flushMs +
                            " last flushed " + log.lastFlushTime() + " time since last flush: " + timeSinceLastFlush);
                    if (timeSinceLastFlush >= log.config.flushMs)
                        log.flush();
                } catch (Throwable e) {
                    logger.error("Error flushing topic " + topicAndPartition.topic, e);
                    if (e instanceof IOException) {
                        logger.error("Halting due to unrecoverable I/O error while flushing logs: " + e.getMessage(), e);
                        System.exit(1);
                    }
                }
            }
        });
    }

    /**
     * Parse the topic and partition out of the directory name of a log
     */
    private TopicAndPartition parseTopicPartitionName(String name) {
        int index = name.lastIndexOf('-');
        return new TopicAndPartition(name.substring(0, index), Integer.parseInt(name.substring(index + 1)));
    }
}
