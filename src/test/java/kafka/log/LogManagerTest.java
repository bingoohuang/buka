package kafka.log;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import kafka.common.KafkaException;
import kafka.common.OffsetOutOfRangeException;
import kafka.common.TopicAndPartition;
import kafka.message.ByteBufferMessageSet;
import kafka.server.OffsetCheckpoint;
import kafka.utils.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.UnsupportedEncodingException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

public class LogManagerTest {

    MockTime time = new MockTime();
    int maxRollInterval = 100;
    int maxLogAgeMs = 10 * 60 * 60 * 1000;
    LogConfig logConfig = new LogConfig();

    {
        logConfig.segmentSize = 1024;
        logConfig.maxIndexSize = 4096;
        logConfig.retentionMs = maxLogAgeMs;
    }

    File logDir = null;
    LogManager logManager = null;
    String name = "kafka";
    long veryLargeLogFlushInterval = 10000000L;
    CleanerConfig cleanerConfig = new CleanerConfig();

    {
        cleanerConfig.enableCleaner = false;
    }

    @Before
    public void setUp() {
        logDir = TestUtils.tempDir();
        logManager = new LogManager(/*logDirs = Array*/Lists.newArrayList(logDir),
                /*topicConfigs = Map()*/ Maps.<String, LogConfig>newHashMap(),
                /*defaultConfig = */logConfig,
                /*cleanerConfig = */cleanerConfig,
                /*flushCheckMs = */1000L,
                /*flushCheckpointMs = */100000L,
                /*retentionCheckMs = */1000L,
                time.scheduler,
                time);
        logManager.startup();
        logDir = logManager.logDirs.get(0);
    }

    @After
    public void tearDown() {
        if (logManager != null) logManager.shutdown();
        Utils.rm(logDir);
        Utils.foreach(logManager.logDirs, new Callable1<File>() {
            @Override
            public void apply(File _) {
                Utils.rm(_);
            }
        });
    }

    /**
     * Test that getOrCreateLog on a non-existent log creates a new log and that we can append to the new log.
     */
    @Test
    public void testCreateLog() {
        Log log = logManager.createLog(new TopicAndPartition(name, 0), logConfig);
        File logFile = new File(logDir, name + "-0");
        assertTrue(logFile.exists());
        log.append(TestUtils.singleMessageSet("test".getBytes()));
    }

    /**
     * Test that get on a non-existent returns None and no log is created.
     */
    @Test
    public void testGetNonExistentLog() {
        Log log = logManager.getLog(new TopicAndPartition(name, 0));
        assertEquals("No log should be found.", null, log);
        File logFile = new File(logDir, name + "-0");
        assertTrue(!logFile.exists());
    }

    /**
     * Test time-based log cleanup. First append messages, then set the time into the future and run cleanup.
     */
    @Test
    public void testCleanupExpiredSegments() {
        Log log = logManager.createLog(new TopicAndPartition(name, 0), logConfig);
        long offset = 0L;
        for (int i = 0; i < 200; ++i) {
            ByteBufferMessageSet set = TestUtils.singleMessageSet("test".getBytes());
            LogAppendInfo info = log.append(set);
            offset = info.lastOffset;
        }
        assertTrue("There should be more than one segment now.", log.numberOfSegments() > 1);

        Utils.foreach(log.logSegments(), new Callable1<LogSegment>() {
            @Override
            public void apply(LogSegment _) {
                _.log.file.setLastModified(time.milliseconds());
            }
        });

        time.sleep(maxLogAgeMs + 1);
        assertEquals("Now there should only be only one segment in the index.", 1, log.numberOfSegments());
        time.sleep(log.config.fileDeleteDelayMs + 1);
        assertEquals("Files should have been deleted", log.numberOfSegments() * 2, log.dir.list().length);
        assertEquals("Should get empty fetch off new log.", 0, log.read(offset + 1, 1024).sizeInBytes());

        try {
            log.read(0, 1024);
            fail("Should get exception from fetching earlier.");
        } catch (OffsetOutOfRangeException e) {
            // "This is good."
        }
        // log should still be appendable
        log.append(TestUtils.singleMessageSet("test".getBytes()));
    }

    /**
     * Test size-based cleanup. Append messages, then run cleanup and check that segments are deleted.
     */
    @Test
    public void testCleanupSegmentsToMaintainSize() throws UnsupportedEncodingException {
        int setSize = TestUtils.singleMessageSet("test".getBytes("UTF-8")).sizeInBytes();
        logManager.shutdown();

        LogConfig config = logConfig.clone();

        config.segmentSize = 10 * (setSize - 1);
        config.retentionSize = 5L * 10L * setSize + 10L;
        logManager = new LogManager(Lists.newArrayList(logDir), Maps.<String, LogConfig>newHashMap(), config,
                cleanerConfig, 1000L, 100000L, 1000L, time.scheduler, time);
        logManager.startup();

        // create a log
        Log log = logManager.createLog(new TopicAndPartition(name, 0), config);
        long offset = 0L;

        // add a bunch of messages that should be larger than the retentionSize
        int numMessages = 200;
        for (int i = 0; i < numMessages; ++i) {
            ByteBufferMessageSet set = TestUtils.singleMessageSet("test".getBytes("UTF-8"));
            LogAppendInfo info = log.append(set);
            offset = info.firstOffset;
        }

        assertEquals("Check we have the expected number of segments.",
                numMessages * setSize / config.segmentSize, log.numberOfSegments());

        // this cleanup shouldn't find any expired segments but should delete some to reduce size
        // logManager.cleanupLogs();
        time.sleep(logManager.InitialTaskDelayMs);
        assertEquals("Now there should be exactly 6 segments", 6, log.numberOfSegments());
        time.sleep(log.config.fileDeleteDelayMs + 1);
        assertEquals("Files should have been deleted", log.numberOfSegments() * 2, log.dir.list().length);
        assertEquals("Should get empty fetch off new log.", 0, log.read(offset + 1, 1024).sizeInBytes());
        try {
            log.read(0, 1024);
            fail("Should get exception from fetching earlier.");
        } catch (OffsetOutOfRangeException e) {
            // "This is good."
        }
        // log should still be appendable
        log.append(TestUtils.singleMessageSet("test".getBytes()));
    }

    /**
     * Test that flush is invoked by the background scheduler thread.
     */
    @Test
    public void testTimeBasedFlush() {
        logManager.shutdown();
        LogConfig config = logConfig.clone();
        config.flushMs = 1000;
        logManager = new LogManager(Lists.newArrayList(logDir), Maps.<String, LogConfig>newHashMap(),
                config, cleanerConfig, 1000L, 10000L, 1000L, time.scheduler, time);
        logManager.startup();
        Log log = logManager.createLog(new TopicAndPartition(name, 0), config);
        long lastFlush = log.lastFlushTime();
        for (int i = 0; i < 200; ++i) {
            ByteBufferMessageSet set = TestUtils.singleMessageSet("test".getBytes());
            log.append(set);
        }
        time.sleep(logManager.InitialTaskDelayMs);
        assertTrue("Time based flush should have been triggered triggered", lastFlush != log.lastFlushTime());
    }

    /**
     * Test that new logs that are created are assigned to the least loaded log directory
     */
    @Test
    public void testLeastLoadedAssignment() {
        // create a log manager with multiple data directories
        List<File> dirs = Lists.newArrayList(TestUtils.tempDir(),
                TestUtils.tempDir(),
                TestUtils.tempDir());
        logManager.shutdown();
        logManager = new LogManager(dirs, Maps.<String, LogConfig>newHashMap(),
                logConfig, cleanerConfig, 1000L, 10000L, 1000L, time.scheduler, time);

        // verify that logs are always assigned to the least loaded partition
        for (int partition = 0; partition < 20; ++partition) {
            logManager.createLog(new TopicAndPartition("test", partition), logConfig);
            assertEquals("We should have created the right number of logs", partition + 1, Iterables.size(logManager.allLogs()));
            List<Long> counts = Utils.mapList(Utils.groupBy(logManager.allLogs(), new Function1<Log, Tuple2<String, Log>>() {
                @Override
                public Tuple2<String, Log> apply(Log _) {
                    return Tuple2.make(_.dir.getParent(), _);
                }
            }).values(), new Function1<Log, Long>() {
                @Override
                public Long apply(Log arg) {
                    return arg.size();
                }
            });

            Collections.sort(counts);
            // val counts = logManager.allLogs.groupBy(_.dir.getParent).values.map(_.size)
            assertTrue("Load should balance evenly", counts.get(counts.size() - 1) <= counts.get(0) + 1);
        }
    }

    /**
     * Test that it is not possible to open two log managers using the same data directory
     */
    @Test
    public void testTwoLogManagersUsingSameDirFails() {
        try {
            new LogManager(Lists.newArrayList(logDir), Maps.<String, LogConfig>newHashMap(),
                    logConfig, cleanerConfig, 1000L, 10000L, 1000L, time.scheduler, time);
            fail("Should not be able to create a second log manager instance with the same data directory");
        } catch (KafkaException e) {
            // this is good
        }
    }

    /**
     * Test that recovery points are correctly written out to disk
     */
    @Test
    public void testCheckpointRecoveryPoints() {
        TopicAndPartition topicA = new TopicAndPartition("test-a", 1);
        TopicAndPartition topicB = new TopicAndPartition("test-b", 1);
        Log logA = this.logManager.createLog(topicA, logConfig);
        Log logB = this.logManager.createLog(topicB, logConfig);
        for (int i = 0; i < 50; ++i)
            logA.append(TestUtils.singleMessageSet("test".getBytes()));
        for (int i = 0; i < 100; ++i)
            logB.append(TestUtils.singleMessageSet("test".getBytes()));
        logA.flush();
        logB.flush();
        logManager.checkpointRecoveryPointOffsets();
        Map<TopicAndPartition, Long> checkpoints = new OffsetCheckpoint(new File(logDir, logManager.RecoveryPointCheckpointFile)).read();
        assertEquals("Recovery point should equal checkpoint", (long) checkpoints.get(topicA), logA.recoveryPoint);
        assertEquals("Recovery point should equal checkpoint", (long) checkpoints.get(topicB), logB.recoveryPoint);
    }
}
