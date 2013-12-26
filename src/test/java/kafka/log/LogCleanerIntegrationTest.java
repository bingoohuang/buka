package kafka.log;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import kafka.common.TopicAndPartition;
import kafka.message.MessageAndOffset;
import kafka.utils.MockTime;
import kafka.utils.Pool;
import kafka.utils.TestUtils;
import kafka.utils.Utils;
import org.junit.After;
import org.junit.Test;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * This is an integration test that tests the fully integrated log cleaner
 */
public class LogCleanerIntegrationTest {
    MockTime time = new MockTime();
    int segmentSize = 100;
    int deleteDelay = 1000;
    String logName = "log";
    File logDir = TestUtils.tempDir();
    int counter = 0;
    List<TopicAndPartition> topics = Lists.newArrayList(new TopicAndPartition("log", 0),
            new TopicAndPartition("log", 1), new TopicAndPartition("log", 2));

    @Test
    public void cleanerTest() throws InterruptedException {
        LogCleaner cleaner = makeCleaner(/*parts = */3);
        Log log = cleaner.logs.get(topics.get(0));

        Map<Integer, Integer> appends = writeDups(/*numKeys = */100,/* numDups = */3, log);
        long startSize = log.size();
        cleaner.startup();

        long lastCleaned = log.activeSegment().baseOffset;
        // wait until we clean up to base_offset of active segment - minDirtyMessages
        cleaner.awaitCleaned("log", 0, lastCleaned);

        Map<Integer, Integer> read = readFromLog(log);
        assertEquals("Contents of the map shouldn't change.", appends, read);
        assertTrue(startSize > log.size());

        // write some more stuff and validate again
        HashMap<Integer, Integer> appends2 = Maps.newHashMap(appends);
        appends2.putAll(writeDups(/*numKeys = */100, /*numDups =*/ 3, log));
        long lastCleaned2 = log.activeSegment().baseOffset;
        cleaner.awaitCleaned("log", 0, lastCleaned2);
        Map<Integer, Integer> read2 = readFromLog(log);
        assertEquals("Contents of the map shouldn't change.", appends2, read2);

        cleaner.shutdown();
    }

    public Map<Integer, Integer> readFromLog(Log log) {
        Map<Integer, Integer> map = Maps.newHashMap();
        for (LogSegment segment : log.logSegments()) {
            for (MessageAndOffset message : segment.log) {
                int key = Integer.parseInt(Utils.readString(message.message.key()));
                int value = Integer.parseInt(Utils.readString(message.message.payload()));
                map.put(key, value);
            }
        }

        return map;
    }

    public Map<Integer, Integer> writeDups(int numKeys, int numDups, Log log) {
        Map<Integer, Integer> map = Maps.newHashMap();
        for (int dup = 0; dup < numDups; ++dup) {
            for (int key = 0; key < numKeys; ++key) {
                int count = counter;
                log.append(TestUtils.singleMessageSet(/*payload = */(counter + "").getBytes(), /*key = */(key + "").getBytes()), /*assignOffsets = */true);
                counter += 1;
                map.put(key, count);
            }
        }

        return map;
    }

    @After
    public void teardown() {
        Utils.rm(logDir);
    }

    /* create a cleaner instance and logs with the given parameters */
    public LogCleaner makeCleaner(int parts) {
        return makeCleaner(parts, 0, 1, "dedupe", new HashMap<String, String>());
    }

    public LogCleaner makeCleaner(int parts,
                                  int minDirtyMessages /*= 0*/,
                                  int numThreads /* = 1*/,
                                  String defaultPolicy/*:  "dedupe"*/,
                                  Map<String, String> policyOverrides /* Map()*/) {

        // create partitions and add them to the pool
        Pool<TopicAndPartition, Log> logs = new Pool<TopicAndPartition, Log>();
        for (int i = 0; i < parts; ++i) {
            File dir = new File(logDir, "log-" + i);
            dir.mkdirs();
            LogConfig logConfig = new LogConfig();
            logConfig.segmentSize = segmentSize;
            logConfig.maxIndexSize = 100 * 1024;
            logConfig.fileDeleteDelayMs = deleteDelay;
            logConfig.dedupe = true;
            Log log = new Log(/*dir = */dir,
                    logConfig,
                    /*recoveryPoint =*/ 0L,
                    time.scheduler,
                    time);
            logs.put(new TopicAndPartition("log", i), log);
        }

        CleanerConfig cleanerConfig = new CleanerConfig();
        cleanerConfig.numThreads = numThreads;
        return new LogCleaner(cleanerConfig,
                /*logDirs = */Lists.newArrayList(logDir),
                logs,
                time);
    }
}
