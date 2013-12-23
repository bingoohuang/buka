package kafka.log;

import com.google.common.base.Charsets;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import kafka.common.OptimisticLockFailureException;
import kafka.common.TopicAndPartition;
import kafka.message.ByteBufferMessageSet;
import kafka.message.Message;
import kafka.message.MessageAndOffset;
import kafka.utils.*;
import org.junit.After;
import org.junit.Test;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.*;

import static junit.framework.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Unit tests for the log cleaning logic
 */
public class CleanerTest {

    File dir = TestUtils.tempDir();
    LogConfig logConfig = new LogConfig();

    {
        logConfig.segmentSize = 1024;
        logConfig.maxIndexSize = 1024;
        logConfig.dedupe = true;
    }

    ;

    MockTime time = new MockTime();
    Throttler throttler = new Throttler(/*desiredRatePerSec =*/ Double.MAX_VALUE, /*checkIntervalMs = */Long.MAX_VALUE, true, time);

    @After
    public void teardown() {
        Utils.rm(dir);
    }

    /**
     * Test simple log cleaning
     */
    @Test
    public void testCleanSegments() throws InterruptedException {
        Cleaner cleaner = makeCleaner(Integer.MAX_VALUE);
        LogConfig clone = logConfig.clone();
        clone.segmentSize = 1024;
        Log log = makeLog(dir, clone);

        // append messages to the log until we have four segments
        while (log.numberOfSegments() < 4)
            log.append(message((int) log.logEndOffset(), (int) log.logEndOffset()));
        List<Integer> keysFound = keysInLog(log);
        assertEquals(Utils.flatList(0L, log.logEndOffset(), new Function1<Long, Integer>() {
            @Override
            public Integer apply(Long arg) {
                return arg.intValue();
            }
        }), keysFound);

        // pretend we have the following keys
        final List<Integer> keys = ImmutableList.of(1, 3, 5, 7, 9);
        final FakeOffsetMap map = new FakeOffsetMap(Integer.MAX_VALUE);
        Utils.foreach(keys, new Callable1<Integer>() {
            @Override
            public void apply(Integer k) {
                map.put(key(k), Long.MAX_VALUE);
            }
        });

        // clean the log
        cleaner.cleanSegments(log, Utils.take(log.logSegments(), 3), map, 0, 0L);
        List<Integer> shouldRemain = Utils.filter(keysInLog(log), new Predicate<Integer>() {
            @Override
            public boolean apply(Integer _) {
                return !keys.contains(_);
            }
        });
        assertEquals(shouldRemain, keysInLog(log));
    }

    @Test
    public void testCleaningWithDeletes() throws InterruptedException {
        Cleaner cleaner = makeCleaner(Integer.MAX_VALUE);
        LogConfig clone = logConfig.clone();
        clone.segmentSize = 1024;
        Log log = makeLog(dir, clone);

        // append messages with the keys 0 through N
        while (log.numberOfSegments() < 2)
            log.append(message((int) log.logEndOffset(), (int) log.logEndOffset()));

        // delete all even keys between 0 and N
        long leo = log.logEndOffset();
        for (int key = 0; key < leo; key += 2)
            log.append(deleteMessage(key));

        // append some new unique keys to pad out to a new active segment
        while (log.numberOfSegments() < 4)
            log.append(message((int) log.logEndOffset(), (int) log.logEndOffset()));

        cleaner.clean(new LogToClean(new TopicAndPartition("test", 0), log, 0));
        final Set<Integer> keys = Sets.newHashSet(keysInLog(log));
        assertTrue("None of the keys we deleted should still exist.",
                Utils.forall(0, (int) leo, 2, new Predicate<Integer>() {

                    @Override
                    public boolean apply(Integer _) {
                        return !keys.contains(_);
                    }
                }));
    }

    /* extract all the keys from a log */
    public List<Integer> keysInLog(Log log) {
        return Utils.mapLists(log.logSegments(), new Function1<LogSegment, Collection<Integer>>() {
            @Override
            public Collection<Integer> apply(LogSegment s) {
                return Utils.mapList(Utils.filter(s.log, new Predicate<MessageAndOffset>() {
                    @Override
                    public boolean apply(MessageAndOffset _) {
                        return !_.message.isNull();
                    }
                }), new Function1<MessageAndOffset, Integer>() {
                    @Override
                    public Integer apply(MessageAndOffset m) {
                        return Integer.parseInt(Utils.readString(m.message.key()));
                    }
                });
            }
        });
    }


    /**
     * Test that a truncation during cleaning throws an OptimisticLockFailureException
     */
    @Test
    public void testCleanSegmentsWithTruncation() throws InterruptedException {
        Cleaner cleaner = makeCleaner(Integer.MAX_VALUE);
        LogConfig clone = logConfig.clone();
        clone.segmentSize = 1024;
        Log log = makeLog(dir, clone);

        // append messages to the log until we have four segments
        while (log.numberOfSegments() < 2)
            log.append(message((int) log.logEndOffset(), (int) log.logEndOffset()));

        log.truncateTo(log.logEndOffset() - 2);
        List<Integer> keys = keysInLog(log);
        final FakeOffsetMap map = new FakeOffsetMap(Integer.MAX_VALUE);
        Utils.foreach(keys, new Callable1<Integer>() {
            @Override
            public void apply(Integer k) {
                map.put(key(k), Long.MAX_VALUE);
            }
        });

        try {
            cleaner.cleanSegments(log, Utils.take(log.logSegments(), 3), map, 0, 0L);
            fail();
        } catch (OptimisticLockFailureException ex) {

        }

    }

    /**
     * Validate the logic for grouping log segments together for cleaning
     */
    @Test
    public void testSegmentGrouping() {
        Cleaner cleaner = makeCleaner(Integer.MAX_VALUE);
        LogConfig clone = logConfig.clone();
        clone.segmentSize = 300;
        clone.indexInterval = 1;

        Log log = makeLog(dir, clone);

        // append some messages to the log
        int i = 0;
        while (log.numberOfSegments() < 10) {
            log.append(TestUtils.singleMessageSet("hello".getBytes(Charsets.UTF_8)));
            i += 1;
        }

        // grouping by very large values should result in a single group with all the segments in it
        List<List<LogSegment>> groups = cleaner.groupSegmentsBySize(log.logSegments(), /*maxSize = */Integer.MAX_VALUE,/* maxIndexSize = */Integer.MAX_VALUE);
        assertEquals(1, groups.size());
        assertEquals(log.numberOfSegments(), groups.get(0).size());
        checkSegmentOrder(groups);

        // grouping by very small values should result in all groups having one entry
        groups = cleaner.groupSegmentsBySize(log.logSegments(), /*maxSize = */1, /*maxIndexSize = */Integer.MAX_VALUE);
        assertEquals(log.numberOfSegments(), groups.size());
        assertTrue("All groups should be singletons.", Utils.forall(groups, new Predicate<List<LogSegment>>() {
            @Override
            public boolean apply(List<LogSegment> _) {
                return _.size() == 1;
            }
        }));
        checkSegmentOrder(groups);
        groups = cleaner.groupSegmentsBySize(log.logSegments(), /*maxSize = */Integer.MAX_VALUE,/* maxIndexSize =*/ 1);
        assertEquals(log.numberOfSegments(), groups.size());
        assertTrue("All groups should be singletons.", Utils.forall(groups, new Predicate<List<LogSegment>>() {
            @Override
            public boolean apply(List<LogSegment> _) {
                return _.size() == 1;
            }
        }));
        checkSegmentOrder(groups);

        final int groupSize = 3;

        // check grouping by log size
        int logSize = Utils.foldLeft(Utils.take(log.logSegments(), groupSize), 0L, new Function2<Long, LogSegment, Long>() {
            @Override
            public Long apply(Long arg1, LogSegment _) {
                return arg1 + _.size();
            }
        }).intValue() + 1;
        groups = cleaner.groupSegmentsBySize(log.logSegments(), /*maxSize = */logSize, /*maxIndexSize = */Integer.MAX_VALUE);
        checkSegmentOrder(groups);
        assertTrue("All but the last group should be the target size.", Utils.forall(Utils.dropRight(groups, 1), new Predicate<List<LogSegment>>() {
            @Override
            public boolean apply(List<LogSegment> _) {
                return _.size() == groupSize;
            }
        }));

        // check grouping by index size
        int indexSize = Utils.foldLeft(Utils.take(log.logSegments(), groupSize), 0, new Function2<Integer, LogSegment, Integer>() {
            @Override
            public Integer apply(Integer arg1, LogSegment _) {
                return arg1 + _.index.sizeInBytes();
            }
        }) + 1;
        groups = cleaner.groupSegmentsBySize(log.logSegments(), /*maxSize = */Integer.MAX_VALUE, /*maxIndexSize = */indexSize);
        checkSegmentOrder(groups);
        assertTrue("All but the last group should be the target size.", Utils.forall(Utils.dropRight(groups, 1), new Predicate<List<LogSegment>>() {
            @Override
            public boolean apply(List<LogSegment> _) {
                return _.size() == groupSize;
            }
        }));
    }

    private void checkSegmentOrder(List<List<LogSegment>> groups) {
        List<Long> offsets = Utils.mapLists(groups, new Function1<List<LogSegment>, Collection<Long>>() {
            @Override
            public Collection<Long> apply(List<LogSegment> _) {
                return Utils.mapList(_, new Function1<LogSegment, Long>() {
                    @Override
                    public Long apply(LogSegment arg) {
                        return arg.baseOffset;
                    }
                });
            }
        });

        List<Long> offsets2 = Lists.newArrayList(offsets);
        Collections.sort(offsets);


        assertEquals("Offsets should be in increasing order.", offsets, offsets2);
    }

    /**
     * Test building an offset map off the log
     */
    @Test
    public void testBuildOffsetMap() throws InterruptedException {
        FakeOffsetMap map = new FakeOffsetMap(1000);
        Log log = makeLog();
        Cleaner cleaner = makeCleaner(Integer.MAX_VALUE);
        int start = 0;
        int end = 500;
        List<Long> offsets = writeToLog(log, Utils.zip(start, end, start, end));

        List<LogSegment> segments = Lists.newArrayList(log.logSegments());
        checkRange(log, cleaner, map, 0, (int) (segments.get(1).baseOffset));
        checkRange(log, cleaner, map, (int) (segments.get(1).baseOffset), (int) segments.get(3).baseOffset);
        checkRange(log, cleaner, map, (int) (segments.get(3).baseOffset), (int) log.logEndOffset());
    }

    void checkRange(Log log, Cleaner cleaner, FakeOffsetMap map, int start, int end) throws InterruptedException {
        long endOffset = cleaner.buildOffsetMap(log, start, end, map) + 1;
        assertEquals("Last offset should be the end offset.", end, endOffset);
        assertEquals("Should have the expected number of messages in the map.", end - start, map.size());
        for (int i = start; i < end; ++i)
            assertEquals("Should find all the keys", (long) i, map.get(key(i)));
        assertEquals("Should not find a value too small", -1L, map.get(key(start - 1)));
        assertEquals("Should not find a value too large", -1L, map.get(key(end)));
    }

    public Log makeLog() {
        return makeLog(dir, logConfig);
    }

    public Log makeLog(File dir, LogConfig config) {
        return new Log(dir, config,/* recoveryPoint = */0L, time.scheduler, time);
    }

    public Cleaner makeCleaner(int capacity) {
        return new Cleaner(/*id = */0,
                        /*offsetMap = */new FakeOffsetMap(capacity),
    /*ioBufferSize = */64 * 1024,
   /* maxIoBufferSize = */64 * 1024,
    /*dupBufferLoadFactor =*/ 0.75,
    /*throttler =*/ throttler,
    /*time = */time);
    }

    public List<Long> writeToLog(Log log, List<Tuple2<Integer, Integer>> seq) {
        List<Long> ret = Lists.newArrayList();
        for (Tuple2<Integer, Integer> kv : seq) {
            ret.add(log.append(message(kv._1, kv._2)).firstOffset);
        }
        return ret;
    }

    public ByteBuffer key(int id) {
        return ByteBuffer.wrap((id + "").getBytes(Charsets.UTF_8));
    }

    public ByteBufferMessageSet message(int key, int value) {
        return new ByteBufferMessageSet(new Message((value + "").getBytes(Charsets.UTF_8), (key + "").getBytes(Charsets.UTF_8)));
    }

    public ByteBufferMessageSet deleteMessage(int key) {
        return new ByteBufferMessageSet(new Message(null, (key + "").getBytes(Charsets.UTF_8)));
    }

    static class FakeOffsetMap extends OffsetMap {
        public int slots;

        FakeOffsetMap(int slots) {
            this.slots = slots;
        }

        public HashMap<String, Long> map = Maps.newHashMap();

        private String keyFor(ByteBuffer key) {
            return new String(Utils.readBytes(key.duplicate()), Charsets.UTF_8);
        }

        @Override
        public int slots() {
            return slots;
        }

        public void put(ByteBuffer key, long offset) {
            map.put(keyFor(key), offset);
        }

        public long get(ByteBuffer key) {
            String k = keyFor(key);
            if (map.containsKey(k))
                return map.get(k);
            else
                return -1L;
        }

        public void clear() {
            map.clear();
        }

        public int size() {
            return map.size();
        }

    }
}
