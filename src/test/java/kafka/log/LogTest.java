package kafka.log;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import kafka.common.KafkaException;
import kafka.common.KafkaStorageException;
import kafka.common.OffsetOutOfRangeException;
import kafka.message.*;
import kafka.server.KafkaConfig;
import kafka.utils.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.*;

public class LogTest {

    File logDir = null;
    MockTime time = new MockTime(0);
    KafkaConfig config = null;
    LogConfig logConfig = new LogConfig();

    @Before
    public void setUp() {
        logDir = TestUtils.tempDir();
        Properties props = TestUtils.createBrokerConfig(0, -1);
        config = new KafkaConfig(props);
    }

    @After
    public void tearDown() {
        Utils.rm(logDir);
    }

    public void createEmptyLogs(File dir, long... offsets) {
        try {
            for (long offset : offsets) {
                Logs.logFilename(dir, offset).createNewFile();
                Logs.indexFilename(dir, offset).createNewFile();
            }
        } catch (IOException e) {
            throw new KafkaException(e);
        }
    }

    /**
     * Tests for time based log roll. This test appends messages then changes the time
     * using the mock clock to force the log to roll and checks the number of segments.
     */
    @Test
    public void testTimeBasedLogRoll() {
        ByteBufferMessageSet set = TestUtils.singleMessageSet("test".getBytes());

        LogConfig clone = logConfig.clone();
        clone.segmentMs = 1 * 60 * 60L;
        // create a log
        Log log = new Log(logDir,
                clone,
                /*recoveryPoint = */0L,
                /*scheduler = */time.scheduler,
                /*time = */time);
        assertEquals("Log begins with a single empty segment.", 1, log.numberOfSegments());
        time.sleep(log.config.segmentMs + 1);
        log.append(set);
        assertEquals("Log doesn't roll if doing so creates an empty segment.", 1, log.numberOfSegments());

        log.append(set);
        assertEquals("Log rolls on this append since time has expired.", 2, log.numberOfSegments());

        for (int numSegments = 3; numSegments < 5; ++numSegments) {
            time.sleep(log.config.segmentMs + 1);
            log.append(set);
            assertEquals("Changing time beyond rollMs and appending should create a new segment.", numSegments, log.numberOfSegments());
        }

        int numSegments = log.numberOfSegments();
        time.sleep(log.config.segmentMs + 1);
        log.append(new ByteBufferMessageSet());
        assertEquals("Appending an empty message set should not roll log even if succient time has passed.", numSegments, log.numberOfSegments());
    }

    /**
     * Test that appending more than the maximum segment size rolls the log
     */
    @Test
    public void testSizeBasedLogRoll() {
        ByteBufferMessageSet set = TestUtils.singleMessageSet("test".getBytes());
        int setSize = set.sizeInBytes();
        int msgPerSeg = 10;
        int segmentSize = msgPerSeg * (setSize - 1); // each segment will be 10 messages

        LogConfig clone = logConfig.clone();
        clone.segmentSize = segmentSize;
        // create a log
        Log log = new Log(logDir, clone, /*recoveryPoint = */0L, time.scheduler, time);
        assertEquals("There should be exactly 1 segment.", 1, log.numberOfSegments());

        // segments expire in size
        for (int i = 1; i <= (msgPerSeg + 1); ++i) {
            log.append(set);
        }
        assertEquals("There should be exactly 2 segments.", 2, log.numberOfSegments());
    }

    /**
     * Test that we can open and append to an empty log
     */
    @Test
    public void testLoadEmptyLog() {
        createEmptyLogs(logDir, 0);
        Log log = new Log(logDir, logConfig, /*recoveryPoint = */0L, time.scheduler, time);
        log.append(TestUtils.singleMessageSet("test".getBytes()));
    }

    /**
     * This test case appends a bunch of messages and checks that we can read them all back using sequential offsets.
     */
    @Test
    public void testAppendAndReadWithSequentialOffsets() {
        LogConfig clone = logConfig.clone();
        clone.segmentSize = 71;
        Log log = new Log(logDir, clone, /*recoveryPoint =*/ 0L, time.scheduler, time);
        List<Message> messages = Utils.flatList(0, 100, 2, new Function1<Integer, Message>() {
            @Override
            public Message apply(Integer id) {
                return new Message(id.toString().getBytes());
            }
        });

        for (int i = 0; i < messages.size(); ++i)
            log.append(new ByteBufferMessageSet(NoCompressionCodec.instance, messages.get(i)));
        for (long i = 0; i < messages.size(); ++i) {
            MessageAndOffset read = Utils.head(log.read(i, 100, (i + 1)));
            assertEquals("Offset read should match order appended.", i, read.offset);
            assertEquals("Message should match appended.", messages.get((int) i), read.message);
        }
        assertEquals("Reading beyond the last message returns nothing.", 0, Iterables.size(log.read((long) messages.size(), 100, null)));
    }

    /**
     * This test appends a bunch of messages with non-sequential offsets and checks that we can read the correct message
     * from any offset less than the logEndOffset including offsets not appended.
     */
    @Test
    public void testAppendAndReadWithNonSequentialOffsets() {
        LogConfig clone = logConfig.clone();
        clone.segmentSize = 71;
        Log log = new Log(logDir, clone, /*recoveryPoint =*/ 0L, time.scheduler, time);

        List<Integer> messageIds = Utils.flatList(0, 50);
        messageIds.addAll(Utils.flatList(50, 200, 7));

        // val messageIds = ((0 until 50) ++ (50 until 200 by 7)).toArray

        List<Message> messages = Utils.mapList(messageIds, new Function1<Integer, Message>() {
            @Override
            public Message apply(Integer id) {
                return new Message((id + "").getBytes());
            }
        });

        // now test the case that we give the offsets and use non-sequential offsets
        for (int i = 0; i < messages.size(); ++i)
            log.append(new ByteBufferMessageSet(NoCompressionCodec.instance,
                    new AtomicLong(messageIds.get(i)), messages.get(i)));
        for (int i = 50; i < messages.size(); ++i) {
            final int finalI = i;
            Integer idx = Utils.indexWhere(messageIds, new Predicate<Integer>() {
                @Override
                public boolean apply(Integer _) {
                    return _ >= finalI;
                }
            });
            MessageAndOffset read = Utils.head(log.read(i, 100, null));
            assertEquals("Offset read should match message id.", (long) (idx), read.offset);
            assertEquals("Message should match appended.", messages.get(idx), read.message);
        }
    }

    /**
     * This test covers an odd case where we have a gap in the offsets that falls at the end of a log segment.
     * Specifically we create a log where the last message in the first segment has offset 0. If we
     * then read offset 1, we should expect this read to come from the second segment, even though the
     * first segment has the greatest lower bound on the offset.
     */
    @Test
    public void testReadAtLogGap() {
        LogConfig config = logConfig.clone();
        config.segmentSize = 300;
        Log log = new Log(logDir, config, /*recoveryPoint =*/ 0L, time.scheduler, time);

        // keep appending until we have two segments with only a single message in the second segment
        while (log.numberOfSegments() == 1)
            log.append(new ByteBufferMessageSet(NoCompressionCodec.instance, new Message("42".getBytes())));

        // now manually truncate off all but one message from the first segment to create a gap in the messages
        Utils.head(log.logSegments()).truncateTo(1);

        assertEquals("A read should now return the last message in the log", log.logEndOffset() - 1, Utils.head(log.read(1L, 200, null)).offset);
    }

    /**
     * Test reading at the boundary of the log, specifically
     * - reading from the logEndOffset should give an empty message set
     * - reading beyond the log end offset should throw an OffsetOutOfRangeException
     */
    @Test
    public void testReadOutOfRange() {
        createEmptyLogs(logDir, 1024);
        LogConfig clone = logConfig.clone();
        clone.segmentSize = 1024;
        Log log = new Log(logDir, clone, /*recoveryPoint =*/ 0L, time.scheduler, time);
        assertEquals("Reading just beyond end of log should produce 0 byte read.", 0, log.read(1024L, 1000).sizeInBytes());
        try {
            log.read(0L, 1024);
            fail("Expected exception on invalid read.");
        } catch (OffsetOutOfRangeException e) {
            //  "This is good."
        }
        try {
            log.read(1025L, 1000);
            fail("Expected exception on invalid read.");
        } catch (OffsetOutOfRangeException e) {
            // This is good.
        }
    }

    /**
     * Test that covers reads and writes on a multisegment log. This test appends a bunch of messages
     * and then reads them all back and checks that the message read and offset matches what was appended.
     */
    @Test
    public void testLogRolls() {
    /* create a multipart log with 100 messages */
        LogConfig clone = logConfig.clone();
        clone.segmentSize = 100;
        final Log log = new Log(logDir, clone, /*recoveryPoint =*/ 0L, time.scheduler, time);
        int numMessages = 100;
        List<ByteBufferMessageSet> messageSets = Utils.flatList(0, numMessages, new Function1<Integer, ByteBufferMessageSet>() {
            @Override
            public ByteBufferMessageSet apply(Integer i) {
                return TestUtils.singleMessageSet((i + "").getBytes());
            }
        });

        Utils.foreach(messageSets, new Callable1<ByteBufferMessageSet>() {
            @Override
            public void apply(ByteBufferMessageSet _) {
                log.append(_);
            }
        });

        log.flush();

    /* do successive reads to ensure all our messages are there */
        long offset = 0L;
        for (int i = 0; i < numMessages; ++i) {
            MessageSet messages = log.read(offset, 1024 * 1024);
            assertEquals("Offsets not equal", offset, Utils.head(messages).offset);
            assertEquals("Messages not equal at offset " + offset, Utils.head(messageSets.get(i)).message, Utils.head(messages).message);
            offset = Utils.head(messages).offset + 1;
        }

        MessageSet lastRead = log.read(/*startOffset = */numMessages, /*maxLength = */1024 * 1024, /*maxOffset = Some*/(long) (numMessages + 1));
        assertEquals("Should be no more messages", 0, Iterables.size(lastRead));

        // check that rolling the log forced a flushed the log--the flush is asyn so retry in case of failure
        TestUtils.retry(1000L, new Runnable() {
            @Override
            public void run() {
                assertTrue("Log role should have forced flush", log.recoveryPoint >= log.activeSegment().baseOffset);
            }
        });
    }

    /**
     * Test reads at offsets that fall within compressed message set boundaries.
     */
    @Test
    public void testCompressedMessages() {
    /* this log should roll after every messageset */
        LogConfig clone = logConfig.clone();
        clone.segmentSize = 100;
        final Log log = new Log(logDir, clone, /*recoveryPoint =*/ 0L, time.scheduler, time);

    /* append 2 compressed message sets, each with two messages giving offsets 0, 1, 2, 3 */
        log.append(new ByteBufferMessageSet(DefaultCompressionCodec.instance, new Message("hello".getBytes()), new Message("there".getBytes())));
        log.append(new ByteBufferMessageSet(DefaultCompressionCodec.instance, new Message("alpha".getBytes()), new Message("beta".getBytes())));

        Function1<Integer, ByteBufferMessageSet> read = new Function1<Integer, ByteBufferMessageSet>() {
            @Override
            public ByteBufferMessageSet apply(Integer offset) {
                return ByteBufferMessageSets.decompress(Utils.head(log.read((long) offset, 4096)).message);
            }
        };


    /* we should always get the first message in the compressed set when reading any offset in the set */
        assertEquals("Read at offset 0 should produce 0", 0, Utils.head(read.apply(0)).offset);
        assertEquals("Read at offset 1 should produce 0", 0, Utils.head(read.apply(1)).offset);
        assertEquals("Read at offset 2 should produce 2", 2, Utils.head(read.apply(2)).offset);
        assertEquals("Read at offset 3 should produce 2", 2, Utils.head(read.apply(3)).offset);
    }

    /**
     * Test garbage collecting old segments
     */
    @Test
    public void testThatGarbageCollectingSegmentsDoesntChangeOffset() {
        for (int messagesToAppend : Lists.newArrayList(0, 1, 25)) {
            logDir.mkdirs();
            // first test a log segment starting at 0
            LogConfig config = logConfig.clone();
            config.segmentSize = 100;
            Log log = new Log(logDir, config, /*recoveryPoint =*/ 0L, time.scheduler, time);
            for (int i = 0; i < messagesToAppend; ++i)
                log.append(TestUtils.singleMessageSet((i + "").getBytes()));

            long currOffset = log.logEndOffset();
            assertEquals(currOffset, messagesToAppend);

            // time goes by; the log file is deleted
            log.deleteOldSegments(new Predicate<LogSegment>() {
                @Override
                public boolean apply(LogSegment input) {
                    return true;
                }
            });

            assertEquals("Deleting segments shouldn't have changed the logEndOffset", currOffset, log.logEndOffset());
            assertEquals("We should still have one segment left", 1, log.numberOfSegments());
            assertEquals("Further collection shouldn't delete anything", 0, (int) log.deleteOldSegments(new Predicate<LogSegment>() {
                @Override
                public boolean apply(LogSegment input) {
                    return true;
                }
            }));
            assertEquals("Still no change in the logEndOffset", currOffset, log.logEndOffset());
            assertEquals("Should still be able to append and should get the logEndOffset assigned to the new append",
                    currOffset,
                    (long) log.append(TestUtils.singleMessageSet("hello".getBytes())).firstOffset);

            // cleanup the log
            log.delete();
        }
    }

    /**
     * We have a max size limit on message appends, check that it is properly enforced by appending a message larger than the
     * setting and checking that an exception is thrown.
     */
    @Test
    public void testMessageSizeCheck() {
        ByteBufferMessageSet first = new ByteBufferMessageSet(NoCompressionCodec.instance, new Message("You".getBytes()), new Message("bethe".getBytes()));
        ByteBufferMessageSet second = new ByteBufferMessageSet(NoCompressionCodec.instance, new Message("change".getBytes()));

        // append messages to log
        int maxMessageSize = second.sizeInBytes() - 1;
        LogConfig config = logConfig.clone();
        config.maxMessageSize = maxMessageSize;

        Log log = new Log(logDir, config, /*recoveryPoint =*/ 0L, time.scheduler, time);

        // should be able to append the small message
        log.append(first);

        try {
            log.append(second);
            fail("Second message set should throw MessageSizeTooLargeException.");
        } catch (KafkaStorageException e) {
            // this is good
        }
    }

    /**
     * Append a bunch of messages to a log and then re-open it both with and without recovery and check that the log re-initializes correctly.
     */
    @Test
    public void testLogRecoversToCorrectOffset() {
        int numMessages = 100;
        int messageSize = 100;
        int segmentSize = 7 * messageSize;
        int indexInterval = 3 * messageSize;
        LogConfig config = logConfig.clone();
        config.segmentSize = segmentSize;
        config.indexInterval = indexInterval;
        config.maxIndexSize = 4096;
        Log log = new Log(logDir, config, /*recoveryPoint =*/ 0L, time.scheduler, time);
        for (int i = 0; i < numMessages; ++i)
            log.append(TestUtils.singleMessageSet(TestUtils.randomBytes(messageSize)));
        assertEquals(String.format("After appending %d messages to an empty log, the log end offset should be %d", numMessages, numMessages), numMessages, log.logEndOffset());
        long lastIndexOffset = log.activeSegment().index.lastOffset;
        int numIndexEntries = log.activeSegment().index.entries();
        long lastOffset = log.logEndOffset();
        log.close();

        log = new Log(logDir, config, /*recoveryPoint = */lastOffset, time.scheduler, time);
        assertEquals(String.format("Should have %d messages when log is reopened w/o recovery", numMessages), numMessages, log.logEndOffset());
        assertEquals("Should have same last index offset as before.", lastIndexOffset, log.activeSegment().index.lastOffset);
        assertEquals("Should have same number of index entries as before.", numIndexEntries, log.activeSegment().index.entries());
        log.close();

        // test recovery case
        log = new Log(logDir, config, /*recoveryPoint =*/ 0L, time.scheduler, time);
        assertEquals(String.format("Should have %d messages when log is reopened with recovery", numMessages), numMessages, log.logEndOffset());
        assertEquals("Should have same last index offset as before.", lastIndexOffset, log.activeSegment().index.lastOffset);
        assertEquals("Should have same number of index entries as before.", numIndexEntries, log.activeSegment().index.entries());
        log.close();
    }

    /**
     * Test that if we manually delete an index segment it is rebuilt when the log is re-opened
     */
    @Test
    public void testIndexRebuild() {
        // publish the messages and close the log
        int numMessages = 200;
        LogConfig config = logConfig.clone();
        config.segmentSize = 200;
        config.indexInterval = 1;
        Log log = new Log(logDir, config, /*recoveryPoint =*/ 0L, time.scheduler, time);
        for (int i = 0; i < numMessages; ++i)
            log.append(TestUtils.singleMessageSet(TestUtils.randomBytes(10)));
        List<File> indexFiles = Utils.mapList(log.logSegments(), new Function1<LogSegment, File>() {
            @Override
            public File apply(LogSegment _) {
                return _.index.file;
            }
        });
        log.close();

        // delete all the index files
        Utils.foreach(indexFiles, new Callable1<File>() {
            @Override
            public void apply(File _) {
                _.delete();
            }
        });

        // reopen the log
        log = new Log(logDir, config, /*recoveryPoint =*/ 0L, time.scheduler, time);
        assertEquals(String.format("Should have %d messages when log is reopened", numMessages), (long) numMessages, (long) log.logEndOffset());
        for (int i = 0; i < numMessages; ++i)
            assertEquals(i, Utils.head(log.read((long) i, 100, null)).offset);
        log.close();
    }

    /**
     * Test the Log truncate operations
     */
    @Test
    public void testTruncateTo() {
        ByteBufferMessageSet set = TestUtils.singleMessageSet("test".getBytes());
        int setSize = set.sizeInBytes();
        int msgPerSeg = 10;
        int segmentSize = msgPerSeg * (setSize - 1); // each segment will be 10 messages

        // create a log
        LogConfig config = logConfig.clone();
        config.segmentSize = segmentSize;
        Log log = new Log(logDir, config, /*recoveryPoint =*/ 0L, /* scheduler = */time.scheduler, time);
        assertEquals("There should be exactly 1 segment.", 1, log.numberOfSegments());

        for (int i = 1; i <= msgPerSeg; ++i)
            log.append(set);

        assertEquals("There should be exactly 1 segments.", 1, log.numberOfSegments());
        assertEquals("Log end offset should be equal to number of messages", msgPerSeg, log.logEndOffset());

        long lastOffset = log.logEndOffset();
        long size = log.size();
        log.truncateTo(log.logEndOffset()); // keep the entire log
        assertEquals("Should not change offset", lastOffset, (long) log.logEndOffset());
        assertEquals("Should not change log size", size, log.size());
        log.truncateTo(log.logEndOffset() + 1); // try to truncate beyond lastOffset
        assertEquals("Should not change offset but should log error", lastOffset, log.logEndOffset());
        assertEquals("Should not change log size", size, log.size());
        log.truncateTo(msgPerSeg / 2L); // truncate somewhere in between
        assertEquals("Should change offset", log.logEndOffset(), msgPerSeg / 2);
        assertTrue("Should change log size", log.size() < size);
        log.truncateTo(0); // truncate the entire log
        assertEquals("Should change offset", 0, log.logEndOffset());
        assertEquals("Should change log size", 0, log.size());

        for (int i = 1; i <= msgPerSeg; ++i)
            log.append(set);

        assertEquals("Should be back to original offset", log.logEndOffset(), lastOffset);
        assertEquals("Should be back to original size", log.size(), size);
        log.truncateFullyAndStartAt(log.logEndOffset() - (msgPerSeg - 1));
        assertEquals("Should change offset", log.logEndOffset(), lastOffset - (msgPerSeg - 1));
        assertEquals("Should change log size", log.size(), 0);

        for (int i = 1; i <= msgPerSeg; ++i)
            log.append(set);

        assertTrue("Should be ahead of to original offset", log.logEndOffset() > msgPerSeg);
        assertEquals("log size should be same as before", size, log.size());
        log.truncateTo(0); // truncate before first start offset in the log
        assertEquals("Should change offset", 0, log.logEndOffset());
        assertEquals("Should change log size", log.size(), 0);
    }

    /**
     * Verify that when we truncate a log the index of the last segment is resized to the max index size to allow more appends
     */
    @Test
    public void testIndexResizingAtTruncation() {
        ByteBufferMessageSet set = TestUtils.singleMessageSet("test".getBytes());
        int setSize = set.sizeInBytes();
        int msgPerSeg = 10;
        int segmentSize = msgPerSeg * (setSize - 1); // each segment will be 10 messages
        LogConfig config = logConfig.clone();
        config.segmentSize = segmentSize;
        Log log = new Log(logDir, config, /*recoveryPoint =*/ 0L, /* scheduler = */time.scheduler, time);
        assertEquals("There should be exactly 1 segment.", 1, log.numberOfSegments());
        for (int i = 1; i < msgPerSeg; ++i)
            log.append(set);
        assertEquals("There should be exactly 1 segment.", 1, log.numberOfSegments());
        for (int i = 1; i < msgPerSeg; ++i)
            log.append(set);
        assertEquals("There should be exactly 2 segment.", 2, log.numberOfSegments());
        assertEquals("The index of the first segment should be trimmed to empty", 0, Utils.head(log.logSegments()).index.maxEntries);
        log.truncateTo(0);
        assertEquals("There should be exactly 1 segment.", 1, log.numberOfSegments());
        assertEquals("The index of segment 1 should be resized to maxIndexSize", log.config.maxIndexSize / 8, Utils.head(log.logSegments()).index.maxEntries);
        for (int i = 1; i < msgPerSeg; ++i)
            log.append(set);
        assertEquals("There should be exactly 1 segment.", 1, log.numberOfSegments());
    }

    /**
     * When we open a log any index segments without an associated log segment should be deleted.
     */
    @Test
    public void testBogusIndexSegmentsAreRemoved() {
        File bogusIndex1 = Logs.indexFilename(logDir, 0L);
        File bogusIndex2 = Logs.indexFilename(logDir, 5L);

        ByteBufferMessageSet set = TestUtils.singleMessageSet("test".getBytes());
        LogConfig config = logConfig.clone();
        config.segmentSize = set.sizeInBytes() * 5;
        config.maxIndexSize = 1000;
        config.indexInterval = 1;
        Log log = new Log(logDir,
                config,
                /*recoveryPoint =*/ 0L,
                time.scheduler,
                time);

        assertTrue("The first index file should have been replaced with a larger file", bogusIndex1.length() > 0);
        assertFalse("The second index file should have been deleted.", bogusIndex2.exists());

        // check that we can append to the log
        for (int i = 0; i < 10; ++i)
            log.append(set);

        log.delete();
    }

    /**
     * Verify that truncation works correctly after re-opening the log
     */
    @Test
    public void testReopenThenTruncate() {
        ByteBufferMessageSet set = TestUtils.singleMessageSet("test".getBytes());
        LogConfig config = logConfig.clone();
        config.segmentSize = set.sizeInBytes() * 5;
        config.maxIndexSize = 1000;
        config.indexInterval = 10000;

        // create a log
        Log log = new Log(logDir,
                config,
                /*recoveryPoint =*/ 0L,
                time.scheduler,
                time);

        // add enough messages to roll over several segments then close and re-open and attempt to truncate
        for (int i = 0; i < 100; ++i)
            log.append(set);
        log.close();
        log = new Log(logDir,
                config,
                /*recoveryPoint =*/ 0L,
                time.scheduler,
                time);
        log.truncateTo(3L);
        assertEquals("All but one segment should be deleted.", 1, log.numberOfSegments());
        assertEquals("Log end offset should be 3.", 3L, (long) log.logEndOffset());
    }

    /**
     * Test that deleted files are deleted after the appropriate time.
     */
    @Test
    public void testAsyncDelete() {
        ByteBufferMessageSet set = TestUtils.singleMessageSet("test".getBytes());
        int asyncDeleteMs = 1000;
        LogConfig config = logConfig.clone();
        config.segmentSize = set.sizeInBytes() * 5;
        config.fileDeleteDelayMs = asyncDeleteMs;
        config.maxIndexSize = 1000;
        config.indexInterval = 10000;
        Log log = new Log(logDir,
                config,
                /*recoveryPoint =*/ 0L,
                time.scheduler,
                time);

        // append some messages to create some segments
        for (int i = 0; i < 100; ++i)
            log.append(set);

        // files should be renamed
        List<LogSegment> segments = Lists.newArrayList(log.logSegments());
        List<File> oldFiles = Utils.mapList(segments, new Function1<LogSegment, File>() {
            @Override
            public File apply(LogSegment _) {
                return _.log.file;
            }
        });
        oldFiles.addAll(Utils.mapList(segments, new Function1<LogSegment, File>() {
            @Override
            public File apply(LogSegment _) {
                return _.index.file;
            }
        }));

        log.deleteOldSegments(new Predicate<LogSegment>() {
            @Override
            public boolean apply(LogSegment input) {
                return true;
            }
        });

        assertEquals("Only one segment should remain.", 1, log.numberOfSegments());
        assertTrue("All log and index files should end in .deleted", Utils.forall(segments, new Predicate<LogSegment>() {
            @Override
            public boolean apply(LogSegment _) {
                return _.log.file.getName().endsWith(Logs.DeletedFileSuffix);
            }
        }) && Utils.forall(segments, new Predicate<LogSegment>() {
            @Override
            public boolean apply(LogSegment _) {
                return _.index.file.getName().endsWith(Logs.DeletedFileSuffix);
            }
        }));
        assertTrue("The .deleted files should still be there.", Utils.forall(segments, new Predicate<LogSegment>() {
            @Override
            public boolean apply(LogSegment _) {
                return _.log.file.exists();
            }
        }) &&
                Utils.forall(segments, new Predicate<LogSegment>() {
                    @Override
                    public boolean apply(LogSegment _) {
                        return _.index.file.exists();
                    }
                }));
        assertTrue("The original file should be gone.", Utils.forall(oldFiles, new Predicate<File>() {
            @Override
            public boolean apply(File _) {
                return (!_.exists());
            }
        }));

        // when enough time passes the files should be deleted
        List<File> deletedFiles = Utils.mapList(segments, new Function1<LogSegment, File>() {
            @Override
            public File apply(LogSegment _) {
                return _.log.file;
            }
        });
        deletedFiles.addAll(Utils.mapList(segments, new Function1<LogSegment, File>() {
            @Override
            public File apply(LogSegment _) {
                return _.index.file;
            }
        }));

        time.sleep(asyncDeleteMs + 1);
        assertTrue("Files should all be gone.", Utils.forall(deletedFiles, new Predicate<File>() {
            @Override
            public boolean apply(File _) {
                return (!_.exists());
            }
        }));
    }

    /**
     * Any files ending in .deleted should be removed when the log is re-opened.
     */
    @Test
    public void testOpenDeletesObsoleteFiles() {
        ByteBufferMessageSet set = TestUtils.singleMessageSet("test".getBytes());
        LogConfig config = logConfig.clone();

        config.segmentSize = set.sizeInBytes() * 5;
        config.maxIndexSize = 1000;
        Log log = new Log(logDir,
                config,
                /*recoveryPoint =*/ 0L,
                time.scheduler,
                time);

        // append some messages to create some segments
        for (int i = 0; i < 100; ++i)
            log.append(set);

        log.deleteOldSegments(new Predicate<LogSegment>() {
            @Override
            public boolean apply(LogSegment input) {
                return true;
            }
        });
        log.close();

        log = new Log(logDir,
                config,
                /*recoveryPoint =*/ 0L,
                time.scheduler,
                time);
        assertEquals("The deleted segments should be gone.", 1, log.numberOfSegments());
    }

    @Test
    public void testAppendMessageWithNullPayload() {
        Log log = new Log(logDir,
                new LogConfig(),
                /*recoveryPoint =*/ 0L,
                time.scheduler,
                time);
        log.append(new ByteBufferMessageSet(new Message((byte[]) null)));
        MessageSet ms = log.read(0L, 4096, null);
        assertEquals(0, Utils.head(ms).offset);
        assertTrue("Message payload should be null.", Utils.head(ms).message.isNull());
    }

    @Test
    public void testCorruptLog() {
        // append some messages to create some segments
        LogConfig config = logConfig.clone();
        config.indexInterval = 1;
        config.maxMessageSize = 64 * 1024;
        config.segmentSize = 1000;
        ByteBufferMessageSet set = TestUtils.singleMessageSet("test".getBytes());
        long recoveryPoint = 50L;
        for (int iteration = 0; iteration < 50; ++iteration) {
            // create a log and write some messages to it
            logDir.mkdirs();
            Log log = new Log(logDir,
                    config,
                    /*recoveryPoint =*/ 0L,
                    time.scheduler,
                    time);
            int numMessages = 50 + TestUtils.random.nextInt(50);
            for (int i = 0; i < numMessages; ++i)
                log.append(set);
            List<MessageAndOffset> messages = Utils.mapLists(log.logSegments(), new Function1<LogSegment, Collection<MessageAndOffset>>() {
                @Override
                public Collection<MessageAndOffset> apply(LogSegment _) {
                    return Lists.newArrayList(_.log.iterator());
                }
            });
            log.close();

            // corrupt index and log by appending random bytes
            TestUtils.appendNonsenseToFile(log.activeSegment().index.file, TestUtils.random.nextInt(1024) + 1);
            TestUtils.appendNonsenseToFile(log.activeSegment().log.file, TestUtils.random.nextInt(1024) + 1);

            // attempt recovery
            log = new Log(logDir, config, recoveryPoint, time.scheduler, time);
            assertEquals(numMessages, log.logEndOffset());
            assertEquals("Messages in the log after recovery should be the same.", messages,
                    Utils.mapLists(log.logSegments(), new Function1<LogSegment, Collection<MessageAndOffset>>() {
                        @Override
                        public Collection<MessageAndOffset> apply(LogSegment _) {
                            return Lists.newArrayList(_.log.iterator());
                        }
                    }));
            Utils.rm(logDir);
        }
    }

    @Test
    public void testCleanShutdownFile() throws IOException {
        // append some messages to create some segments
        LogConfig config = logConfig.clone();
        config.indexInterval = 1;
        config.maxMessageSize = 64 * 1024;
        config.segmentSize = 1000;
        ByteBufferMessageSet set = TestUtils.singleMessageSet("test".getBytes());
        File parentLogDir = logDir.getParentFile();
        assertTrue("Data directory %s must exist", parentLogDir.isDirectory());
        File cleanShutdownFile = new File(parentLogDir, Logs.CleanShutdownFile);
        cleanShutdownFile.createNewFile();
        assertTrue(".kafka_cleanshutdown must exist", cleanShutdownFile.exists());
        long recoveryPoint = 0L;
        // create a log and write some messages to it
        Log log = new Log(logDir,
                config,
                /*recoveryPoint =*/ 0L,
                time.scheduler,
                time);
        for (int i = 0; i < 100; ++i)
            log.append(set);
        log.close();

        // check if recovery was attempted. Even if the recovery point is 0L, recovery should not be attempted as the
        // clean shutdown file exists.
        recoveryPoint = log.logEndOffset();
        log = new Log(logDir, config, 0L, time.scheduler, time);
        assertEquals(recoveryPoint, (long) log.logEndOffset());
        cleanShutdownFile.delete();
    }
}
