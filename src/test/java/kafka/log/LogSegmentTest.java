package kafka.log;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import kafka.message.*;
import kafka.utils.*;
import org.junit.After;
import org.junit.Test;

import java.io.File;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.*;

public class LogSegmentTest {

    List<LogSegment> segments = Lists.newArrayList();

    /* create a segment with the given base offset */
    public LogSegment createSegment(long offset) {
        File msFile = TestUtils.tempFile();
        FileMessageSet ms = new FileMessageSet(msFile);
        File idxFile = TestUtils.tempFile();
        idxFile.delete();
        OffsetIndex idx = new OffsetIndex(idxFile, offset, 1000);
        LogSegment seg = new LogSegment(ms, idx, offset, 10, SystemTime.instance);
        segments.add(seg);
        return seg;
    }

    /* create a ByteBufferMessageSet for the given messages starting from the given offset */
    public ByteBufferMessageSet messages(long offset, String... messages) {
        return new ByteBufferMessageSet(NoCompressionCodec.instance,
                new AtomicLong(offset),
                Utils.mapList(messages, new Function1<String, Message>() {
                    @Override
                    public Message apply(String arg) {
                        return new Message(arg.getBytes());
                    }
                }));
    }

    @After
    public void teardown() {
        for (LogSegment seg : segments) {
            seg.index.delete();
            seg.log.delete();
        }
    }

    /**
     * A read on an empty log segment should return null
     */
    @Test
    public void testReadOnEmptySegment() {
        LogSegment seg = createSegment(40);
        MessageSet read = seg.read(/*startOffset =*/ 40, /* maxOffset =*/ null, /*maxSize =*/ 300);
        assertNull("Read beyond the last offset in the segment should be null", read);
    }

    /**
     * Reading from before the first offset in the segment should return messages
     * beginning with the first message in the segment
     */
    @Test
    public void testReadBeforeFirstOffset() {
        LogSegment seg = createSegment(40);
        ByteBufferMessageSet ms = messages(50, "hello", "there", "little", "bee");
        seg.append(50, ms);
        MessageSet read = seg.read(/*startOffset =*/ 41, /*maxOffset = */null, /*maxSize =*/ 300);
        assertEquals(Lists.newArrayList(ms), Lists.newArrayList(read));
    }

    /**
     * If we set the startOffset and maxOffset for the read to be the same value
     * we should get only the first message in the log
     */
    @Test
    public void testMaxOffset() {
        int baseOffset = 50;
        final LogSegment seg = createSegment(baseOffset);
        final ByteBufferMessageSet ms = messages(baseOffset, "hello", "there", "beautiful");
        seg.append(baseOffset, ms);

        Callable1<Long> validate = new Callable1<Long>() {
            @Override
            public void apply(final Long offset) {
                assertEquals(Utils.filter(ms, new Predicate<MessageAndOffset>() {
                    @Override
                    public boolean apply(MessageAndOffset _) {
                        return _.offset == offset;
                    }
                }),
                        Lists.newArrayList(seg.read(/*startOffset =*/ offset, /*maxOffset = Some*/(offset + 1), /*maxSize =*/ 1024)));
            }
        };

        validate.apply(50L);
        validate.apply(51L);
        validate.apply(52L);
    }

    /**
     * If we read from an offset beyond the last offset in the segment we should get null
     */
    @Test
    public void testReadAfterLast() {
        LogSegment seg = createSegment(40);
        ByteBufferMessageSet ms = messages(50, "hello", "there");
        seg.append(50, ms);
        MessageSet read = seg.read(/*startOffset = */52,/*maxOffset = */ null,/* maxSize =*/ 200);
        assertNull("Read beyond the last offset in the segment should give null", read);
    }

    /**
     * If we read from an offset which doesn't exist we should get a message set beginning
     * with the least offset greater than the given startOffset.
     */
    @Test
    public void testReadFromGap() {
        LogSegment seg = createSegment(40);
        ByteBufferMessageSet ms = messages(50, "hello", "there");
        seg.append(50, ms);
        ByteBufferMessageSet ms2 = messages(60, "alpha", "beta");
        seg.append(60, ms2);
        MessageSet read = seg.read(/*startOffset = */55, /*maxOffset =*/ null, /*maxSize = */200);
        assertEquals(Lists.newArrayList(ms2), Lists.newArrayList(read));
    }

    /**
     * In a loop append two messages then truncate off the second of those messages and check that we can read
     * the first but not the second message.
     */
    @Test
    public void testTruncate() {
        LogSegment seg = createSegment(40);
        int offset = 40;
        for (int i = 0; i < 30; ++i) {
            ByteBufferMessageSet ms1 = messages(offset, "hello");
            seg.append(offset, ms1);
            ByteBufferMessageSet ms2 = messages(offset + 1, "hello");
            seg.append(offset + 1, ms2);
            // check that we can read back both messages
            MessageSet read = seg.read(offset, null, 10000);
            assertEquals(Lists.newArrayList(Utils.head(ms1), Utils.head(ms2)), Lists.newArrayList(read));
            // now truncate off the last message
            seg.truncateTo(offset + 1);
            MessageSet read2 = seg.read(offset, null, 10000);
            assertEquals(1, Iterables.size(read2));
            assertEquals(Utils.head(ms1), Utils.head(read2));
            offset += 1;
        }
    }

    /**
     * Test truncating the whole segment, and check that we can reappend with the original offset.
     */
    @Test
    public void testTruncateFull() {
        // test the case where we fully truncate the log
        LogSegment seg = createSegment(40);
        seg.append(40, messages(40, "hello", "there"));
        seg.truncateTo(0);
        assertNull("Segment should be empty.", seg.read(0, null, 1024));
        seg.append(40, messages(40, "hello", "there"));
    }

    /**
     * Test that offsets are assigned sequentially and that the nextOffset variable is incremented
     */
    @Test
    public void testNextOffsetCalculation() {
        LogSegment seg = createSegment(40);
        assertEquals(40, seg.nextOffset());
        seg.append(50, messages(50, "hello", "there", "you"));
        assertEquals(53, seg.nextOffset());
    }

    /**
     * Test that we can change the file suffixes for the log and index files
     */
    @Test
    public void testChangeFileSuffixes() {
        LogSegment seg = createSegment(40);
        File logFile = seg.log.file;
        File indexFile = seg.index.file;
        seg.changeFileSuffixes("", ".deleted");
        assertEquals(logFile.getAbsolutePath() + ".deleted", seg.log.file.getAbsolutePath());
        assertEquals(indexFile.getAbsolutePath() + ".deleted", seg.index.file.getAbsolutePath());
        assertTrue(seg.log.file.exists());
        assertTrue(seg.index.file.exists());
    }

    /**
     * Create a segment with some data and an index. Then corrupt the index,
     * and recover the segment, the entries should all be readable.
     */
    @Test
    public void testRecoveryFixesCorruptIndex() {
        LogSegment seg = createSegment(0);
        for (int i = 0; i < 100; ++i)
            seg.append(i, messages(i, i + ""));

        File indexFile = seg.index.file;
        TestUtils.writeNonsenseToFile(indexFile, 5, (int) indexFile.length());
        seg.recover(64 * 1024);
        for (long i = 0; i < 100; ++i)
            assertEquals(i, Utils.head(seg.read(i, i + 1, 1024)).offset);
    }

    /**
     * Randomly corrupt a log a number of times and attempt recovery.
     */
    @Test
    public void testRecoveryWithCorruptMessage() {
        int messagesAppended = 20;
        for (int iteration = 0; iteration < 10; ++iteration) {
            LogSegment seg = createSegment(0);
            for (int i = 0; i < messagesAppended; ++i)
                seg.append(i, messages(i, i + ""));

            int offsetToBeginCorruption = TestUtils.random.nextInt(messagesAppended);
            // start corrupting somewhere in the middle of the chosen record all the way to the end
            int position = seg.log.searchFor(offsetToBeginCorruption, 0).position + TestUtils.random.nextInt(15);
            TestUtils.writeNonsenseToFile(seg.log.file, position, (int) seg.log.file.length() - position);
            seg.recover(64 * 1024);
            assertEquals("Should have truncated off bad messages.",
                    Utils.flatList(0, offsetToBeginCorruption, new Function1<Integer, Long>() {
                        @Override
                        public Long apply(Integer arg) {
                            return (long) arg;
                        }
                    }), Utils.mapList(seg.log, new Function1<MessageAndOffset, Long>() {
                @Override
                public Long apply(MessageAndOffset arg) {
                    return arg.offset;
                }
            }));
            seg.delete();
        }
    }
}
