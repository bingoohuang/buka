package kafka.log;

import com.google.common.collect.Lists;
import kafka.message.*;
import kafka.utils.Function1;
import kafka.utils.TestUtils;
import kafka.utils.Utils;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static kafka.utils.TestUtils.checkEquals;
import static kafka.utils.TestUtils.singleMessageSet;
import static kafka.utils.Utils.head;
import static kafka.utils.Utils.tail;
import static org.junit.Assert.assertEquals;

public class FileMessageSetTest extends BaseMessageSetTestCases {
    FileMessageSet messageSet = createMessageSet(messages);

    @Override
    public FileMessageSet createMessageSet(List<Message> messages) {
        FileMessageSet set = new FileMessageSet(TestUtils.tempFile());
        set.append(new ByteBufferMessageSet(NoCompressionCodec.instance, messages));
        set.flush();
        return set;
    }


    /**
     * Test that the cached size variable matches the actual file size as we append messages
     */
    @Test
    public void testFileSize() throws IOException {
        assertEquals(messageSet.channel.size(), messageSet.sizeInBytes());
        for (int i = 0; i < 20; ++i) {
            messageSet.append(singleMessageSet("abcd".getBytes()));
            assertEquals(messageSet.channel.size(), messageSet.sizeInBytes());
        }
    }

    /**
     * Test that adding invalid bytes to the end of the log doesn't break iteration
     */
    @Test
    public void testIterationOverPartialAndTruncation() throws IOException {
        testPartialWrite(0, messageSet);
        testPartialWrite(2, messageSet);
        testPartialWrite(4, messageSet);
        testPartialWrite(5, messageSet);
        testPartialWrite(6, messageSet);
    }

    public void testPartialWrite(int size, FileMessageSet messageSet) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(size);
        long originalPosition = messageSet.channel.position();
        for (int i = 0; i < size; ++i)
            buffer.put((byte) 0);
        buffer.rewind();
        messageSet.channel.write(buffer);
        // appending those bytes should not change the contents
        checkEquals(messages.iterator(), Utils.mapList(messageSet, new Function1<MessageAndOffset, Message>() {
            @Override
            public Message apply(MessageAndOffset m) {
                return m.message;
            }
        }).iterator());
    }

    /**
     * Iterating over the file does file reads but shouldn't change the position of the underlying FileChannel.
     */
    @Test
    public void testIterationDoesntChangePosition() throws IOException {
        long position = messageSet.channel.position();
        checkEquals(messages.iterator(), Utils.mapList(messageSet, new Function1<MessageAndOffset, Message>() {
            @Override
            public Message apply(MessageAndOffset m) {
                return m.message;
            }
        }).iterator());
        assertEquals(position, messageSet.channel.position());
    }

    /**
     * Test a simple append and read.
     */
    @Test
    public void testRead() {
        FileMessageSet read = messageSet.read(0, messageSet.sizeInBytes());
        checkEquals(messageSet.iterator(), read.iterator());
        List<MessageAndOffset> items = Lists.newArrayList(read);
        MessageAndOffset sec = head(tail(items));
        read = messageSet.read(MessageSets.entrySize(sec.message), messageSet.sizeInBytes());
        assertEquals("Try a read starting from the second message", tail(items), Lists.newArrayList(read));
        read = messageSet.read(MessageSets.entrySize(sec.message), MessageSets.entrySize(sec.message));
        assertEquals("Try a read of a single message starting from the second message", Lists.newArrayList(head(tail(items))), Lists.newArrayList(read));
    }

    /**
     * Test the MessageSet.searchFor API.
     */
    @Test
    public void testSearch() {
        // append a new message with a high offset
        Message lastMessage = new Message("test".getBytes());
        messageSet.append(new ByteBufferMessageSet(NoCompressionCodec.instance, new AtomicLong(50), lastMessage));
        int position = 0;
        assertEquals("Should be able to find the first message by its offset",
                new OffsetPosition(0L, position),
                messageSet.searchFor(0, 0));
        position += MessageSets.entrySize(head(messageSet).message);
        assertEquals("Should be able to find second message when starting from 0",
                new OffsetPosition(1L, position),
                messageSet.searchFor(1, 0));
        assertEquals("Should be able to find second message starting from its offset",
                new OffsetPosition(1L, position),
                messageSet.searchFor(1, position));
        position += MessageSets.entrySize(head(tail(messageSet)).message) + MessageSets.entrySize(head(tail(tail(messageSet))).message);
        assertEquals("Should be able to find fourth message from a non-existant offset",
                new OffsetPosition(50L, position),
                messageSet.searchFor(3, position));
        assertEquals("Should be able to find fourth message by correct offset",
                new OffsetPosition(50L, position),
                messageSet.searchFor(50, position));
    }

    /**
     * Test that the message set iterator obeys start and end slicing
     */
    @Test
    public void testIteratorWithLimits() {
        MessageAndOffset message = Lists.newArrayList(messageSet).get(1);
        int start = messageSet.searchFor(1, 0).position;
        int size = message.message.size();
        FileMessageSet slice = messageSet.read(start, size);
        assertEquals(Lists.newArrayList(message), Lists.newArrayList(slice));
    }

    /**
     * Test the truncateTo method lops off messages and appropriately updates the size
     */
    @Test
    public void testTruncate() {
        MessageAndOffset message = Lists.newArrayList(messageSet).get(0);
        int end = messageSet.searchFor(1, 0).position;
        messageSet.truncateTo(end);
        assertEquals(Lists.newArrayList(message), Lists.newArrayList(messageSet));
        assertEquals(MessageSets.entrySize(message.message), messageSet.sizeInBytes());
    }
}
