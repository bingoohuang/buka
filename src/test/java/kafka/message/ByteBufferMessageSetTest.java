package kafka.message;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import kafka.utils.TestUtils;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ByteBufferMessageSetTest extends BaseMessageSetTestCases {
    @Override
    public MessageSet createMessageSet(List<Message> messages) {
        return new ByteBufferMessageSet(NoCompressionCodec.instance, messages);
    }

    @Test
    public void testValidBytes() {
        {
            ByteBufferMessageSet messages = new ByteBufferMessageSet(NoCompressionCodec.instance,
                    Lists.newArrayList(new Message("hello".getBytes()), new Message("there".getBytes())));
            ByteBuffer buffer = ByteBuffer.allocate(messages.sizeInBytes() + 2);
            buffer.put(messages.buffer);
            buffer.putShort((short) 4);
            ByteBufferMessageSet messagesPlus = new ByteBufferMessageSet(buffer);
            assertEquals("Adding invalid bytes shouldn't change byte count", messages.validBytes(), messagesPlus.validBytes());
        }

        // test valid bytes on empty ByteBufferMessageSet
        {
            assertEquals("Valid bytes on an empty ByteBufferMessageSet should return 0", 0,
                    MessageSets.Empty.validBytes());
        }
    }

    @Test
    public void testValidBytesWithCompression() {
        {
            ByteBufferMessageSet messages = new ByteBufferMessageSet(DefaultCompressionCodec.instance,
                    new Message("hello".getBytes()), new Message("there".getBytes()));
            ByteBuffer buffer = ByteBuffer.allocate(messages.sizeInBytes() + 2);
            buffer.put(messages.buffer);
            buffer.putShort((short) 4);
            ByteBufferMessageSet messagesPlus = new ByteBufferMessageSet(buffer);
            assertEquals("Adding invalid bytes shouldn't change byte count", messages.validBytes(), messagesPlus.validBytes());
        }
    }

    @Test
    public void testEquals() {
        ByteBufferMessageSet messages = new ByteBufferMessageSet(DefaultCompressionCodec.instance,
                new Message("hello".getBytes()), new Message("there".getBytes()));
        ByteBufferMessageSet moreMessages = new ByteBufferMessageSet(DefaultCompressionCodec.instance,
                new Message("hello".getBytes()), new Message("there".getBytes()));

        assertTrue(messages.equals(moreMessages));

        messages = new ByteBufferMessageSet(NoCompressionCodec.instance,
                new Message("hello".getBytes()), new Message("there".getBytes()));
        moreMessages = new ByteBufferMessageSet(NoCompressionCodec.instance,
                new Message("hello".getBytes()), new Message("there".getBytes()));

        assertTrue(messages.equals(moreMessages));
    }


    @Test
    public void testIterator() {
        List<Message> messageList = Lists.newArrayList(
                new Message("msg1".getBytes()),
                new Message("msg2".getBytes()),
                new Message("msg3".getBytes())
        );

        // test for uncompressed regular messages
        {
            ByteBufferMessageSet messageSet = new ByteBufferMessageSet(NoCompressionCodec.instance, messageList);
            TestUtils.checkEquals(messageList.iterator(), TestUtils.getMessageIterator(messageSet.iterator()));
            //make sure ByteBufferMessageSet is re-iterable.
            TestUtils.checkEquals(messageList.iterator(), TestUtils.getMessageIterator(messageSet.iterator()));

            //make sure shallow iterator is the same as deep iterator
            TestUtils.checkEquals(TestUtils.getMessageIterator(messageSet.shallowIterator()),
                    TestUtils.getMessageIterator(messageSet.iterator()));
        }

        // test for compressed regular messages
        {
            ByteBufferMessageSet messageSet = new ByteBufferMessageSet(DefaultCompressionCodec.instance, messageList);
            TestUtils.checkEquals(messageList.iterator(), TestUtils.getMessageIterator(messageSet.iterator()));
            //make sure ByteBufferMessageSet is re-iterable.
            TestUtils.checkEquals(messageList.iterator(), TestUtils.getMessageIterator(messageSet.iterator()));
            verifyShallowIterator(messageSet);
        }

        // test for mixed empty and non-empty messagesets uncompressed
        {
            List<Message> emptyMessageList = Lists.newArrayList();
            ByteBufferMessageSet emptyMessageSet = new ByteBufferMessageSet(NoCompressionCodec.instance, emptyMessageList);
            ByteBufferMessageSet regularMessgeSet = new ByteBufferMessageSet(NoCompressionCodec.instance, messageList);
            ByteBuffer buffer = ByteBuffer.allocate(emptyMessageSet.buffer.limit() + regularMessgeSet.buffer.limit());
            buffer.put(emptyMessageSet.buffer);
            buffer.put(regularMessgeSet.buffer);
            buffer.rewind();
            ByteBufferMessageSet mixedMessageSet = new ByteBufferMessageSet(buffer);
            TestUtils.checkEquals(messageList.iterator(), TestUtils.getMessageIterator(mixedMessageSet.iterator()));
            //make sure ByteBufferMessageSet is re-iterable.
            TestUtils.checkEquals(messageList.iterator(), TestUtils.getMessageIterator(mixedMessageSet.iterator()));
            //make sure shallow iterator is the same as deep iterator
            TestUtils.checkEquals(TestUtils.getMessageIterator(mixedMessageSet.shallowIterator()),
                    TestUtils.getMessageIterator(mixedMessageSet.iterator()));
        }

        // test for mixed empty and non-empty messagesets compressed
        {
            List<Message> emptyMessageList = Lists.newArrayList();
            ByteBufferMessageSet emptyMessageSet = new ByteBufferMessageSet(DefaultCompressionCodec.instance, emptyMessageList);
            ByteBufferMessageSet regularMessgeSet = new ByteBufferMessageSet(DefaultCompressionCodec.instance, messageList);
            ByteBuffer buffer = ByteBuffer.allocate(emptyMessageSet.buffer.limit() + regularMessgeSet.buffer.limit());
            buffer.put(emptyMessageSet.buffer);
            buffer.put(regularMessgeSet.buffer);
            buffer.rewind();
            ByteBufferMessageSet mixedMessageSet = new ByteBufferMessageSet(buffer);
            TestUtils.checkEquals(messageList.iterator(), TestUtils.getMessageIterator(mixedMessageSet.iterator()));
            //make sure ByteBufferMessageSet is re-iterable.
            TestUtils.checkEquals(messageList.iterator(), TestUtils.getMessageIterator(mixedMessageSet.iterator()));
            verifyShallowIterator(mixedMessageSet);
        }
    }

    @Test
    public void testOffsetAssignment() {
        ByteBufferMessageSet messages = new ByteBufferMessageSet(NoCompressionCodec.instance,
                new Message("hello".getBytes()),
                new Message("there".getBytes()),
                new Message("beautiful".getBytes()));

        List<Message> listMessages = Lists.newArrayList();
        for (MessageAndOffset messageAndOffset : messages) {
            listMessages.add(messageAndOffset.message);
        }
        ByteBufferMessageSet compressedMessages = new ByteBufferMessageSet(DefaultCompressionCodec.instance,
                listMessages);
        // check uncompressed offsets 
        checkOffsets(messages, 0);
        int offset = 1234567;
        checkOffsets(messages.assignOffsets(new AtomicLong(offset), NoCompressionCodec.instance), offset);

        // check compressed messages
        checkOffsets(compressedMessages, 0);
        checkOffsets(compressedMessages.assignOffsets(new AtomicLong(offset), DefaultCompressionCodec.instance), offset);
    }

    /* check that offsets are assigned based on byte offset from the given base offset */
    public void checkOffsets(ByteBufferMessageSet messages, long baseOffset) {
        long offset = baseOffset;
        for (MessageAndOffset entry : messages) {
            assertEquals("Unexpected offset in message set iterator", offset, entry.offset);
            offset += 1;
        }
    }

    public void verifyShallowIterator(ByteBufferMessageSet messageSet) {
        //make sure the offsets returned by a shallow iterator is a subset of that of a deep iterator
        Iterator<MessageAndOffset> messageAndOffsetIterator = messageSet.shallowIterator();
        Set<Long> shallowOffsets = Sets.newHashSet();
        while (messageAndOffsetIterator.hasNext()) {
            MessageAndOffset next = messageAndOffsetIterator.next();
            shallowOffsets.add(next.offset);
        }

        Iterator<MessageAndOffset> iterator = messageSet.iterator();
        Set<Long> deepOffsets = Sets.newHashSet();
        while (iterator.hasNext()) {
            MessageAndOffset next = iterator.next();
            deepOffsets.add(next.offset);
        }


        assertTrue(deepOffsets.containsAll(shallowOffsets));
    }
}
