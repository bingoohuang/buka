package kafka.message;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import kafka.log.FileMessageSet;
import org.junit.Test;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;

import static kafka.utils.TestUtils.checkEquals;
import static kafka.utils.TestUtils.tempFile;
import static org.junit.Assert.assertEquals;

public abstract class BaseMessageSetTestCases {
    protected List<Message> messages = new ArrayList<Message>() {{
        add(new Message("abcd".getBytes()));
        add(new Message("efgh".getBytes()));
        add(new Message("ijkl".getBytes()));
    }};

    public abstract MessageSet createMessageSet(List<Message> messages);

    @Test
    public void testWrittenEqualsRead() {
        MessageSet messageSet = createMessageSet(messages);
        List<Message> set = Lists.newArrayList();
        for(MessageAndOffset messageAndOffset : messageSet) {
            set.add(messageAndOffset.message);
        }
        checkEquals(messages.iterator(), set.iterator());
    }

    @Test
    public void testIteratorIsConsistent() {
        MessageSet m =  createMessageSet(messages);
        // two iterators over the same set should give the same results
        checkEquals(m.iterator(), m.iterator());
    }

    @Test
    public void testSizeInBytes() {
        assertEquals("Empty message set should have 0 bytes.",
                0,
                createMessageSet(Lists.<Message>newArrayList()).sizeInBytes());
        assertEquals("Predicted size should equal actual size.",
                MessageSets.messageSetSize(messages),
                createMessageSet(messages).sizeInBytes());
    }

    @Test
    public void testWriteTo() {
        // test empty message set
        testWriteToWithMessageSet(createMessageSet(Lists.<Message>newArrayList()));
        testWriteToWithMessageSet(createMessageSet(messages));
    }

    public void testWriteToWithMessageSet(MessageSet set) {
        // do the write twice to ensure the message set is restored to its orginal state
        try {
            for(int i : ImmutableList.of(0, 1)) {
                File file = tempFile();
                FileChannel channel = new RandomAccessFile(file, "rw").getChannel();
                int written = set.writeTo(channel, 0L, 1024);
                assertEquals("Expect to write the number of bytes in the set.", set.sizeInBytes(), written);
                FileMessageSet newSet = new FileMessageSet(file, channel);
                checkEquals(set.iterator(), newSet.iterator());
            }
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }
    }
}
