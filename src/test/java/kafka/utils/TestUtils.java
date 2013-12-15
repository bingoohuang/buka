package kafka.utils;

import kafka.common.KafkaException;
import kafka.message.Message;
import kafka.message.MessageAndOffset;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class TestUtils {
    public static Random random = new Random();

    /**
     * Check that the buffer content from buffer.position() to buffer.limit() is equal
     */
    public static void checkEquals(ByteBuffer b1, ByteBuffer b2) {
        assertEquals("Buffers should have equal length", b1.limit() - b1.position(), b2.limit() - b2.position());
        for (int i = 0, ii = b1.limit() - b1.position(); i < ii; ++i)
            assertEquals("byte " + i + " byte not equal.", b1.get(b1.position() + i), b2.get(b1.position() + i));
    }

    /**
     * Throw an exception if the two iterators are of differing lengths or contain
     * different messages on their Nth element
     */
    public static <T> void checkEquals(Iterator<T> expected, Iterator<T> actual) {
        int length = 0;
        while(expected.hasNext() && actual.hasNext()) {
            length += 1;
            assertEquals(expected.next(), actual.next());
        }

        // check if the expected iterator is longer
        if (expected.hasNext()) {
            int length1 = length;
            while (expected.hasNext()) {
                expected.next();
                length1 += 1;
            }
            assertFalse("Iterators have uneven length-- first has more: "+length1 + " > " + length, true);
        }

        // check if the actual iterator was longer
        if (actual.hasNext()) {
            int length2 = length;
            while (actual.hasNext()) {
                actual.next();
                length2 += 1;
            }
            assertFalse("Iterators have uneven length-- second has more: "+length2 + " > " + length, true);
        }
    }

    /**
     * Create a temporary file
     */
    public static File tempFile() {
        File f = null;
        try {
            f = File.createTempFile("kafka", ".tmp");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        f.deleteOnExit();
        return f;
    }


    public static Iterator<Message> getMessageIterator(final Iterator<MessageAndOffset> iter) {
        return new IteratorTemplate<Message>() {
            @Override
            public Message makeNext() {
            if (iter.hasNext())
                return iter.next().message;
            else
                return allDone();
            }
        };
    }

    public static void writeNonsenseToFile(File fileName, long position, int size) {
        try {
            RandomAccessFile file = new RandomAccessFile(fileName, "rw");
            file.seek(position);
            for(int i = 0; i <  size; ++i)
                file.writeByte(random.nextInt(255));
            file.close();
        } catch (IOException e) {
            throw new KafkaException(e);
        }
    }
}
