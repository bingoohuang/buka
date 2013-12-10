package kafka.utils;

import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;

public class TestUtils {
    /**
     * Check that the buffer content from buffer.position() to buffer.limit() is equal
     */
    public static void checkEquals(ByteBuffer b1, ByteBuffer b2) {
        assertEquals("Buffers should have equal length", b1.limit() - b1.position(), b2.limit() - b2.position());
        for (int i = 0, ii = b1.limit() - b1.position(); i < ii; ++i)
            assertEquals("byte " + i + " byte not equal.", b1.get(b1.position() + i), b2.get(b1.position() + i));
    }
}
