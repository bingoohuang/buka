package kafka.message;

import com.google.common.collect.Lists;
import kafka.utils.TestUtils;
import kafka.utils.Utils;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;

import static org.junit.Assert.*;

public class MessageTest {
    public static class MessageTestVal {
        public final byte[] key;
        public final byte[] payload;
        public final CompressionCodec codec;
        public final Message message;

        public MessageTestVal(byte[] key, byte[] payload, CompressionCodec codec, Message message) {
            this.key = key;
            this.payload = payload;
            this.codec = codec;
            this.message = message;
        }
    }

    List<MessageTestVal> messages = Lists.newArrayList();

    @Before
    public void setUp() {
        byte[][] keys = new byte[][]{null, "key".getBytes(), "".getBytes()};
        byte[][] vals = new byte[][]{"value".getBytes(), "".getBytes(), null};
        CompressionCodec[] codecs = new CompressionCodec[]{NoCompressionCodec.instance, GZIPCompressionCodec.instance};
        for (byte[] k : keys) {
            for (byte[] v : vals) {
                for (CompressionCodec codec : codecs) {
                    messages.add(new MessageTestVal(k, v, codec, new Message(v, k, codec)));
                }
            }
        }

    }

    @Test
    public void testFieldValues() {
        for (MessageTestVal v : messages) {
            if (v.payload == null) {
                assertTrue(v.message.isNull());
                assertEquals("Payload should be null", null, v.message.payload());
            } else {
                TestUtils.checkEquals(ByteBuffer.wrap(v.payload), v.message.payload());
            }
            assertEquals(Messages.CurrentMagicValue, v.message.magic());
            if (v.message.hasKey())
                TestUtils.checkEquals(ByteBuffer.wrap(v.key), v.message.key());
            else
                assertEquals(null, v.message.key());
            assertEquals(v.codec, v.message.compressionCodec());
        }
    }

    @Test
    public void testChecksum() {
        for (MessageTestVal v : messages) {
            assertTrue("Auto-computed checksum should be valid", v.message.isValid());
            // garble checksum
            int badChecksum = (int) ((v.message.checksum() + 1) % Integer.MAX_VALUE);
            Utils.writeUnsignedInt(v.message.buffer, Messages.CrcOffset, badChecksum);
            assertFalse("Message with invalid checksum should be invalid", v.message.isValid());
        }
    }

    @Test
    public void testEquality() {
        for (MessageTestVal v : messages) {
            assertFalse("Should not equal null", v.message.equals(null));
            assertFalse("Should not equal a random string", v.message.equals("asdf"));
            assertTrue("Should equal itself", v.message.equals(v.message));
            Message copy = new Message(/*bytes = */v.payload, /*key = */v.key, /*codec = */v.codec);
            assertTrue("Should equal another message with the same content.", v.message.equals(copy));
        }
    }

    @Test
    public void testIsHashable() {
        // this is silly, but why not
        HashMap<Message, Message> m = new HashMap<Message, Message>();
        for (MessageTestVal v : messages)
            m.put(v.message, v.message);
        for (MessageTestVal v : messages)
            assertEquals(v.message, m.get(v.message));
    }
}
