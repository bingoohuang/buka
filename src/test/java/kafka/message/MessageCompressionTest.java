package kafka.message;

import com.google.common.collect.Lists;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class MessageCompressionTest {
    @Test
    public void testSimpleCompressDecompress() {
        List<CompressionCodec> codecs = Lists.newArrayList(GZIPCompressionCodec.instance);
        if (isSnappyAvailable())
            codecs.add(SnappyCompressionCodec.instance);
        for (CompressionCodec codec : codecs)
            testSimpleCompressDecompress(codec);
    }

    public void testSimpleCompressDecompress(CompressionCodec compressionCodec) {
        List<Message> messages = Lists.newArrayList(new Message("hi there".getBytes()),
                new Message("I am fine".getBytes()),
                new Message("I am not so well today".getBytes()));
        ByteBufferMessageSet messageSet = new ByteBufferMessageSet(/*compressionCodec = */compressionCodec, messages);
        assertEquals(compressionCodec, messageSet.shallowIterator().next().message.compressionCodec());
        List<Message> decompressed = Lists.newArrayList();
        for (MessageAndOffset messageAndOffset : messageSet) {
            decompressed.add(messageAndOffset.message);
        }
        assertEquals(messages, decompressed);
    }

    @Test
    public void testComplexCompressDecompress() {
        List<Message> messages = Lists.newArrayList(new Message("hi there".getBytes()),
                new Message("I am fine".getBytes()), new Message("I am not so well today".getBytes()));
        ByteBufferMessageSet message = new ByteBufferMessageSet(/*compressionCodec = */DefaultCompressionCodec.instance,
                messages.subList(0, 2));
        List<Message> complexMessages = Lists.newArrayList(message.shallowIterator().next().message);
        complexMessages.addAll(messages.subList(2, 3));
        ByteBufferMessageSet complexMessage = new ByteBufferMessageSet(DefaultCompressionCodec.instance, complexMessages);
        List<Message> decompressedMessages = Lists.newArrayList();
        for (MessageAndOffset messageAndOffset : complexMessage) {
            decompressedMessages.add(messageAndOffset.message);
        }

        assertEquals(messages, decompressedMessages);
    }

    public boolean isSnappyAvailable() {
        try {
            new org.xerial.snappy.SnappyOutputStream(new ByteArrayOutputStream());
            return true;
        } catch (UnsatisfiedLinkError e) {
            return false;
        } catch (org.xerial.snappy.SnappyError e) {
            return false;
        } catch (IOException e) {
            return true;
        }
    }
}
