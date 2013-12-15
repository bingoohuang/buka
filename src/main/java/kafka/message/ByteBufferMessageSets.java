package kafka.message;

import kafka.common.KafkaException;
import kafka.utils.Utils;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class ByteBufferMessageSets {

    public static ByteBuffer create(AtomicLong offsetCounter, CompressionCodec compressionCodec, List<Message> messages) {
        if (messages.size() == 0) {
            return MessageSets.Empty.buffer;
        } else if (compressionCodec == NoCompressionCodec.instance) {
            ByteBuffer buffer = ByteBuffer.allocate(MessageSets.messageSetSize(messages));
            for (Message message : messages)
                writeMessage(buffer, message, offsetCounter.getAndIncrement());
            buffer.rewind();
            return buffer;
        } else {
            ByteArrayOutputStream byteArrayStream = new ByteArrayOutputStream(MessageSets.messageSetSize(messages));
            DataOutputStream output = new DataOutputStream(CompressionFactory.apply(compressionCodec, byteArrayStream));
            long offset = -1L;
            try {
                for (Message message : messages) {
                    offset = offsetCounter.getAndIncrement();
                    output.writeLong(offset);
                    output.writeInt(message.size());
                    output.write(message.buffer.array(), message.buffer.arrayOffset(), message.buffer.limit());
                }
            } catch (IOException e) {
                throw new KafkaException(e);
            } finally {
                Utils.closeQuietly(output);
            }
            byte[] bytes = byteArrayStream.toByteArray();
            Message message = new Message(bytes, compressionCodec);
            ByteBuffer buffer = ByteBuffer.allocate(message.size() + MessageSets.LogOverhead);
            writeMessage(buffer, message, offset);
            buffer.rewind();
            return buffer;
        }
    }

    public static ByteBufferMessageSet decompress(Message message) {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        InputStream inputStream = new ByteBufferBackedInputStream(message.payload());
        byte[] intermediateBuffer = new byte[1024];
        InputStream compressed = CompressionFactory.apply(message.compressionCodec(), inputStream);
        try {
            int dataRead = 0;
            while ((dataRead = compressed.read(intermediateBuffer)) > 0) {
                outputStream.write(intermediateBuffer, 0, dataRead);
            }
        } catch (IOException e) {
            throw new KafkaException(e);
        } finally {
            Utils.closeQuietly(compressed);
        }
        ByteBuffer outputBuffer = ByteBuffer.allocate(outputStream.size());
        outputBuffer.put(outputStream.toByteArray());
        outputBuffer.rewind();
        return new ByteBufferMessageSet(outputBuffer);
    }

    public static void writeMessage(ByteBuffer buffer, Message message, long offset) {
        buffer.putLong(offset);
        buffer.putInt(message.size());
        buffer.put(message.buffer);
        message.buffer.rewind();
    }
}
