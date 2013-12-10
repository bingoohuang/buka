package kafka.message;

import com.google.common.collect.Lists;
import kafka.common.KafkaException;
import kafka.utils.IteratorTemplate;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.GatheringByteChannel;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A sequence of messages stored in a byte buffer
 * <p/>
 * There are two ways to create a ByteBufferMessageSet
 * <p/>
 * Option 1: From a ByteBuffer which already contains the serialized message set. Consumers will use this method.
 * <p/>
 * Option 2: Give it a list of messages along with instructions relating to serialization format. Producers will use this method.
 */
public class ByteBufferMessageSet extends MessageSet {
    public final ByteBuffer buffer;

    public ByteBufferMessageSet(ByteBuffer buffer) {
        this.buffer = buffer;
    }

    private int shallowValidByteCount = -1;

    public ByteBufferMessageSet(CompressionCodec compressionCodec, Message... messages) {
        this(compressionCodec, Lists.newArrayList(messages));
    }
    public ByteBufferMessageSet(CompressionCodec compressionCodec, List<Message> messages) {
        this(ByteBufferMessageSets.create(new AtomicLong(0), compressionCodec, messages));
    }

    public ByteBufferMessageSet(CompressionCodec compressionCodec, AtomicLong offsetCounter, List<Message> messages) {
        this(ByteBufferMessageSets.create(offsetCounter, compressionCodec, messages));
    }

    public ByteBufferMessageSet(List<Message> messages) {
        this(NoCompressionCodec.instance, new AtomicLong(0), messages);
    }

    private int shallowValidBytes() {
        if (shallowValidByteCount < 0) {
            int bytes = 0;
            Iterator<MessageAndOffset> iter = this.internalIterator(true);
            while (iter.hasNext()) {
                MessageAndOffset messageAndOffset = iter.next();
                bytes += MessageSets.entrySize(messageAndOffset.message);
            }
            this.shallowValidByteCount = bytes;
        }
        return shallowValidByteCount;
    }

    /**
     * Write the messages in this set to the given channel
     */
    public int writeTo(GatheringByteChannel channel, long offset, int size) {
        // Ignore offset and size from input. We just want to write the whole buffer to the channel.
        buffer.mark();
        int written = 0;
        try {
            while (written < sizeInBytes())
                written += channel.write(buffer);
        } catch (IOException e) {
            throw new KafkaException(e);
        }
        buffer.reset();

        return written;
    }

    /**
     * default iterator that iterates over decompressed messages
     */
    @Override
    public Iterator<MessageAndOffset> iterator() {
        return internalIterator(false);
    }

    /**
     * iterator over compressed messages without decompressing
     */
    public Iterator<MessageAndOffset> shallowIterator() {
        return internalIterator(true);
    }

    /**
     * When flag isShallow is set to be true, we do a shallow iteration: just traverse the first level of messages. *
     */
    private Iterator<MessageAndOffset> internalIterator() {
        return internalIterator(false);
    }

    private Iterator<MessageAndOffset> internalIterator(final boolean isShallow /*= false*/) {
        return new IteratorTemplate<MessageAndOffset>() {
            ByteBuffer topIter = buffer.slice();
            Iterator<MessageAndOffset> innerIter = null;

            public boolean innerDone() {
                return (innerIter == null || !innerIter.hasNext());
            }

            public MessageAndOffset makeNextOuter() {
                // if there isn't at least an offset and size, we are done
                if (topIter.remaining() < 12)
                    return allDone();
                long offset = topIter.getLong();
                int size = topIter.getInt();
                if (size < Messages.MinHeaderSize)
                    throw new InvalidMessageException("Message found with corrupt size (" + size + ")");

                // we have an incomplete message
                if (topIter.remaining() < size)
                    return allDone();

                // read the current message and check correctness
                ByteBuffer message = topIter.slice();
                message.limit(size);
                topIter.position(topIter.position() + size);
                Message newMessage = new Message(message);

                if (isShallow) {
                    return new MessageAndOffset(newMessage, offset);
                } else {
                    if (newMessage.compressionCodec() == NoCompressionCodec.instance) {
                        innerIter = null;
                        return new MessageAndOffset(newMessage, offset);
                    } else {
                        innerIter = ByteBufferMessageSets.decompress(newMessage).internalIterator();
                        if (!innerIter.hasNext())
                            innerIter = null;
                        return makeNext();
                    }
                }
            }

            @Override
            public MessageAndOffset makeNext() {
                if (isShallow) {
                    return makeNextOuter();
                } else {
                    if (innerDone())
                        return makeNextOuter();
                    else
                        return innerIter.next();
                }
            }

        };
    }

    /**
     * Update the offsets for this message set. This method attempts to do an in-place conversion
     * if there is no compression, but otherwise recopies the messages
     */
    public ByteBufferMessageSet assignOffsets(AtomicLong offsetCounter, CompressionCodec codec) {
        if (codec == NoCompressionCodec.instance) {
            // do an in-place conversion
            int position = 0;
            buffer.mark();
            while (position < sizeInBytes() - MessageSets.LogOverhead) {
                buffer.position(position);
                buffer.putLong(offsetCounter.getAndIncrement());
                position += MessageSets.LogOverhead + buffer.getInt();
            }
            buffer.reset();
            return this;
        } else {
            // messages are compressed, crack open the messageset and recompress with correct offset
            Iterator<MessageAndOffset> iter = this.internalIterator(/*isShallow = */false);
            List<Message> messages = Lists.newArrayList();
            while (iter.hasNext()) {
                MessageAndOffset messageAndOffset = iter.next();
                messages.add(messageAndOffset.message);
            }

            return new ByteBufferMessageSet(/*compressionCodec = */codec,/* offsetCounter = */offsetCounter, messages);
        }
    }


    /**
     * The total number of bytes in this message set, including any partial trailing messages
     */
    public int sizeInBytes() {
        return buffer.limit();
    }

    /**
     * The total number of bytes in this message set not including any partial, trailing messages
     */
    public int validBytes() {
        return shallowValidBytes();
    }

    /**
     * Two message sets are equal if their respective byte buffers are equal
     */
    @Override
    public boolean equals(Object other) {
        if (other instanceof ByteBufferMessageSet) {
            return buffer.equals(((ByteBufferMessageSet) other).buffer);
        }

        return false;
    }

    @Override
    public int hashCode() {
        return buffer.hashCode();
    }
}