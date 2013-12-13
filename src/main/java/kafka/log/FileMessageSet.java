package kafka.log;

import kafka.common.KafkaException;
import kafka.message.*;
import kafka.utils.IteratorTemplate;
import kafka.utils.NonThreadSafe;
import kafka.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.GatheringByteChannel;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;

@NonThreadSafe
public class FileMessageSet extends MessageSet implements Closeable{
    public volatile File file;
    private FileChannel channel;
    private final int start;
    private final int end;
    public final boolean isSlice;

    /**
     * An on-disk message set. An optional start and end position can be applied to the message set
     * which will allow slicing a subset of the file.
     *
     * @param file    The file name for the underlying log data
     * @param channel the underlying file channel used
     * @param start   A lower bound on the absolute position in the file from which the message set begins
     * @param end     The upper bound on the absolute position in the file at which the message set ends
     * @param isSlice Should the start and end parameters be used for slicing?
     */
    public FileMessageSet(File file, FileChannel channel, int start, int end, boolean isSlice) {
        this.file = file;
        this.channel = channel;
        this.start = start;
        this.end = end;
        this.isSlice = isSlice;

        init();
    }

    /* the size of the message set in bytes */
    private AtomicInteger _size;


    public void init() {
        try {
            _size = (isSlice)
                    ? new AtomicInteger(end - start) // don't check the file size if this is just a slice view
                    : new AtomicInteger(Math.min((int) channel.size(), end) - start);

            /* if this is not a slice, update the file pointer to the end of the file */
            if (!isSlice)
                /* set the file position to the last byte in the file */
                channel.position(channel.size());
        } catch (IOException e) {
            throw new KafkaException(e);
        }
    }

    /**
     * Create a file message set with no slicing.
     */
    public FileMessageSet(File file, FileChannel channel) {
        this(file, channel, /*start = */0, /*end = */Integer.MAX_VALUE, /*isSlice = */false);
    }

    /**
     * Create a file message set with no slicing
     */
    public FileMessageSet(File file) {
        this(file, Utils.openChannel(file,/* mutable = */true));
    }

    /**
     * Create a file message set with mutable option
     */
    public FileMessageSet(File file, boolean mutable) {
        this(file, Utils.openChannel(file, mutable));
    }

    /**
     * Create a slice view of the file message set that begins and ends at the given byte offsets
     */
    public FileMessageSet(File file, FileChannel channel, int start, int end) {
        this(file, channel, start, end, /*isSlice = */true);
    }

    /**
     * Return a message set which is a view into this set starting from the given position and with the given size limit.
     * <p/>
     * If the size is beyond the end of the file, the end will be based on the size of the file at the time of the read.
     * <p/>
     * If this message set is already sliced, the position will be taken relative to that slicing.
     *
     * @param position The start position to begin the read from
     * @param size     The number of bytes after the start position to include
     * @return A sliced wrapper on this message set limited based on the given position and size
     */
    public FileMessageSet read(int position, int size) {
        if (position < 0)
            throw new IllegalArgumentException("Invalid position: " + position);
        if (size < 0)
            throw new IllegalArgumentException("Invalid size: " + size);
        return new FileMessageSet(file,
                channel,
                /*start = */this.start + position,
                /*end = */Math.min(this.start + position + size, sizeInBytes()));
    }

    /**
     * Search forward for the file position of the last offset that is greater than or equal to the target offset
     * and return its physical position. If no such offsets are found, return null.
     *
     * @param targetOffset     The offset to search for.
     * @param startingPosition The starting position in the file to begin searching from.
     */
    public OffsetPosition searchFor(long targetOffset, int startingPosition) {
        int position = startingPosition;
        ByteBuffer buffer = ByteBuffer.allocate(MessageSets.LogOverhead);
        int size = sizeInBytes();
        try {
            while (position + MessageSets.LogOverhead < size) {
                buffer.rewind();
                channel.read(buffer, position);
                if (buffer.hasRemaining())
                    throw new IllegalStateException(String.format(
                            "Failed to read complete buffer for targetOffset %d startPosition %d in %s"
                            , targetOffset, startingPosition, file.getAbsolutePath()));
                buffer.rewind();
                long offset = buffer.getLong();
                if (offset >= targetOffset)
                    return new OffsetPosition(offset, position);
                int messageSize = buffer.getInt();
                if (messageSize < Messages.MessageOverhead)
                    throw new IllegalStateException("Invalid message size: " + messageSize);
                position += MessageSets.LogOverhead + messageSize;
            }
        } catch (IOException e) {
            throw new KafkaException(e);
        }

        return null;
    }

    Logger logger = LoggerFactory.getLogger(FileMessageSet.class);

    /**
     * Write some of this set to the given channel.
     *
     * @param destChannel   The channel to write to.
     * @param writePosition The position in the message set to begin writing from.
     * @param size          The maximum number of bytes to write
     * @return The number of bytes actually written.
     */
    public int writeTo(GatheringByteChannel destChannel, long writePosition, int size) {
        try {
            // Ensure that the underlying size has not changed.
            int newSize = (int) (Math.min(channel.size(), end) - start);
            if (newSize < _size.get()) {
                throw new KafkaException(String.format(
                        "Size of FileMessageSet %s has been truncated during write: old size %d, new size %d"
                        , file.getAbsolutePath(), _size.get(), newSize));
            }
            int bytesTransferred = (int) channel.transferTo(start + writePosition, Math.min(size, sizeInBytes()), destChannel);
            logger.trace("FileMessageSet {} : bytes transferred : {} bytes requested for transfer : {}",
                    file.getAbsolutePath(), bytesTransferred, Math.min(size, sizeInBytes()));
            return bytesTransferred;
        } catch (IOException e) {
            throw new KafkaException(e);
        }
    }

    /**
     * Get a shallow iterator over the messages in the set.
     */
    @Override
    public Iterator<MessageAndOffset> iterator() {
        return iterator(Integer.MAX_VALUE);
    }

    /**
     * Get an iterator over the messages in the set. We only do shallow iteration here.
     *
     * @param maxMessageSize A limit on allowable message size to avoid allocating unbounded memory.
     *                       If we encounter a message larger than this we throw an InvalidMessageException.
     * @return The iterator.
     */
    public Iterator<MessageAndOffset> iterator(final int maxMessageSize) {
        return new IteratorTemplate<MessageAndOffset>() {
            int location = start;
            ByteBuffer sizeOffsetBuffer = ByteBuffer.allocate(12);

            @Override
            public MessageAndOffset makeNext() {
                if (location >= end)
                    return allDone();

                // read the size of the item
                sizeOffsetBuffer.rewind();
                Utils.readChannel(channel, sizeOffsetBuffer, location);
                if (sizeOffsetBuffer.hasRemaining())
                    return allDone();

                sizeOffsetBuffer.rewind();
                long offset = sizeOffsetBuffer.getLong();
                int size = sizeOffsetBuffer.getInt();
                if (size < Messages.MinHeaderSize)
                    return allDone();
                if (size > maxMessageSize)
                    throw new InvalidMessageException("Message size exceeds the largest allowable message size (%d).", maxMessageSize);

                // read the item itself
                ByteBuffer buffer = ByteBuffer.allocate(size);
                Utils.readChannel(channel, buffer, location + 12);
                if (buffer.hasRemaining())
                    return allDone();
                buffer.rewind();

                // increment the location and return the item
                location += size + 12;
                return new MessageAndOffset(new Message(buffer), offset);
            }
        };
    }

    /**
     * The number of bytes taken up by this file set
     */
    public int sizeInBytes() {
        return _size.get();
    }

    /**
     * Append these messages to the message set
     */
    public void append(ByteBufferMessageSet messages) {
        int written = messages.writeTo(channel, 0, messages.sizeInBytes());
        _size.getAndAdd(written);
    }

    /**
     * Commit all written data to the physical disk
     */
    public void flush() {
        Utils.force(channel, true);
    }

    /**
     * Close this message set
     */
    public void close() {
        flush();
        Utils.closeQuietly(channel);
    }

    /**
     * Delete this message set from the filesystem
     *
     * @return True iff this message set was deleted.
     */
    public boolean delete() {
        Utils.closeQuietly(channel);
        return file.delete();
    }

    /**
     * Truncate this file message set to the given size in bytes. Note that this API does no checking that the
     * given size falls on a valid message boundary.
     *
     * @param targetSize The size to truncate to.
     * @return The number of bytes truncated off
     */
    public int truncateTo(int targetSize) {
        int originalSize = sizeInBytes();
        if (targetSize > originalSize || targetSize < 0)
            throw new KafkaException("Attempt to truncate log segment to %d bytes failed, " +
                    " size of this log segment is %d bytes.", targetSize, originalSize);
        Utils.truncate(channel, targetSize);
        Utils.position(channel, targetSize);
        _size.set(targetSize);
        return originalSize - targetSize;
    }

    /**
     * Read from the underlying file into the buffer starting at the given position
     */
    public ByteBuffer readInto(ByteBuffer buffer, int position) {
        Utils.read(channel, buffer, position);
        buffer.flip();
        return buffer;
    }

    /**
     * Rename the file that backs this message set
     *
     * @return true iff the rename was successful
     */
    public boolean renameTo(File f) {
        boolean success = this.file.renameTo(f);
        this.file = f;
        return success;
    }
}
