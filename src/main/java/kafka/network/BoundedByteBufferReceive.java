package kafka.network;

import com.google.common.base.Throwables;
import kafka.utils.NonThreadSafe;
import kafka.utils.Utils;

import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;

/**
 * Represents a communication between the client and server
 */
@NonThreadSafe
public class BoundedByteBufferReceive extends Receive {
    public int maxSize;

    public BoundedByteBufferReceive(int maxSize) {
        this.maxSize = maxSize;
    }

    private ByteBuffer sizeBuffer = ByteBuffer.allocate(4);
    private ByteBuffer contentBuffer = null;

    public BoundedByteBufferReceive() {
        this(Integer.MAX_VALUE);
    }

    private boolean complete = false;

    /**
     * Get the content buffer for this transmission
     */
    @Override
    public ByteBuffer buffer() {
        expectComplete();
        return contentBuffer;
    }


    /**
     * Read the bytes in this response from the given channel
     */
    @Override
    public int readFrom(ReadableByteChannel channel) {
        expectIncomplete();
        int read = 0;
        // have we read the request size yet?
        if (sizeBuffer.remaining() > 0)
            read += Utils.read(channel, sizeBuffer);
        // have we allocated the request buffer yet?
        if (contentBuffer == null && !sizeBuffer.hasRemaining()) {
            sizeBuffer.rewind();
            int size = sizeBuffer.getInt();
            if (size <= 0)
                throw new InvalidRequestException(size + " is not a valid request size.");
            if (size > maxSize)
                throw new InvalidRequestException(String.format(
                        "Request of length %d is not valid, it is larger than the maximum size of %d bytes.", size, maxSize));
            contentBuffer = byteBufferAllocate(size);
        }
        // if we have a buffer read some stuff into it
        if (contentBuffer != null) {
            read = Utils.read(channel, contentBuffer);
            // did we get everything?
            if (!contentBuffer.hasRemaining()) {
                contentBuffer.rewind();
                complete = true;
            }
        }
        return read;
    }

    private ByteBuffer byteBufferAllocate(int size) {
        ByteBuffer buffer;
        try {
            buffer = ByteBuffer.allocate(size);
        } catch (OutOfMemoryError e) {
            logger.error("OOME with size {}", size, e);
            throw e;
        } catch (Throwable e) {
            throw Throwables.propagate(e);
        }
        return buffer;
    }

    @Override
    public boolean complete() {
        return complete;
    }
}
