package kafka.network;

import kafka.api.RequestOrResponse;
import kafka.utils.NonThreadSafe;
import kafka.utils.Utils;

import java.nio.ByteBuffer;
import java.nio.channels.GatheringByteChannel;

@NonThreadSafe
public class BoundedByteBufferSend extends Send {
    public final ByteBuffer buffer;

    public BoundedByteBufferSend(ByteBuffer buffer) {
        this.buffer = buffer;
        init();
    }

    private ByteBuffer sizeBuffer = ByteBuffer.allocate(4);

    private void init() {
        // Avoid possibility of overflow for 2GB-4 byte buffer
        if (buffer.remaining() > Integer.MAX_VALUE - sizeBuffer.limit())
            throw new IllegalStateException("Attempt to create a bounded buffer of " + buffer.remaining() + " bytes, but the maximum " +
                    "allowable size for a bounded buffer is " + (Integer.MAX_VALUE - sizeBuffer.limit()) + ".");
        sizeBuffer.putInt(buffer.limit());
        sizeBuffer.rewind();

    }

    private boolean complete;

    public boolean complete() {
        return complete;
    }

    public BoundedByteBufferSend(int size) {
        this(ByteBuffer.allocate(size));
    }

    public BoundedByteBufferSend(RequestOrResponse request) {
        this(request.sizeInBytes() + (request.requestId != null ? 2 : 0));
        if (request.requestId != null) {
            buffer.putShort(request.requestId);
        }

        request.writeTo(buffer);
        buffer.rewind();
    }


    public int writeTo(GatheringByteChannel channel) {
        expectIncomplete();
        long written = Utils.write(channel, new ByteBuffer[]{sizeBuffer, buffer});
        // if we are done, mark it off
        if (!buffer.hasRemaining())
            complete = true;
        return (int) written;
    }
}
