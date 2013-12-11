package kafka.api;

import java.nio.ByteBuffer;

public class OffsetRequestReader implements RequestReader{
    public static final RequestReader instance = new OffsetRequestReader();

    @Override
    public RequestOrResponse readFrom(ByteBuffer buffer) {
        return null;
    }
}
