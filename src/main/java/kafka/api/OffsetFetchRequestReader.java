package kafka.api;

import java.nio.ByteBuffer;

public class OffsetFetchRequestReader implements RequestReader{
    public static final RequestReader instance = new OffsetFetchRequestReader();

    @Override
    public RequestOrResponse readFrom(ByteBuffer buffer) {
        return null;
    }
}
