package kafka.api;

import java.nio.ByteBuffer;

public class OffsetCommitRequestReader implements RequestReader{
    public static final RequestReader instance = new OffsetCommitRequestReader();

    @Override
    public RequestOrResponse readFrom(ByteBuffer buffer) {
        return null;
    }
}
