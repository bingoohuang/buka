package kafka.api;

import java.nio.ByteBuffer;

public class LeaderAndIsrRequestReader implements RequestReader{
    public static final RequestReader instance = new LeaderAndIsrRequestReader();

    @Override
    public RequestOrResponse readFrom(ByteBuffer buffer) {
        return null;
    }
}
