package kafka.api;

import java.nio.ByteBuffer;

public class StopReplicaRequestReader implements RequestReader{
    public static final RequestReader instance = new StopReplicaRequestReader();

    @Override
    public RequestOrResponse readFrom(ByteBuffer buffer) {
        return null;
    }
}
