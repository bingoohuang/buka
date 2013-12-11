package kafka.api;

import java.nio.ByteBuffer;

public class FetchRequestReader implements RequestReader {
    public static final RequestReader instance = new FetchRequestReader();

    @Override
    public RequestOrResponse readFrom(ByteBuffer buffer) {
        return null;
    }
}
