package kafka.api;

import java.nio.ByteBuffer;

public class FetchRequestReader implements RequestReader {
    public static final short CurrentVersion = 0;
    public static final int DefaultMaxWait = 0;
    public static final int DefaultMinBytes = 0;
    public static final int DefaultCorrelationId = 0;

    public static final RequestReader instance = new FetchRequestReader();

    @Override
    public RequestOrResponse readFrom(ByteBuffer buffer) {
        return null;
    }
}
