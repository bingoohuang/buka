package kafka.api;

import java.nio.ByteBuffer;

public class ProducerRequestReader implements RequestReader {
    public static final RequestReader instance = new ProducerRequestReader();

    @Override
    public RequestOrResponse readFrom(ByteBuffer buffer) {
        return null;
    }
}
