package kafka.api;

import java.nio.ByteBuffer;

public interface RequestReader {
    RequestOrResponse readFrom(ByteBuffer buffer);
}
