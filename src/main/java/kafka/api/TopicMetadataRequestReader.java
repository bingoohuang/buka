package kafka.api;

import java.nio.ByteBuffer;

public class TopicMetadataRequestReader implements RequestReader {
    public static final RequestReader instance = new TopicMetadataRequestReader();

    @Override
    public RequestOrResponse readFrom(ByteBuffer buffer) {
        return null;
    }
}
