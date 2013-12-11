package kafka.api;

import java.nio.ByteBuffer;

public class UpdateMetadataRequestReader implements RequestReader{
    public static final RequestReader instance = new UpdateMetadataRequestReader();
    @Override
    public RequestOrResponse readFrom(ByteBuffer buffer) {
        return null;
    }
}
