package kafka.api;

import kafka.common.ErrorMapping;

import java.nio.ByteBuffer;

public class UpdateMetadataResponse extends RequestOrResponse {


    public static UpdateMetadataResponse readFrom(ByteBuffer buffer) {
        int correlationId = buffer.getInt();
        short errorCode = buffer.getShort();
        return new UpdateMetadataResponse(correlationId, errorCode);
    }

    public short errorCode;

    public UpdateMetadataResponse(int correlationId) {
        this(correlationId, ErrorMapping.NoError);
    }

    public UpdateMetadataResponse(int correlationId, short errorCode) {
        super(correlationId);
        this.errorCode = errorCode;
    }

    @Override
    public int sizeInBytes() {
        return  4 /* correlation id */ + 2 /* error code */;
    }

    @Override
    public void writeTo(ByteBuffer buffer) {
        buffer.putInt(correlationId);
        buffer.putShort(errorCode);
    }
}
