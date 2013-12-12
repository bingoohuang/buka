package kafka.api;

import kafka.network.Request;
import kafka.network.RequestChannel;

import java.nio.ByteBuffer;

public abstract class RequestOrResponse {
    public Short requestId;
    public final int correlationId;

    public RequestOrResponse(int correlationId) {
        this(null, correlationId);
    }

    public RequestOrResponse(Short requestId, int correlationId) {
        this.requestId = requestId;
        this.correlationId = correlationId;
    }

    public abstract int sizeInBytes();

    public abstract void writeTo(ByteBuffer buffer);

    public void handleError(Throwable e, RequestChannel requestChannel, Request request) {
    }
}
