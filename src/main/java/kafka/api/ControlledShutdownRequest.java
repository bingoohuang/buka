package kafka.api;

import com.google.common.collect.Sets;
import kafka.common.ErrorMapping;
import kafka.common.TopicAndPartition;
import kafka.network.BoundedByteBufferSend;
import kafka.network.Request;
import kafka.network.RequestChannel;
import kafka.network.Response;

import java.nio.ByteBuffer;

public class ControlledShutdownRequest extends RequestOrResponse {
    public final short versionId;
    public final int brokerId;

    public ControlledShutdownRequest(short versionId, int correlationId, int brokerId) {
        super(RequestKeys.ControlledShutdownKey, correlationId);

        this.versionId = versionId;
        this.brokerId = brokerId;
    }

    public ControlledShutdownRequest(int correlationId, int brokerId) {
        this(ControlledShutdownRequestReader.CurrentVersion, correlationId, brokerId);
    }

    public void writeTo(ByteBuffer buffer) {
        buffer.putShort(versionId);
        buffer.putInt(correlationId);
        buffer.putInt(brokerId);
    }

    public int sizeInBytes() {
        return 2 +  /* version id */
                4 + /* correlation id */
                4 /* broker id */;
    }

    @Override
    public String toString() {
        StringBuilder controlledShutdownRequest = new StringBuilder();
        controlledShutdownRequest.append("Name: " + this.getClass().getSimpleName());
        controlledShutdownRequest.append("; Version: " + versionId);
        controlledShutdownRequest.append("; CorrelationId: " + correlationId);
        controlledShutdownRequest.append("; BrokerId: " + brokerId);
        return controlledShutdownRequest.toString();
    }

    @Override
    public void handleError(Throwable e, RequestChannel requestChannel, Request request) {
        ControlledShutdownResponse errorResponse =
                new ControlledShutdownResponse(correlationId, ErrorMapping.codeFor(e.getClass()), Sets.<TopicAndPartition>newHashSet());
        requestChannel.sendResponse(new Response(request, new BoundedByteBufferSend(errorResponse)));
    }
}
