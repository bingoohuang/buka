package kafka.api;

import java.nio.ByteBuffer;

public class ControlledShutdownRequestReader implements RequestReader{
    public static final RequestReader instance = new ControlledShutdownRequestReader();

    public static final short CurrentVersion = 0;
    public static final String DefaultClientId = "";

    @Override
    public RequestOrResponse readFrom(ByteBuffer buffer) {
        short versionId = buffer.getShort();
        int correlationId = buffer.getInt();
        int brokerId = buffer.getInt();
        return new ControlledShutdownRequest(versionId, correlationId, brokerId);
    }
}
