package kafka.api;

import java.nio.ByteBuffer;

public class OffsetRequestReader implements RequestReader{
    public static final RequestReader instance = new OffsetRequestReader();

    public static final short CurrentVersion = 0;
    public static final String DefaultClientId = "";

    public static final String SmallestTimeString = "smallest";
    public static final String LargestTimeString = "largest";
    public static final long LatestTime = -1L;
    public static final long EarliestTime = -2L;

    @Override
    public RequestOrResponse readFrom(ByteBuffer buffer) {
        return null;
    }
}
