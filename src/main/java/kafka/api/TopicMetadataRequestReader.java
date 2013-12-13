package kafka.api;

import kafka.utils.Function1;
import kafka.utils.Range;
import kafka.utils.Utils;

import java.nio.ByteBuffer;
import java.util.List;

import static kafka.api.ApiUtils.readIntInRange;
import static kafka.api.ApiUtils.readShortString;

public class TopicMetadataRequestReader implements RequestReader {
    public static final RequestReader instance = new TopicMetadataRequestReader();

    public static short CurrentVersion = 0;
    public static String DefaultClientId = "";

    /**
     * TopicMetadataRequest has the following format -
     * number of topics (4 bytes) list of topics (2 bytes + topic.length per topic) detailedMetadata (2 bytes) timestamp (8 bytes) count (4 bytes)
     */
    @Override
    public RequestOrResponse readFrom(final ByteBuffer buffer) {
        short versionId = buffer.getShort();
        int correlationId = buffer.getInt();
        String clientId = readShortString(buffer);
        int numTopics = readIntInRange(buffer, "number of topics", Range.make(0, Integer.MAX_VALUE));
        List<String> topics = Utils.flatList(0, numTopics, new Function1<Integer, String>() {
            @Override
            public String apply(Integer arg) {
                return readShortString(buffer);
            }
        });

        return new TopicMetadataRequest(versionId, correlationId, clientId, topics);
    }
}
